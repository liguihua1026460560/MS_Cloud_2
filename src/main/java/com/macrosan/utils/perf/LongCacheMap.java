package com.macrosan.utils.perf;

import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.SampleConnection;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import io.vertx.reactivex.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_TOKEN_INDEX;

/**
 * 缓存限流的token和quota
 *
 * @author gaozhiyuan
 * @date 2019.10.25d
 */
@Log4j2
public class LongCacheMap {
    private final static int UPDATE_FREQUENCY = 10;
    /**
     * 保存Task
     */
    private ConcurrentLinkedQueue<Task> queue = new ConcurrentLinkedQueue<>();
    /**
     * 保存qutoa、tokens等的值
     */
    private Map<String, Long> map = new ConcurrentHashMap<>();
    /**
     * 保存要进行redis操作的值
     */
    private Map<String, Long> addMap = new ConcurrentHashMap<>();
    private GetValueHandler getHandler;
    private GetValueEmptyHandler emptyHandler;
    private UpdateValueHandler updateHandler;
    private long waitMils;
    private SampleConnection connection;
    private int count = 0;
    private boolean update = false;
    private Vertx vertx = ServerConfig.getInstance().getVertx();
    private Map<String, Long> tmpMap = new HashMap<>();

    public LongCacheMap(long waitMils, GetValueHandler getHandler, GetValueEmptyHandler emptyHandler, UpdateValueHandler updateHandler) {
        this.waitMils = waitMils;
        this.emptyHandler = emptyHandler;
        this.getHandler = getHandler;
        this.updateHandler = updateHandler;

        connection = new SampleConnection(Redis6380ConnPool.getInstance().getMainNodes(REDIS_TOKEN_INDEX).get(0));
        ServerConfig.getInstance().getVertx().setTimer(waitMils / UPDATE_FREQUENCY, l -> handler());
    }

    /**
     * 如果map中不存在key的值，查询,将新的Task添加入queue，类型为SET
     *
     * @param key 查询的key
     * @return 查询到的值
     */
    public Mono<Long> get(String key) {
        Long res = map.get(key);
        if (null == res) {
            return getHandler.getValue(key)
                    .switchIfEmpty(emptyHandler.handlerEmpty(key))
                    .timeout(Duration.ofSeconds(10))
                    .map(value -> {
                        queue.add(new Task(Task.TaskType.SET, key, value));
                        return value;
                    });

        }
        tmpMap.put(key, res);
        return Mono.just(res);
    }

    /**
     * 添加新的Task给queue，类型为ADD
     *
     * @param key   待添加的key
     * @param value 待添加的value
     */
    public void add(String key, Long value) {
        queue.add(new Task(Task.TaskType.ADD, key, value));
    }

    /**
     * 根据TaskType对任务进行处理
     *
     * @param task 任务
     */
    private void handlerTask(Task task) {
        try {
            switch (task.type) {
                case ADD:
                    long curVal = addMap.getOrDefault(task.key, 0L);
                    addMap.put(task.key, curVal + task.value);
                    Long value = map.get(task.key);
                    if (null != value) {
                        map.put(task.key, value + task.value);
                    }
                    break;
                case SET:
                    map.put(task.key, task.value);
                    break;
                default:
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void handler() {
        try {
            if (count < UPDATE_FREQUENCY) {
                while (!queue.isEmpty()) {
                    handlerTask(queue.poll());
                }
                count++;
            } else {
                if (!update) {
                    update = true;

                    try {
                        Mono<Long> mono = Mono.just(0L);
                        /*
                         * 将addMap中的键值对更新至redis,
                         */
                        List<String> getList = tmpMap.keySet().stream().filter(key -> !addMap.containsKey(key)).collect(Collectors.toList());
                        for (Map.Entry<String, Long> entry : addMap.entrySet()) {
                            mono = mono.map(l -> entry)
                                    .flatMap(e -> updateHandler.updateValue(connection, e.getKey(), e.getValue())
                                            .switchIfEmpty(emptyHandler.handlerEmpty(e.getKey()).map(l -> true))
                                            .map(b -> e))
                                    .flatMap(e -> getHandler.getValue(e.getKey())
                                            .switchIfEmpty(emptyHandler.handlerEmpty(e.getKey()))
                                            .map(newValue -> {
                                                tmpMap.put(e.getKey(), newValue);
                                                return 0L;
                                            }));
                        }
                        for (String key : getList) {
                            mono = mono.flatMap(e -> getHandler.getValue(key))
                                    .switchIfEmpty(emptyHandler.handlerEmpty(key))
                                    .map(newValue -> {
                                        tmpMap.put(key, newValue);
                                        return 0L;
                                    });
                        }

                        mono.timeout(Duration.ofSeconds(300))
                                .doOnError(e -> {
                                    map.putAll(tmpMap);
                                    tmpMap.clear();
                                    addMap.clear();
                                    count = 0;
                                    update = false;
                                }).subscribe(l -> {
                                    for (String key : map.keySet()) {
                                        if (!tmpMap.containsKey(key)) {
                                            map.remove(key);
                                        }
                                    }
                                    map.putAll(tmpMap);
                                    tmpMap.clear();
                                    addMap.clear();
                                    count = 0;
                                    update = false;
                                });
                    } catch (Throwable e) {
                        log.error("", e);

                        map.putAll(tmpMap);
                        tmpMap.clear();
                        addMap.clear();
                        count = 0;
                        update = false;
                    }
                }
            }
        } finally {
            ErasureServer.DISK_SCHEDULER.schedule(this::handler, waitMils / UPDATE_FREQUENCY, TimeUnit.MILLISECONDS);
        }
    }

    public interface GetValueHandler {
        /**
         * 通过key的value的方法，在缓存未命中或缓存更新时调用方法获得缓存值
         *
         * @param key 缓存key
         * @return 缓存值
         */
        Mono<Long> getValue(String key);
    }

    public interface GetValueEmptyHandler {
        /**
         * 在获得缓存值为空时调用方法，处理缓存key未初始化的情况并返回默认值
         *
         * @param key 缓存key
         * @return 默认的缓存值
         */
        Mono<Long> handlerEmpty(String key);
    }

    public interface UpdateValueHandler {
        /**
         * 更新缓存时调用，将缓存数据更新到数据库
         *
         * @param connection 数据库连接
         * @param key        需要更新的key
         * @param value      需要更新的value
         * @return 更新是否成功
         */
        Mono<Boolean> updateValue(SampleConnection connection, String key, long value);
    }

    @AllArgsConstructor
    private static class Task {
        private TaskType type;
        private String key;
        private long value;

        private enum TaskType {
            /**
             * 设置缓存值的任务
             */
            SET,
            /**
             * 增加缓存值的任务
             */
            ADD
        }
    }
}
