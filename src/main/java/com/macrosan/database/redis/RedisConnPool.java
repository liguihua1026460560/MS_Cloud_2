package com.macrosan.database.redis;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.utils.functional.Tuple3;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.lang.reflect.Proxy;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.macrosan.constants.RedisConstants.*;
import static com.macrosan.constants.ServerConstants.PROC_NUM;

/**
 * RedisConnPool
 * Lettuce连接池。Lettuce构建在Netty上，连接多数时候是线程安全的，但是如下几种情况除外：
 * 1.禁用flush after command然后用命令的批量下发
 * 2.BLPOP之类的阻塞操作
 * 3.事务
 * 4.多数据库的切换
 * 故本连接池维持了两种连接，一种是可复用的Shared连接,跟数据库绑定，一种是负责事务操作的Thread-Local连接，跟线程名字绑定
 *
 * @author liyixin
 * @date 2018/11/20
 */
public class RedisConnPool {

    private static final Logger logger = LogManager.getLogger(RedisConnPool.class.getName());

    private static RedisConnPool instance;

    private final String masterVip1 = ServerConfig.getInstance().getMasterVip1();

    private final String masterVip2 = ServerConfig.getInstance().getMasterVip2();

    private UnifiedMap<String, AbstractConnection> threadLocalConnectionMap;

    private IntObjectHashMap<AbstractConnection> sharedConnectionMap;

    private UnicastProcessor<Tuple3<MonoProcessor<Boolean>, String, String>> syncRedisProcessor = UnicastProcessor.create(
            Queues.<Tuple3<MonoProcessor<Boolean>, String, String>>unboundedMultiproducer().get());

    public static RedisConnPool getInstance() {
        return instance;
    }

    private static final Class<?>[] CLS = new Class<?>[]{RedisCommands.class};

    public static void init() {
        if (instance == null) {
            instance = new RedisConnPool();
        }

        ServerConfig.getInstance().initValueFromRedis();
    }

    private RedisConnPool() {
        RedisClient client = RedisClient.create(VertxEventLoopProvider.resources);

        sharedConnectionMap = new IntObjectHashMap<>(22);
        threadLocalConnectionMap = new UnifiedMap<>((int) (PROC_NUM / 0.75) + 1);

        /* 初始化跟数据库绑定的线程间共享的连接 */
        IntStream.range(0, 16).forEach(n -> sharedConnectionMap.put(n, new SharedConnection(client, getNodes(n))));

        /* 这里node都是一样的，所以放在循环外面new */
        final FastList<RedisURI> threadLocalNodes = getNodes();
        /* 初始化跟线程绑定的线程独占连接 */
        IntStream.range(1, PROC_NUM + 1).forEach(n
                -> threadLocalConnectionMap.put(THREAD_NAME + n, new ThreadLocalConnection(client, threadLocalNodes)));

        syncRedisProcessor.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(res -> {
            List<Tuple3<MonoProcessor<Boolean>, String, String>> list = new LinkedList<>();
            list.add(res);
            Tuple3<MonoProcessor<Boolean>, String, String> r;
            while ((r = syncRedisProcessor.poll()) != null) {
                list.add(r);
            }

            try {
                Set<String> masterRunIds = list.stream().map(t -> t.var2).collect(Collectors.toSet());
                Set<String> removeNodes = list.stream().map(t -> t.var3).collect(Collectors.toSet());

                if (masterRunIds.size() > 1) {
                    throw new RuntimeException("redis master run id is not same");
                }
                String masterRunId = masterRunIds.toArray(new String[1])[0];
                String removeNode = removeNodes.toArray(new String[1])[0];

                syncAllRedis(masterRunId, removeNode);
                for (Tuple3<MonoProcessor<Boolean>, String, String> r0 : list) {
                    r0.var1.onNext(true);
                }
            } catch (Exception e) {
                logger.error("sync redis fail", e);
                for (Tuple3<MonoProcessor<Boolean>, String, String> r0 : list) {
                    r0.var1.onError(e);
                }
            }
        });
    }

    /**
     * getSocketUri
     * <p>
     * 根据数据库编号和redis host来构造uri，用于Shared SharedConnection
     *
     * @param host     redis地址
     * @param database 要连接的数据库
     * @return 表明redis的uri
     */
    private RedisURI getSocketUri(String host, int database) {
        return RedisURI.Builder.socket(host)
                .withPassword(REDIS_PASSWD)
                .withDatabase(database)
                .build();
    }

    /**
     * getUri
     * <p>
     * 根据数据库编号和redis host来构造uri，用于Shared SharedConnection
     *
     * @param host     redis地址
     * @param database 要连接的数据库
     * @return 表明redis的uri
     */
    private RedisURI getUri(String host, int database) {
        return RedisURI.Builder.redis(host, 6379)
                .withPassword(REDIS_PASSWD)
                .withDatabase(database)
                .build();
    }

    /**
     * getUri
     * <p>
     * 根据redis host来构造uri，用于Thread Local Connection
     *
     * @param host redis地址
     * @return 表明redis的uri
     */
    private RedisURI getUri(String host) {
        return RedisURI.Builder.redis(host, 6379)
                .withPassword(REDIS_PASSWD)
                .build();
    }

    /**
     * getNodes
     * <p>
     * 用LOCALHOST，masterVip1和masterVip2构造一个静态拓扑，用于Shared SharedConnection。
     *
     * @param database 要连接的数据库
     * @return 静态拓扑
     */
    private FastList<RedisURI> getNodes(int database) {
        return new FastList<RedisURI>(4)
                .with(getSocketUri(UDS_ADDRESS, database), getUri(masterVip1, database), getUri(masterVip2, database));
    }

    /**
     * getNodes
     * <p>
     * 用LOCALHOST，masterVip1和masterVip2构造一个静态拓扑，用于Thread Local SharedConnection。
     *
     * @return 静态拓扑
     */
    private FastList<RedisURI> getNodes() {
        return new FastList<RedisURI>(4)
                .with(getUri(LOCALHOST), getUri(masterVip1), getUri(masterVip2));
    }

    public AbstractConnection getSharedConnection(int database) {
        if (database < 0 || database > 15) {
            logger.error("Database number error :" + database);
            return null;
        }
        return sharedConnectionMap.get(database);
    }

    /**
     * 用于简单的获取同步命令集
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    public RedisCommands<String, String> getCommand(int database) {
        return getSharedConnection(database).sync();
    }

    /**
     * 用于简单的获取操作主的同步命令集,短连接版本，每次新建连接，用完后自动释放
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    @SuppressWarnings("unchecked")
    public RedisCommands<String, String> getShortMasterCommand(int database) {
        StatefulRedisConnection<String, String> tmpConnection = getSharedConnection(database).newMaster();
        RedisCommands<String, String> target = tmpConnection.sync();
        return (RedisCommands<String, String>) Proxy.newProxyInstance(RedisCommands.class.getClassLoader(), CLS
                , (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(target, args);
                        if (args.length > 0 && args[0] instanceof String) {
                            target.publish("redis-cache-" + database, (String) args[0]);
                        }
                        tmpConnection.close();
                    } finally {
                        if (tmpConnection.isOpen()) {
                            tmpConnection.close();
                        }
                    }
                    return result;
                });
    }

    private static String getReplOffset(String info, String field) {
        int start = info.indexOf(field);
        if (start <= 0) {
            return null;
        }
        char c;
        do {
            start++;
            c = info.charAt(start);
        } while (c < '0' || c > '9');

        int end = start;

        do {
            end++;
            c = info.charAt(end);
        } while (c >= '0' && c <= '9');

        return info.substring(start, end);
    }

    public Mono<Boolean> syncRedis(String masterRunId, String removeNode) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        syncRedisProcessor.onNext(new Tuple3<>(res, masterRunId, removeNode));
        return res;
    }

    /**
     * 确保调用之前的所有redis命令在所有节点的redis中刷盘
     * 1.获得master的复制偏移量
     * 2.遍历所有节点
     * 3.等待复制偏移量>=master的复制偏移量
     * 4.bgsave刷盘
     * 5.等待lastsave>=刷盘前的时间
     */
    private void syncAllRedis(String masterRunId, String removeNode) {
        try (StatefulRedisConnection<String, String> tmpConnection = getSharedConnection(0).newMaster()) {
            String info = tmpConnection.sync().info("replication");

            long masterOffset = Long.parseLong(getReplOffset(info, "master_repl_offset"));
            boolean isRemoveNode = StringUtils.isNotEmpty(removeNode);
            for (String ip : RabbitMqUtils.HEART_IP_LIST) {
                if (isRemoveNode && removeNode.equals(RabbitMqUtils.getUUID(ip))) {
                    continue;
                }
                try (StatefulRedisConnection<String, String> conn = AbstractConnection.client.connect(getUri(ip))) {
                    RedisCommands<String, String> commands = conn.sync();
                    String tmp = getReplOffset(commands.info("replication"), "slave_repl_offset");
                    if (null == tmp) {
                        continue;
                    }

                    long slaveOffset = Long.parseLong(tmp);
                    while (slaveOffset < masterOffset) {
                        synchronized (Thread.currentThread()) {
                            try {
                                Thread.currentThread().wait(10);
                            } catch (InterruptedException e) {
                                logger.error("", e);
                            }
                        }
                        slaveOffset = Long.parseLong(getReplOffset(commands.info("replication"), "slave_repl_offset"));
                        long newMasterOffset = Long.parseLong(getReplOffset(tmpConnection.sync().info("replication"), "master_repl_offset"));
                        if (newMasterOffset < masterOffset) {
                            masterOffset = newMasterOffset;
                        }
                    }

                    long beforeSaveTime = Long.parseLong(commands.time().get(0)) * 1000;
                    try {
                        commands.bgsave();
                    } catch (Exception e) {

                    }
                    long lastSaveTime = commands.lastsave().getTime();
                    while (lastSaveTime < beforeSaveTime) {
                        synchronized (Thread.currentThread()) {
                            try {
                                Thread.currentThread().wait(10);
                            } catch (InterruptedException e) {
                                logger.error("", e);
                            }
                        }
                        lastSaveTime = commands.lastsave().getTime();
                    }
                }
            }

            // 刷盘结束判断执行bgsave的主节点和数据写入时的主节点是否一致
            String server = tmpConnection.sync().info("Server");
            String currentMasterRunId = getRedisInfoField(server, "run_id");
            if (!Objects.equals(currentMasterRunId, masterRunId)) {
                throw new IllegalStateException("sync all redis error, redis master run id changed");
            }
        }
    }

    /**
     * 用于简单的获取响应式命令集
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    public RedisReactiveCommands<String, String> getReactive(int database) {
        return getSharedConnection(database).reactive();
    }

    /**
     * 用于简单的获取主的响应式命令集
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    public RedisReactiveCommands<String, String> getMasterReactive(int database) {
        return getSharedConnection(database).master().reactive();
    }

    public AbstractConnection getThreadLocalConnection() {
        String threadName = Thread.currentThread().getName();
        AbstractConnection res = threadLocalConnectionMap.get(threadName);
        if (res == null) {
            logger.error("Thread :" + threadName + " does not have connection");
        }
        return res;
    }

    public RedisURI getMainNodes(int database) {
        return getUri(masterVip1, database);
    }

    /**
     * 解析redis的配置信息
     * @param info 配置信息
     * @param field 字段
     * @return 字段的值
     */
    public static String getRedisInfoField(String info, String field) {
        int index = info.indexOf(field);
        if (index < 0) {
            return null;
        }
        int startIndex = index + field.length() + 1;
        if (startIndex >= info.length()) {
            return null;
        }
        int endIndex = startIndex;
        for (int i = startIndex; i < info.length(); i++) {
            if (info.charAt(i) == '\r') {
                break;
            }
            endIndex++;
        }
        return info.substring(startIndex, endIndex);
    }

    /**
     * 获取主节点的runId
     * @return 主节点的runId
     */
    public String getMasterRunId() {
        try {
            String serverInfo = getShortMasterCommand(0).info("Server");
            return getRedisInfoField(serverInfo, "run_id");
        } catch (Exception e) {
            logger.error("getMasterRunId fail", e);
            return null;
        }
    }
}
