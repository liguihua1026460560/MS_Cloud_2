package com.macrosan.filesystem.tier;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.move.CacheFlushConfigRefresher;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.NODE_SERVER_STATE;
import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;

/**
 * @author DaiFengtao
 * {@code @date} 2026年03月16日 18:54
 */
@Slf4j
public class FileTierMove implements Runnable {
    protected MSRocksDB mqDB = MSRocksDB.getRocksDB(Utils.getMqRocksKey());
    String node = ServerConfig.getInstance().getHostUuid();
    protected RedisConnPool redisConnPool = RedisConnPool.getInstance();
    public static final String FILE_TIER_MOVE_KEY = "file_tier_move";
    public static final String FILE_TIER_RUN_KEY = "file_tier_run";
    AtomicBoolean locked = new AtomicBoolean();
    public static boolean cacheSwitch = true;

    private static final Scheduler FS_TIER_SCHEDULER;
    private Map<String, List<StoragePool>> strategyMap;
    private Map<String, Set<String>> poolStrategyMap;

    //一次回迁任务最多回迁10000个数据块
    public static final int ONCE = 10_000;


    static {
        MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-tier-keep-lock"));
        FS_TIER_SCHEDULER = Schedulers.fromExecutor(executor);
    }

    public FileTierMove(Map<String, List<StoragePool>> map) {
        this.strategyMap = map;
        poolStrategyMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, List<StoragePool>> entry : map.entrySet()) {
            List<StoragePool> list = entry.getValue();
            for (StoragePool pool : list) {
                poolStrategyMap.computeIfAbsent(pool.getVnodePrefix(), k -> new ConcurrentHashSet<>()).add(entry.getKey());
            }
        }
    }


    public void refreshPoolMap(String strategyName, List<StoragePool> newList) {
        strategyMap.put(strategyName, newList);
        for (StoragePool pool : newList) {
            poolStrategyMap.computeIfAbsent(pool.getVnodePrefix(), k -> new ConcurrentHashSet<>()).add(strategyName);
        }
    }

    protected boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(30);
            String setKey = redisConnPool.getShortMasterCommand(0).set(FILE_TIER_MOVE_KEY, this.node, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
                redisConnPool.getShortMasterCommand(0).set(FILE_TIER_RUN_KEY, this.node);
            }
            return res;
        } catch (Exception e) {
            return false;
        }
    }

    private void keepLock() {
        if (locked.get()) {
            SetArgs setArgs = SetArgs.Builder.xx().ex(30);
            try (StatefulRedisConnection<String, String> tmpConnection =
                         redisConnPool.getSharedConnection(0).newMaster()) {
                RedisCommands<String, String> target = tmpConnection.sync();
                target.watch(FILE_TIER_MOVE_KEY);
                String lockNode = target.get(FILE_TIER_MOVE_KEY);
                if (node.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set(FILE_TIER_MOVE_KEY, this.node, setArgs);
                    target.exec();
                    FS_TIER_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                FS_TIER_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
            }
        }
    }

    protected boolean canRun() {
        try {
            String lastRunNode = redisConnPool.getShortMasterCommand(0).get(FILE_TIER_RUN_KEY);
            if (StringUtils.isBlank(lastRunNode)) {
                return true;
            }

            List<String> totalNodes = redisConnPool.getCommand(REDIS_NODEINFO_INDEX).keys("*");
            totalNodes.sort(String::compareTo);
            int index = -1;
            int lastRun = -1;
            int i = 0;
            for (String node : totalNodes) {
                if (node.equalsIgnoreCase(this.node)) {
                    index = i;
                }

                if (node.equalsIgnoreCase(lastRunNode)) {
                    lastRun = i;
                }

                i++;
            }

            i = (lastRun + 1) >= totalNodes.size() ? 0 : lastRun + 1;
            while (i != index) {
                String serverState = redisConnPool.getCommand(REDIS_NODEINFO_INDEX).hget(totalNodes.get(i), NODE_SERVER_STATE);
                if ("0".equalsIgnoreCase(serverState) || notExistCacheLun(totalNodes.get(i))) {
                    //节点不在线或节点不存在使用的缓存盘
                    i++;
                    if (i >= totalNodes.size()) {
                        i = 0;
                    }
                } else {
                    return false;
                }
            }
            if (notExistCacheLun(this.node)) {
                //检查本地节点是否有缓存盘
                return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    protected boolean notExistCacheLun(String node) {
        //判断传入节点是否不存在缓存盘
        int count = 0;
        List<StoragePool> storagePools = strategyMap.values().stream().flatMap(Collection::stream).distinct().collect(Collectors.toList());
        for (StoragePool pool : storagePools) {
            for (String lun : pool.getCache().lunSet) {
                String diskNode = lun.split("@")[0];
                if (node.equalsIgnoreCase(diskNode)) {
                    if (!RemovedDisk.getInstance().contains(lun)) {
                        count++;
                    }
                }
            }
        }
        if (count == 0) {
            //如果当前节点不存在缓存盘，那么返回true
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        if ((cacheSwitch && canStart() && canRun() && tryGetLock())) {
            try {
                //TODO 执行分层迁移,回迁至缓存池,判断缓存池是否容量足够
                long start = System.currentTimeMillis();
                FileTierRunner runner = new FileTierRunner(mqDB, this);
                for (int i = 0; i < 5; i++) {
                    runner.run();
                }
                runner.res.delayElement(Duration.ofSeconds(10))
                        .subscribe(res -> {
                            if (runner.getTotal() > 0) {
                                long end = System.currentTimeMillis();
                                log.info("tier back move {} end cost {} ms ", runner.getTotal(), end - start);
                            }
                            long clearStart = System.currentTimeMillis();
                            FileTierCleanTask fileTierCleanTask = new FileTierCleanTask(mqDB);
                            for (int i = 0; i < 5; i++) {
                                fileTierCleanTask.run();
                            }
                            fileTierCleanTask.res
                                    .doFinally((signal) -> {
                                        runAgain();
                                    })
                                    .subscribe(r -> {
                                        if (fileTierCleanTask.getTotal() > 0) {
                                            long clearEnd = System.currentTimeMillis();
                                            log.info("tier back clear end {} cost {} ms", fileTierCleanTask.getTotal(), clearEnd - clearStart);
                                        }
                                    });
                        }, e -> {
                            log.error("", e);
                            runAgain();
                        });
            } catch (Exception e) {
                log.error("", e);
                runAgain();
            }
        } else {
            FsUtils.fsExecutor.schedule(this, 10, TimeUnit.SECONDS);
        }
    }

    public void runAgain() {
        try {
            redisConnPool.getShortMasterCommand(0).del(FILE_TIER_MOVE_KEY);
        } finally {
            locked.set(false);
            FsUtils.fsExecutor.schedule(this, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * 判断是否有缓存池开启了冷热分层的功能
     *
     * @return res
     */
    public boolean canStart() {
        if (poolStrategyMap.isEmpty()) {
            return false;
        }
        for (String poolKey : poolStrategyMap.keySet()) {
            boolean enableAccessTimeFlush = CacheFlushConfigRefresher.getInstance().getFlushConfig(poolKey).isEnableAccessTimeFlush();
            if (enableAccessTimeFlush) {
                return true;
            }
        }
        return false;
    }

    public void putMq(String key, String value) {
        try {
            mqDB.put(key.getBytes(), value.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }

}
