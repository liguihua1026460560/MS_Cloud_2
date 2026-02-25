package com.macrosan.storage.move;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.Utils;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.RequestResponseServerHandler;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.CacheFlushConfig;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.getFileMetaKeyByCacheOrderKey;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.storage.strategy.CacheSmallObjectStrategy.*;


/**
 * @author gaozhiyuan
 */
@Log4j2
public class CacheMove implements Runnable {

    public static final String MQ_KEY_PREFIX = "move_";
    public static final String MQ_KEY_PREFIX_V1 = "moveV1_";  // move前缀版本升级

    private static final Scheduler MOVE_SCHEDULER;
    public static int runNum = 1;
    public static boolean cacheSwitch = true;

    static {
        MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("move-keep-lock"));
        MOVE_SCHEDULER = Schedulers.fromExecutor(executor);
    }

    private CacheMove(StoragePool... pools) {
        this.pools = pools;
    }

    private CacheMove(Map<String, List<StoragePool>> map) {
        this.strategyMap = map;
        poolStrategyMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, List<StoragePool>> entry : map.entrySet()) {
            List<StoragePool> list = entry.getValue();
            for (StoragePool pool : list) {
                poolStrategyMap.computeIfAbsent(pool.getVnodePrefix(), k -> new ConcurrentHashSet<>()).add(entry.getKey());
            }
        }
    }

    protected StoragePool[] pools;
    String node = ServerConfig.getInstance().getHostUuid();
    protected RedisConnPool redisConnPool = RedisConnPool.getInstance();
    protected MSRocksDB mqDB = MSRocksDB.getRocksDB(Utils.getMqRocksKey());
    AtomicBoolean locked = new AtomicBoolean();

    private Map<String, List<StoragePool>> strategyMap;

    private Map<String, Set<String>> poolStrategyMap;

    private boolean deleteOldKeyCompeleted = false;
    private final CacheFlushConfigRefresher cacheFlushConfigRefresher = CacheFlushConfigRefresher.getInstance();

    @Override
    public void run() {
        if (cacheSwitch && canRun() && tryGetLock()) {
            Stream<StoragePool> stream = strategyMap.values().stream().flatMap(Collection::stream);
            Flux.fromStream(stream)
                    .distinct(StoragePool::getVnodePrefix)
                    .flatMap(pool -> {
                        try {
                            CacheFlushConfig flushConfig = cacheFlushConfigRefresher.getFlushConfig(pool.getVnodePrefix());
                            int low = flushConfig.getLow();
                            int high = flushConfig.getHigh();
                            // 清理旧版本存储在mq/rocks_db中的key-value
                            if (!deleteOldKeyCompeleted) {
                                log.info("Starting deletion of old task key");
                                MSRocksIterator iterator = mqDB.newIterator();
                                String keyPrefix = getOldKeyPrefix(pool.getVnodePrefix());
                                iterator.seek(keyPrefix.getBytes());
                                while (iterator.isValid()) {
                                    String key = new String(iterator.key());
                                    if (!key.startsWith(keyPrefix)) {
                                        break;
                                    }
                                    deleteMq(iterator.key());
                                    iterator.next();
                                }
                                iterator.close();
                                deleteOldKeyCompeleted = true;
                                log.info("Completed deletion of old task key");
                            }

                            int runNum;
                            long singleUsed = 0L;
                            long singleTotal = 0L;
                            float used = 0.0f;
                            singleUsed = pool.getCache().size;
                            singleTotal = pool.getCache().totalSize;
                            if (singleTotal != 0L) {
                                used = singleUsed * 100.0f / singleTotal;//每一个缓存池都单独计算水位线
                            }
                            // 判断是否开启延迟下刷
                            if (flushConfig.isEnableDelayedFlush() && used <= flushConfig.getDelayedFlushWaterMark() + 0.1f) {
                                return Mono.just(true);
                            }

                            if (used < low) {
                                runNum = LOW_RUN_NUM;
                            } else if (used > high) {
                                runNum = HIGH_RUN_NUM;
                            } else {
                                runNum = (int) ((used - low) * (HIGH_RUN_NUM - LOW_RUN_NUM) / (high - low) + LOW_RUN_NUM);
                            }

                            long start = System.nanoTime();
                            int n = scan(pool, RUN_ONCE);
                            if (n > 0) {
                                long time = (System.nanoTime() - start) / 1000_000L;
                                log.info("scan {} {} {}ms", pool.getVnodePrefix(), n, time);
                            }
                            Set<String> set = poolStrategyMap.get(pool.getVnodePrefix());
                            List<String> list = new ArrayList<>(set);
                            NameSpace ns = null;
                            if (list.size() == 1) {
                                ns = StorageStrategy.STRATEGY_NAMESPACE_MAP.get(list.get(0));
                            } else {
                                log.debug("vnodePrefix:{} {}", pool.getVnodePrefix(), list);
                            }
                            TaskRunner runner = new TaskRunner(pool, mqDB, ns);
                            for (int i = 0; i < runNum; i++) {
                                runner.run();
                            }

                            return runner.res.delayElement(Duration.ofSeconds(10)).flatMap(b -> {
                                        if (n > 0) {
                                            log.info("start clear {} {} {}", pool.getVnodePrefix(), n, runNum);
                                        }
                                        ClearTaskRunner clearRunner = new ClearTaskRunner(pool, mqDB);
                                        for (int i = 0; i < runNum; i++) {
                                            clearRunner.run();
                                        }
                                        return clearRunner.res;
                                    })
                                    .defaultIfEmpty(false)
                                    .onErrorReturn(false)
                                    .doFinally(s -> {
                                        if (n > 0) {
                                            log.info("move end {} {}", pool.getVnodePrefix(), n);
                                        }
                                    });
                        } catch (Exception e) {
                            return Mono.just(false);
                        }
                    }).doFinally(s -> {
                        try {
                            redisConnPool.getShortMasterCommand(0).del("move");
                        } finally {
                            locked.set(false);
                            ErasureServer.DISK_SCHEDULER.schedule(this, 10, TimeUnit.SECONDS);
                        }
                    }).subscribe();
        } else {
            ErasureServer.DISK_SCHEDULER.schedule(this, 10, TimeUnit.SECONDS);
        }
    }

    protected String[] getCurNodePoolDisk(StoragePool pool) {
        List<String> list = new LinkedList<>();
        for (String lun : pool.getCache().lunSet) {
            String node = lun.split("@")[0];
            if (this.node.equalsIgnoreCase(node)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    list.add(lun.split("@")[1]);
                }
            }
        }

        return list.toArray(new String[0]);
    }

    protected boolean canRun() {
        try {
            String lastRunNode = redisConnPool.getShortMasterCommand(0).get("move_run");
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
                if ("0".equalsIgnoreCase(serverState) || notExistCacheLun(totalNodes.get(i))) {//节点不在线或节点不存在使用的缓存盘
                    i++;
                    if (i >= totalNodes.size()) {
                        i = 0;
                    }
                } else {
                    return false;
                }
            }
            if (notExistCacheLun(this.node)) {//检查本地节点是否有缓存盘
                return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    protected boolean notExistCacheLun(String node) {//判断传入节点是否不存在缓存盘
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
        if (count == 0) {//如果当前节点不存在缓存盘，那么返回true
            return true;
        }
        return false;
    }

    protected boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(30);
            String setKey = redisConnPool.getShortMasterCommand(0).set("move", this.node, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
                redisConnPool.getShortMasterCommand(0).set("move_run", this.node);
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
                target.watch("move");
                String lockNode = target.get("move");
                if (node.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set("move", this.node, setArgs);
                    target.exec();
                    MOVE_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                MOVE_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
            }
        }
    }

    private int scan(StoragePool pool, int max) {
        String[] disks = getCurNodePoolDisk(pool);
        // 先按照上传时间进行扫描
        int total = orderedScan(pool, max, new LinkedList<>(Arrays.asList(disks)));
        if (total == max) {
            return total;
        }

        int remainingSacnNum = max - total;
        for (String disk : disks) {
            remainingSacnNum -= scanCacheDisk(pool, disk, remainingSacnNum, false);
            if (remainingSacnNum == 0) {
                break;
            }
        }

        return max - remainingSacnNum;
    }

    /**
     * 按照上传时间扫描
     * 按照平均值对所有盘进行轮流扫描，直到扫描数量达到期望数量
     *
     * @param pool    缓存池
     * @param scanMax 期望扫描数量
     * @param disks   盘
     * @return 实际扫描数量
     */
    private int orderedScan(StoragePool pool, int scanMax, List<String> disks) {
        int diskSize = disks.size();
        // 每个磁盘应该扫描的平均数量
        int avgScanNum = scanMax / diskSize;
        int completeScanDiskCount = 0;
        int total = 0;
        boolean firstDisk = true;
        // 求平均值后的余数，将其加上到第一个盘扫描的数量中
        int remainingScanNum = scanMax - (avgScanNum * diskSize);
        Iterator<String> iterator = disks.iterator();
        while (iterator.hasNext()) {
            String disk = iterator.next();
            int expectedScanNum = firstDisk ? avgScanNum + remainingScanNum : avgScanNum;
            int realScanNum = scanCacheDisk(pool, disk, expectedScanNum, true);
            if (expectedScanNum == realScanNum) {
                // 该磁盘扫描数量已到指定值
                completeScanDiskCount++;
            } else {
                // 该磁盘扫描数量未到指定值，说明没有数据了，则从集合中移除
                iterator.remove();
            }
            total += realScanNum;
            firstDisk = false;
            log.debug("order scan cache:{} disk:{} expectedScanNum:{} realSacnNum:{}", pool.getVnodePrefix(), disk, expectedScanNum, realScanNum);
        }

        if (disks.isEmpty() || completeScanDiskCount == diskSize) {
            return total;
        }
        // 扫描数量未达到期望，则进行下一轮递归扫描
        return total + orderedScan(pool, (scanMax - total), disks);
    }

    public int scanCacheDisk(StoragePool pool, String disk, int scanNum, boolean orderedScan) {
        int n = 0;
        if (scanNum == 0) {
            return n;
        }
        boolean isMigrating = false;
        String keyPrefix = orderedScan ? ROCKS_CACHE_ORDERED_KEY : ROCKS_FILE_META_PREFIX;
        MSRocksDB cacheDb = MSRocksDB.getRocksDB(disk);
        if (cacheDb == null) {
            return n;
        }
        try (MSRocksIterator iterator = cacheDb.newIterator()) {
            iterator.seek(keyPrefix.getBytes());
            while (iterator.isValid() && n < scanNum) {
                String key = new String(iterator.key());
                if (!key.startsWith(keyPrefix)) {
                    break;
                }
                FileMeta meta;
                if (orderedScan) {
                    String cacheOrderKey = new String(iterator.key());
                    byte[] bytes = cacheDb.get(getFileMetaKeyByCacheOrderKey(cacheOrderKey).getBytes());
                    if (null == bytes) {
                        meta = null;
                    } else {
                        meta = Json.decodeValue(new String(bytes), FileMeta.class);
                    }
                } else {
                    meta = Json.decodeValue(new String(iterator.value()), FileMeta.class);
                }
                if (null != meta && null != meta.getMetaKey()) {
                    try {
                        // 考虑单个大文件(100GB)存在多个(100+)等待下刷文件的情况，避免单个文件每次只能下刷一块数据
                        // 在key后缀增加 _objVnodeId 从 meta.getFileName() 中提取
                        String objVnodeId = meta.getFileName().substring(1).split("_")[0];
                        byte[] taskKey = getTaskKey(pool.getVnodePrefix(), meta.getMetaKey() + "_" + objVnodeId);
                        if (null == mqDB.get(taskKey)) {
                            List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(meta.getFileName())).block();
                            Set<String> fileDiskSet = nodeList.stream()
                                    .filter(t -> t.var1.equalsIgnoreCase(CURRENT_IP))
                                    .map(t -> t.var2)
                                    .collect(Collectors.toSet());
                            if (fileDiskSet.contains(disk)) {
                                n++;
                                long fileOffset = meta.getFileOffset();
                                String value;
                                if (fileOffset >= 0L) {
                                    value = meta.getFileName() + "#" + fileOffset + "#" + meta.getSize();
                                    mqDB.put(taskKey, value.getBytes());
                                }
                            } else {
                                try {
                                    List<String> runningKeys = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).keys("running_*");
                                    for (String runningKey : runningKeys) {
                                        String type = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).type(runningKey);
                                        if ("hash".equalsIgnoreCase(type)) {
                                            Map<String, String> map = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).hgetall(runningKey);
                                            String operate = map.getOrDefault("operate", "add_node");
                                            String diskName = map.getOrDefault("diskName", "cache");
                                            if (!map.isEmpty() && diskName.contains("cache")) {
                                                isMigrating = "add_node".equals(operate) || "add_disk".equals(operate);
                                            }
                                        }
                                        if (isMigrating) {
                                            break;
                                        }
                                    }
                                } catch (Exception e) {
                                    isMigrating = true;
                                }
                                if (!isMigrating) {
                                    //当前文件已经不在vnode的映射中
                                    log.info("delete file {} in {}", meta.getFileName(), disk);
                                    SocketReqMsg msg = new SocketReqMsg("", 0)
                                            .put("fileName", Json.encode(new String[]{meta.getFileName()}))
                                            .put("lun", disk);
                                    RequestResponseServerHandler.deleteFile(DefaultPayload.create(msg.toBytes()))
                                            .subscribe();
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }

                iterator.next();
            }

        } catch (RocksDBException e) {
            log.error("", e);
        }
        return n;
    }

    private void deleteMq(byte[] key) {
        try {
            mqDB.delete(key);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static byte[] getTaskKey(String poolPrefix, String key) {
        return (getKeyPrefix(poolPrefix) + key).getBytes();
    }

    public static String getKeyPrefix(String poolName) {
        return MQ_KEY_PREFIX_V1 + poolName + "_";
    }

    public static String getOldKeyPrefix(String poolName) {
        return MQ_KEY_PREFIX + poolName + "_";
    }

    public void refreshPoolMap(String strategyName, List<StoragePool> newList) {
        strategyMap.put(strategyName, newList);
        for (StoragePool pool : newList) {
            poolStrategyMap.computeIfAbsent(pool.getVnodePrefix(), k -> new ConcurrentHashSet<>()).add(strategyName);
        }
    }

    private static CacheMove move = null;

    public static void dealPools(String strategyName, StoragePool... cachePools) {
        List<StoragePool> list = Arrays.asList(cachePools);
        if (move == null) {
            Map<String, List<StoragePool>> map = new ConcurrentHashMap<>();
            map.put(strategyName, list);
            move = new CacheMove(map);
            ErasureServer.DISK_SCHEDULER.schedule(move, 30_000L, TimeUnit.MILLISECONDS);
        } else {
            move.refreshPoolMap(strategyName, list);
        }
    }

    public static boolean isEnableCacheOrderFlush(StoragePool cachePool) {
        return cachePool.getVnodePrefix().startsWith("cache") && CacheFlushConfigRefresher.getInstance().getFlushConfig(cachePool.getVnodePrefix()).isEnableOrderedFlush();
    }
}
