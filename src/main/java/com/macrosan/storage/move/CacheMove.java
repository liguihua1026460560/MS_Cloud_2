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
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.constants.SysConstants.ROCKS_CACHE_ACCESS_KEY;
import static com.macrosan.ec.Utils.getFileMetaKeyByAccessTimeKey;
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
        /**************************** 开启数据分层 ********************/
        int total = 0;
        log.debug("isEnableCacheAccessTimeFlush:{}", isEnableCacheAccessTimeFlush(pool));
        if (isEnableCacheAccessTimeFlush(pool)) {
            LocalTime startTime = LocalTime.MIN;
            LocalTime endTime = LocalTime.MAX;

            LocalTime now = LocalTime.now();
            if (now.isAfter(startTime) && now.isBefore(endTime)) {
                //开启分层这里只按照最后访问时间表进行扫描
                //且配置一个开启时间，到达每天的指定时间后再触发扫描
                //扫描一定数量的数据，当前时间戳据上次访问时间大于30天进行下刷
                total = scanByAccessTime(pool, max, new LinkedList<>(Arrays.asList(disks)));
                log.debug("scanByAccessTime:{}", total);
            } else {
                return 0;//不执行
            }

        } else {
            // 先按照上传时间进行扫描
            total = orderedScan(pool, max, new LinkedList<>(Arrays.asList(disks)));
        }

        if (total == max) {
            return total;
        }

        int remainingSacnNum = max - total;
        for (String disk : disks) {
            remainingSacnNum -= scanCacheDisk(pool, disk, remainingSacnNum, ScanType.DEFAULT);
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
            int realScanNum = scanCacheDisk(pool, disk, expectedScanNum, ScanType.BY_UPLOAD_TIME);
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

    //按照最后访问时间
    public int scanByAccessTime(StoragePool pool, int scanNum, List<String> disks) {
        //这里还是要应该多个盘同时扫描，目前界面基本限制了副本数的创建，那么基本不存在同节点存在多个同一对象数据块的副本，可以不考虑重复扫描的问题
        int total = 0;
        int diskSize = disks.size();
        int avgScanNum = scanNum / diskSize;
        int completeScanDiskCount = 0;
        int remainingScanNum = scanNum - (avgScanNum * diskSize);
        boolean firstDisk = true;
        Iterator<String> iterator = disks.iterator();
        while (iterator.hasNext()) {
            String disk = iterator.next();
            int expectedScanNum = firstDisk ? avgScanNum + remainingScanNum : avgScanNum;
            int realScanNum = scanCacheDisk(pool, disk, expectedScanNum, ScanType.BY_ACCESS_TIME);
            if (expectedScanNum == realScanNum) {
                // 该磁盘扫描数量已到指定值
                completeScanDiskCount++;
            } else {
                // 该磁盘扫描数量未到指定值，说明没有可扫描数据了，则从集合中移除
                iterator.remove();
            }
            total += realScanNum;
            firstDisk = false;
        }
        if (disks.isEmpty() || completeScanDiskCount == diskSize) {
            return total;
        }
        return total + scanByAccessTime(pool, (scanNum - total), disks);

    }
    public int scanCacheDisk(StoragePool pool, String disk, int scanNum, ScanType scanType) {
        AtomicInteger n = new AtomicInteger(0);
        if (scanNum == 0) {
            return n.get();
        }

        long endStamp = 0;
        String endStampMarker = "";
        if (ScanType.BY_ACCESS_TIME.equals(scanType) || isEnableCacheAccessTimeFlush(pool)) {
            //根据当前时间生成截止日期
            //这里需要获取配置分层时的结束日期
            long days = getLowFrequencyAccessDays(pool);//假设是30天,目前代码这里是每个池单独设置，按规格的话同一个策略下的缓存池配置应该统一
            endStamp = System.currentTimeMillis() - days * 24 * 60 * 60 * 1000;//本次扫描往前推30天
            endStampMarker = Utils.getEndTimeStampMarker(String.valueOf(endStamp));
        }
        String keyPrefix;

        switch (scanType) {
            case BY_UPLOAD_TIME:
                keyPrefix = ROCKS_CACHE_ORDERED_KEY;
                break;
            case BY_ACCESS_TIME:
                keyPrefix = ROCKS_CACHE_ACCESS_KEY;
                break;
            default:
                keyPrefix = ROCKS_FILE_META_PREFIX;
                break;
        }
        MSRocksDB cacheDb = MSRocksDB.getRocksDB(disk);
        if (cacheDb == null) {
            return n.get();
        }

        /**
         * 对于扫描访问时间记录，从头开始扫描，然后判断扫描到的时间是否在应扫描的时间戳内
         * 首先确定结束扫描的时间戳，然后用结束的时间戳生成扫描结束的key，ROCKS_CACHE_ACCESS_KEY + endTimestamp;
         * 扫描key时判断是否大于结束key，大于就不再进行扫描
         */
        boolean isEnd = false;
        String marker = "";
        if (isEnableCacheAccessTimeFlush(pool) && ScanType.DEFAULT.equals(scanType)) {
            marker = redisConnPool.getShortMasterCommand(15).hget("access_flush_marker_" + pool.getVnodePrefix(), this.node);
        }
        try (MSRocksIterator iterator = cacheDb.newIterator()) {
            if (StringUtils.isNotEmpty(marker)) {
                iterator.seek(marker.getBytes());
            } else {
                iterator.seek(keyPrefix.getBytes());
            }
            while (iterator.isValid() && n.get() < scanNum) {
                String key = new String(iterator.key());
                if (!key.startsWith(keyPrefix)) {
                    break;
                }
                FileMeta meta = null;
                byte[] bytes;
                switch (scanType) {
                    case BY_UPLOAD_TIME:
                        //获取key之后进行转换，然后再通过转换得到的fileMeta进行后续处理
                        String cacheOrderKey = new String(iterator.key());
                        bytes = cacheDb.get(getFileMetaKeyByCacheOrderKey(cacheOrderKey).getBytes());//这里拿到orderkey之后又转成了#开头的key去查fileMeta
                        if (null == bytes) {
                            meta = null;
                        } else {
                            meta = Json.decodeValue(new String(bytes), FileMeta.class);
                        }
                        break;
                    case BY_ACCESS_TIME:
                        //获取key之后进行转换，然后再通过转换得到的fileMeta进行后续处理
                        String accessKey = new String(iterator.key());
                        //与结束时间戳进行比较确定是否需要处理
                        if (accessKey.compareTo(endStampMarker) < 0) {
                            bytes = cacheDb.get(getFileMetaKeyByAccessTimeKey(accessKey).getBytes());
                            if (null == bytes) {
                                meta = null;
                            } else {
                                meta = Json.decodeValue(new String(bytes), FileMeta.class);
                            }
                        } else {
                            //当扫到的key大于等于结束的key时跳出循环
                            isEnd = true;
                        }
                        break;
                    default:
                        //这里直接getFileMeta进行下刷
                        meta = Json.decodeValue(new String(iterator.value()), FileMeta.class);
                        break;
                }
                if (isEnd) {
                    log.debug("scan access record over in {}",  disk);
                    break;
                }

                if (null != meta && null != meta.getMetaKey()) {
                    if (isEnableCacheAccessTimeFlush(pool)) {//开启了访问时间分层之后，之前扫“}”表结束，这里继续扫“#”对本次开启分层前已存在的数据进行处理
                        if (StringUtils.isNotEmpty(meta.getLastAccessStamp())) {
                            if (meta.getLastAccessStamp().compareTo(String.valueOf(endStamp)) < 0) {
                                //这里如果fileMeta中的lastAccessStamp是上次开启分层时更新的，然后后面关闭了分层后就没再更新，本次开启分层后还未进行访问，缓存盘中的访问记录还是上次的
                                //如果访问记录中或者fileMeta中上次的访问时间戳在处理范围内，则进行下刷，无论是否访问都按照冷数据处理，就不再统一用本次开启分层的时间作为其访问时间了
                                recordTask(pool, meta, disk, n);
                            }
                        } else {
                            //无访问时间属性，使用开启分层功能的时间作为其最后访问时间
                            log.debug("endStamp:{}, flag:{}", endStamp, getEnableLayeringStamp(pool).compareTo(String.valueOf(endStamp)) < 0);
                            if (getEnableLayeringStamp(pool).compareTo(String.valueOf(endStamp)) < 0) {
                                recordTask(pool, meta, disk, n);
                            }
                        }
                    } else {
                        recordTask(pool, meta, disk, n);
                    }
                }
                if (isEnableCacheAccessTimeFlush(pool) && ScanType.DEFAULT.equals(scanType)) {
                    if (scanNum == n.get()) {
                        marker = key;
                        redisConnPool.getShortMasterCommand(15).hset("access_flush_marker_" + pool.getVnodePrefix(), this.node, marker);//关闭分层后maker也需要被清除
                    }
                }

                iterator.next();
            }

        } catch (RocksDBException e) {
            log.error("", e);
        }
        return n.get();
    }

    public void recordTask(StoragePool pool, FileMeta meta, String disk, AtomicInteger n) {
        boolean isMigrating = false;
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
                    n.incrementAndGet();
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
                                String poolName = map.get("poolName");
                                if (!map.isEmpty()) {
                                    if (poolName != null) {
                                        String role = RedisConnPool.getInstance().getShortMasterCommand(REDIS_POOL_INDEX).hget(poolName, "role");
                                        if ("cache".equals(role)) {
                                            isMigrating = "add_node".equals(operate) || "add_disk".equals(operate) || "expand".equals(operate) || "remove_node".equals(operate);
                                        }
                                    } else {
                                        isMigrating = "add_node".equals(operate) || "add_disk".equals(operate) || "expand".equals(operate) || "remove_node".equals(operate);
                                    }
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

    /**
     * 判断是否开启分层下刷
     * @param cachePool
     * @return
     */
    public static boolean isEnableCacheAccessTimeFlush(StoragePool cachePool) {
        return cachePool.getVnodePrefix().startsWith("cache") && CacheFlushConfigRefresher.getInstance().getFlushConfig(cachePool.getVnodePrefix()).isEnableAccessTimeFlush();
    }

    public static int getLowFrequencyAccessDays(StoragePool cachePool) {
        return CacheFlushConfigRefresher.getInstance().getFlushConfig(cachePool.getVnodePrefix()).getLowFrequencyAccessDays();
    }

    public static String getEnableLayeringStamp(StoragePool cachePool) {
        return CacheFlushConfigRefresher.getInstance().getFlushConfig(cachePool.getVnodePrefix()).getEnableLayeringStamp();
    }



    public enum ScanType {
        BY_UPLOAD_TIME(ROCKS_CACHE_ORDERED_KEY),
        BY_ACCESS_TIME(ROCKS_CACHE_ACCESS_KEY),
        DEFAULT(ROCKS_FILE_META_PREFIX);
        private final String prefix;

        ScanType(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return this.prefix;
        }
    }
}
