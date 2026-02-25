package com.macrosan.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import com.macrosan.ServerStart;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.rebuild.ReBuildRunner;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StorageOperate.PoolType;
import com.macrosan.storage.aggregation.AggregateConfig;
import com.macrosan.storage.aggregation.AggregateFileGcScheduler;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.aggregation.namespace.StorageStrategyNameSpace;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.getMqRocksKey;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_DISK_USED_SIZE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.fs.BlockDevice.*;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.StorageOperate.DATA;
import static com.macrosan.storage.strategy.StorageStrategy.*;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class StoragePoolFactory {
    private static final String DEFAULT_META_POOL_PREFIX = "meta";
    private static final String DEFAULT_DATA_POOL_PREFIX = "";
    public static final int DEFAULT_PACKAGE_SIZE = 128 * 1024;
    public static final long DEFAULT_DIVISION_SIZE = 100L << 20;
    private static final AtomicReference<MonoProcessor<Boolean>> forceReload = new AtomicReference<>(null);
    private static final Map<String, StoragePool> MAP = new ConcurrentHashMap<>();
    private static final Map<String, StoragePool> NO_USED_MAP = new ConcurrentHashMap<>();
    private static final Map<String, String> POOL_NAME_MAP = new ConcurrentHashMap<>();
    private static Set<String> needInitConsumer = new ConcurrentHashSet<>();
    private static Set<String> existsPoolConsumer = new ConcurrentHashSet<>();

    static {
        ErasureServer.DISK_SCHEDULER.schedule(StoragePoolFactory::reload, 60, TimeUnit.SECONDS);
    }

    public static Mono<Boolean> forceReload() {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        if (forceReload.compareAndSet(null, res)) {

        } else {
            res.onNext(true);
        }
        return res;
    }

    private static void reload() {
        try {
            if (!MAP.isEmpty() && !POOL_STRATEGY_MAP.isEmpty()) {
                boolean needUpdate = false;
                List<String> strategyList = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).keys(POOL_STRATEGY_PREFIX + "*");
                Set<String> set = strategyList.stream().filter(strategy -> {
                    Map<String, String> strategyMap = RedisConnPool
                            .getInstance()
                            .getCommand(REDIS_POOL_INDEX)
                            .hgetall(strategy);

                    String metaStr = strategyMap.get("meta");
                    String dataStr = strategyMap.get("data");
                    return !checkPoolsEmpty(metaStr) && !checkPoolsEmpty(dataStr);
                }).collect(Collectors.toSet());

                if (!Sets.symmetricDifference(POOL_STRATEGY_MAP.keySet(), set).isEmpty()) {
                    Sets.difference(POOL_STRATEGY_MAP.keySet(), set).forEach(POOL_STRATEGY_MAP::remove);
                    needUpdate = true;
                } else {
                    for (Map.Entry<String, StorageStrategy> strategyEntry : POOL_STRATEGY_MAP.entrySet()) {
                        String strategyName = strategyEntry.getKey();
                        StorageStrategy strategy = strategyEntry.getValue();
                        Map<String, String> old = strategy.redisMap;
                        Map<String, String> cur = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX)
                                .hgetall(strategyName);

                        if (!old.equals(cur)) {
                            needUpdate = true;
                            break;
                        }
                    }
                }
                if (needUpdate || forceReload.get() != null) {
                    try {
                        log.info("start reload storage pool");
                        load(forceReload.get() != null);
                        BlockDevice.init();
                        ReBuildRunner.getInstance().rabbitMq.initConsumer();
                        log.info("reload storage pool end");
                    } catch (Exception e) {
                        log.error("load storage pool fail", e);
                    }

                    MonoProcessor<Boolean> force = forceReload.get();
                    if (force != null) {
                        //增加检查此时前端包Map缓存是否与redis中配置相同
                        if (checkUsedPools()) {
                            forceReload.compareAndSet(force, null);
                            force.onNext(true);
                        } else {
                            log.info("wait for next load...................");
                        }
                    }
                }
                reloadCompressionAlgorithm();
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            ErasureServer.DISK_SCHEDULER.schedule(StoragePoolFactory::reload, 60, TimeUnit.SECONDS);
        }
    }

    private static void initPoolConsumers() {
        if ("1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(ServerConfig.getInstance().getHostUuid(), "isRemoved"))) {
            //当前节点待移除不再在其他节点初始化队列
            needInitConsumer.clear();
            return;
        }
        for (String prefix : needInitConsumer) {
            String poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget("storage_" + prefix, "pool");
            POOL_NAME_MAP.put(prefix, poolName);
            if (!existsPoolConsumer.contains(poolName)) {
                ObjectConsumer.getInstance().initPoolConsumers(poolName);
                log.info("consumer Prefix: {}, poolName: {}", prefix, poolName);
                existsPoolConsumer.add(poolName);
            }
        }
        needInitConsumer.clear();
    }

    private static boolean checkUsedPools() {
        List<String> strategyList = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).keys(POOL_STRATEGY_PREFIX + "*");//遍历表15下的存储策略

        for (String strategyName : strategyList) {//遍历每个存储策略，比较其配置是否相同
            Map<String, String> strategyMap = RedisConnPool
                    .getInstance()
                    .getCommand(REDIS_POOL_INDEX)
                    .hgetall(strategyName);

            String metaStr = strategyMap.get("meta");
            String dataStr = strategyMap.get("data");
            String cacheStr = strategyMap.get("cache");
            if (!checkPoolsEmpty(metaStr) && !checkPoolsEmpty(dataStr)) {//过滤未关联存储池的策略
                String[] meta = Json.decodeValue(metaStr, String[].class);
                String[] data = Json.decodeValue(dataStr, String[].class);
                if (!MAP.keySet().containsAll(Arrays.asList(data)) || !MAP.keySet().containsAll(Arrays.asList(meta))) {
                    return false;
                }
                if (!checkPoolsEmpty(cacheStr)) {
                    String[] cache = Json.decodeValue(metaStr, String[].class);
                    if (!MAP.keySet().containsAll(Arrays.asList(cache))) {
                        return false;
                    }
                    for (String prefix : cache) {
                        Map<String, String> storageInfo = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hgetall("storage_" + prefix);
                        int k = Integer.parseInt(storageInfo.get("k"));
                        int m = Integer.parseInt(storageInfo.get("m"));
                        if (MAP.get(prefix).getK() != k || MAP.get(prefix).getM() != m) {
                            return false;
                        }
                    }
                }
                //增加检查存储池EC策略或副本策略是否正确
                for (String prefix : meta) {
                    Map<String, String> storageInfo = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hgetall("storage_" + prefix);
                    int k = Integer.parseInt(storageInfo.get("k"));
                    int m = Integer.parseInt(storageInfo.get("m"));
                    if (MAP.get(prefix).getK() != k || MAP.get(prefix).getM() != m) { //1!=2 || 1!=1 =>true || false => true//只要k或者m有一个不相同那么就需要重新reload
                        return false;
                    }
                }
                for (String prefix : data) {
                    Map<String, String> storageInfo = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hgetall("storage_" + prefix);
                    int k = Integer.parseInt(storageInfo.get("k"));
                    int m = Integer.parseInt(storageInfo.get("m"));
                    if (MAP.get(prefix).getK() != k || MAP.get(prefix).getM() != m) {
                        return false;
                    }
                }
            }
        }
        return true;

    }

    private static void load(boolean force) {
        RedisCommands<String, String> syncCommand = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX);
        Map<String, List<String>> vnodeListMap = new HashMap<>();

        List<String> strategyList = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).keys(POOL_STRATEGY_PREFIX + "*");
        TypeReference<Set<String>> setReference = new TypeReference<Set<String>>() {
        };
        Set<String> poolSet = new HashSet<>();
        for (String strategy : strategyList) {
            Map<String, String> strategyMap = RedisConnPool
                    .getInstance()
                    .getCommand(REDIS_POOL_INDEX)
                    .hgetall(strategy);

            String metaStr = strategyMap.get("meta");
            String dataStr = strategyMap.get("data");
            String cacheStr = strategyMap.get("cache");
            if (!checkPoolsEmpty(metaStr)) {
                poolSet.addAll(Json.decodeValue(metaStr, setReference));
            }
            if (!checkPoolsEmpty(dataStr)) {
                poolSet.addAll(Json.decodeValue(dataStr, setReference));
            }
            if (!checkPoolsEmpty(cacheStr)) {
                poolSet.addAll(Json.decodeValue(cacheStr, setReference));
            }
        }

        if (ServerStart.futures != null) {
            for (String key : ServerStart.futures.keySet()) {
                String prefix = getPrefix(key);
                List<String> vnodeList = vnodeListMap.computeIfAbsent(prefix, k -> new LinkedList<>());
                vnodeList.add(key);
            }
        } else {
            ScanIterator<String> iterator = ScanIterator.scan(syncCommand, new ScanArgs().match("*"));

            while (iterator.hasNext()) {
                String key = iterator.next();
                String prefix = getPrefix(key);
                List<String> vnodeList = vnodeListMap.computeIfAbsent(prefix, k -> new LinkedList<>());
                vnodeList.add(key);
            }
        }

        for (String prefix : vnodeListMap.keySet()) {
            if ((!MAP.containsKey(prefix) || force) && poolSet.contains(prefix)) {
                needInitConsumer.add(prefix);
                String strategyName = "storage_" + prefix;
                String poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
                POOL_NAME_MAP.put(prefix, poolName);
                log.info("load pool in MAP: {}", prefix);
                List<String> vnodeList = vnodeListMap.get(prefix);
                int linkSize = (int) (syncCommand.hget(vnodeList.get(0), "link")
                        .chars().filter(c -> c == ',').count() + 1);
                StoragePool pool = loadStorage(prefix, linkSize, vnodeList, vnodeListMap);
                MAP.put(prefix, pool);
                MAX_VNODE_NUM = Math.max(MAX_VNODE_NUM, pool.getVnodeNum());
            } else if (!prefix.startsWith("map-") && !poolSet.contains(prefix) &&
                    (1 == RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).exists("storage_" + prefix))) {
                //如果表6中存在未被存储策略关联的存储池vnode
                log.info("load pool in NO_USED_MAP: {}", prefix);
                List<String> noUsedList = vnodeListMap.get(prefix);
                int linkSize = (int) (syncCommand.hget(noUsedList.get(0), "link")
                        .chars().filter(c -> c == ',').count() + 1);
                StoragePool noUsedPool = loadStorage(prefix, linkSize, noUsedList, vnodeListMap);
                NO_USED_MAP.put(prefix, noUsedPool);
                MAP.remove(prefix);//MAP中只保留存储策略中关联的存储池，移除掉不再使用的存储池,防止在reload时不覆盖同名的旧存储池对象
            }
        }

        for (String strategy : strategyList) {
            String strategyName = strategy.substring(POOL_STRATEGY_PREFIX.length());
            StorageStrategy storageStrategy;
            try {
                Map<String, String> strategyMap = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hgetall(strategy);
                // 是否开启小文件聚合,加载小文件聚合配置
                String aggregation = strategyMap.get("small_file_aggregation");
                if ("on".equals(aggregation)) {
                    AggregateConfig aggregateConfig = new AggregateConfig();
                    String fileSize = strategyMap.get("max_aggregated_file_size");
                    if (StringUtils.isNotEmpty(fileSize)) {
                        try {
                            aggregateConfig.setMaxAggregatedFileSize(Long.parseLong(fileSize));
                        } catch (NumberFormatException e) {
                            log.error("parse max_aggregated_file_size error, use default value: {}", aggregateConfig.getMaxAggregatedFileSize());
                        }
                    }
                    String maxAggregatedFileNum = strategyMap.get("max_aggregated_file_num");
                    if (StringUtils.isNotEmpty(maxAggregatedFileNum)) {
                        try {
                            aggregateConfig.setMaxAggregatedFileNum(Integer.parseInt(maxAggregatedFileNum));
                        } catch (NumberFormatException e) {
                            log.error("parse max_aggregated_file_num error, use default value: {}", aggregateConfig.getMaxAggregatedFileNum());
                        }
                    }
                    String maxHoleRate = strategyMap.get("max_hole_rate");
                    if (StringUtils.isNotEmpty(maxHoleRate)) {
                        try {
                            aggregateConfig.setMaxHoleRate(Double.parseDouble(maxHoleRate));
                        } catch (NumberFormatException e) {
                            log.error("parse max_hole_rate error, use default value: {}", aggregateConfig.getMaxHoleRate());
                        }
                    }
                    boolean useDirect = Boolean.parseBoolean(strategyMap.getOrDefault("small_file_aggregate_use_direct", "false"));
                    aggregateConfig.setUseDirect(useDirect);
                    NameSpace nameSpace = new StorageStrategyNameSpace(strategy);
                    NameSpace computeIfAbsent = STRATEGY_NAMESPACE_MAP.computeIfAbsent(strategy, k -> nameSpace);
                    computeIfAbsent.setAggregateConfig(aggregateConfig);
                    AggregateFileGcScheduler.start(computeIfAbsent);
                } else {
                    STRATEGY_NAMESPACE_MAP.remove(strategy);
                }

                storageStrategy = loadStrategy(strategy, MAP);
                String isDefault = strategyMap.get("is_default");
                if ("true".equals(isDefault)) {
                    DEFAULT = storageStrategy;
                    DEFAULT_STRATEGY_NAME = strategy;
                }
                POOL_STRATEGY_MAP.put(strategy, storageStrategy);
            } catch (Exception e) {
                if (e instanceof UnsupportedOperationException && (e.getMessage().contains("load strategy") || e.getMessage().contains("next pool reload"))) {
                    log.info(e.getMessage());
                } else {
                    log.error("", e);
                }
            }
        }
        if (DEFAULT == null && !POOL_STRATEGY_MAP.isEmpty()) {
            DEFAULT = POOL_STRATEGY_MAP.values().iterator().next();
            DEFAULT_STRATEGY_NAME = POOL_STRATEGY_MAP.keySet().iterator().next();
        } else if (DEFAULT == null) {
            throw new UnsupportedOperationException("default storage strategy does not exist");
        }
        List<String> bucketList = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).keys("*");
        for (String bucket : bucketList) {
            if (BUCKET_NAME_PATTERN.matcher(bucket).matches() && "hash".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).type(bucket))) {
                String storateStrategy = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "storage_strategy");
                Optional.ofNullable(storateStrategy).ifPresent(x -> BUCKET_STRATEGY_NAME_MAP.put(bucket, storateStrategy));
            }
        }
        StoragePool.loadBucketShard();
        initPoolConsumers();
    }

    public static void initStoragePools() {
        load(false);
        update();
    }

    public static int MAX_VNODE_NUM = -1;

    private static final Payload GET_SIZE_PAYLOAD = DefaultPayload.create("", GET_DISK_USED_SIZE.name());

    private static TypeReference<Tuple2<byte[], byte[]>[]> reference = new TypeReference<Tuple2<byte[], byte[]>[]>() {
    };

    private static void update() {
        try {
            Flux.fromStream(RabbitMqUtils.HEART_IP_LIST.stream())
                    .flatMap(ip -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(GET_SIZE_PAYLOAD))
                    .onErrorReturn(ERROR_PAYLOAD)
                    .publishOn(DISK_SCHEDULER)
                    .timeout(Duration.ofSeconds(30))
                    .doFinally(s -> DISK_SCHEDULER.schedule(StoragePoolFactory::update, 60, TimeUnit.SECONDS))
                    .doOnNext(p -> {
                        if (p.getMetadataUtf8().equalsIgnoreCase(SUCCESS.name())) {
                            try {
                                Tuple2<byte[], byte[]>[] array = Json.decodeValue(p.getDataUtf8(), reference);
                                MSRocksDB db = MSRocksDB.getRocksDB(getMqRocksKey(), false);
                                for (Tuple2<byte[], byte[]> t : array) {
                                    db.put(t.var1, t.var2);
                                }
                            } catch (Exception e) {

                            }
                        }
                    }).subscribe();
        } catch (Exception e) {
            log.error("get used size fail", e);
            DISK_SCHEDULER.schedule(StoragePoolFactory::update, 300, TimeUnit.SECONDS);
        }
    }

    static String getPrefix(String key) {
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c >= '0' && c <= '9') {
                return key.substring(0, i);
            }
        }

        return "";
    }

    /**
     * 获得存储池的配置并生成存储池
     * 默认的数据池和元数据池首先尝试用旧版本的方法获得存储池配置
     *
     * @param prefix    前缀
     * @param linkSize  存储池k+m的值
     * @param vnodeList vnodeList
     * @return 存储池
     */
    private static StoragePool loadStorage(String prefix, int linkSize, List<String> vnodeList,
                                           Map<String, List<String>> vnodeListMap) {
        Map<String, String> strategy = RedisConnPool.getInstance()
                .getCommand(REDIS_POOL_INDEX).hgetall("storage_" + prefix);

        if (strategy.isEmpty()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "load storage pool [" + prefix + "] fail");
        }

        int k = Integer.parseInt(strategy.get("k"));
        int m = Integer.parseInt(strategy.get("m"));
        int packageSize = Integer.parseInt(strategy.getOrDefault("packageSize", "" + DEFAULT_PACKAGE_SIZE));
        long divisionSize = Long.parseLong(strategy.getOrDefault("divisionSize", "" + DEFAULT_DIVISION_SIZE));
        StoragePoolType type = StoragePoolType.valueOf(strategy.get("type").toUpperCase());
        String compression = RedisConnPool.getInstance()
                .getCommand(REDIS_POOL_INDEX).hget(strategy.get("pool"), "compression");

        List<String> mapVnodeList;
        if (StringUtils.isNotBlank(strategy.get("map"))) {
            String mapPrefix = strategy.get("map");
            mapVnodeList = vnodeListMap.get(mapPrefix);
        } else {
            mapVnodeList = null;
        }

        String updateECPrefix = null;
        if (StringUtils.isNotBlank(strategy.get("updateECPrefix"))) {//带有updateECPrefix表示数据池正在修改ec，该配置不在存储策略中使用
            updateECPrefix = strategy.get("updateECPrefix");
        }

        return new StoragePool(prefix, type, k, m, packageSize, divisionSize, vnodeList, mapVnodeList, updateECPrefix, compression);
    }

//    public static StoragePool getStoragePoolByStrategyName(String storageStrategy, StorageOperate operate) {
//        return StorageStrategy.DEFAULT.getStoragePool(operate);
//    }

    /**
     * @description: 获取默认策略下的存储池
     **/
    public static StoragePool getStoragePool(StorageOperate operate) {
        return StorageStrategy.DEFAULT.getStoragePool(operate);
    }

    /**
     * @description: 根据桶名获取指定策略下的存储池
     * @author: wanhao
     * @date: 2022/9/16 0016 上午 10:58
     **/
    public static StoragePool getStoragePool(StorageOperate operate, String bucketName) {
        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(POOL_STRATEGY_MAP::get)
                .map(strategy -> strategy.getStoragePool(operate))
                .orElseGet(() -> DEFAULT.getStoragePool(operate));
    }

    public static boolean inBucketStrategy(String storageName, String bucketName) {
        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(POOL_STRATEGY_MAP::get)
                .map(strategy -> strategy.dataPool.contains(storageName))
                .orElse(false);
    }

    public static boolean isCachePoolInBucketStrategy(String storageName, String bucketName) {
        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(POOL_STRATEGY_MAP::get)
                .map(strategy -> strategy.cachePool.contains(storageName))
                .orElse(false);
    }

    public static void getAllStorage(String key, Set<String> dedupFile){
        String suffix = key.split("#")[1];
        int index = key.lastIndexOf("/");
        String prefix = key.substring(0, index + 1);

        for (StoragePool pool : MAP.values()) {
            if (pool.getVnodePrefix().startsWith("data")) {
                String res = prefix + pool.getVnodePrefix() + "#" + suffix;
                dedupFile.add(res);
            }
        }

    }
    public static List<StoragePool> getAvailableStorages(String bucket) {
        List<StoragePool> list = new LinkedList<>();

        for (StoragePool pool : MAP.values()) {
            if (pool.getVnodePrefix().startsWith("data")) {
                if(inBucketStrategy(pool.getVnodePrefix(), bucket)) {
                    list.add(pool);
                }
            }
        }

        return list;
    }

    public static List<StoragePool> getAvailableStoragesWithCachePool(String bucket) {
        List<StoragePool> list = new LinkedList<>();

        for (StoragePool pool : MAP.values()) {
            if (pool.getVnodePrefix().startsWith("data")) {
                if(inBucketStrategy(pool.getVnodePrefix(), bucket)) {
                    list.add(pool);
                }
            } else if (pool.getVnodePrefix().startsWith("cache")) {
                if(isCachePoolInBucketStrategy(pool.getVnodePrefix(), bucket)) {
                    list.add(pool);
                }
            }

        }

        return list;
    }

    public static List<StoragePool> getAvailableStorages(Set<String> dataPoolSet) {
        List<StoragePool> list = new LinkedList<>();

        for (StoragePool pool : MAP.values()) {
            if (dataPoolSet.contains(pool.getVnodePrefix())) {
                list.add(pool);
            }
        }

        return list;
    }


    public static boolean getDeduplicate(String bucketName){
    String strategy = BUCKET_STRATEGY_NAME_MAP.get(bucketName);
    String deduplicate = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategy, "deduplicate");
    return "on".equals(deduplicate);
  }

    /**
     * 根据桶获取重删开关
     * @param bucketName 桶名
     * @return 重删开关
     */
    public static Mono<Boolean> getDeduplicateByBucketName(String bucketName){
        String strategyName = BUCKET_STRATEGY_NAME_MAP.getOrDefault(bucketName, DEFAULT_STRATEGY_NAME);
        return Mono.just(strategyName)
                .flatMap(strategy -> RedisConnPool.getInstance().getReactive(REDIS_POOL_INDEX).hget(strategy, "deduplicate"))
                .defaultIfEmpty("off")
                .map("on"::equals);
    }

    /**
     * 根据存储池策略获取重删开关
     * @param strategyName 桶名
     * @return 重删开关
     */
    public static Mono<Boolean> getDeduplicateByStrategy(String strategyName){
        return RedisConnPool.getInstance().getReactive(REDIS_POOL_INDEX).hget(strategyName, "deduplicate")
                .defaultIfEmpty("off")
                .map("on"::equals);
    }

    public static StoragePool getStoragePoolByDisk(String disk) {
        for (StoragePool pool : MAP.values()) {
            if (pool.contains(disk)) {
                return pool;
            }
        }

        return null;
    }

    public static StoragePool getStoragePoolByStrategyName(String targetStorageStrategy, StorageOperate operate) {
        StorageStrategy storageStrategy = POOL_STRATEGY_MAP.get(targetStorageStrategy);
        return storageStrategy.getStoragePool(operate);
    }

    public static StoragePool getStoragePool(MetaData metaData) {
        return getStoragePool(metaData.storage, metaData.bucket);
    }

    public static StoragePool getStoragePool(PartInfo partInfo) {
        return getStoragePool(partInfo.storage, partInfo.bucket);
    }

    public static StoragePool getStoragePool(InitPartInfo partInfo) {
        return getStoragePool(partInfo.storage, partInfo.bucket);
    }

    public static StoragePool getStoragePool(AggregateFileMetadata metadata) {
        return getStoragePool(metadata.storage, null);
    }

    public static StoragePool getStoragePool(String storage, String bucketName) {
        if (null == storage || StringUtils.isBlank(storage)) {
            return getStoragePool(DATA, bucketName);
        }

        for (StoragePool pool : MAP.values()) {
            if (pool.getVnodePrefix().equalsIgnoreCase(storage)) {
                return pool;
            }
        }

        if (NO_USED_MAP.containsKey(storage)) {
            for (StoragePool pool : NO_USED_MAP.values()) {
                if (pool.getVnodePrefix().equalsIgnoreCase(storage)) {
                    return pool;
                }
            }
        }

        return null;
    }

    public static StoragePool getNoUsedStoragePool(String storage) {
        for (StoragePool pool : NO_USED_MAP.values()) {
            if (pool.getVnodePrefix().equalsIgnoreCase(storage)) {
                return pool;
            }
        }
        return null;
    }

    public static boolean inStorageMAP(String storage){
        return MAP.containsKey(storage);
    }

    public static boolean inNoUsedMap(String storage) {
        return NO_USED_MAP.containsKey(storage);
    }

    public static String getPoolNameByPrefix(String prefix) {
        return POOL_NAME_MAP.get(prefix);
    }

    public static StoragePool getMetaStoragePool(String key) {
        StorageOperate operate = new StorageOperate(PoolType.META, key, -1);
        return getStoragePool(operate);
    }

    public static void reloadCompressionAlgorithm() {
        List<String> poolKeys = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).keys("Pool_*");
        for (String poolKey : poolKeys) {
            String prefix = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolKey, "prefix");
            String compression = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolKey, "compression");
            if (prefix != null && prefix.startsWith("data") && MAP.containsKey(prefix)) {
                StoragePool pool = MAP.get(prefix);
                String beforeCompression = pool.getCompression();
                if (!StringUtils.equals(beforeCompression, compression)) {
                    pool.setCompression(compression);
                    log.info("{} compression update: before:{} , after:{}", poolKey, beforeCompression, compression);
                }
            }
        }

        //更新升级after_compression_size
        //获取开启配置压缩的所有数据池下的盘
        for (String poolKey : poolKeys) {
            String prefix = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolKey, "prefix");
            String compression = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolKey, "compression");
            StoragePool storagePool = StoragePoolFactory.getStoragePool(prefix, null);
            if (prefix != null && prefix.startsWith("data") && MAP.containsKey(prefix)) {
                if (compression == null || "none".equalsIgnoreCase(compression)) {//开过又关掉就不显示了
                    continue;
                }

                String fileSystems = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolKey, "file_system");
                List<String> lunList = Arrays.asList(fileSystems.replaceAll("[\\[\\]]", "")
                        .split(",\\s*"));
                //获取到所有的盘之后根据节点分发消息去各节点查询盘的统计
                //累加每块盘上的压缩前量和压缩后量
                for (int j = 0; j < lunList.size(); j++) {
                    String fsName = lunList.get(j);
                    if (fsName.contains("\"")) {
                        fsName = fsName.replace("\"", "");
                    }
                    String node = fsName.split("@")[0];
                    String disk = fsName.split("@")[1];
                    if (node.equalsIgnoreCase(ServerConfig.getInstance().getHostUuid()) && !RemovedDisk.getInstance().contains(fsName) && null != storagePool) {
                        try {

                            //估算每块盘上的文件数补齐后的大小
                            byte[] fileNumBytes = MSRocksDB.getRocksDB(disk).get(ROCKS_FILE_SYSTEM_FILE_NUM);
                            byte[] fileSizeBytes = MSRocksDB.getRocksDB(disk).get(ROCKS_FILE_SYSTEM_FILE_SIZE);
                            byte[] usedSizeBytes = MSRocksDB.getRocksDB(disk).get(ROCKS_FILE_SYSTEM_USED_SIZE);
                            if (fileNumBytes != null && fileSizeBytes != null && usedSizeBytes != null) {
                                long fileSize = ECUtils.bytes2long(fileSizeBytes);
                                long fileNum = ECUtils.bytes2long(fileNumBytes);
                                if (fileNum != 0) {
                                    long averageSize = fileSize / fileNum;//平均原始大小

                                    long flush = Math.max(averageSize % MIN_ALLOC_SIZE == 0 ? averageSize / MIN_ALLOC_SIZE : averageSize / MIN_ALLOC_SIZE + 1, 1);
                                    long sum = 0;
                                    for (long i = 0; i < flush; i++) {
                                        if (i == flush - 1) {
                                            sum += averageSize % 4096 == 0 ? (averageSize - MIN_ALLOC_SIZE * (flush -1)) : ((averageSize - MIN_ALLOC_SIZE * (flush -1)) / 4096 * 4096 + 4096);
                                        } else {
                                            sum += MIN_ALLOC_SIZE;
                                        }
                                    }

                                    long updateBeforeCompression = Math.max(sum * fileNum, ECUtils.bytes2long(usedSizeBytes));

                                    byte[] beforeCompressionLen = MSRocksDB.getRocksDB(disk).get(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE);
                                    if (beforeCompressionLen == null || ECUtils.bytes2long(beforeCompressionLen) < updateBeforeCompression) {
                                        MSRocksDB.getRocksDB(disk).put(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(updateBeforeCompression));
                                    }
                                }
                            }

                        } catch (RocksDBException e) {
                            log.error("update after compression size fail", e);
                        }
                    }
                }
            }
        }
    }

}
