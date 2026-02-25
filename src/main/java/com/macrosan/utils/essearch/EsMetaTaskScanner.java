package com.macrosan.utils.essearch;


import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.getRabbitmqRecordLun;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOT_FOUND;
import static com.macrosan.message.jsonmsg.EsMeta.*;

/**
 * @author zouhui
 */
@Log4j2
public class EsMetaTaskScanner {
    protected final static RedisConnPool pool = RedisConnPool.getInstance();
    public static final String UUID = ServerConfig.getInstance().getHostUuid();
    public static final int MAX_SCAN_KEYS = 1500;
    public static final int THRESHOLD = 1500;
    public static final int SWITCH_BUCKET_KEYS = MAX_SCAN_KEYS / 5;
    public static final ScheduledThreadPoolExecutor ES_TIMER = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "es-scan"));
    public static final MsExecutor executor = new MsExecutor(4, 2, new MsThreadFactory("es-keep-lock"));
    public static final Scheduler ES_SCHEDULER = Schedulers.fromExecutor(executor);
    public static int RETRY_COUNT = 3;
    public static Map<String, ScanInfo> scanInfoMap = new ConcurrentHashMap<>();
    public static Set<String> lunSet = new HashSet<>();
    public static ScheduledFuture<?> scheduledFuture;
    private final AtomicBoolean locked = new AtomicBoolean();
    private static EsMetaTaskScanner instance;
    public static String lastNode = UUID;
    public static AtomicInteger RETRY = new AtomicInteger(0);
    public static AtomicInteger RUN_COUNT = new AtomicInteger(0);
    public static int SCAN_COUNT = 5;
    public static AtomicBoolean FIRST_LOCK = new AtomicBoolean(false);

    private EsMetaTaskScanner() {

    }

    public static EsMetaTaskScanner getInstance() {
        if (instance == null) {
            instance = new EsMetaTaskScanner();
        }
        return instance;
    }

    public void init() {
        lastNode = pool.getCommand(0).get("es_run");
        //当前启动时当前节点所有index盘
        ScanStream.scan(pool.getReactive(REDIS_LUNINFO_INDEX), new ScanArgs().match(UUID + "@fs*index"))
                .collectList()
                .flatMapMany(Flux::fromIterable)
                .flatMap(lun -> Mono.just(getRabbitmqRecordLun(lun.split("@")[1])))
                .doOnNext(lun -> scanInfoMap.put(lun, new ScanInfo()))
                .collectList().block();
        //处理升级
        if (pool.getCommand(REDIS_SYSINFO_INDEX).exists("metadata_search") > 0
                && "1".equals(pool.getCommand(REDIS_SYSINFO_INDEX).get("metadata_search"))
                && pool.getCommand(REDIS_SYSINFO_INDEX).exists(ES_BUCKET_SET) <= 0) {
            scanBucket();
        }

        //启动时不扫描，只建立订阅关系
        for (String lun : scanInfoMap.keySet()) {
            initSubscribe(lun);
        }
        scan();
    }

    public static void scanBucket() {
        try {
            List<String> bucketList;
            bucketList = ScanStream.scan(pool.getReactive(REDIS_BUCKETINFO_INDEX), new ScanArgs().match("*"))
                    .collectList()
                    .flatMapMany(Flux::fromIterable)
                    .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).type(bucket).flatMap(type -> {
                        return "hash".equals(type) ? pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, "mda")
                                .flatMap(mda -> "on".equals(mda) ? Mono.just(bucket) : Mono.empty()) : Mono.empty();
                    }))
                    .collectList().block();
            if (bucketList == null) {
                bucketList = new ArrayList<>();
            }
            if (!bucketList.isEmpty()) {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(ES_BUCKET_SET, bucketList.toArray(new String[0]));
            }
        } catch (Exception e) {

        }
    }

    public Mono<Boolean> bucketMapToLun(String lun) {
        return Mono.just(true).doOnNext(b -> {
            if (Objects.isNull(scanInfoMap.get(lun))) {
                scanInfoMap.putIfAbsent(lun, new ScanInfo());
                initSubscribe(lun);
            }
            //兼容之前的key
            scanInfoMap.get(lun).bucketQueue.add(ROCKS_ES_KEY);
        }).flatMap(b -> ScanStream.sscan(pool.getReactive(REDIS_SYSINFO_INDEX), ES_BUCKET_SET)
                .flatMap(bucket -> {
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                    String bucketVnode = storagePool.getBucketVnodeId(bucket);
                    return storagePool.mapToNodeInfo(bucketVnode).flatMap(nodeList -> {
                        for (Tuple3<String, String, String> tuple3 : nodeList) {
                            if (UUID.equals(NodeCache.mapIPToNode(tuple3.var1)) && lun.equals(getRabbitmqRecordLun(tuple3.var2))) {
                                ScanInfo scanInfo = scanInfoMap.get(lun);
                                synchronized (scanInfo.bucketQueue) {
                                    scanInfo.bucketQueue.add(bucket);
                                }
                                break;
                            }
                        }
                        return Mono.just(true);
                    });
                }).collectList().map(l -> true));
    }

    @Data
    public static class ScanInfo {
        private AtomicBoolean retry = new AtomicBoolean();
        private AtomicBoolean status = new AtomicBoolean();
        protected UnicastProcessor<Tuple2<String, String>> processor = UnicastProcessor.create(Queues.<Tuple2<String, String>>unboundedMultiproducer().get());
        public final LinkedList<String> bucketQueue = new LinkedList<>();
        public AtomicBoolean needStop = new AtomicBoolean(true);
        public AtomicBoolean needClear = new AtomicBoolean(false);
        public AtomicBoolean remove = new AtomicBoolean(false);
        public AtomicBoolean removeEnd = new AtomicBoolean(false);
        public AtomicReference<String> lastPrefix = new AtomicReference<>("");

        public boolean getRetry() {
            return retry.get();
        }

        public void setRetry(boolean retry) {
            this.retry.set(retry);
        }

        public boolean getStatus() {
            return status.get();
        }

        public void setStatus(boolean status) {
            this.status.set(status);
        }

        public boolean needClear() {
            return needClear.get();
        }

        public String getVnode(boolean next) {
            String bucketVnode = ROCKS_ES_KEY;
            if (bucketQueue.isEmpty()) {
                return "";
            }
            try {
                String bucket;
                if (next) {
                    bucketQueue.poll();
                    if (!bucketQueue.isEmpty()) {
                        bucket = bucketQueue.peek();
                    } else {
                        needClear.set(true);
                        return bucketVnode;
                    }
                } else {
                    bucket = bucketQueue.peek();
                }
                if (ROCKS_ES_KEY.equals(bucket)) {
                    return bucketVnode;
                }
                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                bucketVnode = storagePool.getBucketVnodeId(bucket);
            } catch (Exception e) {
                log.debug("{}", e.getMessage());
                return ROCKS_ES_KEY;
            }
            return ROCKS_ES_KEY + bucketVnode;
        }

        public Tuple2<String, String> removeVnode(String lastBucket, boolean remove) {
            String bucketVnode;
            if (bucketQueue.isEmpty()) {
                return new Tuple2<>("", "");
            }
            String bucket = "";
            try {
                if ("".equals(lastBucket)) {
                    bucket = bucketQueue.peek();
                } else {
                    int index = bucketQueue.indexOf(lastBucket);
                    if (index == -1) {
                        bucket = bucketQueue.peek();
                    } else if (index == bucketQueue.size() - 1) {
                        if (remove) {
                            bucketQueue.removeLast();
                        }
                        return new Tuple2<>("", "");
                    } else {
                        bucket = bucketQueue.get(index + 1);
                        if (remove) {
                            bucketQueue.remove(lastBucket);
                        }
                    }
                }
                if (ROCKS_ES_KEY.equals(bucket)) {
                    return new Tuple2<>(ROCKS_ES_KEY, "");
                }
                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                bucketVnode = storagePool.getBucketVnodeId(bucket);
            } catch (Exception e) {
                return new Tuple2<>(ROCKS_ES_KEY, ROCKS_ES_KEY);
            }
            return new Tuple2<>(ROCKS_ES_KEY + bucketVnode, bucket);
        }
    }


    /**
     * 扫描当前节点的es待上传数据
     */
    public void scan() {
        Optional.ofNullable(scheduledFuture).ifPresent(s -> s.cancel(false));
        scheduledFuture = ES_TIMER.scheduleAtFixedRate(() -> Mono.just(true)
                .filter(b -> pool.getCommand(REDIS_SYSINFO_INDEX).exists("metadata_search") > 0
                        && "1".equals(pool.getCommand(REDIS_SYSINFO_INDEX).get("metadata_search")))
                .flatMap(b -> {
                    if (UUID.equals(pool.getCommand(0).get("es"))) {
                        if (RUN_COUNT.incrementAndGet() > SCAN_COUNT) {
                            try {
                                unLock();
                            } finally {
                                setEnd();
                            }
                        } else {
                            FIRST_LOCK.set(false);
                            setInit();
                            return Mono.just(true);
                        }
                    } else {
                        if (canRun() && tryGetLock()) {
                            FIRST_LOCK.set(true);
                            setInit();
                            RUN_COUNT.set(0);
                            return Mono.just(true);
                        }
                    }
                    setEnd();
                    return Mono.just(false);
                })
                .flatMap(b -> ScanStream.scan(pool.getReactive(REDIS_LUNINFO_INDEX), new ScanArgs().match(UUID + "@fs*index")).collectList())
                .flatMapMany(Flux::fromIterable)
                .flatMap(lun -> Mono.just(getRabbitmqRecordLun(lun.split("@")[1])))
                .doOnNext(lun -> {
                    if (Objects.isNull(scanInfoMap.get(lun))) {
                        scanInfoMap.putIfAbsent(lun, new ScanInfo());
                        initSubscribe(lun);
                    }
                })
                .flatMap(lun -> Mono.just(scanInfoMap.get(lun)).zipWith(Mono.just(lun)))
                .flatMap(tuple2 -> {
                    ScanInfo scanInfo = tuple2.getT1();
                    if (locked.get()) {
                        scanInfo.remove.set(false);
                        if (!tuple2.getT1().getRetry() && !tuple2.getT1().getStatus()) {
                            return Mono.just(true).flatMap(f -> {
                                String lun = tuple2.getT2();
                                if (scanInfo.bucketQueue.isEmpty()) {
                                    scanInfo.removeEnd.set(false);
                                    return bucketMapToLun(lun).flatMap(b -> Mono.just(scanInfo));
                                }
                                return Mono.just(scanInfo);
                            }).doOnNext(f -> {
                                String lastPrefix = scanInfo.getVnode(false);
                                lastPrefix = FIRST_LOCK.get() && lastPrefix.equals(scanInfo.lastPrefix.get()) ? scanInfo.getVnode(true) : lastPrefix;
                                scanInfo.processor.onNext(new Tuple2<>(lastPrefix, ROCKS_ES_KEY));
                            });
                        }
                        return Mono.empty();
                    } else {
                        if (scanInfo.remove.get() || scanInfo.removeEnd.get()) {
                            return Mono.empty();
                        }
                        return Mono.just(true).flatMap(f -> {
                            String lun = tuple2.getT2();
                            if (scanInfo.bucketQueue.isEmpty()) {
                                return bucketMapToLun(lun).flatMap(b -> Mono.just(scanInfo));
                            }
                            return Mono.just(scanInfo);
                        }).doOnNext(f -> scanInfo.processor.onNext(scanInfo.removeVnode("", false)));
                    }
                })
                .subscribe(v -> {}, e -> log.error("scanEsMeta error ",e)), 10, 5, TimeUnit.SECONDS);
    }

    public void initSubscribe(String lun) {
        ScanInfo scanInfo = scanInfoMap.get(lun);
        scanInfo.processor.publishOn(ES_SCHEDULER).flatMap(tuple2 -> {
            String prefix = tuple2.var1;
            if (scanInfo.needClear()) {
                prefix = scanInfo.needStop.get() ? "" : prefix;
                String finalPrefix = prefix;
                scanInfo.needStop.set(true);
                scanInfo.needClear.set(false);
                scanInfo.removeEnd.set(false);
                return bucketMapToLun(lun).flatMap(b -> Mono.just(new Tuple2<>(finalPrefix, tuple2.var2)));
            }
            return Mono.just(new Tuple2<>(prefix, tuple2.var2));
        }).subscribe(tuple -> {
            String prefix = tuple.var1;
            String currBucket = tuple.var2;
            boolean remove = !ROCKS_ES_KEY.equals(currBucket);
            if (remove) {
                if (ROCKS_ES_KEY.equals(prefix)) {
                    currBucket = ROCKS_ES_KEY;
                }
                if (StringUtils.isBlank(prefix) || locked.get()) {
                    if (StringUtils.isBlank(prefix)) {
                        scanInfo.removeEnd.set(true);
                    }
                    scanInfo.remove.set(false);
                    return;
                }
                scanInfo.remove.set(true);
            } else {
                if (StringUtils.isBlank(prefix) || !locked.get()) {
                    scanInfo.lastPrefix.set(prefix);
                    scanInfo.setStatus(false);
                    return;
                }
                scanInfo.setStatus(true);
            }
            String finalCurrBucket = currBucket;
            scanByPrefix(lun, prefix, remove).flatMap(tuple4 -> {
                if (remove) {
                    scanInfo.processor.onNext(scanInfo.removeVnode(finalCurrBucket, "".equals(tuple4.var1)));
                    return Mono.empty();
                }
                Map<String, List<EsMeta>> delMap = tuple4.var4;
                Mono<Boolean> delayMono = Mono.just(true);
                if (delMap.get(INODE_SUFFIX) != null || delMap.get(LINK_SUFFIX) != null) {
                    delayMono = delayMono.flatMap(b -> checkDelay(delMap, INODE_SUFFIX).flatMap(c -> checkDelay(delMap, LINK_SUFFIX)));
                }
                if (!tuple4.var2.isEmpty() || !tuple4.var3.isEmpty()) {
                    delayMono = delayMono.flatMap(b -> checkUpload(tuple4.var2, delMap, NORMAL_SUFFIX)).flatMap(c -> checkUpload(tuple4.var3, delMap, CHECK_SUFFIX));
                }
                if (!tuple4.var3.isEmpty()) {
                    List<EsMeta> tempList = new ArrayList<>(tuple4.var3);
                    return delayMono.flatMap(b -> Flux.fromIterable(tempList).flatMap(esMeta -> Mono.just(StoragePoolFactory.getMetaStoragePool(esMeta.bucketName))
                                            .flatMap(bucketPool -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(esMeta.bucketName, esMeta.objName)))
                                            .flatMap(nodeList -> ErasureClient.getObjectMetaVersionUnlimitedNotRecover(esMeta.bucketName, esMeta.objName, esMeta.versionId, nodeList, null, null, null)
                                                    .zipWith(Mono.just(esMeta))))
                                    .flatMap(tuple2 -> {
                                        MetaData metaData = tuple2.getT1();
                                        EsMeta esMeta = tuple2.getT2();
                                        if (MetaData.NOT_FOUND_META.equals(metaData) || metaData.deleteMark) {
                                            //不上传要删
                                            return Mono.just(new Tuple2<>(esMeta, esMeta.toDelete || esMeta.deleteSource ? 0 : 2));
                                        } else if (MetaData.ERROR_META.equals(metaData)) {
                                            //不上传也不删
                                            return Mono.just(new Tuple2<>(esMeta, 1));
                                        } else {
                                            //要上传 上传后删
                                            if (esMeta.inode > 0 && StringUtils.isBlank(esMeta.sysMetaData)) {
                                                esMeta.sysMetaData = metaData.sysMetaData;
                                                esMeta.userMetaData = metaData.userMetaData;
                                                if (StringUtils.isNotBlank(metaData.tmpInodeStr)){
                                                    Inode inode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                                                    esMeta.objSize = String.valueOf(inode.getSize());
                                                }
                                                JsonObject sysMeta = new JsonObject(metaData.sysMetaData);
                                                esMeta.setUserId(sysMeta.getString("owner"));
                                            }
                                            return Mono.just(new Tuple2<>(esMeta, esMeta.toDelete || esMeta.deleteSource ? 2 : 0));
                                        }
                                    }).collectList())
                            .map(tuple2s -> {
                                for (Tuple2<EsMeta, Integer> tuple2 : tuple2s) {
                                    if (tuple2.var2 == 1) {
                                        tuple4.var3.remove(tuple2.var1);
                                    } else if (tuple2.var2 == 2) {
                                        List<EsMeta> deleteList = delMap.computeIfAbsent(CHECK_SUFFIX, k -> new ArrayList<>());
                                        deleteList.add(tuple2.var1);
                                        tuple4.var3.remove(tuple2.var1);
                                    } else {
                                        tuple4.var2.add(tuple2.var1);
                                    }
                                }
                                return tuple4;
                            });
                }
                return delayMono.flatMap(b -> Mono.just(tuple4));
            }).subscribe(tuple4 -> EsMetaTask.bulkEsMeta(tuple4.var2, tuple4.var3, tuple4.var4, lun, tuple4.var1)
                    , e -> {
                        try {
                            unLock();
                        } finally {
                            setEnd();
                            scanInfo.setStatus(false);
                        }
                        log.error("scanEsMeta error1! ", e);
                    });
        }, e -> {
            try {
                unLock();
            } finally {
                setEnd();
                scanInfo.setStatus(false);
            }
            log.error("scanEsMeta error2!", e);
        });
        scanInfo.processor.onNext(new Tuple2<>("", ROCKS_ES_KEY));
    }

    public Mono<Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>>> scanByPrefix(String lun, String prefix, boolean remove) {
        Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>> tuple3 = new Tuple4<>();
        MonoProcessor<Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>>> res = MonoProcessor.create();
        List<EsMeta> esMetaList = new ArrayList<>();
        List<EsMeta> checkList = new ArrayList<>();
        Map<String, List<EsMeta>> delMap = new ConcurrentHashMap<>();
        try {
            boolean oldPrefix = ROCKS_ES_KEY.equals(prefix);
            boolean oldType = false;
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return processResult(prefix, esMetaList, checkList, delMap, oldType, remove);
            }
            try (Slice lowerSlice = new Slice(prefix.getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice);
                 MSRocksIterator iterator = db.newIterator(readOptions)) {
                iterator.seek(prefix.getBytes());
                int count = 0;
                while (iterator.isValid() && new String(iterator.key()).startsWith(prefix)) {
                    //不需移除桶
                    if (remove) {
                        return processResult(prefix, esMetaList, checkList, delMap, oldType, true);
                    }
                    EsMeta esMeta = Json.decodeValue(new String(iterator.value()), EsMeta.class);
                    String key = new String(iterator.key());
                    if (oldPrefix && count == 0) {
                        String[] split = key.split(File.separator);
                        if (split.length > 2 && split[0].length() > 10) {
                            oldType = true;
                        }
                    }
                    if (oldType) {
                        String[] split = key.split(File.separator);
                        if (split.length > 2 && split[0].length() > 10) {
                            process(key, esMeta, esMetaList, checkList, delMap, lun);
                        }
                    } else {
                        process(key, esMeta, esMetaList, checkList, delMap, lun);
                    }
                    count++;
                    if (count >= MAX_SCAN_KEYS) {
                        return processResult(prefix, esMetaList, checkList, delMap, oldType, false);
                    }
                    iterator.next();
                }
                //需要移除桶
                if (remove) {
                    tuple3.var1 = "";
                    tuple3.var2 = esMetaList;
                    tuple3.var3 = checkList;
                    res.onNext(tuple3);
                    return res;
                }
                return processResult(prefix, esMetaList, checkList, delMap, oldType, false);
            }
        } catch (Exception e) {
            log.error("", e);
            res.onError(e);
            return res;
        }
    }

    private void process(String key, EsMeta esMeta, List<EsMeta> esMetaList, List<EsMeta> checkList,
                         Map<String, List<EsMeta>> delMap, String lun) {

        if (key.endsWith(esMeta.versionId + CHECK_SUFFIX)) {
            if (esMeta.inode > 0) {
                judgeSkip(esMeta, lun, checkList, delMap, CHECK_SUFFIX);
            } else {
                checkList.add(esMeta);
            }
        } else if (key.endsWith(esMeta.versionId + INODE_SUFFIX)) {
            check(esMeta, delMap, INODE_SUFFIX);
        } else if (key.endsWith(esMeta.versionId + LINK_SUFFIX)) {
            check(esMeta, delMap, LINK_SUFFIX);
        } else {
            if (esMeta.inode > 0) {
                judgeSkip(esMeta, lun, esMetaList, delMap, NORMAL_SUFFIX);
            } else {
                esMetaList.add(esMeta);
            }
        }
    }

    private void check(EsMeta esMeta, Map<String, List<EsMeta>> delMap, String type) {
        try {
            if (StringUtils.isNotBlank(esMeta.versionNum)) {
                long versionStamp;
                if (DAVersionUtils.countHyphen(esMeta.versionNum) == 2) {
                    String versionStr = esMeta.versionNum.split("-")[1];
                    versionStamp = Long.parseLong(versionStr.substring(19, 32));
                } else {
                    String versionStr = esMeta.versionNum.split("-")[0];
                    versionStamp = Long.parseLong(versionStr.substring(versionStr.length() - 13));
                }
                long curr = System.currentTimeMillis();
                if (curr - versionStamp > 3 * 60 * 1000 && curr - Long.parseLong(esMeta.stamp) > 3 * 60 * 1000) {
                    List<EsMeta> delList = delMap.computeIfAbsent(type, k -> new ArrayList<>());
                    delList.add(esMeta);
                }
            }
        } catch (Exception e) {

        }
    }

    private Mono<Boolean> checkDelay(Map<String, List<EsMeta>> delMap, String type) {
        List<EsMeta> deleteList = delMap.get(type);
        if (deleteList != null && !deleteList.isEmpty()) {
            return Flux.fromIterable(deleteList).flatMap(esMeta -> {
                //删除inode临时数据时需要所有节点正常并且一致，
                // 不一样将最新的inode数据上传并设置stamp为当前时间，推迟删除
                return EsMetaTask.getEsMeta(esMeta, INODE_SUFFIX.equals(type), LINK_SUFFIX.equals(type))
                        .flatMap(tuple -> {
                            int[] delay = new int[]{0};
                            EsMeta lastEsMeta = tuple.var1;
                            if (NOT_FOUND_ES_META.equals(lastEsMeta) || ERROR_ES_META.equals(lastEsMeta)) {
                                return Mono.just(new Tuple2<>(esMeta, 1));
                            }
                            Tuple2<ErasureServer.PayloadMetaType, EsMeta>[] res = tuple.var2;
                            for (Tuple2<ErasureServer.PayloadMetaType, EsMeta> tuple2 : res) {
                                if (null != tuple2) {
                                    EsMeta meta = tuple2.var2;
                                    //有节点异常
                                    if (ERROR.equals(tuple2.var1)) {
                                        delay[0] = 1;
                                        break;
                                        //各个节点不一致
                                    } else if (NOT_FOUND.equals(tuple2.var1) || !lastEsMeta.versionNum.equals(meta.versionNum)) {
                                        esMeta.versionNum = lastEsMeta.versionNum;
                                        delay[0] = 2;
                                    }
                                }
                            }
                            return Mono.just(new Tuple2<>(esMeta, delay[0]));
                        });
            }, 16).collectList().flatMap(tuple2s -> {
                List<EsMeta> tmpList = new ArrayList<>();
                List<Mono<Boolean>> putMono = new ArrayList<>();
                for (Tuple2<EsMeta, Integer> tuple2 : tuple2s) {
                    if (tuple2.var2 == 2) {
                        tuple2.var1.setStamp(String.valueOf(System.currentTimeMillis()));
                        EsMetaTask.EsMetaOptions options = new EsMetaTask.EsMetaOptions()
                                .setInodeRecord(INODE_SUFFIX.equals(type))
                                .setLinkRecord(LINK_SUFFIX.equals(type));
                        putMono.add(EsMetaTask.putEsMeta(tuple2.var1, options));
                    } else if (tuple2.var2 == 0) {
                        tmpList.add(tuple2.var1);
                    }
                }
                delMap.put(type, tmpList);
                return Flux.fromIterable(putMono).flatMap(v -> v).collectList().map(g -> true);
            });
        }
        return Mono.just(true);
    }


    private Mono<Boolean> checkUpload(List<EsMeta> esMetaList, Map<String, List<EsMeta>> delMap, String type) {
        if (esMetaList != null && !esMetaList.isEmpty()) {
            List<EsMeta> tmpList = new ArrayList<>(esMetaList);
            return Flux.fromIterable(tmpList).filter(esMeta -> esMeta.inode > 0).flatMap(esMeta -> {
                return EsMetaTask.getEsMeta(esMeta, true, false)
                        .flatMap(tuple -> {
                            EsMeta lastEsMeta = tuple.var1;
                            if (ERROR_ES_META.equals(lastEsMeta)) {
                                return Mono.just(new Tuple2<>(esMeta, 1));
                            }
                            if (NOT_FOUND_ES_META.equals(lastEsMeta)) {
                                return Mono.just(new Tuple2<>(esMeta, 2));
                            }
                            if (lastEsMeta.versionNum.compareTo(esMeta.versionNum) > 0) {
                                return Mono.just(new Tuple2<>(esMeta, 2));
                            }
                            return Mono.just(new Tuple2<>(esMeta, 0));
                        });
            }, 16).collectList().map(tuple2s -> {
                for (Tuple2<EsMeta, Integer> tuple2 : tuple2s) {
                    if (tuple2.var2 == 1) {
                        esMetaList.remove(tuple2.var1);
                    } else if (tuple2.var2 == 2) {
                        esMetaList.remove(tuple2.var1);
                        List<EsMeta> delList = delMap.computeIfAbsent(type, k -> new ArrayList<>());
                        delList.add(tuple2.var1);
                    }
                }
                return true;
            });
        }
        return Mono.just(true);
    }

    public void judgeSkip(EsMeta esMeta, String lun, List<EsMeta> putList, Map<String, List<EsMeta>> delMap, String type) {
        if (StringUtils.isNotBlank(esMeta.versionNum) && esMeta.versionNum.endsWith(ADD_LINK)) {
            putList.add(esMeta);
            return;
        }
        String inodeKey = EsMeta.getInodeKey(esMeta);
        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db != null) {
                byte[] inodeBytes = db.get(inodeKey.getBytes());
                if (inodeBytes != null) {
                    EsMeta inodeMeta = Json.decodeValue(new String(inodeBytes), EsMeta.class);
                    if (StringUtils.isNotBlank(inodeMeta.versionNum)
                            && esMeta.versionNum.compareTo(inodeMeta.versionNum) < 0) {
                        List<EsMeta> delList = delMap.computeIfAbsent(type, k -> new ArrayList<>());
                        delList.add(esMeta);
                        return;
                    }
                }
                putList.add(esMeta);
            }
        } catch (Exception e) {
            log.error("get esInodeValue error", e);
        }
    }

    public static boolean judge(EsMeta esMeta, String lun, byte[] curValue) {
        if ((esMeta.inode == 0 && curValue == null) ||
                (esMeta.inode > 0 && StringUtils.isNotBlank(esMeta.versionNum)
                        && esMeta.versionNum.endsWith(ADD_LINK))) {
            return true;
        }
        if (esMeta.inode > 0) {
            String inodeKey = EsMeta.getInodeKey(esMeta);
            try {
                MSRocksDB db = MSRocksDB.getRocksDB(lun);
                if (db != null) {
                    byte[] inodeBytes = db.get(inodeKey.getBytes());
                    if (inodeBytes != null) {
                        EsMeta inodeMeta = Json.decodeValue(new String(inodeBytes), EsMeta.class);
                        if (StringUtils.isNotBlank(inodeMeta.versionNum)
                                && esMeta.versionNum.compareTo(inodeMeta.versionNum) < 0) {
                            return false;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("get esInodeValue error", e);
            }
            return true;
        }
        return false;
    }

    private Mono<Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>>> processResult(String prefix, List<EsMeta> esMetaList, List<EsMeta> checkList,
                                                                                                      Map<String, List<EsMeta>> delMap, boolean oldType, boolean remove) {
        Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>> tuple4 = new Tuple4<>();
        MonoProcessor<Tuple4<String, List<EsMeta>, List<EsMeta>, Map<String, List<EsMeta>>>> res = MonoProcessor.create();
        if (remove) {
            tuple4.var1 = ROCKS_ES_KEY;
        } else {
            boolean oldPrefix = ROCKS_ES_KEY.equals(prefix);
            tuple4.var1 = oldPrefix ? oldType ? ROCKS_ES_KEY : "" : prefix;
        }
        tuple4.var2 = esMetaList;
        tuple4.var3 = checkList;
        tuple4.var4 = delMap;
        res.onNext(tuple4);
        return res;
    }

    public boolean canRun() {
        try {
            String lastRunNode = pool.getShortMasterCommand(0).get("es_run");
            List<String> lunList = pool.getCommand(REDIS_LUNINFO_INDEX).keys("*@fs*index");
            lunSet.addAll(lunList);
            if (StringUtils.isBlank(lastRunNode) && !notExistIndexLun(UUID)) {
                return true;
            }
            List<String> totalNodes = pool.getCommand(REDIS_NODEINFO_INDEX).keys("*");
            totalNodes.sort(String::compareTo);
            int index = -1;
            int lastRun = -1;
            int i = 0;
            for (String node : totalNodes) {
                if (node.equalsIgnoreCase(UUID)) {
                    index = i;
                }

                if (node.equalsIgnoreCase(lastRunNode)) {
                    lastRun = i;
                }

                i++;
            }
            i = (lastRun + 1) >= totalNodes.size() ? 0 : lastRun + 1;
            while (i != index) {
                String serverState = pool.getCommand(REDIS_NODEINFO_INDEX).hget(totalNodes.get(i), NODE_SERVER_STATE);
                if ("0".equalsIgnoreCase(serverState) || notExistIndexLun(totalNodes.get(i))) {
                    i++;
                    if (i >= totalNodes.size()) {
                        i = 0;
                    }
                } else {
                    //添加等待重试机制
                    if (pool.getShortMasterCommand(0).exists("es") == 0 && lastNode.equals(lastRunNode)) {
                        int retryCount = index - lastRun > 0 ? index - lastRun : totalNodes.size() - (lastRun - index);
                        if (RETRY.incrementAndGet() < retryCount) {
                            return false;
                        } else if (RETRY.get() == retryCount) {
                            RETRY.set(0);
                            break;
                        } else {
                            RETRY.set(0);
                        }
                    } else {
                        RETRY.set(0);
                        lastNode = lastRunNode;
                    }
                    return false;
                }
            }
            //检查本地节点是否有索引盘
            return !notExistIndexLun(UUID);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean notExistIndexLun(String node) {
        int count = 0;
        Set<String> rabbitmqLun = scanInfoMap.keySet();
        for (String s : rabbitmqLun) {
            if (RemovedDisk.getInstance().contains(UUID + "@" + s.split(File.separator)[0])) {
                scanInfoMap.remove(s);
            }
        }
        for (String lun : lunSet) {
            String diskNode = lun.split("@")[0];
            if (node.equalsIgnoreCase(diskNode)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    count++;
                }
            }
        }
        return count == 0;
    }

    private boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(15);
            String setKey = pool.getShortMasterCommand(0).set("es", UUID, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
                pool.getShortMasterCommand(0).set("es_run", UUID);
            }
            return res;
        } catch (Exception e) {
            return false;
        } finally {
            RETRY.set(0);
            lastNode = pool.getShortMasterCommand(0).get("es_run");
        }
    }

    private void unLock() {
        try (StatefulRedisConnection<String, String> tmpConnection =
                     pool.getSharedConnection(0).newMaster()) {
            RedisCommands<String, String> target = tmpConnection.sync();
            target.watch("es");
            String lockNode = target.get("es");
            if (UUID.equalsIgnoreCase(lockNode)) {
                target.multi();
                target.del("es");
                target.exec();
            }
            locked.set(false);
        } catch (Exception e) {
            locked.set(false);
        }
    }

    private void keepLock() {
        if (locked.get()) {
            SetArgs setArgs = SetArgs.Builder.xx().ex(25);
            try (StatefulRedisConnection<String, String> tmpConnection =
                         pool.getSharedConnection(0).newMaster()) {
                RedisCommands<String, String> target = tmpConnection.sync();
                target.watch("es");
                String lockNode = target.get("es");
                if (UUID.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set("es", UUID, setArgs);
                    target.exec();
                    ES_SCHEDULER.schedule(this::keepLock, 13, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                ES_SCHEDULER.schedule(this::keepLock, 13, TimeUnit.SECONDS);
            }
        }
    }

    public void setInit() {
        locked.set(true);
        lastNode = UUID;
        RETRY.set(0);
    }

    public void setEnd() {
        locked.set(false);
        RUN_COUNT.set(0);
    }

    @Data
    @Accessors(chain = true)
    @EqualsAndHashCode
    public static class Tuple4<T, R, U, S> {
        public T var1;
        public R var2;
        public U var3;
        public S var4;

        public Tuple4(T var1, R var2, U var3, S var4) {
            this.var1 = var1;
            this.var2 = var2;
            this.var3 = var3;
            this.var4 = var4;
        }

        public Tuple4() {

        }
    }

}