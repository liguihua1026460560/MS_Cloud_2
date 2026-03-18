package com.macrosan.doubleActive.deployment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.deployment.AddClusterUtils.SyncRequest;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.storage.client.ListOriginPartHandler;
import com.macrosan.storage.client.ListPartsClientHandler;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanStream;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.NEW_VERSION_ID;
import static com.macrosan.constants.ServerConstants.VERSIONID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.datasyncIsEnabled;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.putHisSyncRecord;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.part.PartClient.LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.httpserver.MossHttpClient.INDEX_IPS_ENTIRE_MAP;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.ARCHIVE_SUFFIX;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

@Log4j2
public class BucketSyncChecker {
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    /* 待处理的桶 */
    static UnicastProcessor<String> processor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
    private static UnicastProcessor<SyncRequest<MetaData>>[] bsObjProcessors = new UnicastProcessor[16];
    private static UnicastProcessor<Tuple2<String, SyncRequest<Upload>>>[] bsPartProcessors = new UnicastProcessor[8];

    static final int MAX_LIST_AMOUNT = 5000;
    static final long MAX_DEAL_DATA_SIZE = 512 * 1024 * 1024L;

    /**
     * 有几条obj同步请求被发起
     */
    static AtomicInteger dealingCopyAmount = new AtomicInteger();

    /**
     * 本节点是扫描节点，开始扫描时设为true，切扫描节点的时候设成false，用来中断所有扫描list操作
     */
    private static final AtomicBoolean has_data_syncing = new AtomicBoolean(false);

    /**
     * 节点成为扫描节点时，先从redis中读取记录的record后再开启下刷任务
     */
    private static final AtomicBoolean has_flush_record = new AtomicBoolean(false);

    /**
     * 表7中保存桶的一般对象和分段对象历史数据同步状态，0为未同步，1为已同步。
     */
    static final String obj_bucket_sync_finished = "obj_bucket_sync_finished";

    static final String part_bucket_sync_finished = "part_bucket_sync_finished";


    /**
     * 表示桶数据同步的状态，0正在同步，1表示差异记录全部下刷。
     */
    static Map<String, Integer> BUCKETS_OBJ_STATUS_MAP = new ConcurrentHashMap<>();

    static Map<String, Integer> BUCKETS_PART_STATUS_MAP = new ConcurrentHashMap<>();

    private static final TypeReference<Integer> syncFinishedType = new TypeReference<Integer>() {
    };


    /**
     * 存放本地站点站点的DeployRecord。
     */
    static DeployRecord bucketDeploy = new DeployRecord();

    /**
     * 记录需要同步的站点index
     */
    private static Set<Integer> NEED_SYNC_INDEX_SET = new HashSet<>();

    /**
     * 记录在新加站点前正在进行的桶复制任务，不会同步给新加站点
     */
    private static final Map<Integer, Set<String>> BEFORE_ADD_INDEX_BUCKET_SET = new ConcurrentHashMap<>();

    /**
     * 正在list对象的桶，listEnd结束后删除桶
     */
    static Set<String> BS_COM_SYNCING_BUCKET = new ConcurrentHashSet<>();

    static Set<String> BS_PART_SYNCING_BUCKET = new ConcurrentHashSet<>();

    private static ScheduledFuture<?> scheduledFuture1;
    private static ScheduledFuture<?> scheduledFuture2;

    /**
     * 用于list线程数量控制，表示还在list中的bucket总数
     */
    static final AtomicInteger syncingBucketNum = new AtomicInteger();

    /**
     * 桶数据同步开始前记录下所有的桶，这些桶都将进行同步。
     */
    private static final Set<String> bucketsList = new ConcurrentHashSet<>();

    static final Set<String> dealingBuckets = new ConcurrentHashSet<>();

    /**
     * 保存桶和桶第一次尝试同步时的versionNum，用来判断是否所有请求都已经落盘，已经可以开始扫描（mossserver-7364）
     */
    static final Map<String, String> bucketSyncVerMap = new ConcurrentHashMap<>();

    public static void init() {
        try {
            log.info("start bucket sync check.");
            initIndexSet();
            for (int i = 0; i < bsObjProcessors.length; i++) {
                bsObjProcessors[i] = UnicastProcessor.create(Queues.<SyncRequest<MetaData>>unboundedMultiproducer().get());
                bsObjProcessors[i].publishOn(SCAN_SCHEDULER)
                        .doOnNext(syncRequest -> {
                            AtomicInteger count = new AtomicInteger();
                            AtomicBoolean hasFalse = new AtomicBoolean();
                            Flux.fromIterable(NEED_SYNC_INDEX_SET)
                                    .flatMap(index -> {
                                        String bucket = syncRequest.payload.bucket;
                                        if (BEFORE_ADD_INDEX_BUCKET_SET.getOrDefault(index, new ConcurrentHashSet<>()).contains(bucket)) {
                                            return Mono.just(true);
                                        }
                                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, SYNC_INDEX)
                                                .defaultIfEmpty("")
                                                .flatMap(syncIndex -> {
                                                    if (StringUtils.isNotEmpty(syncIndex)) {
                                                        Map<Integer, Set<Integer>> syncMap = Json.decodeValue(syncIndex, new TypeReference<Map<Integer, Set<Integer>>>() {
                                                        });
                                                        if (syncMap.containsKey(LOCAL_CLUSTER_INDEX) && syncMap.get(LOCAL_CLUSTER_INDEX).contains(index)) {
                                                            return dealObjSync(index, syncRequest);
                                                        }
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .doOnError(log::error);
                                    })
                                    .doOnNext(b -> {
                                        if (!b) {
                                            // 只要有一个站点的差异记录写失败，返回false，该轮重新开始i扫描
                                            if (hasFalse.compareAndSet(false, true)) {
                                                syncRequest.res.onNext(false);
                                            }
                                            return;
                                        }

                                        if (count.incrementAndGet() == NEED_SYNC_INDEX_SET.size()) {
                                            syncRequest.res.onNext(true);
                                        }
                                    })
                                    .doOnError(e -> {
                                        log.error("bsObjProcessor, {}, {}", syncRequest.payload.bucket, syncRequest.payload.key, e);
                                        if (hasFalse.compareAndSet(false, true)) {
                                            syncRequest.res.onNext(false);
                                        }
                                    })
                                    .subscribe();
                        })
                        .doOnError(e -> log.error("bucket sync dealObjMeta error,", e))
                        .subscribe();
            }

            for (int i = 0; i < bsPartProcessors.length; i++) {
                bsPartProcessors[i] = UnicastProcessor.create(Queues.<Tuple2<String, SyncRequest<Upload>>>unboundedMultiproducer().get());
                bsPartProcessors[i].publishOn(SCAN_SCHEDULER)
                        .doOnNext(tuple2 -> {
                            String bucketName = tuple2.var1;
                            SyncRequest<Upload> syncRequest = tuple2.var2;
                            AtomicInteger count = new AtomicInteger();
                            AtomicBoolean hasFalse = new AtomicBoolean();
                            Flux.fromIterable(NEED_SYNC_INDEX_SET)
                                    .flatMap(index -> {
                                        if (BEFORE_ADD_INDEX_BUCKET_SET.getOrDefault(index, new ConcurrentHashSet<>()).contains(bucketName)) {
                                            return Mono.just(true);
                                        }
                                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, SYNC_INDEX)
                                                .defaultIfEmpty("")
                                                .flatMap(syncIndex -> {
                                                    if (StringUtils.isNotEmpty(syncIndex)) {
                                                        Map<Integer, Set<Integer>> syncMap = Json.decodeValue(syncIndex, new TypeReference<Map<Integer, Set<Integer>>>() {
                                                        });
                                                        if (syncMap.containsKey(LOCAL_CLUSTER_INDEX) && syncMap.get(LOCAL_CLUSTER_INDEX).contains(index)) {
                                                            return dealPartSync(index, bucketName, syncRequest);
                                                        }
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .doOnError(log::error);
                                    })
                                    .doOnNext(b -> {
                                        if (!b) {
                                            // 只要有一个站点的差异记录写失败，返回false，该轮重新开始i扫描
                                            if (hasFalse.compareAndSet(false, true)) {
                                                syncRequest.res.onNext(false);
                                            }
                                            return;
                                        }
                                        if (count.incrementAndGet() == NEED_SYNC_INDEX_SET.size()) {
                                            syncRequest.res.onNext(true);
                                        }
                                    })
                                    .doOnError(e -> {
                                        log.error("bsPartProcessors, {}, {}", bucketName, syncRequest.payload.getKey(), e);
                                        if (hasFalse.compareAndSet(false, true)) {
                                            syncRequest.res.onNext(false);
                                        }
                                    })
                                    .subscribe();
                        })
                        .doOnError(e -> log.error("bucket sync dealPart error", e))
                        .subscribe();
            }

            Optional.ofNullable(scheduledFuture1).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            scheduledFuture1 = SCAN_TIMER.scheduleAtFixedRate(BucketSyncChecker::initStart, 10, 10, TimeUnit.SECONDS);
            Optional.ofNullable(scheduledFuture2).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            scheduledFuture2 = SCAN_TIMER.scheduleAtFixedRate(BucketSyncChecker::flushCurMaker, 10, 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (isMultiAliveStarted && MainNodeSelector.checkIfSyncNode()) {
                log.error("bucket sync init failed. Retry. ", e);
                initStart();
            }
        }

    }

    public static void initIndexSet() {
        NEED_SYNC_INDEX_SET = new HashSet<>(INDEX_IPS_ENTIRE_MAP.keySet());
        NEED_SYNC_INDEX_SET.remove(LOCAL_CLUSTER_INDEX);

        for (Integer index : NEED_SYNC_INDEX_SET) {
            Set<String> bucketSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(NO_NEED_SYNC_BUCKETS + index);
            BEFORE_ADD_INDEX_BUCKET_SET.computeIfAbsent(index, i -> new ConcurrentHashSet<>()).addAll(bucketSet);
        }
        log.info("BEFORE_ADD_INDEX_BUCKET_SET init success: {}", BEFORE_ADD_INDEX_BUCKET_SET);
    }

    public static void updateIndexSet(Set<Integer> indexSet) {
        if (indexSet != null) {
            for (Integer index : indexSet) {
                if (!NEED_SYNC_INDEX_SET.contains(index)) {
                    Set<String> bucketSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(NEED_SYNC_BUCKETS);
                    BEFORE_ADD_INDEX_BUCKET_SET.computeIfAbsent(index, i -> new ConcurrentHashSet<>()).addAll(bucketSet);
                    bucketSet.forEach(b -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(NO_NEED_SYNC_BUCKETS + index, b));
                    log.info("BEFORE_ADD_INDEX_BUCKET_SET update index {} success: {}", index, BEFORE_ADD_INDEX_BUCKET_SET);
                }
            }
        }

        NEED_SYNC_INDEX_SET = new HashSet<>(INDEX_IPS_ENTIRE_MAP.keySet());
        NEED_SYNC_INDEX_SET.remove(LOCAL_CLUSTER_INDEX);
    }

    private static void initStart() {
        try {
            // 非主节点递归，预备切主。
            if (!MainNodeSelector.checkIfSyncNode()) {
                has_data_syncing.compareAndSet(true, false);
                has_flush_record.compareAndSet(true, false);
                bucketDeploy.objMarkerMap.clear();
                bucketDeploy.partMarkerMap.clear();
                bucketsList.clear();
                syncingBucketNum.set(0);
                log.debug("not master, keep going for possibly node switch.");
                return;
            }

            // 恢复中断扫描数据
            if (has_data_syncing.compareAndSet(false, true)) {
                String str = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, BUCKET_DEPLOY_RECORD);
                if (!StringUtils.isEmpty(str)) {
                    DeployRecord record = Json.decodeValue(str, new TypeReference<DeployRecord>() {
                    });
                    if (record.objMarkerMap != null || record.partMarkerMap != null) {
                        bucketDeploy = record;
                        log.info("restart bucket sync: {}", bucketDeploy);
                    }
                }
                has_flush_record.compareAndSet(false, true);
            }

            log.debug("bucketList： {}, dealingBuckets: {}, syncingBucketNum: {}", bucketsList, dealingBuckets, syncingBucketNum);
            ScanStream.sscan(pool.getReactive(REDIS_SYSINFO_INDEX), NEED_SYNC_BUCKETS)
                    .publishOn(SCAN_SCHEDULER)
                    .doOnNext(bucketName -> {
                        if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName) == 1 && datasyncIsEnabled(bucketName) && syncTimeIsEnabled(bucketName) && needSyncScan(bucketName)) {
                            synchronized (bucketsList) {
                                if (pool.getCommand(REDIS_SYSINFO_INDEX).sismember(NEED_SYNC_BUCKETS, bucketName) && bucketsList.add(bucketName)) {
                                    log.info("start bucket synchronization: {}", bucketName);
                                    checkSyncStatus(bucketName);
                                }
                            }
                        } else {
                            log.info("The bucket does not require data sync: {}", bucketName);
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(NEED_SYNC_BUCKETS, bucketName);
                        }
                    })
                    .doOnComplete(() -> log.debug("end bucket synchronization"))
                    .doOnError(e -> log.error("start bucket synchronization error", e))
                    .subscribe();

        } catch (Exception e) {
            log.error("initStart error", e);
        }
    }

    private static void checkSyncStatus(String bucket) {
        Mono.just(bucket)
                .filter(bucketName -> BUCKET_NAME_PATTERN.matcher(bucketName).matches())
                .flatMap(bucketName ->
                        AddClusterUtils.startCheckPut(bucketName, bucketSyncVerMap)
                                .flatMap(l -> AddClusterUtils.checkAllNodesPutDone(bucketName, bucketSyncVerMap)
                                        .publishOn(SCAN_SCHEDULER)
                                        .doOnNext(b -> {
                                            if (!b) {
                                                bucketsList.remove(bucket);
                                            }
                                        })
                                        .filter(b -> b)
                                        .map(b -> bucketName))
                )
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(bucketName -> {
                    pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                            .publishOn(SCAN_SCHEDULER)
                            .defaultIfEmpty(new HashMap<>())
                            .doOnNext(map -> {
                                if (map.isEmpty()) {
                                    bucketsList.remove(bucketName);
                                    return;
                                }

                                if (syncingBucketNum.incrementAndGet() > 10) {
                                    bucketsList.remove(bucketName);
                                    syncingBucketNum.decrementAndGet();
                                    return;
                                }

                                BucketSyncSwitchCache.getInstance().check(bucketName, map);
                                dealingBuckets.add(bucketName);
                                bucketSyncVerMap.remove(bucketName);
                                log.info("start bucket sync scan, bucket: {}, bucketList： {}, dealingBuckets: {}, syncingBucketNum: {}", bucketName, bucketsList, dealingBuckets, syncingBucketNum);
                                String objStr = map.get(obj_bucket_sync_finished);
                                Integer objBucketSync = objStr != null ? Json.decodeValue(objStr, syncFinishedType) : 0;
                                String timeStamp = map.get(BUCKET_SYNC_TIMESTAMP);
                                if (StringUtils.isEmpty(timeStamp)) {
                                    bucketsList.remove(bucketName);
                                    dealingBuckets.remove(bucketName);
                                    syncingBucketNum.decrementAndGet();
                                    return;
                                }

                                if (1 != objBucketSync) {
                                    BUCKETS_OBJ_STATUS_MAP.computeIfAbsent(bucketName, k -> 0);
                                    BS_COM_SYNCING_BUCKET.add(bucketName);
                                    listLifeMetaRotation(bucketName, timeStamp);
                                } else {
                                    BUCKETS_OBJ_STATUS_MAP.computeIfAbsent(bucketName, k -> 1);
                                }

                                String partStr = map.get(part_bucket_sync_finished);
                                Integer partBucketSyncMap = partStr != null ? Json.decodeValue(partStr, syncFinishedType) : 0;
                                if (1 != partBucketSyncMap) {
                                    BUCKETS_PART_STATUS_MAP.computeIfAbsent(bucketName, k -> 0);
                                    BS_PART_SYNCING_BUCKET.add(bucketName);
                                    listMultiPartInfo(bucketName, Long.parseLong(timeStamp));
                                } else {
                                    BUCKETS_PART_STATUS_MAP.computeIfAbsent(bucketName, k -> 1);
                                }

                                if (objBucketSync == 1 && partBucketSyncMap == 1) {
                                    log.info("bucket produce record success, bucket: {}, bucketList： {}, dealingBuckets: {}, syncingBucketNum: {}", bucketName, bucketsList, dealingBuckets,
                                            syncingBucketNum);
                                    syncingBucketNum.decrementAndGet();
                                }
                            })
                            .doOnError(e -> {
                                log.error("checkSyncStatus Error1, ", e);
                                bucketsList.remove(bucketName);
                                dealingBuckets.remove(bucketName);
                                syncingBucketNum.decrementAndGet();
                            })
                            .subscribe();
                })
                .doOnError(e -> log.error("checkSyncStatus Error2, ", e))
                .subscribe();
    }

    /**
     * 桶是否启用了数据同步
     */
    public static boolean syncTimeIsEnabled(String bucketName) {
        String timestamp = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_SYNC_TIMESTAMP);
        return StringUtils.isNotEmpty(timestamp);
    }

    public static boolean needSyncScan(String bucketName) {
        final String syncIndex = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, SYNC_INDEX);
        if (StringUtils.isNotEmpty(syncIndex)) {
            Map<Integer, Set<Integer>> syncMap = Json.decodeValue(syncIndex, new TypeReference<Map<Integer, Set<Integer>>>() {
            });
            return syncMap.containsKey(LOCAL_CLUSTER_INDEX) && !syncMap.get(LOCAL_CLUSTER_INDEX).isEmpty();
        }
        return false;
    }

    static final ConcurrentHashSet<String> SHARD_BUCKET_SET = new ConcurrentHashSet<>();

    /**
     * 扫描生命周期的对象元数据。该类元数据按时间排序。
     */
    private static void listLifeMetaRotation(String bucketName, String endStamp) {
        log.info("bucket {} start list object", bucketName);
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);

        AtomicInteger vnodeCount = new AtomicInteger();

        for (String bucketVnode : bucketVnodeList) {
            UnicastProcessor<Tuple3<String, String, String>> bucketVnodeProcessor = UnicastProcessor.create(Queues.<Tuple3<String, String, String>>unboundedMultiproducer().get());
            AtomicLong findNum = new AtomicLong();
            // 扫描开始的位置为redis中记录的对象，若未记录则为0，从头扫描。
            String stamp;
            String key;
            String versionId;
            int maxKey = 1000;
            String[] tempMarker = bucketDeploy.objMarkerMap.get(bucketName + "_" + bucketVnode);
            if (tempMarker != null) {
                key = tempMarker[1];
                versionId = tempMarker[2];
                stamp = tempMarker[3];
            } else {
                stamp = "0";
                key = "";
                versionId = "";
            }

            bucketVnodeProcessor
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(tuple3 ->
                            pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                    .filter(b -> {
                                        if (b > 0 || ShardingWorker.contains(bucketName)) {
                                            // 如果桶正在散列，在散列完成后从头开始扫描所有vnode。SERVER-985
                                            log.debug("has bucket sharding, {}", bucketName);
                                            synchronized (bucketDeploy) {
                                                bucketDeploy.dealingObjMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).clear();
                                                bucketDeploy.objMarkerMap.put(bucketName + "_" + bucketVnode, new String[]{bucketName, "", "", "0"});
                                                if (SHARD_BUCKET_SET.add(bucketName)) {
                                                    log.info("bucket is under sharding: {}", bucketName);
                                                }
                                            }
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(new Tuple3<>("", "", "0")));
                                            return false;
                                        } else {
                                            SHARD_BUCKET_SET.remove(bucketName);
                                        }
                                        if (!has_data_syncing.get()) {
                                            log.info("Stop obj listing. {} {}", bucketName, bucketVnode);
                                            bucketVnodeProcessor.onComplete();
                                            return false;
                                        }

                                        if (dealingCopyAmount.get() > MAX_LIST_AMOUNT) {
                                            log.debug("too many listing obj, {}", bucketName);
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(tuple3));
                                            return false;
                                        }
                                        return true;
                                    })
                                    .map(b -> tuple3)
                    )
                    .subscribe(tuple3 -> {
                        String beginPrefix;
                        AtomicBoolean interrupt = new AtomicBoolean();
                        if ("0".equals(tuple3.var3)) {
                            beginPrefix = getLifeCycleStamp(bucketVnode, bucketName, tuple3.var3);
                        } else {
                            beginPrefix = getLifeCycleMetaKey(bucketVnode, bucketName, tuple3.var1, tuple3.var2, tuple3.var3);
                        }
                        SocketReqMsg reqMsg = new SocketReqMsg("", 0);
                        reqMsg.put("bucket", bucketName)
                                .put("maxKeys", String.valueOf(maxKey))
                                // 列出的对象名不超过该stamp生成的rocksKey前缀
                                .put("stamp", endStamp)
                                .put("retryTimes", "0")
                                .put("beginPrefix", beginPrefix)
                                .put("syncHis", "1");
                        metaPool.mapToNodeInfo(bucketVnode)
                                .publishOn(SCAN_SCHEDULER)
                                .flatMap(infoList -> {
                                    String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                                    reqMsg.put("vnode", nodeArr[0]);
                                    List<SocketReqMsg> msgs = infoList.stream().map(info -> reqMsg.copy().put("lun", info.var2)).collect(Collectors.toList());
                                    ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT,
                                            new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
                                            }, infoList);
                                    ListSyncObjectClientHandler clientHandler = new ListSyncObjectClientHandler(responseInfo, nodeArr[0], reqMsg, bucketName);
                                    responseInfo.responses.publishOn(SCAN_SCHEDULER).subscribe(clientHandler::handleResponse, e -> log.error("", e),
                                            clientHandler::completeResponse);
                                    return clientHandler.res;
                                })
                                .subscribe(metaDataList -> {
                                    try {
                                        if (metaDataList.isEmpty()) {
                                            // 最后一个分片且桶下无历史对象，直接设置redis中的状态
                                            if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                                                synchronized (Objects.requireNonNull(BUCKETS_OBJ_STATUS_MAP.put(bucketName, 1))) {
                                                    Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                            .subscribe(s -> {
                                                                BucketSyncUtils.endList(bucketName, obj_bucket_sync_finished);
                                                                RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                                                        .hset(bucketName, obj_bucket_sync_finished, "1");
                                                            });
                                                }
                                            }
                                            bucketVnodeProcessor.onComplete();
                                        } else {
                                            findNum.addAndGet(metaDataList.size());
                                            log.debug("listObject: {}", findNum.get());
                                            AtomicInteger preCount = new AtomicInteger(Math.min(maxKey, metaDataList.size()));
                                            AtomicInteger dealCount = new AtomicInteger();
                                            for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                                MetaData lifeMeta = metaDataList.get(i).getMetaData();
                                                if (lifeMeta.equals(MetaData.ERROR_META) || lifeMeta.equals(MetaData.NOT_FOUND_META)) {
                                                    checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, findNum);
                                                    continue;
                                                }
                                                if (lifeMeta.deleteMark && StringUtils.isEmpty(lifeMeta.sysMetaData)) {
                                                    checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, findNum);
                                                    continue;
                                                }
                                                BucketSyncUtils.preDealObj(metaPool.getBucketVnodeId(lifeMeta.bucket, lifeMeta.key), lifeMeta);
                                                SyncRequest<MetaData> syncRquest = new SyncRequest<>();
                                                syncRquest.payload = lifeMeta;
                                                syncRquest.res = MonoProcessor.create();
                                                syncRquest.res
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .timeout(Duration.ofMinutes(5))
                                                        .doOnError(e -> log.error("bucket sync meta syncRequest res error1: {}, {}", bucketName, lifeMeta.key, e))
                                                        .doFinally(s -> {
                                                            dealingCopyAmount.decrementAndGet();
                                                            // 如果中断了这轮不移除，防止flush到的是后面的对象，此时关机重新开始扫描中间的对象将丢失
                                                            if (!interrupt.get()) {
                                                                BucketSyncUtils.finishDealObj(metaPool.getBucketVnodeId(lifeMeta.bucket, lifeMeta.key), lifeMeta);
                                                            }
                                                            syncRquest.res.dispose();
                                                            syncRquest.payload = null;
                                                        })
                                                        .subscribe(b -> {
                                                            if (!b) {
                                                                if (interrupt.compareAndSet(false, true)) {
                                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                                                }
                                                                return;
                                                            }
                                                            checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, findNum);
                                                        }, e -> {
                                                            log.error("bucket sync meta syncRequest res error2: {}, {}", bucketName, lifeMeta.key, e);
                                                            if (interrupt.compareAndSet(false, true)) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                                            }
                                                        });

                                                dealingCopyAmount.incrementAndGet();
                                                int index = ThreadLocalRandom.current().nextInt(bsObjProcessors.length);
                                                bsObjProcessors[index].onNext(syncRquest);
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error("listBucketSyncObj Error1, {} ", bucketName, e);
                                        if (interrupt.compareAndSet(false, true)) {
                                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                        }
                                    }
                                }, e -> {
                                    log.error("listBucketSyncObj Error2, {}", bucketName, e);
                                    if (interrupt.compareAndSet(false, true)) {
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                    }
                                });
                    });
            bucketVnodeProcessor.onNext(new Tuple3<>(key, versionId, stamp));
        }

    }


    public static void checkThisComplete(UnicastProcessor<Tuple3<String, String, String>> listController, String bucketName, List<LifecycleClientHandler.Counter> metaDataList,
                                         AtomicInteger preCount, AtomicInteger dealCount, String bucketVnode, AtomicInteger vnodeCount, List<String> bucketVnodeList, AtomicLong findNum) {
        if (dealCount.incrementAndGet() == preCount.get()) {
            if (metaDataList.size() > 1000) {
                MetaData metaData = metaDataList.get(1000).getMetaData();
                listController.onNext(new Tuple3<>(metaData.key, metaData.versionId, metaData.stamp));
            } else {
                // 最后一个分片且桶下无历史对象，直接设置redis中的状态
                log.info("scan all meta2 from bucket " + bucketName + "(" + bucketVnode + ") . {}", findNum.get());
                if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                    log.info("scan bucket obj end, {}", bucketName);
                    BucketSyncUtils.endList(bucketName, obj_bucket_sync_finished);
                }
                listController.onComplete();
            }
        }
    }

    private static void listMultiPartInfo(String bucketName, long endStamp) {
        log.info("bucket {} start list part", bucketName);
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);

        AtomicInteger vnodeCount = new AtomicInteger();

        for (String bucketVnode : bucketVnodeList) {
            UnicastProcessor<Tuple2<String, String>> bucketVnodeProcessor = UnicastProcessor.create(Queues.<Tuple2<String, String>>unboundedMultiproducer().get());
            int maxUploadsInt = 1000;
            bucketVnodeProcessor
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(tup2 ->
                            pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                    .filter(b -> {
                                        if (b > 0 || ShardingWorker.contains(bucketName)) {
                                            // 如果桶正在散列，在散列完成后从头开始扫描所有vnode。SERVER-985
                                            log.debug("has bucket sharding, {}", bucketName);
                                            synchronized (bucketDeploy) {
                                                bucketDeploy.dealingPartMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).clear();
                                                bucketDeploy.partMarkerMap.put(bucketName + "_" + bucketVnode, "");
                                                if (SHARD_BUCKET_SET.add(bucketName)) {
                                                    log.info("bucket is under sharding: {}", bucketName);
                                                }
                                            }
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(new Tuple2<>("", "")));
                                            return false;
                                        } else {
                                            SHARD_BUCKET_SET.remove(bucketName);
                                        }
                                        if (!has_data_syncing.get()) {
                                            log.info("Stop partMulti listing. {} {}", bucketName, bucketVnode);
                                            bucketVnodeProcessor.onComplete();
                                            return false;
                                        }

                                        if (dealingCopyAmount.get() > MAX_LIST_AMOUNT) {
                                            log.debug("too many listing obj, {}", bucketName);
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(tup2));
                                            return false;
                                        }
                                        return true;
                                    })
                                    .map(b -> tup2)
                    )
                    .subscribe(tup2 -> {
                        AtomicBoolean interrupt = new AtomicBoolean();
                        pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                                .defaultIfEmpty(new HashMap<>(0))
                                .flatMap(bucketInfo ->
                                        pool.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                                                .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                                                .zipWith(metaPool.mapToNodeInfo(bucketVnode))
                                )
                                .flatMap(tuple2 -> {
                                    MonoProcessor<ListMultipartUploadsResult> res = MonoProcessor.create();
                                    ListMultipartUploadsResult listMultiUploadsRes = new ListMultipartUploadsResult()
                                            .setBucket(bucketName)
                                            .setMaxUploads(maxUploadsInt)
                                            .setKeyMarker(tup2.var1);

                                    SocketReqMsg reMsg = new SocketReqMsg("", 0)
                                            .put("vnode", tuple2.getT2().get(0).var3)
                                            .put("bucket", bucketName)
                                            .put("maxUploads", String.valueOf(maxUploadsInt))
                                            .put("prefix", "")
                                            .put("marker", tup2.var1)
                                            .put("delimiter", "")
                                            .put("uploadIdMarker", tup2.var2);

                                    List<SocketReqMsg> msgs = tuple2.getT2().stream()
                                            .map(tuple -> reMsg.copy().put("lun", tuple.var2))
                                            .collect(Collectors.toList());

                                    ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo =
                                            ClientTemplate.oneResponse(msgs, LIST_MULTI_PART_UPLOAD, LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE, tuple2.getT2());

                                    ListOriginPartHandler handler = new ListOriginPartHandler(tuple2.getT1(), listMultiUploadsRes, responseInfo, tuple2.getT2(), null, bucketName);

                                    responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);

                                    handler.res.publishOn(SCAN_SCHEDULER).doOnNext(b -> {
                                        if (!b) {
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(s -> bucketVnodeProcessor.onNext(tup2));
                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "List MultiPart Uploads fail");
                                        }
                                    }).subscribe(b -> res.onNext(listMultiUploadsRes));
                                    return res;
                                })
                                .subscribe(listMultiUploadsRes -> {
                                    if (listMultiUploadsRes.getUploads().isEmpty()) {
                                        synchronized (Objects.requireNonNull(BUCKETS_PART_STATUS_MAP.put(bucketName, 1))) {
                                            Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                    .subscribe(s -> {
                                                        BucketSyncUtils.endList(bucketName, part_bucket_sync_finished);
                                                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                                                .hset(bucketName, part_bucket_sync_finished, Json.encode(1));
                                                    });
                                        }
                                        bucketVnodeProcessor.onComplete();
                                    } else {
                                        AtomicInteger preCount = new AtomicInteger(listMultiUploadsRes.getUploads().size());
                                        AtomicInteger dealCount = new AtomicInteger();

                                        for (Upload upload : listMultiUploadsRes.getUploads()) {
                                            if (interrupt.get()) {
                                                break;
                                            }
                                            if (MsDateUtils.dateToStamp(upload.getInitiated()) < endStamp) {
                                                BucketSyncUtils.preDealPartList(bucketName, bucketVnode, upload);
                                                SyncRequest<Upload> syncRequest = new SyncRequest<>();
                                                syncRequest.payload = upload;
                                                syncRequest.res = MonoProcessor.create();
                                                syncRequest.res
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .timeout(Duration.ofMinutes(10))
                                                        .doOnError(e -> log.error("bucket sync part syncRequest res error1: ", e))
                                                        .doFinally(s -> {
                                                            if (!interrupt.get()) {
                                                                BucketSyncUtils.finishDealPartList(bucketName, bucketVnode, upload);
                                                            }
                                                            syncRequest.res.dispose();
                                                            syncRequest.payload = null;
                                                        })
                                                        .subscribe(res -> {
                                                            if (!res) {
                                                                if (interrupt.compareAndSet(false, true)) {
                                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                                                }
                                                                return;
                                                            }
                                                            checkMultiPartListComplete(bucketName, bucketVnodeProcessor, listMultiUploadsRes, preCount, dealCount, bucketVnode, vnodeCount,
                                                                    bucketVnodeList);

                                                        }, e -> {
                                                            log.error("bucket sync part syncRequest res error2: ", e);
                                                            if (interrupt.compareAndSet(false, true)) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                                            }
                                                        });
                                                int index = ThreadLocalRandom.current().nextInt(bsPartProcessors.length);
                                                bsPartProcessors[index].onNext(new Tuple2<>(bucketName, syncRequest));
                                            } else {
                                                checkMultiPartListComplete(bucketName, bucketVnodeProcessor, listMultiUploadsRes, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList);
                                            }
                                        }
                                    }
                                }, e -> {
                                    log.error("listMultiPartInfo1 bucketName:{}", bucketName, e);
                                    if (interrupt.compareAndSet(false, true)) {
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                    }
                                });
                    }, e -> log.error("listBucketSyncPart Error, {}", bucketName, e));

            String key = bucketDeploy.partMarkerMap.get(bucketName);
            bucketVnodeProcessor.onNext(new Tuple2<>(key == null ? "" : key, ""));
        }
    }

    public static void checkMultiPartListComplete(String bucketName, UnicastProcessor<Tuple2<String, String>> listController, ListMultipartUploadsResult listMultiUploadsRes,
                                                  AtomicInteger preCount, AtomicInteger dealCount, String bucketVnode, AtomicInteger vnodeCount, List<String> bucketVnodeList) {
        if (dealCount.incrementAndGet() == preCount.get()) {
            if (!listMultiUploadsRes.isTruncated()) {
                log.info("scan all multiPart2 from bucket " + bucketName + "(" + bucketVnode + ") . {}");
                if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                    log.info("scan bucket multiPart end, {} ", bucketName);
                    BucketSyncUtils.endList(bucketName, part_bucket_sync_finished);
                }
                listController.onComplete();
            } else {
                String nextMarker = listMultiUploadsRes.getNextKeyMarker();
                String nextUploadIdMarker = listMultiUploadsRes.getNextUploadIdMarker();
                listController.onNext(new Tuple2<>(nextMarker, nextUploadIdMarker));
            }
        }
    }

    private static void flushCurMaker() {
        try {
            if (!MainNodeSelector.checkIfSyncNode()) {
                has_data_syncing.compareAndSet(true, false);
                log.debug("Stop bucket sync, turn to another node. ");
                return;
            }

            if (!has_flush_record.get()) {
                log.info("bucketDeploy not init. ");
                return;
            }

            log.debug("flushCurMakerPeriodicly. bucket sync dealingCopyAmount: {}", dealingCopyAmount.get());
            DeployRecord deployRecord = bucketDeploy;
            Set<String> isSyncBucket = new ConcurrentHashSet<>();
            synchronized (deployRecord) {
                deployRecord.dealingObjMap.forEach((key, rocksKeyMetaMap) -> {
                    String bucket = key.split("_")[0];
                    rocksKeyMetaMap.entrySet().stream().findFirst().ifPresent(entry -> {
                        MetaData metaData = entry.getValue();
                        deployRecord.objMarkerMap.put(key, new String[]{bucket, metaData.key, metaData.versionId, metaData.stamp});
                    });
                });

                deployRecord.dealingPartMap.forEach((key, map) -> {
                    map.entrySet().stream().findFirst().ifPresent(entry -> {
                        deployRecord.partMarkerMap.put(key, entry.getValue());
                    });
                });
            }
            BUCKETS_OBJ_STATUS_MAP.forEach((bucket, i) -> {
                if (i == 1 && !SHARD_BUCKET_SET.contains(bucket)) {
                    isSyncBucket.add(bucket);
                }
            });

            BUCKETS_PART_STATUS_MAP.forEach((bucket, i) -> {
                if (i == 1 && !SHARD_BUCKET_SET.contains(bucket)) {
                    if (!isSyncBucket.add(bucket)) {
                        log.info("bucket {} sync record flushed.", bucket);
                        Mono.just("1").publishOn(SCAN_SCHEDULER)
                                .subscribe(l -> {
                                    synchronized (bucketsList) {
                                        DataSynChecker.updateBucSyncState(bucket);
                                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(NEED_SYNC_BUCKETS, bucket);
                                        BUCKETS_PART_STATUS_MAP.remove(bucket);
                                        BUCKETS_OBJ_STATUS_MAP.remove(bucket);
                                        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucket);
                                        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucket);
                                        for (String vnode : bucketVnodeList) {
                                            deployRecord.objMarkerMap.remove(bucket + "_" + vnode);
                                            deployRecord.partMarkerMap.remove(bucket + "_" + vnode);
                                            deployRecord.dealingObjMap.remove(bucket + "_" + vnode);
                                            deployRecord.dealingPartMap.remove(bucket + "_" + vnode);
                                        }
                                        bucketsList.remove(bucket);
                                        dealingBuckets.remove(bucket);
                                        NEED_SYNC_INDEX_SET.forEach(index -> {
                                            if (BEFORE_ADD_INDEX_BUCKET_SET.getOrDefault(index, new ConcurrentHashSet<>()).contains(bucket)) {
                                                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(NO_NEED_SYNC_BUCKETS + index, bucket);
                                                BEFORE_ADD_INDEX_BUCKET_SET.getOrDefault(index, new ConcurrentHashSet<>()).remove(bucket);
                                            }
                                        });
                                    }
                                });

                    }
                }
            });

            // 记录当前处理记录状态
            Mono.just("1").publishOn(SCAN_SCHEDULER)
                    .subscribe(l -> {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, BUCKET_DEPLOY_RECORD, Json.encode(bucketDeploy));
                    });

            log.debug("bucket sync dealingCopyAmount: {}", dealingCopyAmount.get());
        } catch (Exception e) {
            log.error("flushCurMaker error. ", e);
        }
    }

    /**
     * 发送数据同步的http请求
     *
     * @param clusterIndex 需要同步的站点
     */
    protected static Mono<Boolean> dealObjSync(int clusterIndex, SyncRequest<MetaData> syncRequest) {
        MetaData lifeMeta = syncRequest.payload;
        try {
//        log.info("index {},object {}", clusterIndex, lifeMeta.key);
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(lifeMeta.bucket);
            UnSynchronizedRecord record;
            if (lifeMeta.deleteMarker) {
                record = AddClusterUtils.buildSyncRecord(clusterIndex, HttpMethod.DELETE, lifeMeta);
                record.headers.put(NEW_VERSION_ID, lifeMeta.versionId);
                record.versionId = null;
                record.headers.remove(VERSIONID);
            } else {
                record = AddClusterUtils.buildSyncRecord(clusterIndex, HttpMethod.PUT, lifeMeta);
                record.setUri(record.uri + "?syncHistory");
            }
            String bucketVnode = metaPool.getBucketVnodeId(lifeMeta.bucket);

//        BucketSyncUtils.preDealObj(metaPool.getBucketVnodeId(lifeMeta.bucket, lifeMeta.key), lifeMeta);
            //生成记录，具体是一般对象还是分段对象去DataSynChecker处理
            return metaPool.mapToNodeInfo(bucketVnode)
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(nodeList -> putHisSyncRecord(record, nodeList))
                    .doOnError(e -> log.error("deal bucket objMeta put record error, ", e))
                    .onErrorResume(b -> Mono.just(false));
        } catch (Exception e) {
            log.error("dealObjSync err, {}, {},", lifeMeta.bucket, lifeMeta.key, e);
            return Mono.just(false);
        }
    }

    protected static Mono<Boolean> dealPartSync(int clusterIndex, String bucketName, SyncRequest<Upload> syncRequest) {
        Upload upload = syncRequest.payload;
        try {
            UnSynchronizedRecord partInitSyncRecord = AddClusterUtils.buildPartSyncRecord(clusterIndex, bucketName, upload, null);
            StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnodeId = pool.getBucketVnodeId(bucketName);
            return pool.mapToNodeInfo(bucketVnodeId)
                    .publishOn(SCAN_SCHEDULER)
//                .doOnNext(vnodeList -> BucketSyncUtils.preDealPartList(bucketObjectVnodeId, bucketName, upload))
                    .flatMap(bucketVnodeList -> putHisSyncRecord(partInitSyncRecord, bucketVnodeList))
                    .doOnError(e -> log.error("dealUpload put record error, ", e))
                    .onErrorResume(b -> Mono.just(false))
                    .flatMap(b -> {
                        if (!b) {
                            syncRequest.res.onNext(false);
                            return Mono.just(false);
                        }
                        return listParts(clusterIndex, bucketName, syncRequest);
                    });
        } catch (Exception e) {
            log.error("dealPartSync err, {}, {},", bucketName, upload.getKey(), e);
            return Mono.just(false);
        }
    }

    private static Mono<Boolean> listParts(int clusterIndex, String bucketName, SyncRequest<Upload> syncRequest) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Upload upload = syncRequest.payload;
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, upload.getKey());

        UnicastProcessor<Integer> listController = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        listController
                .subscribe(marker -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                        .flatMap(bucketInfo ->
                                pool.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                                        .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                                        .zipWith(storagePool.mapToNodeInfo(bucketVnode))
                        )
                        .subscribe(tuple2 -> {
                            AtomicBoolean interrupt = new AtomicBoolean();
                            if (!has_data_syncing.get()) {
                                log.info("Stop part listing.");
                                listController.onComplete();
                            }

                            ListPartsResult listPartsResult = new ListPartsResult()
                                    .setBucket(bucketName)
                                    .setMaxParts(1000)
                                    .setKey(upload.getKey())
                                    .setUploadId(upload.getUploadId())
                                    .setPartNumberMarker(marker);

                            SocketReqMsg msg = new SocketReqMsg("", 0)
                                    .put("vnode", tuple2.getT2().get(0).var3)
                                    .put("bucket", listPartsResult.getBucket())
                                    .put("uploadId", listPartsResult.getUploadId())
                                    .put("object", listPartsResult.getKey())
                                    .put("maxParts", String.valueOf(listPartsResult.getMaxParts()))
                                    .put("partNumberMarker", String.valueOf(listPartsResult.getPartNumberMarker()));

                            List<SocketReqMsg> msgs = tuple2.getT2().stream()
                                    .map(tuple -> msg.copy().put("lun", tuple.var2))
                                    .collect(Collectors.toList());

                            ClientTemplate.ResponseInfo<PartInfo[]> responseInfo =
                                    ClientTemplate.oneResponse(msgs, LIST_PART, PartInfo[].class, tuple2.getT2());

                            ListPartsClientHandler handler = new ListPartsClientHandler(listPartsResult, responseInfo, tuple2.getT2(), bucketName);
                            responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);

                            handler.res.doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "list part fail");
                                }
                            }).subscribe(b -> {
                                // 该分段任务下无分段，直接返回结果
                                if (listPartsResult.getParts().isEmpty()) {
                                    res.onNext(true);
                                    listController.onComplete();
                                    return;
                                }
                                AtomicInteger dealCount = new AtomicInteger();
                                AtomicInteger preCount = new AtomicInteger(listPartsResult.getParts().size());
                                AtomicBoolean errorFlag = new AtomicBoolean(false);
                                for (Part part : listPartsResult.getParts()) {
                                    if (errorFlag.compareAndSet(true, false)) {
                                        return;
                                    }
                                    dealingCopyAmount.incrementAndGet();
                                    PartClient.getPartInfo(bucketName, upload.getKey(), upload.getUploadId(), String.valueOf(part.getPartNumber()))
                                            .publishOn(SCAN_SCHEDULER)
                                            .doFinally(s -> dealingCopyAmount.decrementAndGet())
                                            .subscribe(partInfo -> {
                                                if (partInfo.equals(PartInfo.NOT_FOUND_PART_INFO) || partInfo.delete || partInfo.equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO)) {
                                                    log.info("no partInfo: {}", partInfo);
                                                    if (dealCount.incrementAndGet() == preCount.get()) {
                                                        if (!listPartsResult.isTruncated()) {
                                                            res.onNext(true);
                                                            listController.onComplete();
                                                        } else {
                                                            listController.onNext(listPartsResult.getNextPartNumberMarker());
                                                        }
                                                    }
                                                    return;
                                                }
                                                if (partInfo.equals(PartInfo.ERROR_PART_INFO)) {
                                                    log.info("error partInfo: {}", partInfo);
                                                    errorFlag.set(true);
                                                    res.onNext(false);
                                                    listController.onComplete();
                                                    return;
                                                }
                                                UnSynchronizedRecord record = AddClusterUtils.buildPartSyncRecord(clusterIndex, bucketName, upload, partInfo);
                                                storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(bucketName))
                                                        .flatMap(nodeList -> putHisSyncRecord(record, nodeList))
                                                        .doOnNext(s -> {
                                                            if (!s) {
                                                                if (interrupt.compareAndSet(false, true)) {
                                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                                                }
                                                                return;
                                                            }
                                                            // 每轮partList结束才继续下一轮
                                                            // 该分段任务下所有的碎片都写入差异记录完毕，才认为该分段任务处理完毕，res.onNext执行
                                                            if (dealCount.incrementAndGet() == preCount.get()) {
                                                                if (!listPartsResult.isTruncated()) {
                                                                    res.onNext(true);
                                                                    listController.onComplete();
                                                                } else {
                                                                    listController.onNext(listPartsResult.getNextPartNumberMarker());
                                                                }
                                                            }
                                                        })
                                                        .doOnError(e -> {
                                                            log.error("listParts put record error, {}, {}", bucketName, upload.getKey(), e);
                                                            if (interrupt.compareAndSet(false, true)) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                                            }
                                                        })
                                                        .subscribe();
                                            });
                                }
                            }, e -> {
                                log.error("listPart1 bucketName: {}", bucketName, e);
                                if (interrupt.compareAndSet(false, true)) {
                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                }
                            });
                        }, e -> {
                            log.error("listPart2 bucketName: {}", bucketName, e);
                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                        }));

        listController.onNext(0);
        return res;
    }

}
