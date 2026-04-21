package com.macrosan.doubleActive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.archive.ArchiveAnalyzer;
import com.macrosan.doubleActive.deployment.AddClusterUtils;
import com.macrosan.doubleActive.deployment.BucketSyncUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.part.PartClient;
import com.macrosan.filesystem.async.FSUnsyncRecordHandler;
import com.macrosan.filesystem.async.SyncRecordListCache;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.CompleteMultipartUpload;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.message.xmlmsg.worm.Retention;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListSyncRecorderHandler;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.perf.BucketPerfLimiter;
import com.macrosan.utils.perf.DataSyncPerfLimiter;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.ScanStream;
import io.netty.channel.ConnectTimeoutException;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.action.core.BaseService.isCurrentTimeWithinRange;
import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.DATA_SYNC_STATE.*;
import static com.macrosan.doubleActive.DataSyncHandler.isSynced;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.HeartBeatChecker.*;
import static com.macrosan.doubleActive.SyncRecordLimiter.*;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.doubleActive.archive.ArchiveAnalyzer.ARCHIVE_ANALYZER_KEY;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.index_his_sync;
import static com.macrosan.ec.VersionUtil.getVersionNum;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.HAVA_SYNC_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_SYNC_RECORD;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.PartInfo.ERROR_PART_INFO;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.SIGNAL_RECORD;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.isSignal;
import static com.macrosan.storage.client.ListSyncRecorderHandler.MAX_COUNT;
import static com.macrosan.utils.lifecycle.LifecycleUtils.getLifecycleEndStamp;
import static com.macrosan.utils.lifecycle.LifecycleUtils.getLifecycleStartStamp;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;
import static com.macrosan.utils.worm.WormUtils.EXPIRATION;
import static com.macrosan.utils.worm.WormUtils.SYNC_WORM_EXPIRE;

/**
 * @auther wuhaizhong
 * @date 2021/4/6
 */
@Log4j2
public class DataSynChecker {
    private static final Logger delObjLogger = LogManager.getLogger("DeleteObjLog.StreamService");

    private static final RedisConnPool pool = RedisConnPool.getInstance();

    public static DATA_SYNC_STATE currSyncSate = SYNCED;

    public static Map<Integer, DATA_SYNC_STATE> currClusterSyncMap = new ConcurrentHashMap<>();

    private static final AtomicBoolean IS_REMOVE_BUCKET_CHECKING = new AtomicBoolean();

    /**
     * 记录同步桶同步状态映射。桶状态被更新时会加入该集合。
     */
    public static final Map<Integer, Map<String, DATA_SYNC_STATE>> SYNC_BUCKET_STATE_MAP = new ConcurrentHashMap<>(20);

    /**
     * 正常数据同步检查时间2min
     */
    private static final int NORMAL_SYNC_INTERVAL = 10_000;

    /**
     * 同步失败同步时间10min
     */
    private static final int INTERUP_SYNC_INTERVAL = 30_000;

    /**
     * 记录待同步桶与同步失败对象映射 <index, <bucketName, objectName>>
     */
    private static final Map<Integer, Map<String, Set<String>>> SYNC_BUCKET_ERROR_MAP = new ConcurrentHashMap<>(30);

    // 用来判断是否要开始扫描
    private static final Map<Integer, Set<String>> SYNCING_BUCKET_MAP = new ConcurrentHashMap<>(30);

    // 用来判断是否已经有线程在扫描
    private static final Map<Integer, Set<String>> SCANNING_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<Integer, Set<String>> DELETE_DEAL_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<Integer, Set<String>> DELETE_SOURCE_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<Integer, String> EXTRA_TAPE_LIBRARY_TOKEN_MAP = new ConcurrentHashMap<>(30);

    public enum DATA_SYNC_STATE {
        /**
         * 同步中
         */
        SYNCING(3),
        /**
         * 已同步
         */
        SYNCED(1),
        /**
         * 同步中断
         */
        INTERRUPTED(4),
        /**
         * 同步暂停
         */
        SUSPEND(2);

        int level;

        DATA_SYNC_STATE(int value) {
            this.level = value;
        }

        public int compareLevel(DATA_SYNC_STATE other) {
            Objects.requireNonNull(other);
            return this.level - other.level;
        }
    }


    private static DataSynChecker instance = null;

    public static DataSynChecker getInstance() {
        if (instance == null) {
            instance = new DataSynChecker();
        }
        return instance;
    }

    public static final int PROCESSOR_NUM = 48;

    /**
     * 用于订阅待处理记录。记录会根据桶名和对象名生成的hash分配到制定线程上。
     */
    public static UnicastProcessor<SyncRquest>[] recordProcessors;

    public static final Scheduler SCAN_SCHEDULER;

    public static final ScheduledThreadPoolExecutor SCAN_TIMER = new ScheduledThreadPoolExecutor(PROC_NUM, runnable -> new Thread(runnable, "scan-timer"));

    /**
     * 分布式异步复制请求发送所使用的流
     */
    static Map<String, UnicastProcessor<SyncRquest>[]> syncReqProcessors = new ConcurrentHashMap<>();

    /**
     * 异步复制请求一次往request中写入的bytes长度。
     */
    private static final int arrayLength = 128 * 1024;

    private static Map<String, ScheduledFuture[]> signalRecordSchedules = new HashMap<>();

    static {
        recordProcessors = new UnicastProcessor[PROCESSOR_NUM];

        Scheduler scheduler = null;
        try {
            ThreadFactory DISK_THREAD_FACTORY = new MsThreadFactory("sync-scan");
            MsExecutor executor = new MsExecutor(PROC_NUM * 4, 16, DISK_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("static ", e);
        }
        SCAN_SCHEDULER = scheduler;
    }

    private static final int SYNC_DISTRI_PROCESS_AMOUNT = 4;

    public void init() {
//        if (IS_ASYNC_CLUSTER) {
//            if (allAsyncComplete()){
//                return;
//            }
//        }

        log.info("DataSynChecker start. ");
        Map<Integer, Integer> clusterStatusMap = new HashMap<>();
        INDEX_IPS_ENTIRE_MAP.forEach((index, ips) -> {
            if (index.equals(LOCAL_CLUSTER_INDEX)) {
                for (String ip : ips) {
                    if (!ip.equals(LOCAL_NODE_IP)) {
                        syncReqProcessors.put(ip, new UnicastProcessor[SYNC_DISTRI_PROCESS_AMOUNT]);
                    }
                }
            }
            currClusterSyncMap.put(index, SYNCED);
            clusterStatusMap.put(index, 1);
            SYNC_BUCKET_STATE_MAP.put(index, new ConcurrentHashMap<>());
        });

        // 获取待同步的桶
        Set<String> set = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(SYNC_BUCKET_SET);
        if (set.size() == 0) {
            currClusterSyncMap.keySet().forEach(index -> currClusterSyncMap.put(index, SYNCED));
            Mono.just(SCAN_SCHEDULER).publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE, Json.encode(currClusterSyncMap)));
        }

        // 初始化redis中的clusters_status、clusters_sync_state。
        String hget = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS);
        if (StringUtils.isBlank(hget)) {
            Mono.just(SCAN_SCHEDULER).publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(clusterStatusMap)));
        }

        String hget1 = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE);
        if (StringUtils.isBlank(hget1)) {
            String oldSyncState = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, SYNC_STATE)).orElse("SYNCED");
            currClusterSyncMap.keySet().forEach(index -> currClusterSyncMap.put(index, DATA_SYNC_STATE.valueOf(oldSyncState)));
            Mono.just(SCAN_SCHEDULER).publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE, Json.encode(currClusterSyncMap)));
        } else {
            Map<Integer, DATA_SYNC_STATE> map = Json.decodeValue(hget1, new TypeReference<Map<Integer, DATA_SYNC_STATE>>() {
            });
            currClusterSyncMap.putAll(map);
        }

        // 将每个桶状态设置为oldSyncState
        set.forEach(bucket -> {
            INDEX_IPS_ENTIRE_MAP.keySet().forEach(index -> {
                if (index.equals(LOCAL_CLUSTER_INDEX)) {
                    updateBucSyncState(index, bucket, SYNCED);
                    return;
                }
                SYNC_BUCKET_ERROR_MAP.computeIfAbsent(index, i -> new ConcurrentHashMap<>()).computeIfAbsent(bucket, b -> new ConcurrentHashSet<>());
                updateBucSyncState(index, bucket, currClusterSyncMap.getOrDefault(index, SYNCED));
            });
        });

        // 初始化recordProcessors订阅每条请求，用于处理修复记录。
        for (int i = 0; i < recordProcessors.length; i++) {
            //  ipList修改时会重新初始化。如果该订阅已经开了，跳过
            if (recordProcessors[i] != null) {
                continue;
            }
            recordProcessors[i] = UnicastProcessor.create(Queues.<SyncRquest>unboundedMultiproducer().get());
            recordProcessors[i].publishOn(SCAN_SCHEDULER).subscribe(request -> {
                try {
                    UnSynchronizedRecord record = request.record;
                    // 归档理论上不会生成同名对象的record，delSameObj必为false
                    boolean delSameObj = record.headers.containsKey("del_same") && !record.headers.containsKey(ARCHIVE_ANALYZER_KEY);
                    boolean delSource = StringUtils.isNotEmpty(request.deleteSource) && !SWITCH_OFF.equals(request.deleteSource);
                    boolean switchClose = StringUtils.isNotEmpty(request.bucketSwitch) && SWITCH_CLOSED.equals(request.bucketSwitch);
                    final boolean[] needArchiveCount = {record.headers.containsKey(ARCHIVE_ANALYZER_KEY) && !record.headers.containsKey("overWriteFlag")};
                    String analyzerKey = record.headers.get(ARCHIVE_ANALYZER_KEY);
//                    record.headers.remove(ARCHIVE_ANALYZER_KEY);
                    Function<Void, Boolean> processRecordHandler = (vi) -> {
                        Mono.just(switchClose)
                                .flatMap(b -> {
                                    if (!b && !delSameObj) {
                                        return dealRecord(record).flatMap(res -> {
                                            if (delSource && res) {
                                                UnSynchronizedRecord.Type type = record.type();
                                                switch (type) {
                                                    case ERROR_PUT_OBJECT:
                                                    case ERROR_PUT_OBJECT_VERSION:
                                                    case ERROR_SYNC_HIS_OBJECT:
                                                    case ERROR_COMPLETE_PART:
                                                        if (record.headers.containsKey("overWriteFlag")) {
                                                            return Mono.just(true);
                                                        }
                                                        return deleteSourceData(record);
                                                    default:
                                                        return Mono.just(true);
                                                }
                                            } else {
                                                return Mono.just(res);
                                            }
                                        }).doFinally(f -> {
                                            if (record.headers.containsKey("overWriteFlag")) {
                                                needArchiveCount[0] = false;
                                            }
                                            record.headers.remove("overWriteFlag");
                                        });
                                    }
                                    return Mono.just(true);
                                })
                                .publishOn(SCAN_SCHEDULER)
                                .timeout(Duration.ofMinutes(timeoutMinute))
                                .flatMap(b -> {
                                    if (switchClose || delSameObj) {
                                        delObjLogger.info("delete key {} {}", record.rocksKey(), record.commited);
                                        return deleteUnsyncRecordMono(record.bucket, record.rocksKey(), null, "null")
                                                .map(deleted -> b);
                                    } else {
                                        if (b) {
                                            if (needArchiveCount[0]) {
                                                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                                                String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
                                                return storagePool.mapToNodeInfo(bucketVnode)
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .flatMap(nodeList -> ArchiveAnalyzer.deleteAndCount(record, analyzerKey, nodeList));
                                            }
                                            //删除record
                                            if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(record.index) && !record.headers.containsKey("deleteSource")) {
                                                Set<Integer> successSet = record.successIndexSet != null ? record.successIndexSet : new HashSet<>();
                                                successSet.add(getOtherSiteIndex());
                                                record.setSuccessIndexSet(successSet);
                                                return deleteUnsyncRecordMono(record.bucket, record.rocksKey(), record, UPDATE_ASYNC_RECORD)
                                                        .map(deleted -> b);
                                            } else {
                                                return deleteUnsyncRecordMono(record.bucket, record.rocksKey(), null, "null")
                                                        .map(deleted -> b);
                                            }
                                        } else {
                                            SYNC_BUCKET_ERROR_MAP.computeIfAbsent(record.index, k -> new ConcurrentHashMap<>())
                                                    .computeIfAbsent(record.bucket, k -> new ConcurrentHashSet<>()).add(record.object);
                                            return Mono.just(b);
                                        }
                                    }
                                })
                                .doFinally(s -> {
                                    //只处理本地的record推送。
                                    if (!record.headers.containsKey("Sync-Distributed")) {
//                                        releaseSyncingPayload(record);
                                    }
                                })
                                .subscribe(request.res::onNext, e -> {

                                    boolean b = EXTRA_AK_SK_MAP.get(record.bucket) == null && EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index);
                                    if (!b) {
                                        log.error("deal record fail ,bucket {} obj {} type {} {}", record.bucket, record.object, record.type(), e);
                                    }
                                    if (e instanceof NullPointerException && !b) {
                                        for (StackTraceElement s : e.getStackTrace()) {
                                            log.error("null pointer error, {}", s.toString());
                                        }
                                    }
                                    request.res.onNext(false);
                                    SYNC_BUCKET_ERROR_MAP.computeIfAbsent(record.index, k -> new ConcurrentHashMap<>())
                                            .computeIfAbsent(record.bucket, k -> new ConcurrentHashSet<>()).add(record.object);
//                                runningMap.get(record.bucket).remove(request.key);
                                });
                        return true;
                    };

                    // 异步复制吞吐量进行限制
                    pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(SYNC_QOS_RULE)
                            .flatMap(map -> {
                                if (isCurrentTimeWithinRange(map.get(START_TIME), map.get(END_TIME))) {
                                    return DataSyncPerfLimiter.getInstance().limits(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA, 1L)
                                            .flatMap(globalWaitTime -> DataSyncPerfLimiter.getInstance()
                                                    .limits(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA + "_" + record.index, 1L)
                                                    .map(waitTime -> globalWaitTime + waitTime))
                                            .flatMap(globalWaitTime -> BucketPerfLimiter.getInstance()
                                                    .limits(record.bucket, DATASYNC_THROUGHPUT_QUOTA, 1L)
                                                    .map(waitTime -> globalWaitTime + waitTime))
                                            .flatMap(globalWaitTime -> BucketPerfLimiter.getInstance()
                                                    .limits(record.bucket, DATASYNC_THROUGHPUT_QUOTA + "_" + record.index, 1L)
                                                    .map(waitTime -> globalWaitTime + waitTime));
                                } else {
                                    return Mono.just(0L);
                                }
                            })
                            .subscribe(waitMillis -> {
                                if (waitMillis == 0) {
                                    processRecordHandler.apply(null);
                                } else {
                                    Mono.delay(Duration.ofMillis(waitMillis)).subscribe(b -> processRecordHandler.apply(null));
                                }
                            });

                } catch (Exception e) {
                    request.res.onNext(false);
                    log.error("recorProcessor err1", e);
                }
            }, e -> {
                log.error("recorProcessor err2 ", e);
            });
        }

        // 分布异步复制请求发送
        for (Map.Entry<String, UnicastProcessor<SyncRquest>[]> entry : syncReqProcessors.entrySet()) {
            String ip = entry.getKey();
            UnicastProcessor<SyncRquest>[] processors = entry.getValue();
            for (int i = 0; i < processors.length; i++) {
                // 如果本地原有的ip被更改，将原先ip下的signal定时器全关闭。record分发的订阅不会关。
                // 如果强行关闭会造成正在进行的allocSyncRequest异常。目前认为不关对内存没有影响，除非在不重启前端包的前提下改成百上千次ip。
                if (!Arrays.asList(INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX)).contains(ip)) {
                    log.info("close signal scheduler under ip {}", ip);
                    for (ScheduledFuture scheduledFuture : signalRecordSchedules.get(ip)) {
                        try {
                            scheduledFuture.cancel(true);
                        } catch (Exception e) {
                        }
                    }
                    continue;
                }

                // 如果该ip下的索引为i的订阅已经开了，跳过
                if (processors[i] != null) {
                    continue;
                }
                int finalI = i;
                processors[i] = UnicastProcessor.create(Queues.<SyncRquest>unboundedMultiproducer().get());
                List<SyncRquest> syncRequestList = new LinkedList<>();
                AtomicLong dataSizeSum = new AtomicLong();
                AtomicLong loopStartTime = new AtomicLong();

                UnicastProcessor<LinkedList<SyncRquest>> sendProcessor = UnicastProcessor.create(Queues.<LinkedList<SyncRquest>>unboundedMultiproducer().get());

                processors[i].publishOn(SCAN_SCHEDULER).subscribe(syncRquest -> {
                    try {
                        if (!isSignal(syncRquest.record)) {
                            syncRequestList.add(syncRquest);
                            long objSize = Long.parseLong(syncRquest.record.headers.getOrDefault(CONTENT_LENGTH, "0"));
                            dataSizeSum.addAndGet(objSize);
                        }

                        SyncRquest request = processors[finalI].poll();
                        while (null != request) {
                            if (!isSignal(request.record)) {
                                syncRequestList.add(request);
                                long objSize = Long.parseLong(request.record.headers.getOrDefault(CONTENT_LENGTH, "0"));
                                dataSizeSum.addAndGet(objSize);
                            }

                            if (DateChecker.getCurrentTime() - loopStartTime.get() >= 1000
                                    || syncRequestList.size() >= 100 || dataSizeSum.get() >= 10 * 1024 * 1024) {
                                break;
                            }

                            request = processors[finalI].poll();
                        }

                        synchronized (syncRequestList) {
                            loopStartTime.set(DateChecker.getCurrentTime());
                            // 处理单个signal
                            if (syncRequestList.isEmpty()) {
                                return;
                            }
                            LinkedList<SyncRquest> syncRequests = new LinkedList<>(syncRequestList);
                            sendProcessor.onNext(syncRequests);
                            log.debug("{} {} full syncRequestList. amount: {}, data size: {}", ip, finalI, syncRequests.size(), dataSizeSum.get());
                            syncRequestList.clear();
                            dataSizeSum.set(0L);
                        }
                    } catch (Exception e) {
                        log.error("syncReqProcessors error, ", e);
                    }
                });

                //订阅合并后的record并发送到其他节点
                sendProcessor.publishOn(SCAN_SCHEDULER)
                        .subscribe(syncRquests -> {
                            // 定期会推送来SIGNAL_RECORD，防止syncRequestList中有残留
                            if (syncRquests.isEmpty()) {
                                return;
                            }

                            List<SyncRquest> workList = new LinkedList<>();
                            for (SyncRquest syncRequest : syncRquests) {
                                if (syncRequest.record != null) {
                                    syncRequest.record.setSyncFlag(true);
                                    workList.add(syncRequest);
                                }
                            }

                            HttpClientRequest request0 = MossHttpClient.getClient().request(HttpMethod.PUT, DA_PORT, ip, "?sync").setHost(ip);
                            MsClientRequest request = new MsClientRequest(request0);
//                            addRequest(record.rocksKey(), request);
                            Disposable[] disposables = new Disposable[2];

                            disposables[0] = Mono.just(true)
                                    .subscribeOn(SCAN_SCHEDULER)
                                    .doFinally(auh -> {
//                                        for (SyncRquest syncRequest : workList) {
//                                            releaseSyncingPayload(syncRequest.record);
//                                        }
//                                        closeConn(request);
                                        DoubleActiveUtil.streamDispose(disposables);
                                    })
                                    .subscribe(auth -> {
                                        byte[] originBytes = Json.encode(workList).getBytes();

                                        request.putHeader(EXPECT, EXPECT_100_CONTINUE)
                                                .putHeader(CONTENT_LENGTH, String.valueOf(originBytes.length))
                                                .putHeader(SYNC_AUTH, PASSWORD);
                                        UnicastProcessor<Integer> streamController = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                                        request.putHeader(CLUSTER_ALIVE_HEADER, ip)
                                                .setHost(ip + ":" + DA_PORT)
                                                .setTimeout(timeoutMinute * 60 * 1000)
                                                .setWriteQueueMaxSize(WriteQueueMaxSize)
                                                .continueHandler(b -> {
                                                    AtomicBoolean waitForFetch = new AtomicBoolean();
                                                    //写请求体时的Pump
                                                    disposables[1] = streamController.subscribe(pos -> {
                                                        try {
                                                            if (pos >= originBytes.length) {
                                                                streamController.onComplete();
                                                                return;
                                                            }
                                                            int length = arrayLength;
                                                            if (originBytes.length - pos < arrayLength) {
                                                                length = originBytes.length - pos;
                                                            }
                                                            byte[] writtenBytes = new byte[length];
                                                            System.arraycopy(originBytes, pos, writtenBytes, 0, length);

                                                            request.write(Buffer.buffer(writtenBytes), done -> {
                                                                if (!waitForFetch.get()) {
                                                                    streamController.onNext(pos + arrayLength);
                                                                }
                                                            });

                                                            if (!waitForFetch.get() && request.writeQueueFull()) {
                                                                waitForFetch.set(true);
                                                                request.drainHandler(done -> {
                                                                    waitForFetch.set(false);
                                                                    streamController.onNext(pos + arrayLength);
                                                                });
                                                            }
                                                        } catch (Exception e) {
                                                            log.error("sync err1", e);
                                                        }
                                                    }, e -> log.error("sync err2", e), () -> {
                                                        synchronized (request.getDelegate().connection()) {
                                                            request.end();
                                                        }
                                                    });
                                                    streamController.onNext(0);
                                                    request.addResponseCloseHandler(s -> DoubleActiveUtil.streamDispose(disposables));
                                                })
                                                .exceptionHandler(e -> {
                                                    if (!(e instanceof TimeoutException) && !"Connection was closed".equals(e.getMessage())
                                                            && !(e instanceof ConnectTimeoutException) && !(e instanceof ConnectionPoolTooBusyException)) {
                                                        log.error("send {} request to {} {} error!{}, {}", request.getDelegate().method().name(), request.getHost(), request.uri(),
                                                                e.getClass().getName(), e.getMessage());
                                                    } else {
                                                        log.debug("send {} request to {} {} error!{}, {}", request.getDelegate().method().name(), request.getHost(), request.uri(),
                                                                e.getClass().getName(), e.getMessage());
//                                                        countLimiter.decLimit(e.getMessage());
                                                    }
                                                    for (SyncRquest syncRequest : workList) {
                                                        syncRequest.res.onNext(false);
                                                    }
                                                    DoubleActiveUtil.streamDispose(disposables);
//                                                    closeConn(request);
                                                })
                                                .handler(response -> {
                                                    // 返回未同步成功的索引
                                                    if (response.statusCode() == SUCCESS) {
                                                        response.bodyHandler(buf -> {
                                                            HashSet<Integer> unDoneSet;
                                                            if (StringUtils.isBlank(buf.toString())) {
                                                                unDoneSet = new HashSet<>();
                                                            } else {
                                                                unDoneSet = Json.decodeValue(buf.toString(), new TypeReference<HashSet<Integer>>() {
                                                                });
                                                            }
                                                            for (int j = 0; j < workList.size(); j++) {
                                                                SyncRquest syncRequest = workList.get(j);
                                                                syncRequest.res.onNext(!unDoneSet.contains(j));
                                                            }
                                                        });
                                                    } else {
                                                        workList.forEach(syncRequest -> syncRequest.res.onNext(false));
                                                    }
//                                                    closeConn(request);
                                                    disposables[0].dispose();
                                                }).sendHead();
                                    }, e -> log.error("sendSyncReq error1, ", e));
                        }, e -> log.error("sendSyncReq error2, ", e));

                ScheduledFuture<?> scheduledFuture = SCAN_TIMER.scheduleAtFixedRate(() -> {
                    try {
                        SyncRquest syncRquest = new SyncRquest();
                        syncRquest.record = SIGNAL_RECORD;
                        processors[finalI].onNext(syncRquest);
                    } catch (Exception e) {
                        log.error("signal timer error, ", e);
                    }
                }, 5, 5, TimeUnit.SECONDS);
                signalRecordSchedules.computeIfAbsent(ip, s -> new ScheduledFuture[SYNC_DISTRI_PROCESS_AMOUNT])[finalI] = scheduledFuture;

            }
        }

        try {
            // redis-cli -a Gw@uUp8tBedfrWDy -n 2 hset local_cluster timeout 30
            String timeout = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, "timeout");
            if (StringUtils.isBlank(timeout)) {
                Map<String, String> hgetall = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(DATA_SYNC_QUOTA);
                for (Map.Entry<String, String> entry : hgetall.entrySet()) {
                    if (entry.getKey().startsWith(BAND_WIDTH_QUOTA)) {
                        long bw = Long.parseLong(entry.getValue());
                        if (bw != 0 && bw <= 5 * 1024 * 1024L) {
                            timeoutMinute = 60;
                        }
                    }
                }
            } else {
                timeoutMinute = Long.parseLong(timeout);
            }
        } catch (Exception e) {
            log.error("set timeout err0, {}", timeoutMinute, e);
        }
        log.info("set timeout in minute to {}", timeoutMinute);

        // 同步双活记录
        setSchedule(NORMAL_SYNC_INTERVAL);
        // 同步状态检查
//        SCAN_TIMER.scheduleAtFixedRate(DataSynChecker::syncStateCheck, 10, 10, TimeUnit.SECONDS);
        checkTimer();
        // 自适应调节限流
    }

    public void close() {
        try {
            Optional.ofNullable(dataSyncCheckerSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            Optional.ofNullable(preDataSyncSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            Optional.ofNullable(checkTimerSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            Optional.ofNullable(checkLimitSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            signalRecordSchedules.forEach((ip, sfs) -> {
                for (ScheduledFuture sf : sfs) {
                    sf.cancel(false);
                }
            });

            SCAN_TIMER.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            close();
        }
    }

    static Map<Integer, AtomicBoolean> syncSuspendMap = new ConcurrentHashMap<>();

    static AtomicInteger runNum = new AtomicInteger(0);

    static Map<Integer, AtomicInteger> clusterlastIMap = new ConcurrentHashMap<>();

    static Map<Integer, AtomicInteger> clusterDeletelastIMap = new ConcurrentHashMap<>();

    static Map<Integer, AtomicInteger> clusterDeleteSourcelastIMap = new ConcurrentHashMap<>();

    static AtomicInteger clusterDeleteRunNum = new AtomicInteger(0);

    static AtomicInteger clusterDeleteSourceRunNum = new AtomicInteger(0);

    static Map<Integer, AtomicInteger> scanCountMap = new ConcurrentHashMap<>();

    public static AtomicBoolean syncPartLimiter = new AtomicBoolean();

    static final int maxRunningNum = 20;

    public static boolean isDebug = false;

    public static boolean isSyncLog = false;

    public static long timeoutMinute = 30;

    public static synchronized void syncDataCheck() {
        try {
            String timeout = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, "timeout");
            if (StringUtils.isBlank(timeout)) {
                Map<String, String> hgetall = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(DATA_SYNC_QUOTA);
                boolean hasSmallBW = false;
                for (Map.Entry<String, String> entry : hgetall.entrySet()) {
                    if (entry.getKey().startsWith(BAND_WIDTH_QUOTA)) {
                        long bw = Long.parseLong(entry.getValue());
                        if (bw != 0 && bw <= 5 * 1024 * 1024L) {
                            if (timeoutMinute != 60) {
                                log.info("set timeout in minute from {} to 60", timeoutMinute);
                                timeoutMinute = 60;
                            }
                            hasSmallBW = true;
                            break;
                        }
                    }
                }
                if (!hasSmallBW) {
                    if (timeoutMinute != 30) {
                        log.info("set timeout in minute from {} to 30", timeoutMinute);
                        timeoutMinute = 30;
                    }
                }
            } else if (StringUtils.isNotBlank(timeout) && timeoutMinute != Long.parseLong(timeout)) {
                log.info("set timeout in minute from {} to {}", timeoutMinute, timeout);
                timeoutMinute = Long.parseLong(timeout);
            }
        } catch (Exception e) {
            log.error("set timeout err, {}", timeoutMinute, e);
        }
//        if (IS_ASYNC_CLUSTER && allAsyncComplete()) {
//            return;
//        }
        // 是否打开日志
        String debug = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, "debug");
        String syncLog = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, "syncLog");
        isDebug = "1".equals(debug);
        isSyncLog = "1".equals(syncLog);

        if (!MainNodeSelector.checkIfSyncNode()) {
            //非主节点、不存在双活配置返回
            setSchedule(NORMAL_SYNC_INTERVAL);
            log.debug("not master or not have double active!");
            clearDataCount();
            return;
        }

        // 每轮扫描检查从redis中把sync_bucket_set与内存中的SYNC_BUCKET_STATE_MAP同步。
        pool.getCommand(REDIS_SYSINFO_INDEX).smembers(SYNC_BUCKET_SET)
                .forEach(bucketName ->
                        INDEX_IPS_ENTIRE_MAP.forEach((index, ips) -> {
                            SYNC_BUCKET_STATE_MAP.get(index).putIfAbsent(bucketName, SYNCED);
                        }));

        // 判断最小分段大小是否被修改到5MB以下过
        if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(PART_CONFIG_SIGN) == 1) {
            syncPartLimiter.compareAndSet(false, true);
        }

        syncStateCheckAsync();

        // 心跳异常，不进行数据恢复，记录下一次尝试同步的时间。
        String linkState = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE);
        if (isInterrupt()) {
            Mono.just(true).publishOn(SCAN_SCHEDULER)
                    .subscribe(v -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX)
                            .hset(LOCAL_CLUSTER, NEXT_SYNC_TIME, MsDateUtils.stampToGMT(System.currentTimeMillis() + INTERUP_SYNC_INTERVAL)));
            setSchedule(NORMAL_SYNC_INTERVAL);
            return;
        }

        log.debug("start data sync set {}", Json.encode(SYNC_BUCKET_STATE_MAP));

        log.debug("recordSet size:{}, dealDataSize:{}, partSize: {}", TOTAL_SYNCING_AMOUNT.get(), TOTAL_SYNCING_SIZE.get(), PART_SYNCING_AMOUNT.get());
        log.debug("succ1:{} succ2:{} fail2:{}", succOneCount.get(), succTwoCount.get(), failTwoCount.get());

        if (INDEX_STATUS_MAP.get(LOCAL_CLUSTER_INDEX) == 0) {
            setSchedule(NORMAL_SYNC_INTERVAL);
            return;
        }

        for (Map.Entry<Integer, Map<String, DATA_SYNC_STATE>> entry : SYNC_BUCKET_STATE_MAP.entrySet()) {
            Integer clusterIndex = entry.getKey();
            if (IS_ASYNC_CLUSTER) {
                // 后台数据校验会生成差异记录。async站点也需要扫描这些记录。
                Integer firstDAIndex = DA_INDEX_IPS_ENTIRE_MAP.keySet().stream().findFirst().get();
                if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex) && clusterIndex != firstDAIndex) {
                    // 双活站点的rocksKey格式一样。只需要开一个扫描。
                    continue;
                }

                if (INDEX_STATUS_MAP.getOrDefault(clusterIndex, 1) == 0) {
                    continue;
                }
            } else {
                if (clusterIndex.equals(LOCAL_CLUSTER_INDEX) || clusterIndex < 0) {
                    // clusterIndex可能为-1，因为单站点时调用了getOtherIndex
                    continue;
                }

                if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex) && INDEX_STATUS_MAP.getOrDefault(clusterIndex, 1) == 0) {
                    continue;
                }

                if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex) && INDEX_STATUS_MAP.getOrDefault(clusterIndex, 1) == 0 && otherIsInterrupt(clusterIndex)) {
                    continue;
                }
            }

            // 判断是否在异步复制同步时间内
            String startTime = pool.getCommand(REDIS_SYSINFO_INDEX).hget(SYS_DATA_SYNC_TIME, SYNC_START_TIME + clusterIndex);
            String endTime = pool.getCommand(REDIS_SYSINFO_INDEX).hget(SYS_DATA_SYNC_TIME, SYNC_END_TIME + clusterIndex);
            if (StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)) {
                if (!startTime.equals(endTime)) {
                    long currentStamp = System.currentTimeMillis();
                    long startStamp = getLifecycleStartStamp(startTime);
                    long endStamp = getLifecycleStartStamp(endTime);
                    long realEndStamp = getLifecycleEndStamp(startTime, endTime);
                    if (endStamp == realEndStamp) {
                        if (currentStamp < startStamp || currentStamp > endStamp) {
                            if (syncSuspendMap.get(clusterIndex).compareAndSet(false, true)) {
                                log.info("Stop site sync data, index: {} running time: {} to {}.", clusterIndex, startTime, endTime);
                            }
                            continue;
                        }
                    } else {
                        if (currentStamp > endStamp && currentStamp < startStamp) {
                            if (syncSuspendMap.get(clusterIndex).compareAndSet(false, true)) {
                                log.info("Stop site sync data, index: {} running time: {} to {}.", clusterIndex, startTime, endTime);
                            }
                            continue;
                        }
                    }
                }
            }
            if (syncSuspendMap.get(clusterIndex).compareAndSet(true, false)) {
                log.info("Recover site sync data, index: {}  running time: {} to {}.", clusterIndex, startTime, endTime);
            }

            Map<String, DATA_SYNC_STATE> bucStateMap = entry.getValue();

            String[] syncBuckets = bucStateMap.keySet().toArray(new String[0]);
            List<Disposable> disposables = new LinkedList<>();
            AtomicInteger lastBucketIndex = clusterlastIMap.computeIfAbsent(clusterIndex, k -> new AtomicInteger());

            try {
                for (int j = 0; j < syncBuckets.length; j++) {
                    // 平均分配每个站点的扫描进程个数。
                    AtomicInteger count = scanCountMap.computeIfAbsent(clusterIndex, k -> new AtomicInteger());
                    if (count.incrementAndGet() > maxRunningNum / (SYNC_BUCKET_STATE_MAP.size() - 1)) {
                        count.decrementAndGet();
                        break;
                    }

                    SYNCING_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    SCANNING_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    DELETE_DEAL_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    DELETE_SOURCE_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    String tempBucketName;
                    if (lastBucketIndex.get() >= syncBuckets.length) {
                        lastBucketIndex.set(0);
                    }
                    try {
                        tempBucketName = syncBuckets[lastBucketIndex.get()];
                    } catch (Exception e) {
                        tempBucketName = syncBuckets[0];
                        lastBucketIndex.set(0);
                    }
                    String bucketName = tempBucketName;

                    // 当同步给异构站点，桶的ak还没被加载
                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex) && !EXTRA_AK_SK_MAP.containsKey(bucketName)) {
                        count.decrementAndGet();
                        log.debug("bucket {} no ak and sk to index {}", bucketName, clusterIndex);
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                        continue;
                    }

                    if (runNum.incrementAndGet() > maxRunningNum) {
                        count.decrementAndGet();
                        runNum.decrementAndGet();
                        continue;
                    } else {
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                    }
                    if (!SYNCING_BUCKET_MAP.get(clusterIndex).add(bucketName)) {
                        runNum.decrementAndGet();
                        count.decrementAndGet();
                        continue;
                    }

                    if (SCANNING_BUCKET_MAP.get(clusterIndex).contains(bucketName)) {
                        // 如果已经有扫描的线程，也不开始扫描流程
                        log.debug("already has scanning process, {} ,{}", clusterIndex, bucketName);
                        SYNCING_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                        continue;
                    }

                    Disposable disposable = scanRecord(bucketName, clusterIndex, count, false, false, false);
                    disposables.add(disposable);
                    if (scanOldRecordPath.get()) {
                        runNum.incrementAndGet();
                        count.incrementAndGet();
                        Disposable disposable2 = scanRecord(bucketName, clusterIndex, count, true, false, false);
                        disposables.add(disposable2);
                    }
                }
            } catch (Exception e) {
                log.error("syncDataCheck error, ", e);
                DoubleActiveUtil.streamDispose(disposables);
                setSchedule(NORMAL_SYNC_INTERVAL);
            }
        }


        int interVal = currSyncSate != INTERRUPTED ? NORMAL_SYNC_INTERVAL : INTERUP_SYNC_INTERVAL;
        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(v -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX)
                        .hset(LOCAL_CLUSTER, NEXT_SYNC_TIME, MsDateUtils.stampToGMT(System.currentTimeMillis() + interVal)));
        setSchedule(interVal);
    }

    public static synchronized void syncDataCheckClose() {
        if (IS_ASYNC_CLUSTER) {
            return;
        }
        if (!MainNodeSelector.checkIfSyncNode()) {
            //非主节点、不存在双活配置返回
            log.debug("not master or not have double active!");
            return;
        }

        // 心跳异常，不进行数据恢复，记录下一次尝试同步的时间。
        if (INDEX_STATUS_MAP.getOrDefault(LOCAL_CLUSTER_INDEX, 1) == 0) {
            return;
        }

        Set<String> syncBucketSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(SYNC_BUCKET_SET);
        for (Integer clusterIndex : INDEX_IPS_ENTIRE_MAP.keySet()) {
            if (LOCAL_CLUSTER_INDEX.equals(clusterIndex)) {
                continue;
            }
            String[] syncBuckets = syncBucketSet.toArray(new String[0]);
            List<Disposable> disposables = new LinkedList<>();
            AtomicInteger lastBucketIndex = clusterDeletelastIMap.computeIfAbsent(clusterIndex, k -> new AtomicInteger());

            try {
                for (int j = 0; j < syncBuckets.length; j++) {
                    // 平均分配每个站点的扫描进程个数。
                    AtomicInteger count = new AtomicInteger();
                    DELETE_DEAL_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    DELETE_SOURCE_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    SYNCING_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    String tempBucketName;
                    if (lastBucketIndex.get() >= syncBuckets.length) {
                        lastBucketIndex.set(0);
                    }
                    try {
                        tempBucketName = syncBuckets[lastBucketIndex.get()];
                    } catch (Exception e) {
                        tempBucketName = syncBuckets[0];
                        lastBucketIndex.set(0);
                    }
                    String bucketName = tempBucketName;
                    Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
                    // 桶开关on/suspend的桶不进行扫描
                    if (isSwitchOn(bucketInfo)) {
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                        continue;
                    }
                    if (clusterDeleteRunNum.incrementAndGet() > 3) {
                        clusterDeleteRunNum.decrementAndGet();
                        continue;
                    } else {
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                    }
                    if (!DELETE_DEAL_BUCKET_MAP.get(clusterIndex).add(bucketName)) {
                        clusterDeleteRunNum.decrementAndGet();
                        continue;
                    }
                    count.incrementAndGet();
                    Disposable disposable = scanRecord(bucketName, clusterIndex, count, false, true, false);
                    disposables.add(disposable);
//                    if (scanOldRecordPath.get()) {
//                        runNum.incrementAndGet();
//                        count.incrementAndGet();
//                        Disposable disposable2 = scanRecord(bucketName, clusterIndex, count, true);
//                        disposables.add(disposable2);
//                    }
                }
            } catch (Exception e) {
                log.error("syncDataCheck error, ", e);
                DoubleActiveUtil.streamDispose(disposables);
                setSchedule(NORMAL_SYNC_INTERVAL);
            }
        }


        int interVal = currSyncSate != INTERRUPTED ? NORMAL_SYNC_INTERVAL : INTERUP_SYNC_INTERVAL;
        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(v -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX)
                        .hset(LOCAL_CLUSTER, NEXT_SYNC_TIME, MsDateUtils.stampToGMT(System.currentTimeMillis() + interVal)));
        setSchedule(interVal);
    }

    public static synchronized void syncDataCheckDelete() {
        if (IS_ASYNC_CLUSTER) {
            return;
        }
        if (!MainNodeSelector.checkIfSyncNode()) {
            //非主节点、不存在双活配置返回
            log.debug("not master or not have double active!");
            return;
        }

        // 心跳异常，不进行数据恢复，记录下一次尝试同步的时间。
        if (INDEX_STATUS_MAP.getOrDefault(LOCAL_CLUSTER_INDEX, 1) == 0) {
            return;
        }

        Set<String> syncBucketSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(SYNC_BUCKET_SET);
        for (Integer clusterIndex : INDEX_IPS_ENTIRE_MAP.keySet()) {
            if (!LOCAL_CLUSTER_INDEX.equals(clusterIndex)) {
                continue;
            }
            String[] syncBuckets = syncBucketSet.toArray(new String[0]);
            List<Disposable> disposables = new LinkedList<>();
            AtomicInteger lastBucketIndex = clusterDeleteSourcelastIMap.computeIfAbsent(clusterIndex, k -> new AtomicInteger());

            try {
                for (int j = 0; j < syncBuckets.length; j++) {
                    // 平均分配每个站点的扫描进程个数。
                    AtomicInteger count = new AtomicInteger();
                    DELETE_DEAL_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    DELETE_SOURCE_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    SYNCING_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    String tempBucketName;
                    if (lastBucketIndex.get() >= syncBuckets.length) {
                        lastBucketIndex.set(0);
                    }
                    try {
                        tempBucketName = syncBuckets[lastBucketIndex.get()];
                    } catch (Exception e) {
                        tempBucketName = syncBuckets[0];
                        lastBucketIndex.set(0);
                    }
                    String bucketName = tempBucketName;
                    Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
                    // 只扫描桶开关开启或暂停的桶
                    if (!isSwitchOn(bucketInfo) && !isSwitchSuspend(bucketInfo)) {
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                        continue;
                    }
                    if (clusterDeleteSourceRunNum.incrementAndGet() > 3) {
                        clusterDeleteSourceRunNum.decrementAndGet();
                        continue;
                    } else {
                        lastBucketIndex.set((lastBucketIndex.get() + 1) % syncBuckets.length);
                    }
                    if (!DELETE_SOURCE_BUCKET_MAP.get(clusterIndex).add(bucketName)) {
                        clusterDeleteSourceRunNum.decrementAndGet();
                        continue;
                    }
                    count.incrementAndGet();
                    Disposable disposable = scanRecord(bucketName, clusterIndex, count, false, false, true);
                    disposables.add(disposable);
                }
            } catch (Exception e) {
                log.error("syncDataCheck error, ", e);
                DoubleActiveUtil.streamDispose(disposables);
                setSchedule(NORMAL_SYNC_INTERVAL);
            }
        }


        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(v -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX)
                        .hset(LOCAL_CLUSTER, NEXT_SYNC_TIME, MsDateUtils.stampToGMT(System.currentTimeMillis() + NORMAL_SYNC_INTERVAL)));
        setSchedule(NORMAL_SYNC_INTERVAL);
    }


    private static boolean isDeleteSourceOn(Map<String, String> bucketMap) {
        return SWITCH_ON.equals(bucketMap.getOrDefault(DELETE_SOURCE_SWITCH, "off"));
    }

    public static void scanRemoveSyncBucketCheck() {
        if (!MainNodeSelector.checkIfSyncNode()) {
            //非主节点、不存在双活配置返回
            log.debug("not master or not have double active!");
            return;
        }

        if (INDEX_STATUS_MAP.getOrDefault(LOCAL_CLUSTER_INDEX, 1) == 0) {
            return;
        }

        Mono.just(true)
                .filter(b -> IS_REMOVE_BUCKET_CHECKING.compareAndSet(false, true))
                .flatMapMany(b -> ScanStream.sscan(pool.getReactive(REDIS_SYSINFO_INDEX), SYNC_BUCKET_SET))
                .flatMap(bucketName -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName).zipWith(Mono.just(bucketName)))
                .filter(tuple -> tuple.getT1().isEmpty() || !isSwitchOn(tuple.getT1()))
                .map(reactor.util.function.Tuple2::getT2)
                .doOnNext(bucketName -> Flux.fromStream(SYNC_BUCKET_STATE_MAP.keySet().stream())
                        .filter(index -> !index.equals(LOCAL_CLUSTER_INDEX))
                        .flatMap(index -> hasUnSyncedData(bucketName, index, false, null, true, false))
                        .collectList()
                        .doOnNext(list -> {
                            if (!list.contains(true)) {
                                Mono.just(true).publishOn(SCAN_SCHEDULER)
                                        .subscribe(s -> {
                                            log.info("remove bucket {} from bucket_sync_set. ", bucketName);
                                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(SYNC_BUCKET_SET, bucketName);
                                        });
                            }
                        })
                        .doOnError(e -> log.error("check {} empty error!", bucketName, e))
                        .subscribe())
                .collectList()
                .doOnError(e -> log.error("check bucket empty error!", e))
                .doFinally(b -> IS_REMOVE_BUCKET_CHECKING.set(false))
                .subscribe();
    }


    /**
     * 是否要扫描存在老路径下的记录。
     */
    public static AtomicBoolean scanOldRecordPath = new AtomicBoolean(false);
    public static Set<String> processOldRecordCompleteBuckets = new ConcurrentHashSet<>();

    static {
        boolean needScanOldRecord = pool.getCommand(REDIS_SYSINFO_INDEX).hexists(MASTER_CLUSTER, "scan_old_record");
        scanOldRecordPath.set(needScanOldRecord);
    }

    /**
     * 判断所有需要同步的桶是否已经无旧的record记录
     */
    private static boolean processOldRecordIsFinished() {
        Set<String> tmpSet = new HashSet<>();
        SYNC_BUCKET_STATE_MAP.forEach((index, map) -> {
            if (!index.equals(LOCAL_CLUSTER_INDEX)) {
                tmpSet.addAll(map.keySet().stream().map(bucket -> index + "_" + bucket).collect(Collectors.toList()));
            }
        });
        return processOldRecordCompleteBuckets.containsAll(tmpSet);
    }

    private static Disposable scanRecord(String bucketName, int clusterIndex, AtomicInteger count, boolean metaRocksScan, boolean onlyDelete, boolean deleteSource) {
        boolean[] checkError = new boolean[]{false};
        AtomicBoolean switchClose = new AtomicBoolean();
        return hasUnSyncedData(bucketName, clusterIndex, metaRocksScan, null, onlyDelete, deleteSource)
                .doOnError(e -> checkError[0] = true)
                .subscribeOn(SCAN_SCHEDULER)
                .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .flatMap(tuple -> {
                    // 处理升级流程
                    Boolean hasUnsyncRecord = tuple.getT1();
                    if (scanOldRecordPath.get() && metaRocksScan && !hasUnsyncRecord) {
                        // 记录已经处理完的站点以及其对应的桶
                        processOldRecordCompleteBuckets.add(clusterIndex + "_" + bucketName);
                        log.info("the bucket {} index {} scan old record has completed.", bucketName, clusterIndex);
                        if (processOldRecordIsFinished()) {
                            scanOldRecordPath.set(false);
                            processOldRecordCompleteBuckets.clear();
                            Mono.just(true)
                                    .publishOn(SCAN_SCHEDULER)
                                    .subscribe(l -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(MASTER_CLUSTER, "scan_old_record"));
                        }
                        return Mono.just(false);
                    }

                    Map<String, String> bucketMap = tuple.getT2();
                    BucketSyncSwitchCache.getInstance().check(bucketName, bucketMap);
                    if (deleteSource) {
                        if (hasUnsyncRecord && isDeleteSourceOn(bucketMap)) {
                            return Mono.just(true);
                        } else {
                            return Mono.just(false);
                        }
                    }
                    switchClose.set(isSwitchClose(bucketMap));
                    // 桶同步开关暂停
                    if (isSwitchSuspend(bucketMap)) {
                        updateBucSyncState(clusterIndex, bucketName, SUSPEND);
                        return Mono.just(false);
                    }
                    if (!hasUnsyncRecord) {
                        if (onlyDelete) {
                            // 无record，且桶已不存在，或者又立刻创建了一个未开同步的桶名桶，将该桶从待同步桶移除。
                            if (isSwitchOff(bucketMap) || switchClose.get()) {
                                SYNC_BUCKET_STATE_MAP.get(clusterIndex).remove(bucketName);
                                return Mono.just(false);
                            }
                        } else {
                            // 历史数据或桶复制差异还未生成，
                            return pool.getReactive(REDIS_SYSINFO_INDEX).sismember(NEED_SYNC_BUCKETS, bucketName)
                                    .defaultIfEmpty(false)
                                    .flatMap(b -> {
                                        if (b) {
                                            updateBucSyncState(clusterIndex, bucketName, SYNCING);
                                        } else {
                                            updateBucSyncState(clusterIndex, bucketName, SYNCED);
                                        }
                                        return Mono.just(false);
                                    });
                        }
                    }
                    if (isSwitchOff(bucketMap) || switchClose.get()) {
                        updateBucSyncState(clusterIndex, bucketName, SYNCED);
                        if (onlyDelete) {
                            return Mono.just(true);
                        } else {
                            return Mono.just(false);
                        }
                    }
                    return Mono.just(!onlyDelete);
                })
                .filter(b -> b)
                .flatMap(b -> {
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
                    String bucketVnode = storagePool.getBucketVnodeId(bucketName);
                    return storagePool.mapToNodeInfo(bucketVnode);
                })
                .flatMap(nodeList -> scannerRecorder(nodeList, bucketName, clusterIndex, metaRocksScan, onlyDelete, deleteSource))
                .doFinally(s -> {
                    if (onlyDelete) {
                        clusterDeleteRunNum.decrementAndGet();
                        DELETE_DEAL_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else if (deleteSource) {
                        clusterDeleteSourceRunNum.decrementAndGet();
                        DELETE_SOURCE_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else {
                        runNum.decrementAndGet();
                        SYNCING_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    }
                    count.decrementAndGet();
                })
                .subscribe(res -> {
                    if (onlyDelete) {
                        DELETE_DEAL_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else if (deleteSource) {
                        DELETE_SOURCE_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else {
                        SYNCING_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    }
                    Set<String> errObjSet = SYNC_BUCKET_ERROR_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(bucketName, k -> new ConcurrentHashSet<>());
                    if (errObjSet.size() > 0 && !switchClose.get() && !deleteSource) {
                        //存在同步失败，设置为同步中
                        log.debug("sync data in cluster {} bucket {} interrupted ", clusterIndex, bucketName);
                        updateBucSyncState(clusterIndex, bucketName, SYNCING);
                    } else {
                        //没有同步记录，修改状态为已同步
                        log.debug("cluster: {} {} sync success", clusterIndex, bucketName);
                    }
                }, e -> {
                    if (onlyDelete) {
                        DELETE_DEAL_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else if (deleteSource) {
                        DELETE_SOURCE_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    } else {
                        SYNCING_BUCKET_MAP.get(clusterIndex).remove(bucketName);
                    }
                    if (!checkError[0] && !switchClose.get() && !deleteSource) {
                        updateBucSyncState(clusterIndex, bucketName, SYNCING);
                    }
                    if (!"sync data error!".equals(e.getMessage())) {
                        log.error(bucketName + " deal sync error, cluster: {}", clusterIndex, e);
                    }
//                    if (e instanceof TimeoutException) {
//                        countLimiter.decLimit(e.getMessage());
//                    }
                });
    }

    /**
     * 将表7中的桶写入bucket_sync_set，并从bucket_sync_set移除已不存在的桶。
     */
    private static void dealSyncBuckets() {
        List<String> newAddList = new ArrayList<>();
        List<String> notExistList = new ArrayList<>();

        Set<String> existsBucSet = new HashSet<>();
        Set<String> syncBucSet = new HashSet<>();
        ScanStream.scan(pool.getReactive(REDIS_BUCKETINFO_INDEX))
                .publishOn(SCAN_SCHEDULER)
                .filter(bucketName -> BUCKET_NAME_PATTERN.matcher(bucketName).matches())
                .flatMap(bucketName -> DoubleActiveUtil.datasyncIsEnabled(bucketName)
                        .doOnNext(status -> BucketSyncSwitchCache.getInstance().update(bucketName, status))
                        .filter(status -> SWITCH_ON.equals(status) || SWITCH_SUSPEND.equals(status))
                        .doOnNext(b -> {
                            if (EXTRA_CLUSTER) {
                                BucketSyncUtils.updateExtraMap(bucketName);
                            }
                        })
                        .map(b -> bucketName))
                .doOnNext(existsBucSet::add)
                .doOnComplete(() -> {
                    ScanStream.sscan(pool.getReactive(REDIS_SYSINFO_INDEX), SYNC_BUCKET_SET)
                            .publishOn(SCAN_SCHEDULER)
                            .doOnNext(syncSetBuc -> {
                                syncBucSet.add(syncSetBuc);
                                // bucket_sync_set中的桶已经不存在，放入notExistList。
                                if (!existsBucSet.contains(syncSetBuc)) {
                                    notExistList.add(syncSetBuc);
                                }
                            })
                            .doOnComplete(() -> {
                                // 表7中有桶不在bucket_sync_set中。需要加入。
                                existsBucSet.forEach(buc -> {
                                    if (!syncBucSet.contains(buc)) {
                                        newAddList.add(buc);
                                    }
                                });
//                                // 可能删除了桶但差异记录并未全部就同步，此时不移除
//                                if (notExistList.size() != 0) {
//                                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(SYNC_BUCKET_SET, notExistList.toArray(new String[0]));
//                                }
                                if (newAddList.size() != 0) {
                                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(SYNC_BUCKET_SET, newAddList.toArray(new String[0]));
                                }
                            }).subscribe();
                }).subscribe();
    }

    private static ScheduledFuture dataSyncCheckerSchedule;

    private static ScheduledFuture preDataSyncSchedule;

    private static ScheduledFuture checkTimerSchedule;

    private static ScheduledFuture checkLimitSchedule;

    private static ScheduledFuture checkStampSchedule;

    /**
     * 定期检查timerId，如果不变化了说明扫描线程挂了，需要手动重启。
     */
    private static void checkTimer() {
        preDataSyncSchedule = dataSyncCheckerSchedule;
        checkTimerSchedule = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                log.debug("start checkTimer");
                dealSyncBuckets();
                scanRemoveSyncBucketCheck();

                if (preDataSyncSchedule == dataSyncCheckerSchedule) {
                    dataSyncCheckerSchedule.cancel(true);
                    setSchedule(NORMAL_SYNC_INTERVAL);
                }
                preDataSyncSchedule = dataSyncCheckerSchedule;
                checkIfRecordExpire();
            } catch (Exception e) {
                log.error("checkTimer err, ", e);
            }
        }, 120, 120, TimeUnit.SECONDS);

        checkStampSchedule = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                log.debug("checkStampSchedule running");
                DoubleActiveUtil.checkDealingVersionNum();
            } catch (Exception e) {
                log.error("checkStampSchedule err", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 设置下一次扫描任务的时间，不会中断已进行的任务。
     */
    private static void setSchedule(long delay) {
        if (dataSyncCheckerSchedule != null) {
            dataSyncCheckerSchedule.cancel(false);
        }
        dataSyncCheckerSchedule = SCAN_TIMER.schedule(() -> {
            syncDataCheck();
            syncDataCheckClose();
            syncDataCheckDelete();
        }, delay, TimeUnit.MILLISECONDS);
    }


    /**
     * 是否存在未同步数据记录
     */
    public static Mono<Boolean> hasUnSyncedData(String bucket, Integer clusterIndex, boolean metaRocksScan, String checkRecordKey, boolean onlyDelete, boolean deleteSource) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        MonoProcessor<Boolean> res = MonoProcessor.create();

        storagePool.mapToNodeInfo(bucketVnode)
                .publishOn(SCAN_SCHEDULER)
                .subscribe(list -> {
                    List<SocketReqMsg> msgs = list.stream()
                            .map(t -> {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", MSRocksDB.getSyncRecordLun(t.var2))
                                        .put("bucket", bucket)
                                        .put("clusterIndex", String.valueOf(clusterIndex));
                                if (metaRocksScan) {
                                    msg.put("lun", t.var2);
                                }
                                if (checkRecordKey != null) {
                                    msg.put("checkRecordKey", checkRecordKey);
                                }
                                if (onlyDelete) {
                                    msg.put("onlyDelete", "1");
                                }

                                if (deleteSource) {
                                    msg.put("deleteSource", "1");
                                }
                                return msg;
                            })
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, HAVA_SYNC_RECORD, String.class, list);
                    responseInfo.responses.subscribe(s -> {
                    }, e -> log.error("hasUnSyncedData err", e), () -> {
                        if (responseInfo.errorNum > storagePool.getM()) {
                            res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get sync record error !"));
                        } else if (responseInfo.successNum > 0) {
                            res.onNext(true);
                        } else {
                            // 返回not_found
                            log.debug("no sync data in bucket:{}, cluster:{}", bucket, clusterIndex);
                            res.onNext(false);
                        }
                    });
                });

        return res;
    }

    @CompiledJson
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class SyncRquest {
        @JsonAttribute
        public UnSynchronizedRecord record;
        @JsonIgnore
        public MonoProcessor<Boolean> res;
        @JsonAttribute
        public String bucketSwitch;
        @JsonAttribute
        public String deleteSource;
    }

    private static void clearDisposables(Set<Disposable> disposableSet) {
        try {
            for (Disposable disposable : disposableSet) {
                try {
                    disposable.dispose();
                } catch (Exception e) {
                }

            }
            disposableSet.clear();
        } catch (Exception e) {
            log.error("clear disposables error, ", e);
        }
    }

    public static Mono<Boolean> recordHandler(List<UnSynchronizedRecord> recordList, String bucket, UnicastProcessor<String> listController,
                                              Set<Disposable> disposableSet, Integer clusterIndex, Map<String, String> bucketMap) {
        if (listController.hasCompleted()) {
            for (UnSynchronizedRecord record : recordList) {
                releaseSyncingPayload(record);
            }
            return Mono.just(true);
        }

        if (!isSwitchClose(bucketMap) && !clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
            updateBucSyncState(clusterIndex, bucket, SYNCING);
        }
        MonoProcessor<Boolean> result = MonoProcessor.create();
        if (recordList.isEmpty()) {
            return Mono.just(true);
        }

        log.debug("start sync data in cluster {} bucket {} record size {}", clusterIndex, bucket, recordList.size());
        AtomicBoolean hasSuccess = new AtomicBoolean();
        AtomicInteger preCount = new AtomicInteger(recordList.size());
        AtomicInteger dealCount = new AtomicInteger();

        for (UnSynchronizedRecord record : recordList) {
            if (record.type() == NONE) {
                //如after_init记录，桶关闭开关后删除，不需要确认延迟
            } else {
                boolean skipDeal = false;
                if (isCheckDealingNormal.get() && !record.isFSUnsyncRecord()) {
                    if (needDelayRecordDeal(bucket, record)) {
                        skipDeal = true;
                    }
                } else {
                    String stamp = getStampFromRecord(record);
                    long nodeAmount = record.isFSUnsyncRecord() ? 60 * 60 * 1000L : UPLOAD_MAX_INTERVAL * 1000L;
                    // 没到时间的预提交记录暂不不处理
                    if (!record.commited && ((System.currentTimeMillis() - Long.parseLong(stamp)) < nodeAmount)) {
                        skipDeal = true;
                    }
                }
                if (skipDeal) {
                    releaseSyncingPayload(record);
                    int count = dealCount.incrementAndGet();
                    if (count == preCount.get()) {
                        result.onNext(hasSuccess.get());
                    }
                    continue;
                }
            }

            SyncRquest syncRquest = new SyncRquest();
            syncRquest.record = record;
            syncRquest.bucketSwitch = bucketMap.get(DATA_SYNC_SWITCH);
            syncRquest.deleteSource = bucketMap.getOrDefault(DELETE_SOURCE_SWITCH, SWITCH_OFF);
            syncRquest.res = MonoProcessor.create();
            if (record.isFSUnsyncRecord()) {
                String fsRecordMapKey = FSUnsyncRecordHandler.getFSRecordMapKey(record);
                SyncRecordListCache syncRecordListCache = SyncRecordListCache.getCache(fsRecordMapKey);
                syncRquest.res
                        .publishOn(SCAN_SCHEDULER)
                        .timeout(Duration.ofMinutes(timeoutMinute))
                        .doFinally(s -> {
                            releaseSyncingPayload(syncRquest.record);
                            syncRquest.res.dispose();
                            syncRquest.record = null;
                        })
                        .subscribe(b -> {
                            int count = dealCount.incrementAndGet();
                            if (b && !hasSuccess.get()) {
                                hasSuccess.set(true);
                            }
                            if (b) {
                                syncRecordListCache.onNext();
                            } else {
                                hasSuccess.compareAndSet(true, false);
                                syncRecordListCache.onFail();
                            }
                            if (count == preCount.get()) {
                                result.onNext(hasSuccess.get());
                            }
                        }, e -> {
                            log.error("syncRequest fs res error: ", e);
                            syncRecordListCache.onFail();
                            int count = dealCount.incrementAndGet();
                            if (count == preCount.get()) {
                                result.onNext(hasSuccess.get());
                            }

                        });

                syncRecordListCache.add(syncRquest);
                continue;
            }
//            disposableSet.add(syncRquest.res);
            syncRquest.res
                    .publishOn(SCAN_SCHEDULER)
                    .timeout(Duration.ofMinutes(timeoutMinute))
                    .doFinally(s -> {
                        releaseSyncingPayload(syncRquest.record);
                        syncRquest.res.dispose();
                        syncRquest.record = null;
                    })
                    .subscribe(b -> {
//                        disposableSet.remove(syncRquest.res);
                        int count = dealCount.incrementAndGet();
                        if (b && !hasSuccess.get()) {
                            hasSuccess.set(true);
                        }
                        if (count == preCount.get()) {
                            result.onNext(hasSuccess.get());
                        }
                    }, e -> {
                        log.error("syncRequest res error: ", e);
                        releaseSyncingPayload(syncRquest.record);

                        int count = dealCount.incrementAndGet();
                        if (count == preCount.get()) {
                            result.onNext(hasSuccess.get());
                        }
                    });

            allocSyncRequest(syncRquest);
        }

        return result;
    }

    public static void allocSyncRequest(SyncRquest syncRquest) {
        //随机选择一个可用的本站点的复制链路ip。如果选中了非本节点的ip，则将record发过去。
        int nodeIndex = ThreadLocalRandom.current().nextInt(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX).length);
        String syncIp;
        try {
            syncIp = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)[nodeIndex];
        } catch (Exception e) {
            //可能会空指针，此时发往本站点的processor
            syncIp = LOCAL_NODE_IP;
        }

        try {
            if (isDebug) {
                syncIp = LOCAL_NODE_IP;
            }

            if (!LOCAL_NODE_IP.equals(syncIp)) {
                int processorIndex = ThreadLocalRandom.current().nextInt(syncReqProcessors.get(syncIp).length);
                syncReqProcessors.get(syncIp)[processorIndex].onNext(syncRquest);
            } else {
                int processorIndex = ThreadLocalRandom.current().nextInt(PROCESSOR_NUM);
                recordProcessors[processorIndex].onNext(syncRquest);

            }
        } catch (Exception e) {
            log.error("allocSyncRequest err,", e);
            syncRquest.res.onNext(false);
        }
    }

    /**
     * 根据日志类型进行数据同步具体处理，
     * syncStamp与当前一致，向对端站点同步
     * syncStamp不一致,若为预提交日志，请求对端进行dealRecord处理
     *
     * @param record 同步日志
     * @return 同步处理结果
     */
    private static Mono<Boolean> dealRecord(UnSynchronizedRecord record) {
        if (record.isFSUnsyncRecord()) {
            try {
                return FSUnsyncRecordHandler.dealRecord(record);
            } catch (Exception e) {
                log.error("FS deal record err, ", e);
                throw new RuntimeException("FS deal record err");
            }
        }
        if (record.getHeaders().containsKey(EXTRA_ASYNC_TYPE)) {
            // 异构中间态差异
            return Mono.just(true);
        }

        if (record.getHeaders().containsKey("local_repair")) {
            record.getHeaders().remove("local_repair");
            record.getHeaders().put("from", String.valueOf(LOCAL_CLUSTER_INDEX));
            return notifyOtherSiteDealRecord(record);
        }
        boolean deleteSourceFlag = record.headers.containsKey("deleteSource");

        if (!deleteSourceFlag && getOtherSiteIndex() != -1) {
            if (LOCAL_CLUSTER_INDEX.equals(record.index)) {
                //写入失败节点为本站点,将record写入成功站点
                return notifyOtherDealRecord(record);
            } else if (recordTransfer(record.index) && record.successIndexSet != null && record.successIndexSet.contains(getOtherSiteIndex())) {
                return notifyOtherSiteDealRecord(record.setSuccessIndex(getOtherSiteIndex()));
            } else if (!DA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index) && record.commited
                    && !LOCAL_CLUSTER_INDEX.equals(record.successIndex)) {
                // 同步给复制站点的record，succIndex非本站点，则要转发给对站点。
                return notifyOtherSiteDealRecord(record);
            }
        }

        //本站点为处理成功站点或record为预提交状态
        UnSynchronizedRecord.Type type = record.type();
        MultiMap params = RestfulVerticle.params(record.uri);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        record.headers.put(SYNC_AUTH, PASSWORD);
        String bucketVnodeId = "";
        try {
            bucketVnodeId = bucketPool.getBucketVnodeId(record.bucket, record.object);
        } catch (Exception e) {
            // 不存在的桶会抛出异常
            bucketVnodeId = bucketPool.getBucketVnodeId(record.bucket);
        }

        switch (type) {
//            case ERROR_APPEND_OBJECT:
            case ERROR_PUT_OBJECT:
            case ERROR_PUT_OBJECT_VERSION:
            case ERROR_SYNC_HIS_OBJECT: {
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object,
                                record.versionId, bucketVnodeList).zipWith(Mono.just(bucketVnodeList)))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(tuple -> {
                            MetaData metaData = tuple.getT1();
                            if (ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }

                            boolean needOriginUserMeta = "COPY".equals(record.headers.getOrDefault(X_AMZ_METADATA_DIRECTIVE, "COPY"));
                            if (StringUtils.isNotEmpty(record.headers.get(X_AMZ_COPY_SOURCE))) {
                                record.headers.remove(X_AMZ_COPY_SOURCE);
                                record.headers.remove("X-Amz-Content-Sha256");
                                record.headers.put(VERSIONID, record.headers.get(NEW_VERSION_ID));
                                if (needOriginUserMeta) {
                                    // 如果是copy原对象用户元数据，则将原请求中的用户元数据都移除
                                    record.headers.keySet().removeIf(key -> key.startsWith(USER_META));

                                }
                            }

                            if (record.syncStamp.equals(metaData.syncStamp) && !metaData.deleteMarker) {

                                Map<String, String> stringMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                });
                                // 包含此key-value的对象不进行站点间同步
                                if (stringMap.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(stringMap.get(NO_SYNCHRONIZATION_KEY))) {
                                    // 此类对象不进行删源处理
                                    record.headers.put("overWriteFlag", "1");
                                    return Mono.just(true);
                                }

                                //本站点写入成功，同步至对站点
                                setMetaData(record, metaData, needOriginUserMeta);
                                if (StringUtils.isNotEmpty(metaData.partUploadId) && stringMap.get(ETAG).contains("-")) {
                                    rollBackTokens(record);
                                    return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                            .flatMap(nodeList -> DataSyncHandler.getInstance().rewritePartCopyRecord(bucketPool, record, metaData, nodeList));
                                }

                                return DataSyncHandler.recoverPut(record, metaData, type.equals(UnSynchronizedRecord.Type.ERROR_APPEND_OBJECT));
                            } else {
                                //当前站点未找到对象，可能数据被覆盖或者未写入成功
                                return dealUnfinishedReqsRecord(record);
                            }
                        });
            }
            case ERROR_DELETE_OBJECT: {
                // StringUtils.isEmpty(record.versionId)=true 表示开了多版本但没有指定versionId，“null”表示未开多版本
                // record.headers.get(NEW_VERSION_ID) 的值没开多版本或暂停是“null”，否则是新生成的32位随机字符串
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object,
                                StringUtils.isEmpty(record.versionId) ? record.headers.get(NEW_VERSION_ID) : record.versionId, bucketVnodeList))
                        .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(tuple2 -> {
                            MetaData metaData = tuple2.getT1();
                            if (MetaData.ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }
                            //桶回收站的流程
                            String trashDir = record.headers.get("trashDir");
                            String syncStamp = record.headers.get(SYNC_STAMP);

                            if (StringUtils.isNotBlank(trashDir) && StringUtils.isEmpty(record.headers.get("cleanObject"))) {
                                final List<Tuple3<String, String, String>>[] vnode = new List[]{new LinkedList<>()};
                                if (StringUtils.isNotBlank(record.headers.get("recoverObject"))) {
                                    //恢复流程  对象不存在或者对象被覆盖时 要么被恢复 要么被恢复的也已经被彻底删掉
                                    //commited为false的状态 对象被恢复失败 或者被恢复一半
                                    if (!record.commited || StringUtils.isBlank(record.lastStamp) || !record.lastStamp.equals(metaData.syncStamp)) {
                                        String vnodeId;
                                        try {
                                            vnodeId = bucketPool.getBucketVnodeId(record.bucket, record.object.substring(trashDir.length()));
                                        } catch (Exception e) {
                                            // 不存在的桶会抛出异常
                                            vnodeId = bucketPool.getBucketVnodeId(record.bucket);
                                        }
                                        return bucketPool.mapToNodeInfo(vnodeId)
                                                .flatMap(bucketVnodeList -> {
                                                    vnode[0] = bucketVnodeList;
                                                    return ErasureClient.getObjectMetaVersion(record.bucket, record.object.substring(trashDir.length()),
                                                            StringUtils.isEmpty(record.versionId) ? record.headers.get(NEW_VERSION_ID) : record.versionId, bucketVnodeList);
                                                }).flatMap(meta -> {
                                                    if (MetaData.ERROR_META.equals(meta)) {
                                                        return Mono.just(false);
                                                    }
                                                    // 又删除进了回收站，或者彻底删除
                                                    if (meta.sysMetaData == null) {
                                                        return dealUnfinishedReqsRecord(record);
                                                    }
                                                    // 桶清单和桶日志开启回收站后差异不在站点间同步
                                                    Map<String, String> map = Json.decodeValue(meta.sysMetaData, new TypeReference<Map<String, String>>() {
                                                    });
                                                    if (map.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(map.get(NO_SYNCHRONIZATION_KEY))) {
                                                        return Mono.just(true);
                                                    }
                                                    if (record.syncStamp.equals(meta.syncStamp)) {
                                                        //将主站点复制到从站点
                                                        return Mono.just(AddClusterUtils.buildSyncRecord(record.index, HttpMethod.PUT, meta))
                                                                .flatMap(recoverRecord -> {
                                                                    if (StringUtils.isNotEmpty(metaData.partUploadId) && map.get(ETAG).contains("-")) {
                                                                        rollBackTokens(record);
                                                                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(recoverRecord.bucket))
                                                                                .flatMap(nodeList -> DataSyncHandler.getInstance().rewritePartCopyRecord(bucketPool, recoverRecord, meta, nodeList));
                                                                    }
                                                                    return DataSyncHandler.recoverPut(recoverRecord, meta);
                                                                })
                                                                .doOnNext(res -> {
//                                                                    record.headers.remove("trashDir");
                                                                    record.headers.remove(NEW_VERSION_ID);
                                                                })
                                                                .flatMap(res -> {
                                                                    if (!res) {
                                                                        return Mono.just(false);
                                                                    }
                                                                    if (StringUtils.isNotBlank(record.lastStamp)) {
                                                                        if (record.lastStamp.equals(metaData.syncStamp)) {
                                                                            return dealUnfinishedReqsRecord(record);
                                                                        }
                                                                        record.headers.put(SYNC_STAMP, record.lastStamp);
                                                                    }
                                                                    return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object,
                                                                            record.uri, record.method, record.headers, null)
                                                                            .flatMap(tuple3 -> {
                                                                                // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                                                if (tuple3.var1 == SUCCESS || tuple3.var1 == DEL_SUCCESS) {
                                                                                    return Mono.just(true);
                                                                                }
                                                                                return Mono.just(false);
                                                                            });
                                                                });
                                                    } else {
                                                        return dealUnfinishedReqsRecord(record);
                                                    }
                                                });
                                    } else {
                                        return Mono.just(true);
                                    }
                                } else {
                                    //删除到回收站中
                                    if (!record.syncStamp.equals(metaData.syncStamp)) {
                                        //原对象不存在或者存在同名对象
                                        String vnodeId;
                                        try {
                                            vnodeId = bucketPool.getBucketVnodeId(record.bucket, trashDir + record.object);
                                        } catch (Exception e) {
                                            // 不存在的桶会抛出异常
                                            vnodeId = bucketPool.getBucketVnodeId(record.bucket);
                                        }
                                        return bucketPool.mapToNodeInfo(vnodeId)
                                                .flatMap(bucketVnodeList -> {
                                                    vnode[0] = bucketVnodeList;
                                                    return ErasureClient.getObjectMetaVersion(record.bucket, trashDir + record.object,
                                                            StringUtils.isEmpty(record.versionId) ? record.headers.get(NEW_VERSION_ID) : record.versionId, bucketVnodeList);
                                                }).flatMap(meta -> {
                                                    if (MetaData.ERROR_META.equals(metaData)) {
                                                        return Mono.just(false);
                                                    }
                                                    if (record.syncStamp.equals(meta.syncStamp)) {
                                                        // 桶清单和桶日志开启回收站后差异不在站点间同步
                                                        Map<String, String> map = Json.decodeValue(meta.sysMetaData, new TypeReference<Map<String, String>>() {
                                                        });
                                                        if (map.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(map.get(NO_SYNCHRONIZATION_KEY))) {
                                                            return Mono.just(true);
                                                        }
                                                        //将主站点对象复制从站点
                                                        return Mono.just(AddClusterUtils.buildSyncRecord(record.index, HttpMethod.PUT, meta))
                                                                .flatMap(recoverRecord -> {
                                                                    if (StringUtils.isNotEmpty(metaData.partUploadId) && map.get(ETAG).contains("-")) {
                                                                        rollBackTokens(record);
                                                                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(recoverRecord.bucket))
                                                                                .flatMap(nodeList -> DataSyncHandler.getInstance().rewritePartCopyRecord(bucketPool, recoverRecord, meta, nodeList));
                                                                    }
                                                                    return DataSyncHandler.recoverPut(recoverRecord, meta);
                                                                })
                                                                .doOnNext(res -> {
//                                                                    record.headers.remove("trashDir");
                                                                    record.headers.remove(SYNC_STAMP);
                                                                    record.headers.remove(NEW_VERSION_ID);
                                                                }).flatMap(res -> {
                                                                    if (!res) {
                                                                        return Mono.just(false);
                                                                    }
                                                                    if (StringUtils.isNotBlank(record.lastStamp) && record.lastStamp.equals(metaData.syncStamp)) {
                                                                        return dealUnfinishedReqsRecord(record);
                                                                    }
                                                                    return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object,
                                                                            record.uri, record.method, record.headers, null)
                                                                            .flatMap(tuple3 -> {
                                                                                // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                                                if (tuple3.var1 == SUCCESS || tuple3.var1 == DEL_SUCCESS) {
                                                                                    return Mono.just(true);
                                                                                }
                                                                                return Mono.just(false);
                                                                            });
                                                                });
                                                    } else {
                                                        //回收站点也没有 要么回收失败 或者回收站点里的对象再次被删掉了
                                                        return dealUnfinishedReqsRecord(record);
                                                    }
                                                });
                                    } else {
                                        return Mono.just(true);
                                    }
                                }
                            }

                            if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark) {
                                //对象被删除，需根据该记录是否是deleteMarker来判断
                                // 请求不带versionId参数且桶开了多版本，versionId才是“”。
                                if (StringUtils.isEmpty(record.versionId) || deleteSourceFlag) {
                                    //deleteMarker被删除或生成deleteMarker失败
                                    if (!record.commited) {
                                        return notifyOtherDealRecord(record);
                                    }
                                    //deleteMarker被删除,另一站点也无需再生成deleteMarker
                                    // 删除deleteMarker由另外带verionId的差异记录执行
                                    return Mono.just(true);
                                } else {
                                    //非deleteMarker且对象被删除
                                    record.headers.remove(NEW_VERSION_ID);
                                    return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object, record.uri, record.method, record.headers, null)
                                            .flatMap(tuple3 -> {
                                                // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                if (tuple3.var1 == NOT_FOUND) {
                                                    if ("NoSuchBucket".equals(tuple3.var2)) {
                                                        return Mono.just(true);
                                                    }
                                                }

                                                if ("NO_SUCH_OBJECT".equals(tuple3.var2)) {
                                                    if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                                        // 复制站点无要删除对象，可能双活站点还存在对象和上传差异未同步给复制站点
                                                        if (INDEX_STATUS_MAP.get(record.index) != 1) {
                                                            return Mono.just(false);
                                                        }
                                                        return MossHttpClient.getInstance().sendRequest(getOtherSiteIndex(), record.bucket, record.object, record.uri, record.method, record.headers,
                                                                null)
                                                                .flatMap(tup3 -> {
                                                                    // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                                    if (tup3.var1 == NOT_FOUND) {
                                                                        if ("NoSuchBucket".equals(tup3.var2)) {
                                                                            return Mono.just(true);
                                                                        }
                                                                    }
                                                                    if (tup3.var1 == SUCCESS && !"success_del".equals(tup3.var2)) {
                                                                        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object, record.uri, record.method,
                                                                                record.headers, null)
                                                                                .flatMap(tup -> {
                                                                                    // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                                                    if (tuple3.var1 == NOT_FOUND) {
                                                                                        if ("NoSuchBucket".equals(tuple3.var2)) {
                                                                                            return Mono.just(true);
                                                                                        }
                                                                                    }
                                                                                    if (tup3.var1 == SUCCESS) {
                                                                                        return Mono.just(true);
                                                                                    }
                                                                                    return Mono.just(false);
                                                                                });
                                                                    }
                                                                    return Mono.just(false);
                                                                });
                                                    }
                                                }
                                                return Mono.just(tuple3.var1 == SUCCESS);
                                            })
                                            .flatMap(res -> {
                                                if (StringUtils.isEmpty(record.headers.get("cleanObject")) || !res) {
                                                    return Mono.just(res);
                                                } else {
                                                    // 彻底删除回收站内对象
                                                    record.headers.put(SYNC_STAMP, syncStamp);
                                                    String url = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object.substring(trashDir.length()), "UTF-8");
                                                    return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object.substring(trashDir.length()), url, record.method,
                                                            record.headers, null)
                                                            .flatMap(tuple3 -> {
                                                                log.info("result success {}", tuple3);
                                                                if (tuple3.var1 == NOT_FOUND) {
                                                                    if ("NoSuchBucket".equals(tuple3.var2)) {
                                                                        return Mono.just(true);
                                                                    }
                                                                }
                                                                return Mono.just(tuple3.var1 == SUCCESS);
                                                            });
                                                }
                                            });
                                }
                            } else if (record.syncStamp.equals(metaData.syncStamp)) {
                                if (StringUtils.isEmpty(record.versionId)) {
                                    //生成deleteMarker
                                    Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                    });
                                    record.headers.put(ID, sysMap.get("owner"));
                                    record.headers.put(USERNAME, sysMap.get("displayName"));
                                    return MossHttpClient.getInstance().sendSyncRequest(record);
                                } else if (!record.commited) {
                                    //未提交状态，且对象未删除，通知其他站点处理
                                    return notifyOtherDealRecord(record);
                                } else {
                                    if (deleteSourceFlag) {
                                        record.headers.remove(NEW_VERSION_ID);
                                        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object, record.uri, record.method, record.headers, null)
                                                .flatMap(tuple3 -> {
                                                    // 本站点没有该对象且发送给对站点返回无该桶，认为该对象已不存在
                                                    if (tuple3.var1 == NOT_FOUND) {
                                                        if ("NoSuchBucket".equals(tuple3.var2)) {
                                                            return Mono.just(true);
                                                        }
                                                    }
                                                    return Mono.just(tuple3.var1 == SUCCESS);
                                                })
                                                .flatMap(res -> {
                                                    if (StringUtils.isEmpty(record.headers.get("cleanObject")) || !res) {
                                                        return Mono.just(res);
                                                    } else {
                                                        record.headers.put(SYNC_STAMP, syncStamp);
                                                        String url = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object.substring(trashDir.length()), "UTF-8");
                                                        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket, record.object.substring(trashDir.length()), url, record.method,
                                                                record.headers, null)
                                                                .flatMap(tuple3 -> {
                                                                    log.info("result success {}", tuple3);
                                                                    if (tuple3.var1 == NOT_FOUND) {
                                                                        if ("NoSuchBucket".equals(tuple3.var2)) {
                                                                            return Mono.just(true);
                                                                        }
                                                                    }
                                                                    return Mono.just(tuple3.var1 == SUCCESS);
                                                                });
                                                    }
                                                });
                                    }
                                    return Mono.just(true);
                                }
                            } else {
                                //对象被覆盖让覆盖操作处理
                                return Mono.just(true);
                            }
                        })
                        .doOnError(log::error);
            }
            case ERROR_INIT_PART_UPLOAD: {
                String uploadId = record.headers.get(UPLOAD_ID);
                String bucketVnode = bucketVnodeId;
                return bucketPool.mapToNodeInfo(bucketVnode)
                        .flatMap(nodeList -> PartClient.getInitPartInfo(record.bucket, record.object, record.headers.get(UPLOAD_ID), nodeList, null))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(initPartInfo -> {
                            if (initPartInfo.equals(ERROR_INIT_PART_INFO)) {
                                return Mono.just(false);
                            }

                            // todo f 如果唯一一个有initPartInfo的节点返回了error，会有问题。
                            if (initPartInfo.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                return bucketPool.mapToNodeInfo(bucketVnode)
                                        .flatMap(nodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object, record.versionId, nodeList))
                                        .publishOn(SCAN_SCHEDULER)
                                        .flatMap(metaData -> {
                                            if (ERROR_META.equals(metaData)) {
                                                return Mono.just(false);
                                            }
                                            if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark
                                                    || StringUtils.isBlank(metaData.partUploadId) || !metaData.partUploadId.equals(uploadId)) {
                                                if (!record.commited) {
                                                    return notifyOtherDealRecord(record).doOnNext(b -> {
                                                        if (b && isSyncLog) {
                                                            log.info("success sync: {}", record.getRecordKey());
                                                        }
                                                    });
                                                }
                                                log.info("init meta not found ,delete record! obj:{}, record uploadId {} ,curMeta uploadId: {}", record.object, uploadId, metaData.partUploadId);
                                                return Mono.just(true).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            }
                                            if (StringUtils.isNotBlank(metaData.partUploadId) && metaData.partUploadId.equals(uploadId)) {
                                                setMetaData(record, metaData, true);
                                                record.headers.put(ORIGIN_INDEX_CRYPTO, metaData.getCrypto() == null ? "" : metaData.getCrypto());
                                                return DataSyncHandler.recoverInitPart(record).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            }

                                            return Mono.just(true).doOnNext(b -> {
                                                if (b && isSyncLog) {
                                                    log.info("success sync: {}", record.getRecordKey());
                                                }
                                            });
                                        });
                            } else {
                                record.headers.put("Initiated", initPartInfo.metaData.stamp);
                                // 设置分段对象的worm保护期
                                if (StringUtils.isNotEmpty(initPartInfo.metaData.sysMetaData)) {
                                    Map<String, String> sysMap = Json.decodeValue(initPartInfo.metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                    });
                                    if (StringUtils.isNotEmpty(sysMap.get(EXPIRATION))) {
                                        record.headers.put(SYNC_WORM_EXPIRE, sysMap.get(EXPIRATION));
                                    }
                                    record.headers.put(ORIGIN_INDEX_CRYPTO, initPartInfo.metaData.getCrypto() == null ? "" : initPartInfo.metaData.getCrypto());
                                    record.headers.put(ID, initPartInfo.initAccount);
                                    record.headers.put(USERNAME, initPartInfo.initAccountName);
                                }
                                return DataSyncHandler.recoverInitPart(record).doOnNext(b -> {
                                    if (b && isSyncLog) {
                                        log.info("success sync: {}", record.getRecordKey());
                                    }
                                });
                            }
                        });
            }
            case ERROR_PART_UPLOAD: {
                String uploadId = getOringinUploadId(record);
                String partNum = params.get(PART_NUMBER);
                String bucketVnode = bucketVnodeId;
                return bucketPool.mapToNodeInfo(bucketVnode)
                        .flatMap(nodeList -> PartClient.getInitPartInfo(record.bucket, record.object, uploadId, nodeList, null))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(initPartInfo -> {
                            if (initPartInfo.equals(ERROR_INIT_PART_INFO)) {
                                return Mono.just(false);
                            }
                            record.headers.remove(X_AMZ_COPY_SOURCE);
                            if (initPartInfo.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                //无init元数据，查看是否合并
                                return bucketPool.mapToNodeInfo(bucketVnode)
                                        .flatMap(nodeList -> getMetaDataByUpload(record, uploadId, nodeList))
                                        .publishOn(SCAN_SCHEDULER)
                                        .flatMap(metaData -> {
                                            if (ERROR_META.equals(metaData)) {
                                                return Mono.just(false);
                                            }
                                            if (metaData.sysMetaData == null) {
                                                return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            }
                                            if (StringUtils.isNotBlank(metaData.partUploadId) && metaData.partUploadId.equals(uploadId)) {
                                                //分段对象已合并
                                                Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                                });
                                                PartInfo[] partInfos = metaData.partInfos;
                                                for (PartInfo info : partInfos) {
                                                    if (info.getPartNum().equals(partNum)) {
                                                        String md5 = encodeMd5(info.getEtag());
                                                        record.getHeaders().put(CONTENT_MD5, md5);
                                                        record.headers.put(ID, sysMap.get("owner"));
                                                        record.headers.put(USERNAME, sysMap.get("displayName"));
                                                        info = StringUtils.isEmpty(info.storage) ? info.setStorage(metaData.storage) : info;
                                                        return DataSyncHandler.recoverUploadPart(record, info).doOnNext(b -> {
                                                            if (b && isSyncLog) {
                                                                log.info("success sync: {}", record.getRecordKey());
                                                            }
                                                        });
                                                    }
                                                }
                                                return Mono.just(true).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            } else {
                                                return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            }
                                        });
                            } else {
                                //拿到了旧的partNum，下一次getMeta的时候旧的合并了
                                return PartClient.getPartInfo(record.bucket, record.object, uploadId, partNum)
                                        .publishOn(SCAN_SCHEDULER)
                                        .flatMap(partInfo -> {
                                            if (partInfo.equals(ERROR_PART_INFO)) {
                                                return Mono.just(false);
                                            }
                                            if (partInfo.equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO) ||
                                                    partInfo.equals(PartInfo.NOT_FOUND_PART_INFO) || partInfo.delete) {
                                                //分段任务刚被合并或者被终止
                                                return bucketPool.mapToNodeInfo(bucketVnode)
                                                        .flatMap(nodeList -> PartClient.getInitPartInfo(record.bucket, record.object, uploadId, nodeList, null))
                                                        .flatMap(initInfo -> {
                                                            if (initInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO) ||
                                                                    initInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initInfo.delete) {
                                                                //刚被合并或终止,走已合并流程
                                                                return Mono.just(false);
                                                            } else {
                                                                return bucketPool.mapToNodeInfo(bucketVnode)
                                                                        .flatMap(nodeList -> getMetaDataByUpload(record, uploadId, nodeList))
                                                                        .flatMap(metaData -> {
                                                                            if (ERROR_META.equals(metaData)) {
                                                                                return Mono.just(false);
                                                                            }
                                                                            if (uploadId.equals(metaData.partUploadId)) {
                                                                                if (metaData.sysMetaData == null) {
                                                                                    return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                                                                        if (b && isSyncLog) {
                                                                                            log.info("success sync: {}", record.getRecordKey());
                                                                                        }
                                                                                    });
                                                                                }
                                                                                Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                                                                });
                                                                                PartInfo[] partInfos = metaData.partInfos;
                                                                                for (PartInfo info : partInfos) {
                                                                                    if (info.getPartNum().equals(partNum)) {
                                                                                        String md5 = encodeMd5(info.getEtag());
                                                                                        record.headers.put(ID, sysMap.get("owner"));
                                                                                        record.headers.put(USERNAME, sysMap.get("displayName"));
                                                                                        record.getHeaders().put(CONTENT_MD5, md5);
                                                                                        info = StringUtils.isEmpty(info.storage) ? info.setStorage(metaData.storage) : info;
                                                                                        return DataSyncHandler.recoverUploadPart(record, info).doOnNext(b -> {
                                                                                            if (b && isSyncLog) {
                                                                                                log.info("success sync: {}", record.getRecordKey());
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                                return Mono.just(true).doOnNext(b -> {
                                                                                    if (b && isSyncLog) {
                                                                                        log.info("success sync: {}", record.getRecordKey());
                                                                                    }
                                                                                });
                                                                            }
                                                                            return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                                                                if (b && isSyncLog) {
                                                                                    log.info("success sync: {}", record.getRecordKey());
                                                                                }
                                                                            });
                                                                        });

                                                            }
                                                        });
                                            } else if (!record.syncStamp.equals(partInfo.syncStamp)) {
                                                //分段任务被覆盖
                                                return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            } else {
                                                String md5 = encodeMd5(partInfo.getEtag());
                                                record.getHeaders().put(CONTENT_MD5, md5);
                                                record.headers.put(ID, initPartInfo.initAccount);
                                                record.headers.put(USERNAME, initPartInfo.initAccountName);
                                                partInfo = StringUtils.isEmpty(partInfo.storage) ? partInfo.setStorage(initPartInfo.storage) : partInfo;
                                                return DataSyncHandler.recoverUploadPart(record, partInfo).doOnNext(b -> {
                                                    if (b && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                            }
                                        });
                            }
                        });
            }
            case ERROR_PART_ABORT: {
                String uploadId = getOringinUploadId(record);
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(nodeList -> PartClient.getInitPartInfo(record.bucket, record.object, uploadId, nodeList, null))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(initPartInfo -> {
                            if (initPartInfo.equals(ERROR_INIT_PART_INFO)) {
                                return Mono.just(false);
                            }
                            if (initPartInfo.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                Mono<String> mono = MonoProcessor.create();
                                if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                    mono = ErasureClient.getTargetUploadId(record.bucket, record.afterInitRecordKey());
                                } else {
                                    mono = Mono.just(uploadId);
                                }

                                return mono
                                        .flatMap(targetUploadId -> {
                                            if (targetUploadId.equals("-1") || targetUploadId.equals("-2")) {
                                                return Mono.just(targetUploadId);
                                            }
                                            // 往非moss的集群同步时使用AFTER_INIT_RECORD里的uploadId。
                                            if (!uploadId.equals(targetUploadId)) {
                                                String partUploadUri = "/" + UrlEncoder.encode(record.bucket, "UTF-8")
                                                        + "/" + UrlEncoder.encode(record.object, "UTF-8")
                                                        + "?uploadId=" + targetUploadId;
                                                record.setUri(partUploadUri);
                                            }
                                            return Mono.just("1");
                                        }).flatMap(res -> {
                                            if (res.equals("1")) {
                                                return MossHttpClient.getInstance().sendSyncRequest(record)
                                                        .doOnNext(b -> {
                                                            if (b && EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                                                deleteUnsyncRecord(record.bucket, record.afterInitRecordKey(), null, "null");
                                                            }
                                                        });
                                            } else if ("-1".equals(res)) {
                                                // minio复制获取targetUploadId失败
                                                return Mono.just(false);
                                            } else {
                                                // minio复制获取targetUploadId返回not_found，表示已经merge或abort，删除本条记录
                                                log.info("ABORT: targetUploadId is not found. delete. uploadId:{}, uri:{}", uploadId, record.uri);
                                                return Mono.just(true);
                                            }
                                        });

                            } else {
                                if (!record.commited) {
                                    return notifyOtherDealRecord(record);
                                }
                                return Mono.just(true);
                            }
                        });
            }
            case ERROR_COMPLETE_PART: {
                String uploadId = getOringinUploadId(record);
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(nodeList -> getMetaDataByUpload(record, uploadId, nodeList))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(metaData -> {
                            if (MetaData.ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }
                            if (StringUtils.isNotBlank(metaData.partUploadId) && metaData.partUploadId.equals(uploadId)) {
                                if (metaData.sysMetaData == null) {
                                    return dealUnfinishedReqsRecord(record).doOnNext(b -> {
                                        if (b && isSyncLog) {
                                            log.info("success sync: {}", record.getRecordKey());
                                        }
                                    });
                                }
                                setMetaData(record, metaData, true);
                                CompleteMultipartUpload completeMultipartUpload = new CompleteMultipartUpload();
                                List<Part> parts = new ArrayList<>(metaData.partInfos.length);
                                PartInfo[] partInfos = metaData.getPartInfos();
                                for (PartInfo info : partInfos) {
                                    Part part = new Part();
                                    part.setEtag('"' + info.getEtag() + '"');
                                    part.setPartNumber(Integer.valueOf(info.getPartNum()));
                                    parts.add(part);
                                }
                                if (parts.size() == 0) {
                                    return Mono.just(false);
                                }
                                completeMultipartUpload.setParts(parts);
                                Mono<String> mono = MonoProcessor.create();
                                if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                    mono = ErasureClient.getTargetUploadId(record.bucket, record.afterInitRecordKey());
                                } else {
                                    mono = Mono.just(uploadId);
                                }

                                return mono
                                        .flatMap(targetUploadId -> {
                                            if (targetUploadId.equals("-1") || targetUploadId.equals("-2")) {
                                                return Mono.just(false);
                                            }
                                            // 往非moss的集群同步时使用AFTER_INIT_RECORD里的uploadId。
                                            if (!uploadId.equals(targetUploadId)) {
                                                String partUploadUri = "/" + UrlEncoder.encode(record.bucket, "UTF-8")
                                                        + "/" + UrlEncoder.encode(record.object, "UTF-8")
                                                        + "?uploadId=" + targetUploadId;
                                                record.setUri(partUploadUri);
                                            }
                                            return Mono.just(true).doOnNext(b -> {
                                                if (b && isSyncLog) {
                                                    log.info("success sync: {}", record.getRecordKey());
                                                }
                                            });
                                        }).flatMap(res -> {
                                            if (res) {
                                                return DataSyncHandler.recoverCompletePart(record, JaxbUtils.toByteArray(completeMultipartUpload))
                                                        .flatMap(b -> checkIfDelAfterInitRecord(record, b)).doOnNext(b -> {
                                                            if (b && isSyncLog) {
                                                                log.info("success sync: {}", record.getRecordKey());
                                                            }
                                                        });
                                            } else {
                                                return isSynced(record, false)
                                                        .flatMap(b -> checkIfDelAfterInitRecord(record, b)).doOnNext(b -> {
                                                            if (b && isSyncLog) {
                                                                log.info("success sync: {}", record.getRecordKey());
                                                            }
                                                        });
                                            }
                                        });

                            } else if (!record.commited) {
                                return notifyOtherDealRecord(record).flatMap(b -> {
                                    // 如果既有双活又有minio，需要考虑after_init发往对端
                                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index) && b) {
                                        String bucketVnode = bucketPool.getBucketVnodeId(record.bucket);
                                        return bucketPool.mapToNodeInfo(bucketVnode)
                                                .publishOn(SCAN_SCHEDULER)
                                                .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(record.bucket, record.afterInitRecordKey(), null, nodeList, "null")).doOnNext(a -> {
                                                    if (a && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });
                                    }
                                    return Mono.just(b).doOnNext(a -> {
                                        if (a && isSyncLog) {
                                            log.info("success sync: {}", record.getRecordKey());
                                        }
                                    });
                                });
                            } else {
                                // 对象已被覆盖
                                return Mono.just(true).flatMap(b -> {
                                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                        String bucketVnode = bucketPool.getBucketVnodeId(record.bucket);
                                        return bucketPool.mapToNodeInfo(bucketVnode)
                                                .publishOn(SCAN_SCHEDULER)
                                                .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(record.bucket, record.afterInitRecordKey(), null, nodeList, "null")).doOnNext(a -> {
                                                    if (a && isSyncLog) {
                                                        log.info("success sync: {}", record.getRecordKey());
                                                    }
                                                });

                                    }
                                    return DataSyncHandler.abortObjRequest(record).doOnNext(a -> {
                                        if (a && isSyncLog) {
                                            log.info("success sync: {}", record.getRecordKey());
                                        }
                                    });
                                });
                            }
                        });
            }
            case ERROR_PUT_OBJECT_ACL: {
                if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                    return Mono.just(true);
                }
                String versionId = StringUtils.isNotEmpty(record.versionId) ? record.versionId : record.headers.get(VERSIONID);
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object, versionId, bucketVnodeList))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(metaData -> {
                            if (ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }

                            if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || !metaData.syncStamp.equals(record.syncStamp) || metaData.sysMetaData == null) {
                                if (!record.commited) {
                                    return notifyOtherDealRecord(record);
                                }
                                return Mono.just(true);
                            } else {
                                Map<String, String> stringMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                });
                                // 包含此key-value的对象不进行站点间同步
                                if (stringMap.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(stringMap.get(NO_SYNCHRONIZATION_KEY))) {
                                    return Mono.just(true);
                                }

                                String objectAcl = metaData.getObjectAcl();
                                record.headers.put(ACL_HEADER, objectAcl);
                                record.headers.put(SYNC_STAMP, record.syncStamp);
                                return MossHttpClient.getInstance().sendSyncRequest(record)
                                        .flatMap(b -> {
                                            if (!b) {
                                                // putobjacl失败可能是putobject还未成功，放到最后
                                                return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                                        .flatMap(nodelist -> DataSyncHandler.rewriteSyncRecord(bucketPool, record, nodelist));
                                            }
                                            return Mono.just(b);
                                        });
                            }
                        });
            }
            case ERROR_PUT_OBJECT_WORM: {
                String versionId = StringUtils.isNotEmpty(record.versionId) ? record.versionId : record.headers.get(VERSIONID);
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object, versionId, bucketVnodeList))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(metaData -> {
                            if (ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }
                            if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || !metaData.syncStamp.equals(record.syncStamp)) {
                                if (!record.commited) {
                                    return notifyOtherDealRecord(record);
                                }
                                return Mono.just(true);
                            } else {

                                Map<String, String> stringMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                });
                                // 包含此key-value的对象不进行站点间同步
                                if (stringMap.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(stringMap.get(NO_SYNCHRONIZATION_KEY))) {
                                    return Mono.just(true);
                                }

                                record.headers.put(SYNC_STAMP, record.syncStamp);
                                String sysMetaData = metaData.getSysMetaData();
                                Map<String, String> sysMap = Json.decodeValue(sysMetaData, new TypeReference<Map<String, String>>() {
                                });
                                record.headers.put(ID, sysMap.get("owner"));
                                record.headers.put(USERNAME, sysMap.get("displayName"));
                                String value = sysMap.get(EXPIRATION);
                                String mode = sysMap.getOrDefault("mode", "COMPLIANCE");
                                String date = MsDateUtils.stampToISO8601(value);
                                Retention retention = new Retention();
                                retention.setMode(mode);
                                retention.setRetainUntilDate(date);
                                return DataSyncHandler.recoverRequest(record, JaxbUtils.toByteArray(retention));
                            }
                        });
            }
            case ERROR_PUT_OBJECT_TAG:
            case ERROR_DEL_OBJECT_TAG: {
                if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                    return Mono.just(true);
                }
                String versionId = StringUtils.isNotEmpty(record.versionId) ? record.versionId : record.headers.get(VERSIONID);
                return bucketPool.mapToNodeInfo(bucketVnodeId)
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object, versionId, bucketVnodeList))
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(metaData -> {
                            if (ERROR_META.equals(metaData)) {
                                return Mono.just(false);
                            }

                            if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || !metaData.syncStamp.equals(record.syncStamp) || metaData.sysMetaData == null) {
                                if (!record.commited) {
                                    return notifyOtherDealRecord(record);
                                }
                                return Mono.just(true);
                            } else {
                                Map<String, String> stringMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                });
                                // 包含此key-value的对象不进行站点间同步
                                if (stringMap.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(stringMap.get(NO_SYNCHRONIZATION_KEY))) {
                                    return Mono.just(true);
                                }

                                String userMetaData = metaData.getUserMetaData();
                                record.headers.put(USER_METADATA_HEADER, userMetaData);
                                record.headers.put(SYNC_STAMP, record.syncStamp);
                                return MossHttpClient.getInstance().sendSyncRequest(record)
                                        .flatMap(b -> {
                                            if (!b) {
                                                // putobjacl失败可能是putobject还未成功，放到最后
                                                return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                                        .flatMap(nodelist -> DataSyncHandler.rewriteSyncRecord(bucketPool, record, nodelist));
                                            }
                                            return Mono.just(b);
                                        });
                            }
                        });
            }
            case ERROR_SYNC_DEL_OBJECTS:
                return Mono.just(true);
            default:
                //无匹配的日志类型，可能是无效请求，删除该条记录。
                log.error("no such record type");
                return Mono.just(true);
        }
    }

    private static Mono<Boolean> deleteSourceData(UnSynchronizedRecord record) {
        return deleteSourceByType(record)
                .flatMap(res -> {
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                    String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
                    if (res == 1) {
                        UnSynchronizedRecord r = buildDeleteRecord(record);
                        r.delRocksKey(r.versionNum);

                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(record.bucket)
                                .defaultIfEmpty(new HashMap<>())
                                .doOnNext(map -> {
                                    String versionStatus = map.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
                                    String newVersionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                                            "null" : RandomStringUtils.randomAlphanumeric(32);
                                    r.headers.put(NEW_VERSION_ID, newVersionId);
                                })
                                .flatMap(map -> {
                                    final long delSourceTime = Long.parseLong(map.getOrDefault(DELETE_SOURCE_TIME, "0"));
                                    String[] split = StringUtils.split(record.syncStamp, "-");
                                    final String timeStr = split[1].substring(19, 32);
                                    final long timeStamp = Long.parseLong(timeStr);
                                    if (timeStamp > delSourceTime) {
                                        return storagePool.mapToNodeInfo(bucketVnode).flatMap(nodeList -> updateSyncRecord(r, nodeList));
                                    } else {
                                        if (isDebug) {
                                            log.info("no need delete source ------ {}", r.getRecordKey());
                                        }
                                        return Mono.just(1);
                                    }
                                })
                                .map(i -> i != 2)
                                .doOnNext(c -> {
                                    if (isDebug) {
                                        log.info("updateSyncRecord delete source  {} ------ {}", c, r.getRecordKey());
                                    }
                                })
                                .doOnError(e -> log.error("deal bucket objMeta put record error, ", e))
                                .onErrorResume(c -> Mono.just(false));
                    } else if (res == 2) {
                        UnSynchronizedRecord newRecord = (UnSynchronizedRecord) record.clone();
                        // record根据字典排序
                        newRecord.headers.put(EXTRA_ASYNC_TYPE, TAPE_LIBRARY);
                        newRecord.updateRocksKey(getVersionNum(false), newRecord.type());
                        return storagePool.mapToNodeInfo(bucketVnode)
                                .flatMap(nodeList -> ErasureClient.updateAfterInitRecord(storagePool, newRecord.recordKey, Json.encode(newRecord), nodeList));
                    } else if (res == 3) {
                        UnSynchronizedRecord newRecord = (UnSynchronizedRecord) record.clone();
                        newRecord.headers.remove(EXTRA_ASYNC_TYPE);
                        return storagePool.mapToNodeInfo(bucketVnode)
                                .flatMap(nodeList -> {
                                    if (ERROR_COMPLETE_PART.equals(record.type())) {
                                        newRecord.method = HttpMethod.PUT;
                                        newRecord.uri = UrlEncoder.encode(File.separator + newRecord.bucket + File.separator + newRecord.object, "UTF-8");
                                        newRecord.updateRocksKey(newRecord.versionNum, ERROR_PUT_OBJECT);
                                    }
                                    return ErasureClient.updateAfterInitRecord(storagePool, newRecord.recordKey, Json.encode(newRecord), nodeList);
                                });
                    } else {
                        return Mono.just(false);
                    }
                })
                .doOnError(e -> log.error("deleteSourceData ", e))
                .onErrorResume(e -> Mono.just(false));
    }

    private static MonoProcessor<Integer> deleteSourceByType(UnSynchronizedRecord record) {
        MonoProcessor<Integer> resultProcessor = MonoProcessor.create();
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(EXTRA_CLUSTER_INFO)
                .defaultIfEmpty(new HashMap<>())
                .subscribe(map -> {
                    try {
                        if (map.isEmpty()) {
                            resultProcessor.onNext(1);
                        } else {
                            String type = map.get(EXTRA_ASYNC_TYPE);
                            if (TAPE_LIBRARY.equals(type)) {
                                pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, EXTRA_ASYNC_CLUSTER)
                                        .subscribe(index -> {
                                            final List<String> list = Arrays.asList(index.split(","));
                                            if (!list.contains(record.index.toString())) {
                                                resultProcessor.onNext(1);
                                                return;
                                            }

                                            if (!record.headers.containsKey(EXTRA_ASYNC_TYPE)) {
                                                resultProcessor.onNext(2);
                                                return;
                                            }
                                            int port = Integer.parseInt(map.get("tape_port"));
                                            String username = map.get("username");
                                            String password = map.get("password");

                                            String fileName = "/buckets/" + record.bucket + File.separator + record.object;
                                            log.debug("file {}", fileName);
                                            String encodedFileName = Base64.getEncoder().encodeToString(fileName.getBytes(StandardCharsets.UTF_8));
                                            String uri = "/api/v1/files/" + encodedFileName;
                                            JSONObject jsonObject = new JSONObject();
                                            jsonObject.put("username", username);
                                            jsonObject.put("password", password);
                                            String jsonStr = jsonObject.toJSONString();
                                            int contentLength = jsonStr.getBytes(StandardCharsets.UTF_8).length;
                                            requestToken(record.index, port, jsonStr, contentLength)
                                                    .flatMap(tuple2 -> {
                                                        if (StringUtils.isBlank(tuple2.var2())) {
                                                            return Mono.just(0);
                                                        }
                                                        return requestFileState(record.index, port, uri, tuple2.var1, tuple2.var2);
                                                    })
                                                    .doOnError(e -> {
                                                        log.error("deleteSourceByType1", e);
                                                        resultProcessor.onNext(0);
                                                    })
                                                    .subscribe(resultProcessor::onNext);
                                        }, error -> {
                                            log.error("deleteSourceByType3", error);
                                            resultProcessor.onNext(0);
                                        });
                            } else {
                                resultProcessor.onNext(1);
                            }
                        }
                    } catch (Exception e) {
                        log.error("deleteSourceByType2", e);
                        resultProcessor.onNext(0);
                    }
                }, error -> {
                    log.error("deleteSourceByType3", error);
                    resultProcessor.onNext(0);
                });

        return resultProcessor;
    }

    // 请求 token：发起 POST 请求到 /token，并返回 token 字符串
    private static Mono<Tuple2<Boolean, String>> requestToken(Integer index, int port, String tokenJsonStr, int contentLength) {
        return Mono.create(sink -> {
            try {
                String tokenStr = EXTRA_TAPE_LIBRARY_TOKEN_MAP.getOrDefault(index, "");
                if (StringUtils.isNotEmpty(tokenStr)) {
                    final String[] split = tokenStr.split("_", 2);
                    long nextTime = Long.parseLong(split[0]);
                    if (nextTime > System.currentTimeMillis()) {
                        sink.success(new Tuple2<>(false, split[1]));
                        return;
                    }
                }

                PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.POST, index, "/token", MossHttpClient.getClient());
                MsClientRequest request = poolingRequest.request1(port);
                request.putHeader(CONTENT_TYPE, "application/json");
                request.putHeader(CONTENT_LENGTH, String.valueOf(contentLength));
                request.setWriteQueueMaxSize(WriteQueueMaxSize);
                request.exceptionHandler(e -> {
                    log.error("POST /token error: " + e.getClass().getName() + ", " + e.getMessage());
                    request.reset();
                    sink.success(new Tuple2<>(true, "")); // 发生异常返回空 token
                });
                request.handler(response -> {
                    if (DataSynChecker.isDebug) {
                        log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}", request.getHost(), "/token", response.statusCode(), response.statusMessage());
                    } else {
                        log.info("ip:{}, uri:{}, statusCode:{}, msg:{}", request.getHost(), "/token", response.statusCode(), response.statusMessage());
                    }
                    if (response.statusCode() == SUCCESS) {
                        response.bodyHandler(body -> {
                            String bodyStr = body.toString(StandardCharsets.UTF_8);
                            log.info("Token Response: " + bodyStr);
                            try {
                                JSONObject tokenJson = JSON.parseObject(bodyStr);
                                String token = tokenJson.getString("data");
                                sink.success(new Tuple2<>(true, token == null ? "" : token));
                            } catch (Exception ex) {
                                log.error("Failed to parse token JSON", ex);
                                sink.success(new Tuple2<>(true, ""));
                            }
                        });
                    } else {
                        sink.success(new Tuple2<>(false, ""));
                    }
                });
                request.write(tokenJsonStr);
                request.end();
            } catch (Exception e) {
                log.error("requestToken error", e);
                sink.success(new Tuple2<>(true, ""));
            }
        });
    }

    // 请求文件状态：发起 GET 请求到 /api/v1/files/{encodedFileName}，返回 state 字符串
    private static Mono<Integer> requestFileState(Integer index, int port, String uri, Boolean updateToken, String token) {
        return Mono.create(sink -> {
            try {
                PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, index, uri, MossHttpClient.getClient());
                MsClientRequest getRequest = poolingRequest.request1(port);
                getRequest.putHeader(AUTHORIZATION, "Bearer " + token);
                getRequest.exceptionHandler(e -> {
                    log.error("GET " + uri + " error: " + e.getClass().getName() + ", " + e.getMessage());
                    getRequest.reset();
                    sink.success(0);
                });
                getRequest.handler(response -> {
                    if (DataSynChecker.isDebug) {
                        log.info("ip:{}, uri:{}, statusCode:{}, msg:{}", getRequest.getHost(), uri, response.statusCode(), response.statusMessage());
                    } else {
                        log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}", getRequest.getHost(), uri, response.statusCode(), response.statusMessage());
                    }
                    if (response.statusCode() == SUCCESS) {
                        if (updateToken) {
                            final long updateTime = System.currentTimeMillis() + 2 * 60 * 60 * 1000;
                            EXTRA_TAPE_LIBRARY_TOKEN_MAP.put(index, updateTime + "_" + token);
                        }
                        response.bodyHandler(body -> {
                            String bodyStr = body.toString(StandardCharsets.UTF_8);
                            if (isDebug) {
                                log.info("GET Response Body: " + bodyStr);
                            }
                            try {
                                JSONObject jsonObj = JSON.parseObject(bodyStr);
                                String dataMap = jsonObj.getString("data");
                                Map<String, Object> statusMap = JSON.parseObject(dataMap,
                                        new com.alibaba.fastjson.TypeReference<Map<String, Object>>() {
                                        });
                                String state = statusMap.get("state").toString();
                                if ("Offline".equals(state)) {
                                    sink.success(1);
                                } else if ("Online".equals(state)) {
                                    final String tapes = statusMap.get("tapes").toString();
                                    List<Object> tapesList = JSON.parseObject(tapes,
                                            new com.alibaba.fastjson.TypeReference<List<Object>>() {
                                            });
                                    if (tapesList.size() > 0) {
                                        sink.success(1);
                                    } else {
                                        sink.success(2);
                                    }
                                }
                            } catch (Exception ex) {
                                log.error("Failed to parse file state JSON", ex);
                                sink.success(0);
                            }
                        });

                    } else if (response.statusCode() == NOT_FOUND) {
                        sink.success(3);
                    } else {
                        if (response.statusCode() == UNAUTHORIZED) {
                            EXTRA_TAPE_LIBRARY_TOKEN_MAP.put(index, "");
                        }
                        sink.success(0);
                    }
                });
                getRequest.end();
            } catch (Exception e) {
                log.error("requestFileState error", e);
                sink.success(0);
            }
        });
    }

    private static Mono<Boolean> checkIfDelAfterInitRecord(UnSynchronizedRecord record, Boolean b) {
        if (!b) {
            return Mono.just(false);
        }
        if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
            String minioRewriteMerge = record.headers.get("minio-rewrite");
            if ("1".equals(minioRewriteMerge)) {
                // （minio复制）如果该条记录重新记入，则不删除相关的AFTER_INIT记录
                return Mono.just(b);
            }
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
            String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
            return storagePool.mapToNodeInfo(bucketVnode)
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(record.bucket, record.afterInitRecordKey(), null, nodeList, "null"));
        }
        return Mono.just(b);
    }

    private static Mono<MetaData> getMetaDataByUpload(UnSynchronizedRecord record, String uploadId, List<Tuple3<String, String, String>> nodeList) {
        if (StringUtils.isNotEmpty(record.versionId)) {
            return ErasureClient.getObjectMetaVersion(record.bucket, record.object, record.versionId, nodeList);
        }
        return ErasureClient.getObjectMetaByUploadId(record.bucket, record.object, uploadId, nodeList)
                .flatMap(metaData -> {
                    if (ERROR_META.equals(metaData)) {
                        return Mono.just(ERROR_META);
                    }
                    if (metaData.equals(MetaData.NOT_FOUND_META)) {
                        return Mono.just(MetaData.NOT_FOUND_META);
                    }
                    return ErasureClient.getObjectMetaVersion(record.bucket, record.object, metaData.versionId, nodeList)
                            .doOnNext(meta -> {
                                if (StringUtils.isNotEmpty(meta.versionId)) {
                                    record.setVersionId(meta.versionId).headers.put(VERSIONID, meta.versionId);
                                }
                            });
                });
    }

    public static String getUploadId(UnSynchronizedRecord record) {
        UnSynchronizedRecord.Type type = record.type();
        switch (type) {
            case ERROR_INIT_PART_UPLOAD:
                return record.headers.get(UPLOAD_ID);
            case ERROR_PART_UPLOAD:
            case ERROR_COMPLETE_PART:
            case ERROR_PART_ABORT:
                return RestfulVerticle.params(record.uri).get(URL_PARAM_UPLOADID);
            default:
                return null;
        }
    }

    private static Mono<? extends Boolean> dealUnfinishedReqsRecord(UnSynchronizedRecord record) {
        if (!record.commited) {
//            String stamp = record.headers.get("stamp");
//            // EC_version一秒钟+nodeAmount，客户端速度为1MB/s，此即为5G的对象上传完毕前后的EC_version的变化量。
//            int nodeNum = INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
//            long nodeAmount = UPLOAD_MAX_INTERVAL * nodeNum * 1000L;
//            // 没到时间的预提交记录暂不不处理
//            if (!record.commited && ((System.currentTimeMillis() - Long.parseLong(stamp)) * nodeNum < nodeAmount)) {
//                return Mono.just(false);
//            }

            //未提交，发送至其他站点判断
            return notifyOtherDealRecord(record);
        }
        return Mono.just(true);
    }

    /**
     * 预提交发送至对端进行delRecord操作。
     * 一些本地无法处理的消息，如修复数据时找不到对象，原因有可能是客户向本站点发送请求，但在本站点写入失败对面站点写入成功。此时需要将预提交的记录发往对站点，走对站点的异步复制流程。
     *
     * @param record 预提交日志
     * @return 对端处理结果
     */
    private static Mono<Boolean> notifyOtherDealRecord(UnSynchronizedRecord record) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY)
                .publishOn(SCAN_SCHEDULER)
                .switchIfEmpty(Mono.just("1"))
                .flatMap(policy -> {
                    // 异步复制环境或者该record.index属于async站点，不需要转发
                    if ("1".equals(policy)) {
                        return Mono.just(true);
                    } else if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                        //async站点 TODO 另一双活站点可能跟复制站点链路断开，此时暂不需转发
//                        int otherIndex = getOtherSiteIndex();
//                        if (OTHER_INDEXES_STATUS_MAP.containsKey(otherIndex)
//                                && OTHER_INDEXES_STATUS_MAP.get(otherIndex).get(record.index) == 0) {
//                            return Mono.just(false);
//                        }
                        record.setCommited(true);
                        record.setSuccessIndex(getOtherSiteIndex());
                        record.setSyncFlag(false);
                        return notifyOtherDealRecord(record, record.successIndex);
                    } else {
                        //双活
                        record.setCommited(true);
                        int index0 = record.index;
                        int successIndex0 = record.successIndex;
                        record.setIndex(LOCAL_CLUSTER_INDEX);
                        record.setSuccessIndex(getOtherSiteIndex());
                        record.setSyncFlag(false);
                        return notifyOtherDealRecord(record, record.successIndex)
                                .doOnNext(b -> {
                                    record.setIndex(index0);
                                    record.setSuccessIndex(successIndex0);
                                });
                    }
                });
    }

    private static Mono<Boolean> notifyOtherSiteDealRecord(UnSynchronizedRecord record) {
//        record.setCommited(true);
//        record.setSuccessIndex(getOtherSiteIndex());
        record.setSyncFlag(false);
        return notifyOtherDealRecord(record, record.successIndex);
    }

    /**
     * record发送至其他站点
     *
     * @param record    同步日志
     * @param siteIndex 其他站点index
     * @return 其他站点处理结果
     */
    private static Mono<Boolean> notifyOtherDealRecord(UnSynchronizedRecord record, int siteIndex) {
        Map<String, String> headers = new HashMap<>(3);
        headers.put(SYNC_AUTH, PASSWORD);
        headers.put(IS_SYNCING, "1");
        ArrayList<SyncRquest> syncRequests = new ArrayList<>();
        SyncRquest syncRquest = new SyncRquest();
        syncRquest.record = record;
        syncRquest.bucketSwitch = SWITCH_ON;
        syncRquest.deleteSource = SWITCH_OFF;
        syncRequests.add(syncRquest);
        String requestBody = Json.encode(syncRequests);
        return MossHttpClient.getInstance().sendSyncRequest(siteIndex, "", "", "?sync", HttpMethod.PUT
                , headers, requestBody.getBytes());
    }

    public static void setMetaData(UnSynchronizedRecord record, MetaData metaData, boolean needOriginUserMeta) {
        Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
        });
        Map<String, String> userMap = Json.decodeValue(metaData.getUserMetaData(), new TypeReference<Map<String, String>>() {
        });
        String md5 = encodeMd5(sysMap.get("ETag"));
        record.headers.put(CONTENT_MD5, md5);
        record.headers.put("Initiated", metaData.stamp);
        if (StringUtils.isNotEmpty(sysMap.get(EXPIRATION))) {
            record.headers.put(SYNC_WORM_EXPIRE, sysMap.get(EXPIRATION));
        }
        record.headers.put(ID, sysMap.get("owner"));
        record.headers.put(USERNAME, sysMap.get("displayName"));
        sysMap.entrySet().stream().filter(entry -> !entry.getKey().equals(CONTENT_LENGTH))
                .forEach(entry -> record.headers.put(entry.getKey(), entry.getValue()));
        if (needOriginUserMeta) {
            // 如果是非copy请求或者copy原对象用户元数据，则将原对象的用户元数据都放入headers
            userMap.forEach((key, value) -> record.headers.put(transferToIso8859(key), transferToIso8859(value)));
        }
        insertGrantAcl(record, metaData);
    }

    private static String transferToIso8859(String s) {
        try {
            return new String(s.getBytes(StandardCharsets.UTF_8), "iso8859-1");
        } catch (Exception e) {

        }
        return "";
    }

    /**
     * 所有站点最多开几个list
     */
    private static final int TOTAL_LIST_COUNT = 20;

    /**
     * 开启扫描的桶个数
     */
    public static final AtomicInteger listingCount = new AtomicInteger();

    private static final Map<Integer, AtomicInteger> INDEX_LISTCOUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 扫描待同步桶进行数据同步操作
     *
     * @param nodeList      桶对应nodelist
     * @param bucket        桶名
     * @param metaRocksScan 是否要扫描元数据所在的rocksDB。为了兼容老版本的异步复制扫描时选true。
     * @return 同步结果
     */
    private static Mono<Boolean> scannerRecorder(List<Tuple3<String, String, String>> nodeList, String bucket,
                                                 Integer clusterIndex, boolean metaRocksScan, boolean onlyDelete, boolean deleteSource) {

        MonoProcessor<Boolean> res = MonoProcessor.create();
        UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        SYNC_BUCKET_ERROR_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(bucket, k -> new ConcurrentHashSet<>());
//        runningMap.put(bucket, new ConcurrentHashSet<>());
        List<SocketReqMsg> msg = nodeList.stream().map(info -> {
            SocketReqMsg msg0 = new SocketReqMsg("", 0)
                    .put("bucket", bucket)
                    .put("clusterIndex", String.valueOf(clusterIndex));
            if (metaRocksScan) {
                msg0.put("lun", info.var2);
            } else {
                msg0.put("lun", MSRocksDB.getSyncRecordLun(info.var2));
            }
            if (deleteSource) {
                msg0.put("deleteSource", "1");
            }
            if (onlyDelete) {
                msg0.put("onlyDelete", "1");
            }
            return msg0;
        }).collect(Collectors.toList());


        final String[] curMarker = {UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, metaRocksScan)};
        long startMillis = System.currentTimeMillis();

        final long[] lastMillis = {startMillis};
        AtomicInteger count = new AtomicInteger();

        Disposable[] disposables = new Disposable[1];
        disposables[0] = listController.subscribeOn(SCAN_SCHEDULER)
                .doOnSubscribe(s -> {
                    INDEX_LISTCOUNT_MAP.computeIfAbsent(clusterIndex, k -> new AtomicInteger());
                    SCANNING_BUCKET_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                    if (!onlyDelete && !deleteSource) {
                        listingCount.incrementAndGet();
                        INDEX_LISTCOUNT_MAP.get(clusterIndex).incrementAndGet();
                        SCANNING_BUCKET_MAP.get(clusterIndex).add(bucket);
                    }
                })
                .timeout(Duration.ofSeconds(60))
                .doFinally(s -> {
                    if (onlyDelete) {
                        DELETE_DEAL_BUCKET_MAP.get(clusterIndex).remove(bucket);
                    } else if (deleteSource) {
                        DELETE_SOURCE_BUCKET_MAP.get(clusterIndex).remove(bucket);
                    } else {
                        SYNCING_BUCKET_MAP.get(clusterIndex).remove(bucket);
                        listingCount.decrementAndGet();
                        INDEX_LISTCOUNT_MAP.computeIfAbsent(clusterIndex, k -> new AtomicInteger()).decrementAndGet();
                        SCANNING_BUCKET_MAP.get(clusterIndex).remove(bucket);
                    }
                })
                .subscribe(marker -> {
                            // 十分钟重新开始一轮扫描
                            long curMillis = System.currentTimeMillis();
                            if (!MainNodeSelector.checkIfSyncNode() || syncSuspendMap.get(clusterIndex).get() ||
                                    curMillis - startMillis > 600_000) {
                                listController.onComplete();
                                return;
                            }

                            if (StringUtils.isBlank(marker)) {
                                if (deleteSource) {
                                    curMarker[0] = UnSynchronizedRecord.getRecorderPrefixLocal(bucket);
                                } else if (onlyDelete) {
                                    curMarker[0] = UnSynchronizedRecord.getOnlyDeletePrefix(bucket);
                                } else {
                                    curMarker[0] = UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, metaRocksScan);
                                }

                            } else {
                                curMarker[0] = marker;
                            }

                            int listNow = listingCount.get();
                            if (listNow <= 0) {
                                listNow = 1;
                            }

                            if (curMillis - lastMillis[0] >= 500) {
                                // 每五百毫秒清空计数
                                lastMillis[0] = curMillis;
                                count.set(0);
                            } else {
                                // 每五百毫秒list数量超过上限则延时处理
                                if (!onlyDelete && count.incrementAndGet() >= (TOTAL_LIST_COUNT / listNow)) {
                                    log.debug("Too many list. {}. delay {} list. clusterIndex: {}, old:{} ", listNow, bucket, clusterIndex, metaRocksScan);
                                    Mono.delay(Duration.ofMillis(500)).publishOn(SCAN_SCHEDULER).subscribe(s -> listController.onNext(curMarker[0]));
                                    return;
                                }
                            }


                            String[] markers = new String[nodeList.size()];
                            Arrays.fill(markers, curMarker[0]);

                            SyncRecordLimiter tpLimiter = getBucketLimiter(clusterIndex, bucket, DATASYNC_THROUGHPUT_QUOTA);
                            SyncRecordLimiter bwLimiter = getBucketLimiter(clusterIndex, bucket, DATASYNC_BAND_WIDTH_QUOTA);

                            // 如果存在异构复制站点，优先满足异构复制的限流。这个的前提是默认moss的性能更好，异构复制的配额更小。
                            if (hasExtraLimiter(clusterIndex, DATASYNC_THROUGHPUT_QUOTA) && getExtraLimiter(clusterIndex, DATASYNC_THROUGHPUT_QUOTA).reachLimit()
                                    || hasExtraLimiter(clusterIndex, DATASYNC_BAND_WIDTH_QUOTA) && getExtraLimiter(clusterIndex, DATASYNC_BAND_WIDTH_QUOTA).reachLimit()
                                    || hasSyncLimitation(tpLimiter) && tpLimiter.reachLimit()
                                    || hasSyncLimitation(bwLimiter) && bwLimiter.reachLimit()
                                    || countLimiter.reachLimit()
                                    || sizeLimiter.reachLimit()
                                    || TOTAL_SYNCING_AMOUNT.get() > DEFAULT_MAX_COUNT
                                    || TOTAL_SYNCING_SIZE.get() > DEFAULT_MAX_SIZE) {
                                log.debug("delay {} list. clusterIndex: {}, old:{} ", bucket, clusterIndex, metaRocksScan);
                                if (hasSyncLimitation(tpLimiter) && tpLimiter.reachLimit()) {
                                    log.debug("tpLimiter reachLimit. bucket {}, clusterIndex: {}, limit:{}, token {} ", bucket, clusterIndex, tpLimiter.getlimit(), tpLimiter.getTokensAmount());
                                }
                                if (hasSyncLimitation(bwLimiter) && bwLimiter.reachLimit()) {
                                    log.debug("bwLimiter reachLimit. bucket {}, clusterIndex: {}, limit:{}, token {} ", bucket, clusterIndex, bwLimiter.getlimit(), bwLimiter.getTokensAmount());
                                }

                                if (hasSyncLimitation(bwLimiter) && bwLimiter.reachLimit()) {
                                    int finalListNow = listNow;
                                    pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                                            .publishOn(SCAN_SCHEDULER)
                                            .defaultIfEmpty(new HashMap<>())
                                            .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(SYNC_QOS_RULE).defaultIfEmpty(new HashMap<>()))
                                            .subscribe(tuple2 -> {
                                                Map<String, String> syncQuosRuleMap = tuple2.getT2();

                                                // 将异步复制总配额纳入考量
                                                Flux<Long> syncBwFlux;
                                                // 因为listController.onComplete和onNext是异步的，可能出现final中代码块执行listingCount.derement但是subscribe有流程未结束的情况
                                                int indexListNow = INDEX_LISTCOUNT_MAP.get(clusterIndex).get();
                                                if (indexListNow <= 0) {
                                                    indexListNow = 1;
                                                }
                                                int finalIndexListNow = indexListNow;
                                                if (isCurrentTimeWithinRange(syncQuosRuleMap.get(START_TIME), syncQuosRuleMap.get(END_TIME))) {
                                                    syncBwFlux = DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA).map(l -> l / finalListNow)
                                                            .concatWith(DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA + "_" + clusterIndex).map(l -> l / finalIndexListNow))
                                                            .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_BAND_WIDTH_QUOTA))
                                                            .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_BAND_WIDTH_QUOTA + "_" + clusterIndex));
                                                } else {
                                                    syncBwFlux = Flux.just(0L);
                                                }

                                                syncBwFlux
                                                        .reduce((a, b) -> {
                                                            if (a == 0) {
                                                                return b;
                                                            }
                                                            if (b == 0) {
                                                                return a;
                                                            }
                                                            return Math.min(a, b);
                                                        })
                                                        .subscribe(bwQuota -> updateBucketSyncLimiter(clusterIndex, bucket, DATASYNC_BAND_WIDTH_QUOTA, bwQuota / 1024));
                                            });
                                }
                                Mono.delay(Duration.ofMillis(500)).publishOn(SCAN_SCHEDULER).subscribe(s -> listController.onNext(curMarker[0]));
                            } else {
                                Set<Disposable> disposableSet = new ConcurrentHashSet<>();
                                int finalListNow = listNow;
                                pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                                        .publishOn(SCAN_SCHEDULER)
                                        .defaultIfEmpty(new HashMap<>())
                                        .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(SYNC_QOS_RULE).defaultIfEmpty(new HashMap<>()))
                                        .subscribe(tuple2 -> {
                                            Map<String, String> bucketMap = tuple2.getT1();
                                            Map<String, String> syncQuosRuleMap = tuple2.getT2();
                                            // 如果扫描时桶被删了，直接跳过limiter的初始化，去处理记录（删除记录）
                                            if (bucketMap.isEmpty()) {
                                                listNext(listController, markers, msg, nodeList, disposableSet, clusterIndex, bucketMap);
                                                return;
                                            }
                                            String userId = bucketMap.get(BUCKET_USER_ID);

                                            // 将异步复制总配额纳入考量
                                            Flux<Long> syncTpFlux;
                                            Flux<Long> syncBwFlux;
                                            // 因为listController.onComplete和onNext是异步的，可能出现final中代码块执行listingCount.derement但是subscribe有流程未结束的情况
                                            int indexListNow = INDEX_LISTCOUNT_MAP.get(clusterIndex).get();
                                            if (indexListNow <= 0) {
                                                indexListNow = 1;
                                            }
                                            int finalIndexListNow = indexListNow;
                                            if (isCurrentTimeWithinRange(syncQuosRuleMap.get(START_TIME), syncQuosRuleMap.get(END_TIME))) {
                                                syncTpFlux = DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA).map(l -> l / finalListNow)
                                                        .concatWith(DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA + "_" + clusterIndex).map(l -> l / finalIndexListNow))
                                                        .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_THROUGHPUT_QUOTA))
                                                        .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_THROUGHPUT_QUOTA + "_" + clusterIndex));

                                                syncBwFlux = DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA).map(l -> l / finalListNow)
                                                        .concatWith(DataSyncPerfLimiter.getInstance().getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA + "_" + clusterIndex).map(l -> l / finalIndexListNow))
                                                        .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_BAND_WIDTH_QUOTA))
                                                        .concatWith(BucketPerfLimiter.getInstance().getQuota(bucket, DATASYNC_BAND_WIDTH_QUOTA + "_" + clusterIndex));
                                            } else {
                                                syncTpFlux = Flux.just(0L);
                                                syncBwFlux = Flux.just(0L);
                                            }

                                            // 分别查询该桶的性能配额、异步复制性能配额、相关账户的性能配额，获取最小值作为扫描时的限制
                                            // todo f 统一账户下如果存在很多待同步桶，且设置了账户配额，扫描速度会大于预期
                                            syncTpFlux
                                                    .reduce((a, b) -> {
                                                        if (a == 0) {
                                                            return b;
                                                        }
                                                        if (b == 0) {
                                                            return a;
                                                        }
                                                        return Math.min(a, b);
                                                    })
                                                    .doOnNext(tpQuota -> addNewBucketSyncLimiter(clusterIndex, bucket, DATASYNC_THROUGHPUT_QUOTA, tpQuota))
                                                    .flatMap(tpQuota -> syncBwFlux
                                                            .reduce((a, b) -> {
                                                                if (a == 0) {
                                                                    return b;
                                                                }
                                                                if (b == 0) {
                                                                    return a;
                                                                }
                                                                return Math.min(a, b);
                                                            }))
                                                    .doOnNext(bwQuota -> addNewBucketSyncLimiter(clusterIndex, bucket, DATASYNC_BAND_WIDTH_QUOTA, bwQuota / 1024))
                                                    .publishOn(SCAN_SCHEDULER)
                                                    .subscribe(s -> listNext(listController, markers, msg, nodeList, disposableSet, clusterIndex, bucketMap));
                                        });
                            }
                        },
                        cause -> {
                            if (cause instanceof TimeoutException) {
                                res.onNext(true);
                                return;
                            }

                            if (!"sync data error!".equals(cause.getMessage())) {
                                log.error("list record err, {} {}", bucket, clusterIndex, cause);
                            }
                            res.onError(cause);
                        },
                        () -> {
                            res.onNext(true);
                        });
        String marker = deleteSource ? UnSynchronizedRecord.getRecorderPrefixLocal(bucket) : UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, metaRocksScan);
        if (onlyDelete) {
            marker = UnSynchronizedRecord.getOnlyDeletePrefix(bucket);
        }
        listController.onNext(marker);
        return res;
    }

    /**
     * 以桶名为单位，保存扫描记录的rocksKey的hashcode。
     */
    public static final ConcurrentHashMap<String, ConcurrentHashSet<String>> recordSet = new ConcurrentHashMap<>();

    public static final Map<String, Set<String>> uploadIdRecordSet = new ConcurrentHashMap<>();

    public static final Map<String, Set<String>> initUploadRecordSet = new ConcurrentHashMap<>();

    public static final Map<String, Set<String>> mergeUploadRecordSet = new ConcurrentHashMap<>();

    public static final Map<String, AtomicLong> uploadCountSet = new ConcurrentHashMap<>();

    private static void listNext(UnicastProcessor<String> listController, String[] marker, List<SocketReqMsg> msgs,
                                 List<Tuple3<String, String, String>> nodeList, Set<Disposable> disposableSet,
                                 Integer clusterIndex, Map<String, String> bucketMap) {

        boolean bucketExists = bucketMap.containsKey(DATA_SYNC_SWITCH) && !SWITCH_CLOSED.equals(bucketMap.get(DATA_SYNC_SWITCH));
        boolean isFSbucket = bucketMap.containsKey("fsid");
        Iterator<SocketReqMsg> iterator = msgs.iterator();
        int i = 0;
        String bucketName = msgs.get(0).get("bucket");
        SyncRecordLimiter bucketLimiter = getBucketLimiter(clusterIndex, bucketName, DATASYNC_THROUGHPUT_QUOTA);
        long maxKey = MAX_COUNT;
        if (hasSyncLimitation(bucketLimiter)) {
            if (bucketLimiter.getlimit() > 0) {
                maxKey = Math.min(bucketLimiter.getlimit(), maxKey);
            }
        }
        while (iterator.hasNext()) {
            iterator.next().put("marker", marker[i++])
                    .put("maxKeys", maxKey + "");
        }
        TypeReference<Tuple2<String, UnSynchronizedRecord>[]> reference = new TypeReference<Tuple2<String, UnSynchronizedRecord>[]>() {
        };
        ClientTemplate.ResponseInfo<Tuple2<String, UnSynchronizedRecord>[]> responseInfo =
                ClientTemplate.oneResponse(msgs, LIST_SYNC_RECORD, reference, nodeList);
        ListSyncRecorderHandler clientHandler = new ListSyncRecorderHandler(responseInfo, nodeList, marker, bucketName, bucketExists);
        clientHandler.setMaxKey(maxKey);
        Disposable subscribe = responseInfo.responses.publishOn(SCAN_SCHEDULER)
                .subscribe(clientHandler::handleResponse, listController::onError, clientHandler::handleComplete);
        disposableSet.add(subscribe);

        Disposable[] disposables = new Disposable[1];
        long finalMaxKey = maxKey;
        disposables[0] = clientHandler.recordProcessor
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(recordList -> disposableSet.add(disposables[0]))
                .flatMap(recordList -> {
                    log.debug("cluster {} need deal recordList: {}, {}", clusterIndex, recordList.size(), bucketName);
                    if (clientHandler.isNextList() && !isFSbucket) {
                        listController.onNext(clientHandler.getNextMarker());
                    }
                    return recordHandler(recordList, bucketName, listController, disposableSet, clusterIndex, bucketMap);
                })
                .subscribe(b -> {
                    disposableSet.remove(disposables[0]);
                    disposableSet.remove(subscribe);
                    try {
                        boolean stop = false;
                        if (!b) {
                            log.info("This round all down. bucket:{}, cluster:{}, path: {}", bucketName, msgs.get(0).get("clusterIndex"), msgs.get(0).get("lun"));
                            stop = true;
                            listController.onError(new RejectedExecutionException("sync data error!"));
                            clearDisposables(disposableSet);
                        } else {
                            // 桶的同步数据开关暂停，扫描结果少于MAX_COUNT表示已扫描完毕，结束本轮扫描。
                            if (isSwitchSuspend(bucketMap) || clientHandler.getCount() < finalMaxKey || syncSuspendMap.get(clusterIndex).get()) {
                                stop = true;
                                listController.onComplete();
                            }
                        }
                        if (clientHandler.isNextList() && isFSbucket && !stop) {
                            listController.onNext("");
                        }
                        disposables[0].dispose();
                        subscribe.dispose();
                    } catch (Exception e) {
                        disposables[0].dispose();
                        log.error("listNext err1", e);
                    }
                }, e -> {
                    disposables[0].dispose();
                    log.error("listNext err, ", e);
                });

    }

    /**
     * 添加数据待同步桶。将记录从预提交改为已提交时，将相关的桶放入待同步的桶集合中。
     *
     * @param bucket 待同步桶名
     */
    public void addSyncBucket(Integer clusterIndex, String bucket) {
        try {
            if (SYNC_BUCKET_STATE_MAP.get(clusterIndex) != null && SYNC_BUCKET_STATE_MAP.get(clusterIndex).containsKey(bucket)) {
                return;
            }

            pool.getReactive(REDIS_BUCKETINFO_INDEX).exists(bucket)
                    .publishOn(SCAN_SCHEDULER)
                    .filter(bucketExists -> bucketExists == 1)
                    .map(b -> {
                        String status = BucketSyncSwitchCache.getInstance().get(bucket);
                        if (SWITCH_ON.equals(status)) {
                            updateBucSyncState(clusterIndex, bucket, SYNCED);
                        }
                        return status;
                    })
                    .flatMap(b -> pool.getReactive(REDIS_SYSINFO_INDEX).sismember(SYNC_BUCKET_SET, bucket).zipWith(Mono.just(b)))
                    .filter(tuple2 -> !tuple2.getT1() && !SWITCH_CLOSED.equals(tuple2.getT2()))
                    .publishOn(SCAN_SCHEDULER)
                    .doOnNext(b -> {
                        synchronized (bucket.intern()) {
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(SYNC_BUCKET_SET, bucket);
                        }
                    })
                    .doOnError(e -> log.error("addSyncBucket error, ", e))
                    .subscribe();
        } catch (Exception e) {
            log.error("addSyncBucket error, ", e);
        }
    }

    private static void syncStateCheckAsync() {
        String clusterStatus = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS);
        Map<Integer, Integer> clusterStatusMap = Json.decodeValue(clusterStatus, new TypeReference<Map<Integer, Integer>>() {
        });
        String syncStates = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE);
        Map<Integer, DATA_SYNC_STATE> clusterSyncMap = Json.decodeValue(syncStates, new TypeReference<Map<Integer, DATA_SYNC_STATE>>() {
        });

        boolean stateChanged = false;
        for (Integer index : INDEX_IPS_ENTIRE_MAP.keySet()) {
            syncSuspendMap.computeIfAbsent(index, b -> new AtomicBoolean());
            DATA_SYNC_STATE oldSyncState = clusterSyncMap.getOrDefault(index, SYNCED);
            //index站点链路断开时 同步中断；当前站点链路断开时 与其他节点的同步状态均中断
            if (0 == clusterStatusMap.getOrDefault(index, 1) || 0 == clusterStatusMap.getOrDefault(LOCAL_CLUSTER_INDEX, 1)) {
                currClusterSyncMap.put(index, INTERRUPTED);
                if (!INTERRUPTED.equals(oldSyncState)) {
                    stateChanged = true;
                    log.info("update cluster {} sync state from {} to {}", index, oldSyncState.name(), INTERRUPTED.name());
                }
                continue;
            }

            final String[] toUpdateState = {null};
            SYNC_BUCKET_STATE_MAP.get(index).forEach((bucketName, state) -> {
                if (StringUtils.isEmpty(toUpdateState[0]) || state.compareLevel(DATA_SYNC_STATE.valueOf(toUpdateState[0])) > 0) {
                    toUpdateState[0] = state.name();
                }
            });

            if (SYNC_BUCKET_STATE_MAP.get(index).size() == 0) {
                toUpdateState[0] = SYNCED.name();
            }

            if (syncSuspendMap.get(index).get()) {
                toUpdateState[0] = SUSPEND.name();
            }

            if (StringUtils.isEmpty(toUpdateState[0])) {
                continue;
            }

            DATA_SYNC_STATE toUpdateSyncState = DATA_SYNC_STATE.valueOf(toUpdateState[0]);
            currClusterSyncMap.put(index, toUpdateSyncState);
            if (!toUpdateSyncState.equals(oldSyncState)) {
                stateChanged = true;
                log.info("update cluster {} sync state from {} to {}", index, oldSyncState.name(), toUpdateState[0]);
            }
            checkIfCloseSync(index);
        }
        if (stateChanged) {
            DoubleActiveUtil.setSyncState();
        }
    }

    /**
     * 更新桶的同步状态
     */
    private static void updateBucSyncState(Integer clusterIndex, String bucket, DATA_SYNC_STATE state) {
        SYNC_BUCKET_STATE_MAP.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>());
        DATA_SYNC_STATE curState = SYNC_BUCKET_STATE_MAP.get(clusterIndex).get(bucket);
        if (state != curState) {
            synchronized (bucket.intern()) {
                String state1 = curState == null ? "null" : curState.name();
                log.info("updateBucSyncState {} from {} to {}. clusterIndex: {},  timeStamp:{} ", bucket, state1, state.name(), clusterIndex, System.currentTimeMillis());
                SYNC_BUCKET_STATE_MAP.get(clusterIndex).put(bucket, state);
            }
        }
    }

    public static void updateBucSyncState(String bucket) {
        INDEX_IPS_ENTIRE_MAP.keySet().forEach(index -> {
            if (!LOCAL_CLUSTER_INDEX.equals(index)) {
                DataSynChecker.updateBucSyncState(index, bucket, SYNCING);
            }
        });
    }

    public static boolean checkBucSyncState(String bucket, Integer index) {
        SYNC_BUCKET_STATE_MAP.computeIfAbsent(index, k -> new ConcurrentHashMap<>());
        DATA_SYNC_STATE syncState = SYNC_BUCKET_STATE_MAP.get(index).getOrDefault(bucket, SYNCED);
        return syncState != SYNCED;
    }

    public static String encodeMd5(String md5) {
        String MD5 = "";
        if (md5 != null && md5.contains("-")) {
            return "";
        }
        try {
            MD5 = Base64.getEncoder().encodeToString(Hex.decodeHex(md5));
        } catch (DecoderException e) {
            log.error("Encode md5 error, md5: " + md5, e);
        }
        return MD5;
    }


    /**
     * 将grant acl放入消息头
     *
     * @param record   grant acl
     * @param metaData 元数据
     */
    private static void insertGrantAcl(UnSynchronizedRecord record, MetaData metaData) {
        Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
        });
        StringBuilder sRead = new StringBuilder();
        StringBuilder sReadCap = new StringBuilder();
        StringBuilder sWriteCap = new StringBuilder();
        StringBuilder sFullControl = new StringBuilder();
        for (String key : objectAclMap.keySet()) {
            if ("1".equals(objectAclMap.get(key))) {
                sRead.append("id=").append(key.split("-")[1]).append(",");
            } else if ("2".equals(objectAclMap.get(key))) {
                sReadCap.append("id=").append(key.split("-")[1]).append(",");

            } else if ("4".equals(objectAclMap.get(key))) {
                sWriteCap.append("id=").append(key.split("-")[1]).append(",");
            } else if ("8".equals(objectAclMap.get(key))) {
                sFullControl.append("id=").append(key.split("-")[1]).append(",");
            }
        }

        if (StringUtils.isNotEmpty(sRead.toString())) {
            sRead.deleteCharAt(sRead.length() - 1);
            record.headers.put(PERMISSION_READ_LONG, sRead.toString());
        }
        if (StringUtils.isNotEmpty(sReadCap.toString())) {
            sReadCap.deleteCharAt(sReadCap.length() - 1);
            record.headers.put(PERMISSION_READ_CAP_LONG, sReadCap.toString());
        }
        if (StringUtils.isNotEmpty(sWriteCap.toString())) {
            sWriteCap.deleteCharAt(sWriteCap.length() - 1);
            record.headers.put(PERMISSION_WRITE_CAP_LONG, sWriteCap.toString());
        }
        if (StringUtils.isNotEmpty(sFullControl.toString())) {
            sFullControl.deleteCharAt(sFullControl.length() - 1);
            record.headers.put(PERMISSION_FULL_CON_LONG, sFullControl.toString());
        }

        int aclNum = Integer.parseInt(String.valueOf(objectAclMap.get("acl")));
        int acl = aclNum & OBJECT_PERMISSION_CLEAR_GRANT_NUM;
        String permission = null;
        if ((acl & OBJECT_PERMISSION_SHARE_READ_NUM) != 0) {
            permission = PERMISSION_SHARE_READ;
        } else if ((acl & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            permission = PERMISSION_SHARE_READ_WRITE;
        } else if ((acl & OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) != 0) {
            permission = PERMISSION_BUCKET_OWNER_READ;
        } else if ((acl & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0) {
            permission = PERMISSION_BUCKET_OWNER_FULL_CONTROL;
        } else if ((acl & OBJECT_PERMISSION_PRIVATE_NUM) != 0) {
            permission = PERMISSION_PRIVATE;
        }
        if (StringUtils.isNotEmpty(permission)) {
            record.headers.put("x-amz-acl", permission);
        }
    }

    /**
     * [recordSet响应的key，[放入时的versinNum，record]]
     */
    public static final Map<String, ConcurrentHashMap<String, Tuple2<Long, UnSynchronizedRecord>>> checkRecordMap = new ConcurrentHashMap<>();

    public static void checkIfRecordExpire() {
        for (Map.Entry<String, ConcurrentHashMap<String, Tuple2<Long, UnSynchronizedRecord>>> entry0 : checkRecordMap.entrySet()) {
            ConcurrentHashMap<String, Tuple2<Long, UnSynchronizedRecord>> map = entry0.getValue();
            for (Tuple2<Long, UnSynchronizedRecord> tuple2 : map.values()) {
                long listStamp = tuple2.var1;
                UnSynchronizedRecord record = tuple2.var2;
                // 将进入checkRecordMap超过时间的record释放。
                if (System.currentTimeMillis() - listStamp > Math.max(5 * 1024 * 1000L, 2 * timeoutMinute * 60 * 1000L)) {
                    log.info("release {}", record.object);
                    releaseSyncingPayload(record);
                }
            }
        }
        synchronized (recordSet) {
            Iterator<Map.Entry<String, ConcurrentHashMap<String, Tuple2<Long, UnSynchronizedRecord>>>> iterator = checkRecordMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, ConcurrentHashMap<String, Tuple2<Long, UnSynchronizedRecord>>> entry = iterator.next();
                if (entry == null) {
                    continue;
                }
                if (entry.getValue().isEmpty()) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * 原先是异步复制环境的站点B在部署站点C后历史数据同步流程走完且无差异记录需要同步时，添加标记，表示不再需要同步历史差异记录
     */
    static void checkIfCloseSync(int clusterIndex) {
        // 非待同步状态的站点跳出。
        if (ASYNC_SYNC_COMPLETE_MAP.getOrDefault(clusterIndex, new AtomicInteger(-1)).get() != 0) {
            return;
        }

        // 该站点差异记录未同步完成，就不进行。
        String hget = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE);
        // 防止还未写入CLUSTERS_SYNC_STATE的情况
        if (StringUtils.isBlank(hget)) {
            return;
        }
        Map<Integer, DATA_SYNC_STATE> syncStateMap = Json.decodeValue(hget, new TypeReference<Map<Integer, DATA_SYNC_STATE>>() {
        });
        if (syncStateMap.get(clusterIndex) != DATA_SYNC_STATE.SYNCED) {
            return;
        }

        // 历史数据同步该站点未完成，就不进行。
        Map<String, String> indexHisSyncMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(index_his_sync);
        // 历史同步同步状态和待历史同步的站点数量不匹配，表示有站点的历史同步还未初始化过。
        if (indexHisSyncMap.size() != CLUSTERS_AMOUNT - 1) {
            return;
        }

        if (!"1".equals(indexHisSyncMap.get(clusterIndex))) {
            return;
        }

        log.info("local async cluster stop DataSynChecker.");
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ASYNC_SYNC_COMPLETE_STR, String.valueOf(clusterIndex), "1");
        ASYNC_SYNC_COMPLETE_MAP.get(clusterIndex).set(1);
    }

    /**
     * 异步复制环境部署3DC用。有一个站点未完成async站点的同步，返回false
     */
    public static boolean allAsyncComplete() {
        for (AtomicInteger value : ASYNC_SYNC_COMPLETE_MAP.values()) {
            if (value.get() == 0) {
                return false;
            }
        }
        return true;
    }

}
