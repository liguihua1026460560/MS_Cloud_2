package com.macrosan.doubleActive.archive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.doubleActive.deployment.AddClusterUtils;
import com.macrosan.doubleActive.deployment.ListSyncObjectClientHandler;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.lifecycle.LifecycleConfiguration;
import com.macrosan.message.xmlmsg.lifecycle.Rule;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanIterator;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
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

import static com.macrosan.action.managestream.BucketLifecycleService.BUCKET_BACKUP_RULES;
import static com.macrosan.constants.ServerConstants.NEW_VERSION_ID;
import static com.macrosan.constants.ServerConstants.VERSIONID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.archive.ArchieveUtils.*;
import static com.macrosan.doubleActive.archive.ArchiveAnalyzer.ARCHIVE_ANALYZER_KEY;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.ARCHIVE_SUFFIX;

/**
 * 桶的归档功能。需要兼顾异步复制和异构灾备的情况：
 * 1.一个异步复制的站点B在挂载了异构灾备C的情况下，可以指定站点B的桶往A或者C归档。站点A只能往B归档。开了归档功能的桶不再生成异步复制的差异记录
 * 2.一个站点A挂载了异构灾备B的情况下，可以指定站点A的桶往C归档。
 * 每次启动的时间暂定每天零点。结束时间七点。
 * 桶开归档则默认开数据同步和删源
 */
@Log4j2
public class ArchiveHandler {
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    private static ArchiveHandler instance;

    public static ArchiveHandler getInstance() {
        if (instance == null) {
            instance = new ArchiveHandler();
        }
        return instance;
    }

    /**
     * 本节点是扫描节点，开始扫描时设为true，切扫描节点的时候设成false，用来中断所有扫描list操作
     */
    public static final Map<Integer, AtomicBoolean> archiveScanning = new ConcurrentHashMap<>();

    final static Map<Integer, ConcurrentHashMap<String, ConcurrentSkipListSet<String>>> SCANNING_CONDITION_SET = new ConcurrentHashMap<>();

    final static Map<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>> SCANNING_LAST_KEY_MAP = new ConcurrentHashMap<>();

    public static Map<Integer, AtomicLong> scanRound = new ConcurrentHashMap<>();

    private static final int RECORD_NUM = 100000;

    private static ScheduledFuture<?> scheduledFuture1;

    // 一个check周期内扫描多少次。
    public static int round = 1;

    public static UnicastProcessor<Tuple3<Integer, String, String>> scanBucketProcessor = UnicastProcessor.create(Queues.<Tuple3<Integer, String, String>>unboundedMultiproducer().get());
    public static AtomicInteger countScan = new AtomicInteger();
    final int maxScan = 10;

    public static final int processor_num = 12;
    public static UnicastProcessor<SyncRequest<MetaData>>[] processors = new UnicastProcessor[processor_num];

    public void init() {
        scanBucketProcessor
                .publishOn(SCAN_SCHEDULER)
                .filter(tuple3 -> archiveScanning.get(tuple3.var1).get())
                .filter(tuple3 -> {
                    if (countScan.incrementAndGet() > maxScan) {
                        countScan.decrementAndGet();
                        Mono.delay(Duration.ofSeconds(5)).publishOn(SCAN_SCHEDULER).subscribe(s -> scanBucketProcessor.onNext(tuple3));
                        return false;
                    }
                    return true;
                })
                .flatMap(tuple3 -> listLifeMeta(tuple3.var1, tuple3.var2, tuple3.var3).doFinally(s -> countScan.decrementAndGet())
                )
                .doOnError(e -> log.error("scanBucketProcessor err, ", e))
                .subscribe();

        for (int i = 0; i < processors.length; i++) {
            processors[i] = UnicastProcessor.create(Queues.<SyncRequest<MetaData>>unboundedMultiproducer().get());
            processors[i].publishOn(SCAN_SCHEDULER)
                    .doOnNext(syncRequest -> {
                        try {
                            Mono.just(syncRequest.index)
                                    .flatMap(clusterIndex -> {
                                        MetaData lifeMeta = syncRequest.payload;
                                        try {
                                            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(lifeMeta.bucket);
                                            UnSynchronizedRecord record;
                                            if (lifeMeta.deleteMarker) {
                                                record = AddClusterUtils.buildSyncRecord(clusterIndex, HttpMethod.DELETE, lifeMeta, true);
                                                record.headers.put(NEW_VERSION_ID, lifeMeta.versionId);
                                                record.versionId = null;
                                                record.headers.remove(VERSIONID);
                                            } else {
                                                record = AddClusterUtils.buildSyncRecord(clusterIndex, HttpMethod.PUT, lifeMeta, true);
                                                record.setUri(record.uri + "?syncHistory");
                                            }
                                            record.headers.put(ARCHIVE_SYNC_REC_MARK, "1");
                                            String analyzerKey = BACK_ANALYZER_MAP.get(syncRequest.backupKey);
                                            if (StringUtils.isBlank(analyzerKey)) {
                                                return Mono.just(false);
                                            }
                                            record.headers.put(ARCHIVE_ANALYZER_KEY, analyzerKey);
                                            String bucketVnode = metaPool.getBucketVnodeId(lifeMeta.bucket);

                                            return checkIfSyncRecordAlreadyExists(record, lifeMeta)
                                                    .subscribeOn(SCAN_SCHEDULER)
                                                    .flatMap(res -> {
                                                        if (res == 0) {
                                                            return metaPool.mapToNodeInfo(bucketVnode)
                                                                    .publishOn(SCAN_SCHEDULER)
                                                                    .flatMap(nodeList -> putHisSyncRecord(record, nodeList))
                                                                    .flatMap(b -> {
                                                                        if (b) {
                                                                            return ArchiveAnalyzer.getInstance().incrCount(analyzerKey, ArchiveAnalyzer.countX);
                                                                        } else {
                                                                            return Mono.just(false);
                                                                        }
                                                                    })
                                                                    .doOnError(e -> log.error("deal bucket objMeta put record error, ", e))
                                                                    .onErrorResume(b -> Mono.just(false));
                                                        } else if (res == 1) {
                                                            return Mono.just(true);
                                                        } else {
                                                            return Mono.just(false);
                                                        }
                                                    });
                                        } catch (Exception e) {
                                            log.error("dealObjSync err, {}, {},", lifeMeta.bucket, lifeMeta.key, e);
                                            return Mono.just(false);
                                        }
                                    })
                                    .doOnNext(b -> syncRequest.res.onNext(b))
                                    .doOnError(e -> {
                                        log.error("archiveProcessor, {}, {}", syncRequest.payload.bucket, syncRequest.payload.key, e);
                                        syncRequest.res.onNext(false);
                                    })
                                    .subscribe();

                        } catch (Exception e) {
                            log.error("archiveProcessor error1, {}", Json.encode(syncRequest.payload), e);
                            syncRequest.res.onNext(false);
                        }
                    })
                    .doOnError(e -> {
                        log.error("archiveProcessor error2, ", e);
                    })
                    .subscribe();
        }

        Optional.ofNullable(scheduledFuture1).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
        scheduledFuture1 = SCAN_TIMER.scheduleAtFixedRate(ArchiveHandler::startTime, 30, 120, TimeUnit.SECONDS);
        ArchiveAnalyzer.getInstance().init();
    }

    public static void startTime() {
        for (Integer clusterIndex : MossHttpClient.INDEX_IPS_ENTIRE_MAP.keySet()) {
            if (clusterIndex.equals(MossHttpClient.LOCAL_CLUSTER_INDEX) || SCANNING_CONDITION_SET.containsKey(clusterIndex)) {
                continue;
            }
            archiveScanning.computeIfAbsent(clusterIndex, k -> new AtomicBoolean());
            scanRound.computeIfAbsent(clusterIndex, k -> new AtomicLong());
            SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>());
            SCAN_SCHEDULER.schedule(() -> ArchieveUtils.checkArchiveTimeRotation(clusterIndex), 10, TimeUnit.SECONDS);
        }
    }

    public static class SyncRequest<T> {
        public T payload;
        public MonoProcessor<Boolean> res;
        public Integer index;
        public String backupKey;
    }

    public static final int checkTimeInterval = 30;

    // <bucket, Map<prefix, Set<Rule>>>，一个prefix可能有一个day和一个date相关的replication
    public static final ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> PREFIX_RULES_MAP = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> NONCURRENT_PREFIX_RULES_MAP = new ConcurrentHashMap<>();

    // <bucket, Map<tagJson, Set<Rule>>>
    public static final ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> TAG_RULES_MAP = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> NONCURRENT_TAG_RULES_MAP = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> BUCKET_RULES_MAP = new ConcurrentHashMap<>();

    public synchronized void start(int clusterIndex) {

        // 非主节点递归，预备切主。
        if (!MainNodeSelector.checkIfSyncNode()) {
            close(clusterIndex);
            return;
        }
        // 到达终止时间时停止，重新清空一次信息
        if (!archiveScanning.get(clusterIndex).get()) {
            close(clusterIndex);
            return;
        }
        lock();
        try {
            // 扫描表2所有的archive配置。
            TreeSet<String> existBucket = new TreeSet<>();
            ScanIterator<KeyValue<String, String>> iterator = ScanIterator.hscan(pool.getCommand(REDIS_SYSINFO_INDEX), BUCKET_BACKUP_RULES);
            while (iterator.hasNext()) {
                if (!archiveScanning.get(clusterIndex).get()) {
                    close(clusterIndex);
                    return;
                }
                KeyValue<String, String> next = iterator.next();
                String bucketName = next.getKey();
                String str = next.getValue();
                LifecycleConfiguration lifecycleConfiguration = (LifecycleConfiguration) JaxbUtils.toObject(str);

                if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName) == 0 || !datasyncIsOn(bucketName) || !archiveIsEnable(bucketName, clusterIndex)) {
                    SCANNING_CONDITION_SET.get(clusterIndex).remove(bucketName);
                    SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).remove(bucketName);
                    removeBucketRulesAll(clusterIndex, bucketName);
                    continue;
                }

                // 处理汇总桶的所有归档策略
                if (SCANNING_CONDITION_SET.get(clusterIndex).isEmpty() && putRuleMap(clusterIndex, bucketName, lifecycleConfiguration)) {
                    existBucket.add(bucketName);
                }
            }

            // 已存在的桶均已扫描完毕，开启下一轮扫描
            if (existBucket.isEmpty() && SCANNING_CONDITION_SET.get(clusterIndex).isEmpty()) {
                SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).clear();
                SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).clear();
                scanRound.get(clusterIndex).incrementAndGet();
                log.info("scan bucket set is empty in {}, round {}", clusterIndex, scanRound.get(clusterIndex).get());
            }

            if (scanRound.get(clusterIndex).get() >= round) {
                log.info("BucketScanChecker scan is completed in {}, round {}", clusterIndex, scanRound.get(clusterIndex).get());
                return;
            }

            // 不在bucketVnodeMap中的桶初始化EC
            for (String bucket : existBucket) {
                try {
                    String key = bucket + "_" + clusterIndex;
                    if (BUCKET_RULES_MAP.get(key) != null) {
                        final ConcurrentHashMap<String, String> map = BUCKET_RULES_MAP.get(key);
                        if (map != null && map.keySet().size() > 0) {
                            for (String backupRecord : map.keySet()) {
                                if (SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>())
                                        .computeIfAbsent(bucket, b -> new ConcurrentSkipListSet<>()).contains(backupRecord)) {
                                    continue;
                                }
                                SCANNING_CONDITION_SET.get(clusterIndex).get(bucket).add(backupRecord);
                                scanBucketProcessor.onNext(new Tuple3<>(clusterIndex, bucket, backupRecord));
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("checkExistBucketsRotation err, ", e);
                    SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).remove(bucket);
                    SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).remove(bucket);
                    removeBucketRulesAll(clusterIndex, bucket);
                }
            }

            SCAN_SCHEDULER.schedule(() -> start(clusterIndex), checkTimeInterval, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("startScanRotation err, ", e);
        } finally {
            unlock();
        }
    }

    /**
     * 清掉内存中所有归档策略。结束扫描的时候需要清掉，否则putRuleMap返回false，将无法开始下一轮扫描
     */
    static void removeBucketRules(int clusterIndex, String bucket, String backup) {
        if (BUCKET_RULES_MAP.get(bucket + "_" + clusterIndex) != null) {
            BUCKET_RULES_MAP.get(bucket + "_" + clusterIndex).remove(backup);
            BACK_ANALYZER_MAP.remove(backup);
        }
    }

    static void removeBucketRulesAll(int clusterIndex, String bucket) {
        PREFIX_RULES_MAP.remove(bucket + "_" + clusterIndex);
        TAG_RULES_MAP.remove(bucket + "_" + clusterIndex);
        NONCURRENT_PREFIX_RULES_MAP.remove(bucket + "_" + clusterIndex);
        NONCURRENT_TAG_RULES_MAP.remove(bucket + "_" + clusterIndex);
        BUCKET_RULES_MAP.remove(bucket + "_" + clusterIndex);
    }

    public void close(int clusterIndex) {
        lock();
        try {
            SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).clear();
            SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>()).clear();
            countScan.set(0);
            dealingAmount.set(0);
            scanRound.computeIfAbsent(clusterIndex, i -> new AtomicLong()).set(0);
            archiveScanning.computeIfAbsent(clusterIndex, i -> new AtomicBoolean()).compareAndSet(true, false);
            endStamp.put(clusterIndex, System.currentTimeMillis());
        } finally {
            unlock();
        }
    }

    static final int MAX_LIST_AMOUNT = 5000;

    //有几条obj同步请求被发起
    static AtomicInteger dealingAmount = new AtomicInteger();

    Tuple2<ConcurrentLinkedQueue<String>, Map<String, HashSet<Rule>>> preDealOrdering(int clusterIndex, String bucketName,
                                                                                      ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> rulesMap) {
        ConcurrentLinkedQueue<String> ordering = new ConcurrentLinkedQueue<>();
        // 一个prefix可能有一个day和一个date相关的replication
        Map<String, HashSet<Rule>> map = new HashMap<>();
        if (rulesMap.get(bucketName + "_" + clusterIndex) == null) {
            return new Tuple2<>(ordering, map);
        }
        for (String key : rulesMap.get(bucketName + "_" + clusterIndex).descendingKeySet()) {
            // 按prefix自然排序的逆序作为对象匹配归档策略的优先顺序。如aaa比a要先处理。
            ordering.add(key);
            for (Rule rule : rulesMap.get(bucketName + "_" + clusterIndex).get(key)) {
                map.computeIfAbsent(key, k -> new HashSet<>()).add(rule);
            }
        }
        return new Tuple2<>(ordering, map);
    }

    Mono<Boolean> listLifeMeta(Integer clusterIndex, String bucketName, String backupKey) {
        String key = bucketName + "_" + clusterIndex;
        final String param = BUCKET_RULES_MAP.get(key).get(backupKey);
        JSONObject jsonParam = JSON.parseObject(param);
        String filterType = jsonParam.getString("filterType");
        String prefixOrTag = jsonParam.getString("prefixOrTag");
        Map<String, String> tagMap = new HashMap<>();
        if ("TAG".equals(filterType)) {
            tagMap = JSONObject.parseObject(prefixOrTag, Map.class);
        }
        String version = jsonParam.getString("version");
        boolean isHistoryVersion = "history".equals(version);
        final String timeStr = jsonParam.getString("timeStr");
        long endStamp;
        Integer days = null;
        if (timeStr.startsWith("date_")) {
            endStamp = Long.parseLong(timeStr.split("_")[1]);
        } else {
            days = Integer.valueOf(timeStr.split("_")[1]);
            endStamp = Long.parseLong(timeStr.split("_")[2]);
        }
        SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>())
                .computeIfAbsent(bucketName, b -> new ConcurrentHashMap<>()).clear();
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);

        AtomicInteger vnodeCount = new AtomicInteger();
        String status = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS))
                .orElse("NULL");
        // 每轮扫描是同时开始每个bucketVnode的list，需要在每个bucketVnode的1000个均处理完成后，再开始下一轮扫描。
        for (String bucketVnode : bucketVnodeList) {
            AtomicLong findNum = new AtomicLong();
            // 扫描开始的位置为redis中记录的对象，若未记录则为0，从头扫描。
            int maxKey = 1000;
            String beginPrefix = getLifeCycleStamp(bucketVnode, bucketName, "0");
            String recordKey = getBackupRecord(bucketVnode, filterType, clusterIndex, tagMap, prefixOrTag, isHistoryVersion, endStamp, days);
            String lifecycleRecord = pool.getCommand(REDIS_TASKINFO_INDEX).hget(bucketName + BACKUP_RECORD, recordKey);
            AtomicBoolean restartSign = new AtomicBoolean(StringUtils.isNotEmpty(lifecycleRecord));
            if (restartSign.get()) {
                beginPrefix = lifecycleRecord;
            }
            AtomicInteger count = new AtomicInteger();
            UnicastProcessor<String> bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
            Map<String, String> finalTagMap = tagMap;
            log.info("cluster {} --------------------{} begin archive list Object  -------------{}----------{}--------{}------{}", clusterIndex, bucketName + "_" + bucketVnode, endStamp, version,
                    filterType, beginPrefix);
            bucketVnodeProcessor
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(recordPrefix ->
                            pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                    .filter(b -> {
                                        if (b > 0 || ShardingWorker.contains(bucketName)) {
                                            // 如果桶正在散列，在散列完成后从头开始扫描所有vnode。SERVER-985
                                            log.debug("has bucket sharding, {}", bucketName);
                                            SCANNING_CONDITION_SET.get(clusterIndex).get(bucketName).remove(backupKey);
                                            removeBucketRules(clusterIndex, bucketName, backupKey);
                                            res.onNext(true);
                                            bucketVnodeProcessor.onComplete();
                                            return false;
                                        }
                                        if (!archiveScanning.get(clusterIndex).get()) {
                                            log.info("Stop obj listing. {} {}", bucketName, bucketVnode);
                                            bucketVnodeProcessor.onComplete();
                                            return false;
                                        }

                                        if (dealingAmount.get() > MAX_LIST_AMOUNT) {
                                            log.debug("too many listing obj, {}", bucketName);
                                            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(recordPrefix));
                                            return false;
                                        }
                                        return true;
                                    })
                                    .map(b -> recordPrefix)
                    )
                    .subscribe(recordPrefix -> {
                        log.debug("scan bucket {} ------ {} {}", bucketName, recordPrefix, status);
                        SocketReqMsg reqMsg = new SocketReqMsg("", 0);
                        String stampMarker = endStamp + 1 + "";
                        reqMsg.put("bucket", bucketName)
                                .put("maxKeys", String.valueOf(maxKey))
                                // 列出的对象名不超过该stamp生成的rocksKey前缀
                                .put("stamp", stampMarker)
                                .put("retryTimes", "0")
                                .put("beginPrefix", recordPrefix)
                                .put("showUsermeta", "1")
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
                                            // 桶下无历史对象，直接设置redis中的状态
                                            log.info("scan all meta from bucket {} vnode {}", bucketName, bucketVnode);
                                            if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                                                SCANNING_CONDITION_SET.get(clusterIndex).get(bucketName).remove(backupKey);
                                                removeBucketRules(clusterIndex, bucketName, backupKey);
                                                final String record = SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>())
                                                        .computeIfAbsent(bucketName, b -> new ConcurrentHashMap<>()).get(recordKey);
                                                if (StringUtils.isNotBlank(record)) {
                                                    log.info("scan all meta2 from bucket " + bucketName + " mark {}  ------- {}", record, findNum.get());
                                                    Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(bucketName + BACKUP_RECORD, recordKey
                                                            , record));
                                                }
                                                log.info("scan bucket complete, {}, {}", bucketName, bucketVnodeList);
                                                res.onNext(true);
                                            }
                                            bucketVnodeProcessor.onComplete();
                                        } else {
                                            AtomicInteger preCount = new AtomicInteger(Math.min(maxKey, metaDataList.size()));
                                            AtomicInteger dealCount = new AtomicInteger();
                                            for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                                MetaData lifeMeta = metaDataList.get(i).getMetaData();
                                                String versionId = lifeMeta.getVersionId();
                                                if (lifeMeta.equals(MetaData.ERROR_META) || lifeMeta.equals(MetaData.NOT_FOUND_META)) {
                                                    checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res, findNum,
                                                            clusterIndex, backupKey, recordKey, count);
                                                    continue;
                                                }
                                                if (lifeMeta.deleteMark && StringUtils.isEmpty(lifeMeta.sysMetaData)) {
                                                    checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res, findNum,
                                                            clusterIndex, backupKey, recordKey, count);
                                                    continue;
                                                }
                                                if (restartSign.compareAndSet(true, false)) {
                                                    String record = getLifeCycleMetaKey(bucketVnode, lifeMeta.getBucket(), lifeMeta.key, lifeMeta.versionId, lifeMeta.stamp);
                                                    if (lifecycleRecord.equals(record)) {
                                                        log.info("continue record -------- {}", record);
                                                        checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res, findNum,
                                                                clusterIndex, backupKey, recordKey, count);
                                                        continue;
                                                    }
                                                }

                                                if ("TAG".equals(filterType)) {
                                                    MetaData metaData1 = metaPool.mapToNodeInfo(bucketVnode)
                                                            .flatMap(nodeList -> ErasureClient.getLifecycleMetaVersion(lifeMeta.bucket, lifeMeta.key, versionId, nodeList, null, lifeMeta.snapshotMark))
                                                            .timeout(Duration.ofSeconds(30)).block();

                                                    //判断自定义元数据不包含tags的对象不进行处理
                                                    if (metaData1 == null || metaData1.getJsonUserMetaData() == null || (metaData1.getJsonUserMetaData() != null && !isContain(getUserMetaMap(metaData1), finalTagMap)) || metaData1.isUnView(reqMsg.get("currentSnapshotMark"))) {
                                                        checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res, findNum,
                                                                clusterIndex, backupKey, recordKey, count);
                                                        continue;//未设置用户元数据的对象不参与分层处理
                                                    }
                                                } else {
                                                    if ((StringUtils.isNotEmpty(prefixOrTag) && !lifeMeta.key.startsWith(prefixOrTag)) || lifeMeta.isUnView(reqMsg.get("currentSnapshotMark"))) {
                                                        checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res, findNum,
                                                                clusterIndex, backupKey, recordKey, count);
                                                        continue;
                                                    }
                                                }

                                                //开启多版本
                                                if (!"NULL".equals(status)) {
                                                    if (isHistoryVersion) {
                                                        if (lifeMeta.latest) {
                                                            checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res,
                                                                    findNum,
                                                                    clusterIndex, backupKey, recordKey, count);
                                                            continue;
                                                        }
                                                    } else {
                                                        if (!lifeMeta.latest) {
                                                            checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res,
                                                                    findNum,
                                                                    clusterIndex, backupKey, recordKey, count);
                                                            continue;
                                                        }
                                                    }
                                                }

                                                SyncRequest<MetaData> syncRequest = new SyncRequest<>();
                                                syncRequest.payload = lifeMeta;
                                                syncRequest.res = MonoProcessor.create();
                                                syncRequest.index = clusterIndex;
                                                syncRequest.backupKey = backupKey;
                                                syncRequest.res
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .timeout(Duration.ofMinutes(5))
                                                        .doOnError(e -> log.error("bucket sync meta syncRequest res error1: {}, {}", bucketName, lifeMeta.key, e))
                                                        .doFinally(s -> {
                                                            dealingAmount.decrementAndGet();
                                                            syncRequest.res.dispose();
                                                            syncRequest.payload = null;
                                                            syncRequest.index = null;
                                                        })
                                                        .subscribe(b -> {
                                                            if (!b) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(recordPrefix));
                                                                return;
                                                            }
                                                            checkThisComplete(bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, res,
                                                                    findNum, clusterIndex, backupKey, recordKey, count);
                                                        }, e -> {
                                                            log.error("bucket sync meta syncRequest res error2: {}, {}", bucketName, lifeMeta.key, e);
                                                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(recordPrefix));
                                                        });
                                                dealingAmount.incrementAndGet();
                                                int index = ThreadLocalRandom.current().nextInt(processors.length);
                                                processors[index].onNext(syncRequest);
                                            }// one meta wrap
                                        }
                                    } catch (Exception e) {
                                        log.error("listBucketSyncObj Error1, {} ", bucketName, e);
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(recordPrefix));
                                    }
                                }, e -> {
                                    log.error("listBucketSyncObj Error2, {}", bucketName, e);
                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(recordPrefix));
                                });
                    }, e -> log.error("listBucketSyncObj Error, {}", bucketName, e));
            bucketVnodeProcessor.onNext(beginPrefix);
        }

        return res;
    }


    public static void checkThisComplete(UnicastProcessor<String> bucketVnodeProcessor, String bucketName, List<LifecycleClientHandler.Counter> metaDataList,
                                         AtomicInteger preCount, AtomicInteger dealCount, String bucketVnode, AtomicInteger
                                                 vnodeCount, List<String> bucketVnodeList, MonoProcessor<Boolean> res,
                                         AtomicLong findNum, int clusterIndex, String backupKey, String recordKey, AtomicInteger countNum) {
        if (dealCount.incrementAndGet() == preCount.get()) {
            if (metaDataList.size() > 1000) {
                MetaData metaData = metaDataList.get(1000).getMetaData();
                String recordPrefix = getLifeCycleMetaKey(bucketVnode, metaData.getBucket(), metaData.key, metaData.versionId, metaData.stamp);
                SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>())
                        .computeIfAbsent(bucketName, b -> new ConcurrentHashMap<>()).put(recordKey, recordPrefix);
                int num = countNum.addAndGet(metaDataList.size());
                if (num > RECORD_NUM && metaDataList.size() > 1000) {
                    log.debug("put backup record: {}", metaDataList.get(0).getMetaData().getKey());
                    putBackupRecord(bucketName, bucketVnode, recordKey, metaData);
                    countNum.set(0);
                }
                bucketVnodeProcessor.onNext(recordPrefix);
            } else {
                // 最后一个分片且桶下无历史对象，直接设置redis中的状态
                MetaData metaData = metaDataList.get(metaDataList.size() - 1).getMetaData();
                putBackupRecord(bucketName, bucketVnode, recordKey, metaData);
                SCANNING_LAST_KEY_MAP.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>())
                        .computeIfAbsent(bucketName, b -> new ConcurrentHashMap<>()).remove(recordKey);
                log.info("scan all meta2 from bucket " + bucketName + " mark {}  ------- {}", getLifeCycleMetaKey(bucketVnode, metaData.getBucket(), metaData.key, metaData.versionId,
                        metaData.stamp), findNum.get());
                if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                    SCANNING_CONDITION_SET.computeIfAbsent(clusterIndex, i -> new ConcurrentHashMap<>())
                            .computeIfAbsent(bucketName, b -> new ConcurrentSkipListSet<>()).remove(backupKey);
                    removeBucketRules(clusterIndex, bucketName, backupKey);
                    log.info("scan bucket complete2, {}, {}", bucketName, bucketVnodeList);
                    res.onNext(true);
                }
                bucketVnodeProcessor.onComplete();
            }
        }
    }
}
