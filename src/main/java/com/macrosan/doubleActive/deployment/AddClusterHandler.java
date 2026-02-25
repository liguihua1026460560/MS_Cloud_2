package com.macrosan.doubleActive.deployment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
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
import com.macrosan.storage.client.*;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
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
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_ON;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_SUSPEND;
import static com.macrosan.doubleActive.archive.ArchieveUtils.archiveIsEnable;
import static com.macrosan.doubleActive.deployment.AddClusterUtils.*;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.part.PartClient.LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.storage.client.ListSyncRecorderHandler.MAX_COUNT;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.ARCHIVE_SUFFIX;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * 已有站点的情况下添加新的站点。同步历史数据。
 * 多站点的非主站点，需要扫描successIndex为自己的差异记录以及预提交记录；主站点要扫描已有的对象、分段、successIndex不为自己的差异记录和预提交记录。
 * 扫描过程中出现主从站点的切换，不会改变扫描objMeta和part的站点。
 *
 * @author fanjunxi
 */
@Log4j2
public class AddClusterHandler {
    private static final RedisConnPool POOL = RedisConnPool.getInstance();
    /**
     * 保存待同步的clusterIndex
     */
    private static Set<Integer> needSyncClusters = new ConcurrentHashSet<>();

    private static UnicastProcessor<Tuple2<Integer, SyncRequest<MetaData>>>[] objProcessors = new UnicastProcessor[16];

    private static UnicastProcessor<Tuple3<Integer, String, SyncRequest<Upload>>>[] partProcessors = new UnicastProcessor[8];

    private static UnicastProcessor<Tuple3<Integer, Boolean, SyncRequest<UnSynchronizedRecord>>>[] recordProcessors = new UnicastProcessor[8];

    static final int MAX_LIST_AMOUNT = 5000;

    /**
     * 有几条obj同步请求被发起
     */
    static AtomicInteger dealingAmount = new AtomicInteger();

    /**
     * 存放各个站点索引下的DeployRecord。
     */
    static final Map<Integer, DeployRecord> deploysMap = new ConcurrentHashMap<>();

    private static final TypeReference<Map<Integer, DeployRecord>> deploysMapType = new TypeReference<Map<Integer, DeployRecord>>() {
    };

    /**
     * 表7中保存桶的一般对象和分段对象历史数据同步状态，格式为map[clusterIndex, status]，0为未同步，1为已同步。
     */
    static final String obj_his_sync_finished = "obj_his_sync_finished";

    static final String part_his_sync_finished = "part_his_sync_finished";

    static final String old_record_his_sync_finished = "old_record_his_sync_finished";

    static final String record_his_sync_finished = "record_his_sync_finished";

    private static final TypeReference<Map<Integer, Integer>> syncFinishedMapType = new TypeReference<Map<Integer, Integer>>() {
    };

    /**
     * 保存各桶的一般对象在各站点的同步状态。格式为Map[bucket, Map[clusterIndex, status]]
     */
    static Map<String, Map<Integer, Integer>> COM_BUCKETS_STATUS_MAP = new ConcurrentHashMap<>();

    static Map<String, Map<Integer, Integer>> PART_BUCKETS_STATUS_MAP = new ConcurrentHashMap<>();

    static Map<String, Map<Integer, Integer>> OLD_REC_BUCKETS_STATUS_MAP = new ConcurrentHashMap<>();

    static Map<String, Map<Integer, Integer>> REC_BUCKETS_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 保存待同步站点下的待同步桶。
     */
    static Map<Integer, Set<String>> COM_SYNCING_BUCKET = new ConcurrentHashMap<>();

    static Map<Integer, Set<String>> PART_SYNCING_BUCKET = new ConcurrentHashMap<>();

    static Map<Integer, Set<String>> OLD_REC_SYNCING_BUCKET = new ConcurrentHashMap<>();

    static Map<Integer, Set<String>> REC_SYNCING_BUCKET = new ConcurrentHashMap<>();


    /**
     * 存放于表2 master_cluster，历史数据同步的role，已经初始化无法更改。只有DA站点有该值。
     * 0与2过时。
     * 1表示初始化历史数据扫描时该站点为主站点，需要扫描objMeta、part、successIndex不为自己的差异记录、预提交记录
     * 3表示初始化历史数据扫描时该站点为从站点，需要扫描succssIndex为自己的差异记录、预提交记录。
     */
    private static String history_sync = "history_sync";

    /**
     * 存放于表2，map，key和value表示该索引的站点历史数据同步的状态。0表示该索引初始化完成正待扫描历史数据，1表示flush已完成
     */
    public static String index_his_sync = "index_his_sync";

    /**
     * [clusterIndex, status], status为0表示本站点未完成该站点的历史数据记录扫描。
     */
    static Map<Integer, Integer> INDEX_HIS_SYNC_MAP = new ConcurrentHashMap<>();

    /**
     * 存放于表2，set类型，初次历史数据扫描时写入所有的桶，之后再启动历史数据扫描只会在这些桶中进行。
     */
    private static String need_his_buckets = "need_his_buckets";

    /**
     * 本节点是扫描节点，开始扫描时设为true，切扫描节点的时候设成false，用来中断所有扫描list操作
     */
    private static AtomicBoolean has_data_syncing = new AtomicBoolean(false);

    /**
     * 同一时间只能有一个index进行历史数据的扫描初始化。
     */
    public static AtomicBoolean is_adding_clusters = new AtomicBoolean();

    /**
     * 历史数据同步开始前记录下所有的桶，这些桶都将进行同步。
     */
    private static Map<Integer, Set<String>> bucketsList = new ConcurrentHashMap<>();

    private static Map<Integer, ScheduledFuture> dataScanSchedule = new ConcurrentHashMap<>();

    /**
     * 保存桶和桶第一次尝试同步时的versionNum，用来判断是否所有请求都已经落盘，已经可以开始扫描（mossserver-7364）
     */
    static final Map<Integer, Map<String, String>> bucketSyncVerMap = new ConcurrentHashMap<>();

    /**
     * 防止桶都在等待确认时，flush结束
     */
    static final AtomicLong waitingBucketAmount = new AtomicLong();

    public static void init() {
        for (int i = 0; i < objProcessors.length; i++) {
            objProcessors[i] = UnicastProcessor.create(Queues.<Tuple2<Integer, SyncRequest<MetaData>>>unboundedMultiproducer().get());
            objProcessors[i].publishOn(SCAN_SCHEDULER)
                    .doOnNext(tuple2 -> dealObjMeta(tuple2.var1, tuple2.var2))
                    .doOnError(e -> log.error("dealObjMeta error, ", e))
                    .subscribe();
        }

        for (int i = 0; i < partProcessors.length; i++) {
            partProcessors[i] = UnicastProcessor.create(Queues.<Tuple3<Integer, String, SyncRequest<Upload>>>unboundedMultiproducer().get());
            partProcessors[i].publishOn(SCAN_SCHEDULER)
                    .doOnNext(tuple3 -> dealUpload(tuple3.var1, tuple3.var2, tuple3.var3))
                    .doOnError(e -> log.error("dealUpload error", e))
                    .subscribe();
        }

        for (int i = 0; i < recordProcessors.length; i++) {
            recordProcessors[i] = UnicastProcessor.create(Queues.<Tuple3<Integer, Boolean, SyncRequest<UnSynchronizedRecord>>>unboundedMultiproducer().get());
            recordProcessors[i].publishOn(SCAN_SCHEDULER)
                    .doOnNext(tuple3 -> dealRecord(tuple3.var1, tuple3.var2, tuple3.var3))
                    .doOnError(e -> log.error("dealRecord error", e))
                    .subscribe();
        }

        // 站点重启后重新开始历史数据同步
        checkAddClusterTimeStamp(-1);
    }

    public enum HIS_SYNC_ROLE {
        MASTER, SLAVE, ASYNC, NONE
    }

    static HIS_SYNC_ROLE role = HIS_SYNC_ROLE.NONE;

    /**
     * 设置下一次扫描任务的时间，不会中断已进行的任务。
     */
    private static void setSchedule(long delay, Integer index) {
        Optional.ofNullable(dataScanSchedule.get(index)).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
        dataScanSchedule.put(index, SCAN_TIMER.schedule(() -> checkAddClusterTimeStamp(index), delay, TimeUnit.MILLISECONDS));
    }

    /**
     * 监控添加站点操作。记录添加站点的时间戳（versionNum）。
     */
    public static void checkAddClusterTimeStamp(Integer clusterIndex) {
        if (is_adding_clusters.compareAndSet(false, true)) {
            setSchedule(10_000, clusterIndex);
            return;
        }
//        is_adding_clusters.compareAndSet(false, true);

        String localCluster = POOL.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        Map<String, String> masterClusterMap = POOL.getCommand(REDIS_SYSINFO_INDEX).hgetall(MASTER_CLUSTER);
        String masterClusterName = masterClusterMap.get(CLUSTER_NAME);
        String extraClusterStr = masterClusterMap.getOrDefault(EXTRA_ASYNC_CLUSTER, "-1");
        Integer extraClusterIndex = Integer.valueOf(extraClusterStr);
        Map<String, String> indexHisSyncStr = POOL.getCommand(REDIS_SYSINFO_INDEX).hgetall(index_his_sync);
        indexHisSyncStr.forEach((k, v) -> INDEX_HIS_SYNC_MAP.put(Integer.parseInt(k), Integer.parseInt(v)));
        if (DA_INDEX_IPS_ENTIRE_MAP.size() == 1) {
            // 单站点或只有复制站点时默认单站点为主站点
            masterClusterName = localCluster;
        }

        // 考虑到主站点切换，第一次进入历史数据同步后的站点0应该视为永远的“历史同步主站点”。
        boolean isMasterCluster = LOCAL_CLUSTER_INDEX == 0;

        if ("2".equals(masterClusterMap.get(history_sync))) {
            for (Integer index : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                if (index == LOCAL_CLUSTER_INDEX) {
                    continue;
                }
                INDEX_HIS_SYNC_MAP.put(index, 1);
                Mono.just("1").publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(index_his_sync, String.valueOf(index), "1"));
            }
        }

        // 所有非复制站点的扫描节点执行。
        if (DA_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX) && MainNodeSelector.checkIfSyncNode() && !masterClusterMap.isEmpty()) {
            if (StringUtils.isEmpty(masterClusterMap.get(history_sync)) || "0".equals(masterClusterMap.get(history_sync))
                    || "2".equals(masterClusterMap.get(history_sync))) {
                // 没有history_sync，说明未进行过历史数据同步。根据当前的主从情况设置history_sync。
                if (isMasterCluster) {
                    Mono.just("1").publishOn(SCAN_SCHEDULER)
                            .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, history_sync, "1"));
                    masterClusterMap.put(history_sync, "1");
                } else {
                    Mono.just("1").publishOn(SCAN_SCHEDULER)
                            .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, history_sync, "3"));
                    masterClusterMap.put(history_sync, "3");
                }
            }
        }

        if (!allAsyncComplete()) {
            Mono.just("1").publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, history_sync, "4"));
            masterClusterMap.put(history_sync, "4");
            log.info("need cluster B to sync, {}", clusterIndex);
        }

        if ("1".equals(masterClusterMap.get(history_sync))) {
            role = HIS_SYNC_ROLE.MASTER;
        } else if ("3".equals(masterClusterMap.get(history_sync))) {
            // 部署3DC和三复制时的站点B
            role = HIS_SYNC_ROLE.SLAVE;
        } else if ("4".equals(masterClusterMap.get(history_sync))) {
            // 先复制后部署3DC的站点B
            role = HIS_SYNC_ROLE.ASYNC;
        }

//        if (!masterClusterMap.isEmpty() && !localCluster.equals(masterClusterName) && MainNodeSelector.checkIfSyncNode()
//                && StringUtils.isEmpty(masterClusterMap.get(history_sync))) {
//            Mono.just("1").publishOn(SCAN_SCHEDULER)
//                    .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, history_sync, "2"));
//        }

        // 结束历史数据检测流程。
        if ((ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX) && allAsyncComplete())
                || StringUtils.isEmpty(masterClusterName)
                || allClusterFinished()) {
            log.info("local cluster is not multi-clusters, or history data of {} has synced finish", clusterIndex);
            Optional.ofNullable(dataScanSchedule.get(clusterIndex)).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
            is_adding_clusters.compareAndSet(true, false);
            RocksDBCheckPoint.closeAll(INDEX_HIS_SYNC_MAP);
            return;
        }
        // 非主节点递归，预备切主。
        if (!MainNodeSelector.checkIfSyncNode()) {
            has_data_syncing.compareAndSet(true, false);
            setSchedule(10_000, clusterIndex);
            log.debug("not master, keep going for possibly node switch.");
            return;
        }

        has_data_syncing.compareAndSet(false, true);

        // 第一个双活站点永远不需要别的站点去同步历史数据
        if (INDEX_HIS_SYNC_MAP.get(0) == null && LOCAL_CLUSTER_INDEX != 0) {
            Mono.just("1").publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(index_his_sync, "0", "1"));
        }

        Set<Integer> indexSet = new HashSet<>();
        if (clusterIndex == -1) {
            indexSet.addAll(INDEX_IPS_ENTIRE_MAP.keySet());
            indexSet.remove(LOCAL_CLUSTER_INDEX);
            INDEX_HIS_SYNC_MAP.forEach((k, v) -> {
                if (v == 1) {
                    indexSet.remove(k);
                }
            });
            // ipList的第一个DA站点永远不需要别的站点去同步。
            indexSet.remove(0);
        } else {
            indexSet.add(clusterIndex);
        }

        if (extraClusterIndex != -1) {
            indexSet.remove(extraClusterIndex);
        }

        if (indexSet.isEmpty()) {
            log.info("no cluster needs to sync history data");
            is_adding_clusters.compareAndSet(true, false);
            return;
        }

        Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(b -> {
            log.info("start history sync.");
            Flux.fromIterable(indexSet)
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(index -> {
                        // 双活部署3DC，站点的差异记录历史数据同步需要0站点checkPoint以后其他DA站点才可以checkPoint
                        if (role == HIS_SYNC_ROLE.MASTER) {
                            return Mono.just(index);
                        } else {
                            return checkMasterHisSyncStatus(index).map(aBoolean -> index);
                        }
                    })
                    .flatMap(index -> {
                        if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index) && DA_INDEX_IPS_ENTIRE_MAP.size() > 1) {
                            // 新加站点为async站点，且当前非async站点不止一个，主从都需要建立checkPoint。
                            return createCheckPoint(index);
                        } else if (IS_THREE_SYNC) {
                            // 三复制的主站点要扫描已有的对象、分段（不考虑预提交记录，因为如果成功上传必定能被元数据扫描扫到）；
                            //从站点需要建立checkpoint，防止同步过去的对象站点A扫描不到，差异记录又被删除了，而且还有新的差异记录生成。
                            if (role == HIS_SYNC_ROLE.SLAVE) {
                                return createCheckPoint(index);
                            }
                        } else {
                            // 先复制再部署3DC，主站点不用扫描差异记录；
                            // 从站点需要建立checkpoint，防止同步过去的对象站点A扫描不到，差异记录又被删除了。
                            if (role == HIS_SYNC_ROLE.ASYNC) {
                                return createCheckPoint(index);
                            }
                        }
                        return Mono.just(index);
                    })
                    // redis里是否有该索引已完成的记录。
                    .doOnNext(index ->
                            // 站点状态设置为历史数据待同步。
                            POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(index_his_sync, String.valueOf(index), "0"))
                    .doOnNext(index -> {
                        long timeStamp = System.currentTimeMillis() + 15 * 60_000L;
                        String str = POOL.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, DEPLOEY_RECORD);
                        if (StringUtils.isEmpty(str)) {
                            //初次检测到有新节点的加入，开始同步。计入redis。
                            DeployRecord deployRecord = new DeployRecord()
                                    .setClusterIndex(index)
                                    .setTimeStamp(timeStamp);
                            deploysMap.put(index, deployRecord);
                        } else {
                            Map<Integer, DeployRecord> oldDeploys = Json.decodeValue(str, deploysMapType);
                            //所有非当前站点是否记录了时间戳，已有且一般对象或分段对象status为0则继续同步；没有记录时间戳则从头开始同步。
                            DeployRecord record = oldDeploys.get(index);
                            if (record != null) {
                                deploysMap.put(index, record);
                            } else {
                                DeployRecord deployRecord = new DeployRecord()
                                        .setClusterIndex(index)
                                        .setTimeStamp(timeStamp);
                                deploysMap.put(index, deployRecord);
                            }
                        }
                        needSyncClusters.add(index);
                        Mono.just("1").publishOn(SCAN_SCHEDULER)
                                .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, DEPLOEY_RECORD, Json.encode(deploysMap)));
                    })
                    .flatMap(index -> {
                        log.info("Waiting for history sync check from other clusters...");
                        // 已存在的非async站点关于该index的历史同步初始化均已完成（checkPoint均已建立），才能进行initStart
                        return AddClusterUtils.checkHisSyncStatus(index, indexSet)
                                .filter(bool -> bool)
                                .doOnNext(bool -> initStart(index));
                    })
                    .doOnError(e -> {
                        log.error("start his error, ", e);
                        setSchedule(10_000, clusterIndex);
                    })
                    .doFinally(s -> is_adding_clusters.compareAndSet(true, false))
                    .subscribe();
        });
    }

    public static boolean createCheckPoint = false;

    private static Publisher<? extends Integer> createCheckPoint(Integer index) {
        createCheckPoint = true;
        return POOL.getReactive(REDIS_SYSINFO_INDEX).hexists(index_his_sync, String.valueOf(index))
                .flatMap(bool -> {
                    if (bool) {
                        // 历史数据同步过程中重启了，下次启动历史数据扫描不应再创建checkPoint
                        return Mono.just(index);
                    } else {
                        return AddClusterUtils.createCheckPointAllNodes(index)
                                .flatMap(c -> AddClusterUtils.checkAllNodesCheckPoint(index))
                                .flatMap(c -> Mono.just(index));
                    }
                });
    }

    private static Map<Integer, ScheduledFuture<?>> scheduledFuture1 = new ConcurrentHashMap<>();
    private static Map<Integer, ScheduledFuture<?>> scheduledFuture2 = new ConcurrentHashMap<>();

    private static void initStart(int index) {
        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(a -> {
                    try {
                        if (needSyncClusters.isEmpty()) {
                            return;
                        }
                        log.info("start history synchronization, cluster {}", index);

                        bucketsList.computeIfAbsent(index, k -> new ConcurrentHashSet<>());
                        Set<String> needSyncBucketsSet = POOL.getCommand(REDIS_SYSINFO_INDEX).smembers(need_his_buckets + index);
                        if (!needSyncBucketsSet.isEmpty()) {
                            for (String bucketName : needSyncBucketsSet) {
                                if (POOL.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName) == 1 && datasyncIsEnabled(bucketName) && !archiveIsEnable(bucketName, index)) {
                                    bucketsList.get(index).add(bucketName);
                                } else {
                                    Mono.just("1").publishOn(SCAN_SCHEDULER)
                                            .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(need_his_buckets + index, bucketName));
                                }
                            }
                        } else {
                            ScanIterator<String> iterator = ScanIterator.scan(POOL.getCommand(REDIS_BUCKETINFO_INDEX), new ScanArgs().match("*"));
                            while (iterator.hasNext()) {
                                String bucketName = iterator.next();
                                if (BUCKET_NAME_PATTERN.matcher(bucketName).matches() && datasyncIsEnabled(bucketName) && !archiveIsEnable(bucketName, index)) {
                                    bucketsList.get(index).add(bucketName);
                                }
                            }
                            if (!bucketsList.get(index).isEmpty()) {
                                Mono.just("1").publishOn(SCAN_SCHEDULER)
                                        .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(need_his_buckets + index, bucketsList.get(index).toArray(new String[0])));
                            }
                        }

                        scheduledFuture1.put(index, SCAN_TIMER.scheduleAtFixedRate(() -> {
                            try {
                                checkSyncStatus(index);
                            } catch (Exception e) {
                                log.error("checkSyncStatus err", e);
                            }
                        }, 10, 10, TimeUnit.SECONDS));
                        scheduledFuture2.put(index, SCAN_TIMER.scheduleAtFixedRate(() -> {
                            try {
                                flushCurMaker(index);
                            } catch (Exception e) {
                                log.error("flushCujrMaker err", e);
                            }
                        }, 60, 10, TimeUnit.SECONDS));
                    } catch (Exception e) {
                        if (isMultiAliveStarted && MainNodeSelector.checkIfSyncNode()) {
                            log.error("his sync init failed. Retry. ", e);
                            Optional.ofNullable(scheduledFuture1.get(index)).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
                            Optional.ofNullable(scheduledFuture2.get(index)).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
                            initStart(index);
                        }
                    }
                });

    }

    /**
     * 桶是否启用了数据同步
     */
    public static boolean datasyncIsEnabled(String bucketName) {
        String datasync = POOL.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH);
        return SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync);
    }

    /**
     * 用于list线程数量控制，表示还在list中的bucket总数
     */
    static final AtomicInteger syncingBucketNum = new AtomicInteger();

    private static final Map<Integer, Set<String>> dealingBuckets = new ConcurrentHashMap<>();

    /**
     * 保存桶是否已经开始扫描。[bucket_cluster, boolean]
     */
    private static final Map<String, Boolean> comListStartedMap = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> partListStartedMap = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> recListStartedMap = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> oldRecListStartedMap = new ConcurrentHashMap<>();

    private static final Map<Integer, Boolean> scanHisSyncComplete = new ConcurrentHashMap<>();

    private static void checkHisSyncComplete(int clusterIndex) {
        if (dealingBuckets.get(clusterIndex).size() >= bucketsList.get(clusterIndex).size()
                && waitingBucketAmount.get() == 0) {
            //注意。此时并非所有record都已落盘，只是表示所有bucket都已经启动了扫描进程。
            Optional.ofNullable(scheduledFuture1.get(clusterIndex)).ifPresent(s -> s.cancel(false));
            scanHisSyncComplete.put(clusterIndex, true);
            log.info("history synchronization: start all buckets listing complete. cluster: {}", clusterIndex);

        }
    }

    private static void clearByIndex(int index) {
        Optional.ofNullable(scheduledFuture1.get(index)).ifPresent(s -> s.cancel(false));
        dealingBuckets.computeIfAbsent(index, k -> new HashSet<>()).clear();
        bucketsList.computeIfAbsent(index, k -> new HashSet<>()).clear();
        INDEX_HIS_SYNC_MAP.put(index, 1);
        deploysMap.remove(index);
        bucketSyncVerMap.computeIfAbsent(index, k -> new ConcurrentHashMap<>()).clear();
    }

    public static void close() {
        scheduledFuture1.values().forEach(scheduledFuture -> scheduledFuture.cancel(false));
        dealingBuckets.clear();
        bucketsList.clear();
        INDEX_HIS_SYNC_MAP.clear();
        deploysMap.clear();
        bucketSyncVerMap.clear();
    }

    private static void checkSyncStatus(int clusterIndex) {
        dealingBuckets.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
        log.debug("checkSyncStatus. cluster: {}, dealingBuckets: {}, syncingBucketNum: {}, waitingBucketAmount: {}", clusterIndex, dealingBuckets.get(clusterIndex), syncingBucketNum.get(), waitingBucketAmount.get());
        Flux.fromArray(bucketsList.get(clusterIndex).toArray(new String[0]))
                .publishOn(SCAN_SCHEDULER)
                .filter(bucketName -> BUCKET_NAME_PATTERN.matcher(bucketName).matches())
                .doOnComplete(() -> checkHisSyncComplete(clusterIndex))
                .flatMap(bucketName -> {
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    waitingBucketAmount.incrementAndGet();
                    POOL.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                            .publishOn(SCAN_SCHEDULER)
                            .defaultIfEmpty(new HashMap<>())
                            .filter(map -> {
                                if (map.isEmpty()) {
                                    dealingBuckets.get(clusterIndex).add(bucketName);
                                    waitingBucketAmount.decrementAndGet();
                                    res.onNext(true);
                                    return false;
                                }
                                if (dealingBuckets.get(clusterIndex).contains(bucketName) || !bucketsList.get(clusterIndex).contains(bucketName)) {
                                    waitingBucketAmount.decrementAndGet();
                                    res.onNext(true);
                                    return false;
                                }

                                if (syncingBucketNum.incrementAndGet() > 10) {
                                    syncingBucketNum.decrementAndGet();
                                    waitingBucketAmount.decrementAndGet();
                                    res.onNext(true);
                                    return false;
                                }
                                return true;
                            })
                            .flatMap(map -> {
                                BucketSyncSwitchCache.getInstance().check(bucketName, map);
                                dealingBuckets.get(clusterIndex).add(bucketName);
                                bucketSyncVerMap.computeIfAbsent(clusterIndex, k -> new ConcurrentHashMap<>());
                                return AddClusterUtils.startCheckPut(bucketName, bucketSyncVerMap.get(clusterIndex))
                                        .flatMap(l -> AddClusterUtils.checkAllNodesPutDone(bucketName, bucketSyncVerMap.get(clusterIndex))
                                                .publishOn(SCAN_SCHEDULER)
                                                .doOnNext(b -> {
                                                    if (!b) {
                                                        dealingBuckets.get(clusterIndex).remove(bucketName);
                                                        syncingBucketNum.decrementAndGet();
                                                        waitingBucketAmount.decrementAndGet();
                                                    }
                                                })
                                                .doOnError(e -> {
                                                    log.info("wait check put done error, ", e);
                                                    dealingBuckets.get(clusterIndex).remove(bucketName);
                                                    syncingBucketNum.decrementAndGet();
                                                    waitingBucketAmount.decrementAndGet();
                                                })
                                                .filter(b -> b)
                                                .map(b -> map));
                            })
                            .doOnNext(map -> {
                                AtomicBoolean needToSync = new AtomicBoolean(false);
                                bucketSyncVerMap.get(clusterIndex).remove(bucketName);

                                COM_SYNCING_BUCKET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                                PART_SYNCING_BUCKET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                                REC_SYNCING_BUCKET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());
                                OLD_REC_SYNCING_BUCKET.computeIfAbsent(clusterIndex, k -> new ConcurrentHashSet<>());

                                if (role == HIS_SYNC_ROLE.MASTER) {
                                    log.info("start his sync meta and part, bucket:{}, to {}", bucketName, clusterIndex);
                                    String objStr = map.get(obj_his_sync_finished);
                                    Map<Integer, Integer> objHisSyncMap = objStr != null ? Json.decodeValue(objStr, syncFinishedMapType) : new HashMap<>();
                                    if (1 != objHisSyncMap.getOrDefault(clusterIndex, 0)) {
                                        COM_BUCKETS_STATUS_MAP.computeIfAbsent(bucketName, k -> new ConcurrentHashMap<>()).put(clusterIndex, 0);
                                        COM_SYNCING_BUCKET.get(clusterIndex).add(bucketName);
                                        listLifeMetaRotation(bucketName, clusterIndex);
                                        needToSync.compareAndSet(false, true);
                                    }

                                    String partStr = map.get(part_his_sync_finished);
                                    Map<Integer, Integer> partHisSyncMap = partStr != null ? Json.decodeValue(partStr, syncFinishedMapType) : new HashMap<>();
                                    if (1 != partHisSyncMap.getOrDefault(clusterIndex, 0)) {
                                        PART_BUCKETS_STATUS_MAP.computeIfAbsent(bucketName, k -> new ConcurrentHashMap<>()).put(clusterIndex, 0);
                                        PART_SYNCING_BUCKET.get(clusterIndex).add(bucketName);
                                        listMultiPartInfo(bucketName, clusterIndex);
                                        needToSync.compareAndSet(false, true);
                                    }
                                }

                                // 扫描新旧路径下的差异记录
                                if (createCheckPoint) {
                                    log.info("start his sync unsync record, bucket:{}, to {}", bucketName, clusterIndex);
                                    String recStr = map.get(record_his_sync_finished);
                                    Map<Integer, Integer> recHisSyncMap = recStr != null ? Json.decodeValue(recStr, syncFinishedMapType) : new HashMap<>();
                                    if (1 != recHisSyncMap.getOrDefault(clusterIndex, 0)) {
                                        REC_BUCKETS_STATUS_MAP.computeIfAbsent(bucketName, k -> new ConcurrentHashMap<>()).put(clusterIndex, 0);
                                        REC_SYNCING_BUCKET.get(clusterIndex).add(bucketName);
                                        listUnsyncRecord(bucketName, clusterIndex, role, false);
                                        needToSync.compareAndSet(false, true);
                                    }

                                    if (scanOldRecordPath.get()) {
                                        String oldRecStr = map.get(old_record_his_sync_finished);
                                        Map<Integer, Integer> oldRecHisSyncMap = recStr != null ? Json.decodeValue(oldRecStr, syncFinishedMapType) : new HashMap<>();
                                        if (1 != oldRecHisSyncMap.getOrDefault(clusterIndex, 0)) {
                                            OLD_REC_BUCKETS_STATUS_MAP.computeIfAbsent(bucketName, k -> new ConcurrentHashMap<>()).put(clusterIndex, 0);
                                            OLD_REC_SYNCING_BUCKET.get(clusterIndex).add(bucketName);
                                            listUnsyncRecord(bucketName, clusterIndex, role, true);
                                            needToSync.compareAndSet(false, true);
                                        }
                                    }
                                }

                                if (!needToSync.get()) {
                                    syncingBucketNum.decrementAndGet();
                                }
                                waitingBucketAmount.decrementAndGet();
                                res.onNext(true);
                            })
                            .doOnError(e -> {
                                log.error("checkSyncStatus Error1, ", e);
                                dealingBuckets.get(clusterIndex).remove(bucketName);
                                res.onError(e);
                            })
                            .subscribe();
                    return res;
                })
                .doOnError(e -> log.error("checkSyncStatus Error2, ", e))
                .subscribe();
    }

    /**
     * 扫描历史差异记录生成的record带此标记，会被之后其他站点的历史差异记录扫描过滤掉。
     */
    public static final String HIS_SYNC_REC_MARK = "HIS_SYNC_MARK";

    /**
     * 扫描本站点相关的差异记录，不扫描async记录。
     *
     * @param bucketName    该次扫描要扫描的桶名
     * @param clusterIndex  该次扫描的记录要往该索引的站点同步，不用作扫描记录时的依据。
     * @param role          当前扫描历史记录的节点是主站点还是从站点
     * @param metaRocksScan 是否去扫描元数据目录下的record。rocks_db_sync下的record会新建checkPoint进行历史数据扫描。元数据下的record不会有变化，因此不需要checkPoint。
     */
    private static void listUnsyncRecord(String bucketName, Integer clusterIndex, HIS_SYNC_ROLE role, boolean metaRocksScan) {
        UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = metaPool.getBucketVnodeId(bucketName);
        TypeReference<Tuple2<String, UnSynchronizedRecord>[]> reference = new TypeReference<Tuple2<String, UnSynchronizedRecord>[]>() {
        };
        listController.subscribe(marker -> {
            AtomicBoolean interrupt = new AtomicBoolean();
            if (!has_data_syncing.get()) {
                log.info("Stop record listing.");
                listController.onComplete();
            }

            if (dealingAmount.get() > MAX_LIST_AMOUNT) {
                Mono.delay(Duration.ofSeconds(3)).publishOn(SCAN_SCHEDULER).subscribe(s -> listController.onNext(marker));
                return;
            }

            metaPool.mapToNodeInfo(bucketVnode)
                    .subscribe(nodeList -> {
                        List<SocketReqMsg> msgs = nodeList.stream().map(info -> {
                            SocketReqMsg msg0 = new SocketReqMsg("", 0)
                                    .put("bucket", bucketName)
                                    .put("maxKeys", MAX_COUNT + "")
                                    .put("clusterIndex", String.valueOf(LOCAL_CLUSTER_INDEX))
                                    .put("marker", marker)
                                    .put("role", role.name());
                            if (metaRocksScan) {
                                msg0.put("lun", info.var2);
                            } else {
                                msg0.put("lun", RocksDBCheckPoint.getCheckPointLun(info.var2, clusterIndex));
                            }
                            return msg0;
                        }).collect(Collectors.toList());
                        ClientTemplate.ResponseInfo<Tuple2<String, UnSynchronizedRecord>[]> responseInfo =
                                ClientTemplate.oneResponse(msgs, metaRocksScan ? LIST_SYNC_RECORD : LIST_SYNC_RECORD_CHECKPOINT, reference, nodeList);
                        ListHistoryRecClientHandler clientHandler = new ListHistoryRecClientHandler(responseInfo, nodeList, listController, marker, bucketName);
                        Disposable subscribe = responseInfo.responses.publishOn(SCAN_SCHEDULER)
                                .subscribe(clientHandler::handleResponse, listController::onError, clientHandler::handleComplete);
                        clientHandler.recordProcessor.publishOn(SCAN_SCHEDULER)
                                .subscribe(recordList -> {
                                    if (recordList.isEmpty()) {
                                        String redisStr;
                                        Map<String, Map<Integer, Integer>> bucketStatusMap;
                                        if (metaRocksScan) {
                                            redisStr = old_record_his_sync_finished;
                                            bucketStatusMap = OLD_REC_BUCKETS_STATUS_MAP;
                                        } else {
                                            redisStr = record_his_sync_finished;
                                            bucketStatusMap = REC_BUCKETS_STATUS_MAP;
                                        }

                                        Map<Integer, Integer> clusterStatusMap = bucketStatusMap.get(bucketName);
                                        synchronized (clusterStatusMap) {
                                            clusterStatusMap.put(clusterIndex, 1);
                                            Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                    .subscribe(s -> {
                                                        endList(bucketName, clusterIndex, metaRocksScan ? old_record_his_sync_finished : record_his_sync_finished);
                                                        listController.onComplete();
                                                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                                                .hset(bucketName, redisStr, Json.encode(clusterStatusMap));
                                                    });
                                        }
                                    } else {
                                        AtomicInteger preCount = new AtomicInteger(recordList.size());
                                        AtomicInteger dealCount = new AtomicInteger();

                                        for (UnSynchronizedRecord record : recordList) {
                                            if (interrupt.get()) {
                                                break;
                                            }
                                            if ("1".equals(record.headers.get(HIS_SYNC_REC_MARK))) {
                                                if (dealCount.incrementAndGet() == preCount.get()) {
                                                    if (clientHandler.getCount() >= MAX_COUNT) {
                                                        listController.onNext(clientHandler.getNextMarker());
                                                    } else {
                                                        endList(bucketName, clusterIndex, metaRocksScan ? old_record_his_sync_finished : record_his_sync_finished);
                                                        listController.onComplete();
                                                    }
                                                }
                                                continue;
                                            }

                                            if (role == HIS_SYNC_ROLE.MASTER) {
                                                // 主站点扫描successIndex不为自己的差异记录和预提交记录
                                                if (record.successIndex != LOCAL_CLUSTER_INDEX || !record.commited) {
                                                    recordSendToProcessor(bucketName, clusterIndex, metaRocksScan, listController, clientHandler, preCount, dealCount, record, interrupt, marker);
                                                }
                                            } else if (role == HIS_SYNC_ROLE.SLAVE) {
                                                // 从站点扫描successIndex为自己的差异记录和预提交记录
                                                recordSendToProcessor(bucketName, clusterIndex, metaRocksScan, listController, clientHandler, preCount, dealCount, record, interrupt, marker);
                                            }
                                        }
                                    }

//                                    if (clientHandler.getCount() >= MAX_COUNT) {
//                                        listController.onNext(clientHandler.getNextMarker());
//                                    } else {
//                                        endList(bucketName, clusterIndex, metaRocksScan ? old_record_his_sync_finished : record_his_sync_finished);
//                                        listController.onComplete();
//                                    }
                                }, e -> {
                                    log.error("list His record 1 error. {}, ", bucketName, e);
                                    if (interrupt.compareAndSet(false, true)) {
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                    }
                                });

                    });
        }, e -> log.error("list His record 2 error. ", e));

        String rocksKey = deploysMap.get(clusterIndex).recordMarkerMap.get(bucketName);
        String recorderPrefixAsync = UnSynchronizedRecord.getRecorderPrefixAsync(bucketName, LOCAL_CLUSTER_INDEX, metaRocksScan);
        // 第一次从头开始扫
        listController.onNext(rocksKey == null ? recorderPrefixAsync : rocksKey);
    }

    private static void recordSendToProcessor(String bucketName, Integer clusterIndex, boolean metaRocksScan, UnicastProcessor<String> listController, ListHistoryRecClientHandler clientHandler,
                                              AtomicInteger preCount, AtomicInteger dealCount, UnSynchronizedRecord record, AtomicBoolean interrupt, String curMarker) {
        record.setIndex(clusterIndex);
        record.rocksKeyAsync(clusterIndex);
        AddClusterUtils.preDealRecord(clusterIndex, record.bucket, record, metaRocksScan);
        SyncRequest<UnSynchronizedRecord> syncRquest = new SyncRequest<>();
        syncRquest.payload = record;
        syncRquest.res = MonoProcessor.create();
        syncRquest.res
                .publishOn(SCAN_SCHEDULER)
                .timeout(Duration.ofMinutes(5))
                .doFinally(s -> {
                    dealingAmount.decrementAndGet();
                    if (!interrupt.get()) {
                        AddClusterUtils.finishDealRecord(clusterIndex, record.bucket, record, metaRocksScan);
                    }
                    syncRquest.res.dispose();
                    syncRquest.payload = null;
                })
                .subscribe(b -> {
                    if (!b) {
                        if (interrupt.compareAndSet(false, true)) {
                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(curMarker));
                        }
                        return;
                    }
                    if (dealCount.incrementAndGet() == preCount.get()) {
                        if (clientHandler.getCount() >= MAX_COUNT) {
                            listController.onNext(clientHandler.getNextMarker());
                        } else {
                            endList(bucketName, clusterIndex, metaRocksScan ? old_record_his_sync_finished : record_his_sync_finished);
                            listController.onComplete();
                        }
                    }
                }, e -> {
                    log.error("his unsyncRecord syncRequest res error: ", e);
                    if (interrupt.compareAndSet(false, true)) {
                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(curMarker));
                    }
                });
        dealingAmount.incrementAndGet();
        int index = ThreadLocalRandom.current().nextInt(recordProcessors.length);
        recordProcessors[index].onNext(new Tuple3<>(clusterIndex, metaRocksScan, syncRquest));
        if (metaRocksScan) {
            oldRecListStartedMap.put(bucketName + "_" + clusterIndex, true);
        } else {
            recListStartedMap.put(bucketName + "_" + clusterIndex, true);
        }
    }

    private static void dealRecord(int clusterIndex, boolean metaScan, SyncRequest<UnSynchronizedRecord> syncRequest) {
        UnSynchronizedRecord record = syncRequest.payload;
        try {
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
            String bucketVnode = metaPool.getBucketVnodeId(record.bucket);
//        record.setIndex(clusterIndex);
//        record.rocksKeyAsync(clusterIndex);
//        AddClusterUtils.preDealRecord(clusterIndex, record.bucket, record, metaScan);

            metaPool.mapToNodeInfo(bucketVnode)
                    .publishOn(SCAN_SCHEDULER)
                    // 差异记录写成功与否由putHisSyncRecord内部处理，因此使用dofinally表示该record处理完毕
                    .doFinally(s -> syncRequest.res.onNext(true))
//                .doFinally(s -> AddClusterUtils.finishDealRecord(clusterIndex, record.bucket, record, metaScan))
                    .flatMap(nodeList -> putHisSyncRecord(record, nodeList, false))
                    .doOnNext(b -> syncRequest.res.onNext(b))
                    .doOnError(e -> log.error("dealRecord put record error, ", e))
                    .subscribe();
        } catch (Exception e) {
            log.error("dealRecord err2, {}, {}", record.bucket, record.object, e);
            syncRequest.res.onNext(false);
        }
    }

    /**
     * 定时将各站点同步情况下刷。若同步中断，将从第一个未处理完的对象开始扫描。
     * 同时检查是否还有桶在写相关的record，已经没有在同步的桶了则将flushCurMaker定时任务取消。
     */
    private static void flushCurMaker(int index) {
        if (!MainNodeSelector.checkIfSyncNode()) {
            role = HIS_SYNC_ROLE.NONE;
            has_data_syncing.compareAndSet(true, false);
            setSchedule(10_000, -1);
            log.info("Stop history sync, turn to another node. ");
            scheduledFuture1.values().forEach(scheduledFuture -> scheduledFuture.cancel(true));
            scheduledFuture2.values().forEach(scheduledFuture -> scheduledFuture.cancel(true));
            return;
        }

        log.debug("flushCurMakerPeriodicly. {}", waitingBucketAmount.get());
        DeployRecord deployRecord = deploysMap.get(index);
        if (deployRecord == null) {
            // 可能出现all flushed，但是没有成功cancel的情况
            log.info("cancel flush of index {} certainly.", index);
            Optional.ofNullable(scheduledFuture2.get(index)).ifPresent(s -> s.cancel(true));
            return;
        }
        // 所有站点是否都完成了对象和分段的历史数据同步。
        AtomicBoolean isComSyncFinished = new AtomicBoolean(true);
        AtomicBoolean isPartSyncFinished = new AtomicBoolean(true);
        AtomicBoolean isRecSyncFinished = new AtomicBoolean(true);
        AtomicBoolean isOldRecSyncFinished = new AtomicBoolean(true);
        synchronized (deployRecord) {
            deployRecord.dealingObjMap.forEach((key, rockskeyMetaMap) -> {
                String bucketName = key.split("_")[0];
                rockskeyMetaMap.entrySet().stream().findFirst().ifPresent(entry -> {
                    MetaData metaData = entry.getValue();
                    deployRecord.objMarkerMap.put(key, new String[]{bucketName, metaData.key, metaData.versionId, metaData.stamp});
                });
                if (!deployRecord.dealingObjMap.get(key).isEmpty() || !COM_SYNCING_BUCKET.get(index).isEmpty()
                        || !comListStartedMap.get(bucketName + "_" + index)) {
                    isComSyncFinished.compareAndSet(true, false);
                }
            });

            deployRecord.dealingPartMap.forEach((key, map) -> {
                String bucket = key.split("_")[0];
                map.entrySet().stream().findFirst().ifPresent(entry -> {
                    deployRecord.partMarkerMap.put(key, entry.getValue());
                });
                if (!deployRecord.dealingPartMap.get(key).isEmpty() || !PART_SYNCING_BUCKET.get(index).isEmpty()
                        || !partListStartedMap.get(bucket + "_" + index)) {
                    isPartSyncFinished.compareAndSet(true, false);
                }
            });

            deployRecord.dealingRecordMap.forEach((bucket, map) -> {
                map.entrySet().stream().findFirst().ifPresent(entry -> {
                    deployRecord.recordMarkerMap.put(bucket, entry.getKey());
                });
                if (!deployRecord.dealingRecordMap.get(bucket).isEmpty() || !REC_SYNCING_BUCKET.get(index).isEmpty()
                        || !recListStartedMap.get(bucket + "_" + index)) {
                    isRecSyncFinished.compareAndSet(true, false);
                }
            });

            deployRecord.dealingOldRecordMap.forEach((bucket, map) -> {
                map.entrySet().stream().findFirst().ifPresent(entry -> {
                    deployRecord.oldRecordMarkerMap.put(bucket, entry.getKey());
                });
                if (!deployRecord.dealingOldRecordMap.get(bucket).isEmpty() || !OLD_REC_SYNCING_BUCKET.get(index).isEmpty()
                        || !oldRecListStartedMap.get(bucket + "_" + index)) {
                    isOldRecSyncFinished.compareAndSet(true, false);
                }
            });
        }

        if (isComSyncFinished.get() && isPartSyncFinished.get() && isRecSyncFinished.get() && isOldRecSyncFinished.get()
                && dealingBuckets.get(index).size() >= bucketsList.get(index).size()
                && waitingBucketAmount.get() == 0 && scanHisSyncComplete.getOrDefault(index, false)
                && SHARD_BUCKET_SET.isEmpty()) {
            log.info("All bucket his record flushed. His sync all done. cluster {}", index);
            Mono.just("1").publishOn(SCAN_SCHEDULER)
                    .subscribe(s -> POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(index_his_sync, String.valueOf(index), "1"));
            clearByIndex(index);
            RocksDBCheckPoint.close(index);
            Optional.ofNullable(scheduledFuture2.get(index)).ifPresent(s -> s.cancel(true));
            return;
        }

        Mono.just("1").publishOn(SCAN_SCHEDULER)
                .subscribe(l -> {
                    POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, DEPLOEY_RECORD, Json.encode(deploysMap));
                });

        log.debug("his sync dealingAmount: {}", dealingAmount.get());

    }

    static final ConcurrentHashSet<String> SHARD_BUCKET_SET = new ConcurrentHashSet<>();

    /**
     * 扫描生命周期的对象元数据。该类元数据按时间排序。
     */
    private static void listLifeMetaRotation(String bucketName, int clusterIndex) {
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);

        AtomicInteger vnodeCount = new AtomicInteger();

        for (String bucketVnode : bucketVnodeList) {
            AtomicLong findNum = new AtomicLong();
            String stamp;
            String key;
            String versionId;
            int maxKey = 1000;
            String stampMarker = deploysMap.get(clusterIndex).timeStamp + 1 + "";
            String[] tempMarker = deploysMap.get(clusterIndex).objMarkerMap.get(bucketName + "_" + bucketVnode);
            if (tempMarker != null) {
                key = tempMarker[1];
                versionId = tempMarker[2];
                stamp = tempMarker[3];
            } else {
                stamp = "0";
                key = "";
                versionId = "";
            }
            UnicastProcessor<Tuple3<String, String, String>> bucketVnodeProcessor = UnicastProcessor.create(Queues.<Tuple3<String, String, String>>unboundedMultiproducer().get());
            bucketVnodeProcessor
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(tuple3 ->
                            POOL.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                    .filter(b -> {
                                        if (b > 0 || ShardingWorker.contains(bucketName)) {
                                            // 如果桶正在散列，在散列完成后从头开始扫描所有vnode。SERVER-985
                                            log.debug("has bucket sharding, {}", bucketName);
                                            synchronized (deploysMap) {
                                                deploysMap.get(clusterIndex).dealingObjMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).clear();
                                                deploysMap.get(clusterIndex).objMarkerMap.put(bucketName + "_" + bucketVnode, new String[]{bucketName, "", "", "0"});
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

                                        if (dealingAmount.get() > MAX_LIST_AMOUNT) {
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
                                .put("stamp", stampMarker)
                                .put("retryTimes", "0")
                                .put("beginPrefix", beginPrefix);
                        reqMsg.put("beginPrefix", beginPrefix);
                        metaPool.mapToNodeInfo(bucketVnode)
                                .publishOn(SCAN_SCHEDULER)
                                .flatMap(infoList -> {
                                    String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                                    reqMsg.put("vnode", nodeArr[0]);
                                    List<SocketReqMsg> msgs = infoList.stream().map(info -> reqMsg.copy().put("lun", info.var2)).collect(Collectors.toList());
                                    ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                            String, MetaData>[]>() {
                                    }, infoList);
                                    ListHistoryObjClientHandler clientHandler = new ListHistoryObjClientHandler(responseInfo, nodeArr[0], reqMsg, clusterIndex, bucketName);
                                    responseInfo.responses.publishOn(SCAN_SCHEDULER).subscribe(clientHandler::handleResponse, e -> log.error("", e),
                                            clientHandler::completeResponse);
                                    return clientHandler.res;
                                })
                                .subscribe(metaDataList -> {
                                    if (metaDataList.isEmpty()) {
                                        // 桶下无历史对象，直接设置redis中的状态
                                        if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                                            Map<Integer, Integer> clusterStatusMap = COM_BUCKETS_STATUS_MAP.get(bucketName);
                                            synchronized (clusterStatusMap) {
                                                clusterStatusMap.put(clusterIndex, 1);
                                                Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                        .subscribe(s -> {
                                                            AddClusterUtils.endList(bucketName, clusterIndex, obj_his_sync_finished);
                                                            RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                                                    .hset(bucketName, obj_his_sync_finished, Json.encode(clusterStatusMap));
                                                        });
                                            }
                                        }
                                        bucketVnodeProcessor.onComplete();
                                    } else {
                                        AtomicInteger preCount = new AtomicInteger(Math.min(maxKey, metaDataList.size()));
                                        AtomicInteger dealCount = new AtomicInteger();
                                        findNum.addAndGet(metaDataList.size());
                                        for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                            if (interrupt.get()) {
                                                break;
                                            }
                                            MetaData lifeMeta = metaDataList.get(i).getMetaData();
                                            if (lifeMeta.equals(MetaData.ERROR_META) || lifeMeta.equals(MetaData.NOT_FOUND_META)) {
                                                checkThisComplete(clusterIndex, bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, findNum);
                                                continue;
                                            }
                                            if (lifeMeta.deleteMark && StringUtils.isEmpty(lifeMeta.sysMetaData)) {
                                                checkThisComplete(clusterIndex, bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList, findNum);
                                                continue;
                                            }
                                            AddClusterUtils.preDealObj(clusterIndex, metaPool.getBucketVnodeId(lifeMeta.bucket, lifeMeta.key), lifeMeta);
                                            SyncRequest<MetaData> syncRquest = new SyncRequest<>();
                                            syncRquest.payload = lifeMeta;
                                            syncRquest.res = MonoProcessor.create();
                                            syncRquest.res
                                                    .publishOn(SCAN_SCHEDULER)
                                                    .timeout(Duration.ofMinutes(20))
                                                    .doOnError(e -> log.error("listHistoryObj Error1, {}, {}", bucketName, e))
                                                    .doFinally(s -> {
                                                        dealingAmount.decrementAndGet();
                                                        // 如果此轮扫描中断则不移除该obj key，因为syncRquest.res先返回的可能是列表上靠后的对象，此时关机重新开始扫描中间的对象将丢失。
                                                        // 为防止flush到的是后面的对象，
                                                        if (!interrupt.get()) {
                                                            AddClusterUtils.finishDealObj(clusterIndex, metaPool.getBucketVnodeId(lifeMeta.bucket, lifeMeta.key), lifeMeta);
                                                        }
                                                        syncRquest.res.dispose();
                                                        syncRquest.payload = null;
                                                    })
                                                    .subscribe(b -> {
                                                        // 一条元数据写差异记录失败，从头开始扫描该轮
                                                        if (!b) {
                                                            if (interrupt.compareAndSet(false, true)) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                                            }
                                                            return;
                                                        }
                                                        checkThisComplete(clusterIndex, bucketVnodeProcessor, bucketName, metaDataList, preCount, dealCount, bucketVnode, vnodeCount, bucketVnodeList
                                                                , findNum);
                                                    }, e -> {
                                                        log.error("his meta syncRequest res error: ", e);
                                                        if (interrupt.compareAndSet(false, true)) {
                                                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                                        }
                                                    });

                                            dealingAmount.incrementAndGet();
                                            int index = ThreadLocalRandom.current().nextInt(objProcessors.length);
                                            objProcessors[index].onNext(new Tuple2<>(clusterIndex, syncRquest));
                                        }
                                    }
                                    comListStartedMap.put(bucketName + "_" + clusterIndex, true);
                                }, e -> {
                                    log.error("listHistoryObj Error2, {}, {}", bucketName, e);
                                    if (interrupt.compareAndSet(false, true)) {
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple3));
                                    }
                                });
                    }, e -> {
                        log.error("listHistoryObj Error3, {}, {}", bucketName, e);
                    });

            bucketVnodeProcessor.onNext(new Tuple3<>(key, versionId, stamp));
        }

    }

    public static void checkThisComplete(int clusterIndex, UnicastProcessor<Tuple3<String, String, String>> listController, String bucketName, List<LifecycleClientHandler.Counter> metaDataList,
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
                    AddClusterUtils.endList(bucketName, clusterIndex, obj_his_sync_finished);
                }
                listController.onComplete();
            }
        }
    }

    /**
     * 发送数据同步的http请求
     *
     * @param clusterIndex 需要同步的站点
     * @param syncRequest  不全，需要额外getObjectMetaVersion获取完整的metaData
     */
    protected static void dealObjMeta(int clusterIndex, SyncRequest<MetaData> syncRequest) {
        MetaData lifeMeta = syncRequest.payload;
        try {
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

            //生成记录，具体是一般对象还是分段对象去DataSynChecker处理
            metaPool.mapToNodeInfo(bucketVnode)
                    .publishOn(SCAN_SCHEDULER)
                    // 差异记录写成功与否由putHisSyncRecord内部处理，因此使用dofinally表示该lifeMeta处理完毕
                    .flatMap(nodeList -> putHisSyncRecord(record, nodeList))
                    .doOnNext(b -> syncRequest.res.onNext(b))
                    .doOnError(e -> {
                        log.error("dealObjMeta put record error, {}, {}", lifeMeta.bucket, lifeMeta.key, e);
                        syncRequest.res.onNext(false);
                    })
                    .subscribe();
        } catch (Exception e) {
            log.error("dealObjMeta err2, {}, {}", lifeMeta.bucket, lifeMeta.key, e);
            syncRequest.res.onNext(false);
        }
    }

    private static Mono<Boolean> putHisSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList, boolean setCommited) {
        try {
            // 当出现getObject异常或recoverPut失败的情况，需要将record落盘，通过异步复制流程恢复数据
            if (setCommited) {
                record.setCommited(true);
            }
            record.headers.put(HIS_SYNC_REC_MARK, "1");
            return ECUtils.updateSyncRecord(record, nodeList, false)
                    .publishOn(SCAN_SCHEDULER)
                    .timeout(Duration.ofSeconds(30))
                    .map(res -> {
                        //写记录失败，需要将记录发至本地rabbitmq
                        if (res == 0) {
                            putRecordToMq(record);
                            return false;
                        } else {
                            return true;
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("syncErrorHandler error1, {}, {}", record.bucket, record.recordKey, e);
                        putRecordToMq(record);
                        return Mono.just(false);
                    });
        } catch (Exception e) {
            log.error("syncErrorHandler error2, ", e);
            putRecordToMq(record);
            return Mono.just(false);
        }
    }

    protected static Mono<Boolean> putHisSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList) {
        return putHisSyncRecord(record, nodeList, true);
    }

    private static void listMultiPartInfo(String bucketName, int clusterIndex) {
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);
        AtomicInteger vnodeCount = new AtomicInteger();

        for (String bucketVnode : bucketVnodeList) {
            UnicastProcessor<Tuple2<String, String>> bucketVnodeProcessor = UnicastProcessor.create(Queues.<Tuple2<String, String>>unboundedMultiproducer().get());
            int maxUploadsInt = 1000;
            bucketVnodeProcessor
                    .publishOn(SCAN_SCHEDULER)
                    .flatMap(tup2 ->
                            POOL.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                    .filter(b -> {
                                        if (b > 0 || ShardingWorker.contains(bucketName)) {
                                            // 如果桶正在散列，在散列完成后从头开始扫描所有vnode。SERVER-985
                                            log.debug("has bucket sharding, {}", bucketName);
                                            synchronized (deploysMap) {
                                                deploysMap.get(clusterIndex).dealingPartMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).clear();
                                                deploysMap.get(clusterIndex).partMarkerMap.put(bucketName + "_" + bucketVnode, "");
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

                                        if (dealingAmount.get() > MAX_LIST_AMOUNT) {
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
                        POOL.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                                .defaultIfEmpty(new HashMap<>(0))
                                .flatMap(bucketInfo ->
                                        POOL.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                                                .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                                                .zipWith(metaPool.mapToNodeInfo(bucketVnode))

                                )
                                .flatMap(tuple2 -> {
                                    MonoProcessor<ListMultipartUploadsResult> res = MonoProcessor.create();
                                    ListMultipartUploadsResult listMultiUploadsRes = new ListMultipartUploadsResult()
                                            .setBucket(bucketName)
                                            .setMaxUploads(maxUploadsInt)
                                            .setKeyMarker(tup2.var1);

                                    SocketReqMsg msg = new SocketReqMsg("", 0)
                                            .put("vnode", tuple2.getT2().get(0).var3)
                                            .put("bucket", bucketName)
                                            .put("maxUploads", String.valueOf(maxUploadsInt))
                                            .put("prefix", "")
                                            .put("marker", tup2.var1)
                                            .put("delimiter", "")
                                            .put("uploadIdMarker", tup2.var2);

                                    List<SocketReqMsg> msgs = tuple2.getT2().stream()
                                            .map(tuple -> msg.copy().put("lun", tuple.var2))
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
                                        if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                                            Map<Integer, Integer> clusterStatusMap = PART_BUCKETS_STATUS_MAP.get(bucketName);
                                            clusterStatusMap.put(clusterIndex, 1);
                                            synchronized (clusterStatusMap) {
                                                Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                        .subscribe(s -> {
                                                            endList(bucketName, clusterIndex, part_his_sync_finished);
                                                            RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                                                    .hset(bucketName, part_his_sync_finished, Json.encode(clusterStatusMap));
                                                        });
                                            }
                                            bucketVnodeProcessor.onComplete();
                                        }
                                    } else {
                                        AtomicInteger preCount = new AtomicInteger(listMultiUploadsRes.getUploads().size());
                                        AtomicInteger dealCount = new AtomicInteger();

                                        for (Upload upload : listMultiUploadsRes.getUploads()) {
                                            if (interrupt.get()) {
                                                break;
                                            }
                                            if (MsDateUtils.dateToStamp(upload.getInitiated()) < deploysMap.get(clusterIndex).timeStamp) {
                                                AddClusterUtils.preDealPartList(clusterIndex, bucketName, bucketVnode, upload);
                                                SyncRequest<Upload> syncRquest = new SyncRequest<>();
                                                syncRquest.payload = upload;
                                                syncRquest.res = MonoProcessor.create();
                                                syncRquest.res
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .timeout(Duration.ofMinutes(20))
                                                        .doOnError(e -> log.error("his part syncRequest res error1: ", e))
                                                        .doFinally(s -> {
                                                            // 如果中断了这轮不移除，防止flush到的是后面的对象，此时关机重新开始扫描中间的对象将丢失
                                                            if (!interrupt.get()) {
                                                                AddClusterUtils.finishDealPartList(clusterIndex, bucketName, bucketVnode, upload);
                                                            }
                                                            syncRquest.res.dispose();
                                                            syncRquest.payload = null;
                                                        })
                                                        .subscribe(res -> {
                                                            if (!res) {
                                                                if (interrupt.compareAndSet(false, true)) {
                                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                                                }
                                                                return;
                                                            }
                                                            checkMultiPartListComplete(bucketName, clusterIndex, bucketVnodeProcessor, listMultiUploadsRes, preCount, dealCount, bucketVnode,
                                                                    vnodeCount, bucketVnodeList);
                                                        }, e -> {
                                                            log.error("his part syncRequest res error2: ", e);
                                                            if (interrupt.compareAndSet(false, true)) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                                            }
                                                        });

                                                int index = ThreadLocalRandom.current().nextInt(partProcessors.length);
                                                partProcessors[index].onNext(new Tuple3<>(clusterIndex, bucketName, syncRquest));
                                            } else {
                                                checkMultiPartListComplete(bucketName, clusterIndex, bucketVnodeProcessor, listMultiUploadsRes, preCount, dealCount, bucketVnode,
                                                        vnodeCount, bucketVnodeList);
                                            }
                                        }
                                        partListStartedMap.put(bucketName + "_" + clusterIndex, true);
                                    }
                                }, e -> {
                                    log.error("listMultiPartInfo1 bucketName:{}", bucketName, e);
                                    if (interrupt.compareAndSet(false, true)) {
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tup2));
                                    }
                                });
                    }, e -> log.error("listMultiPartInfo2 bucketName:{}", bucketName, e));

            String key = deploysMap.get(clusterIndex).partMarkerMap.get(bucketName + "_" + bucketVnode);
            bucketVnodeProcessor.onNext(new Tuple2<>(key == null ? "" : key, ""));
        }
    }

    public static void checkMultiPartListComplete(String bucketName, int clusterIndex, UnicastProcessor<Tuple2<String, String>> listController, ListMultipartUploadsResult listMultiUploadsRes,
                                                  AtomicInteger preCount, AtomicInteger dealCount, String bucketVnode, AtomicInteger vnodeCount, List<String> bucketVnodeList) {
        if (dealCount.incrementAndGet() == preCount.get()) {
            if (!listMultiUploadsRes.isTruncated()) {
                log.info("scan all multiPart2 from bucket " + bucketName + "(" + bucketVnode + ") . {}");
                if (vnodeCount.incrementAndGet() == bucketVnodeList.size()) {
                    log.info("scan bucket multiPart end, {} ", bucketName);
                    endList(bucketName, clusterIndex, part_his_sync_finished);
                }
                listController.onComplete();
            } else {
                String nextMarker = listMultiUploadsRes.getNextKeyMarker();
                String nextUploadIdMarker = listMultiUploadsRes.getNextUploadIdMarker();
                listController.onNext(new Tuple2<>(nextMarker, nextUploadIdMarker));
            }
        }
    }

    private static void listParts(int clusterIndex, String bucketName, SyncRequest<Upload> syncRequest) {
        Upload upload = syncRequest.payload;
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = pool.getBucketVnodeId(bucketName, upload.getKey());
        //UnicastProcessor<String> bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        //AtomicInteger nextRunNumber = new AtomicInteger(1);

        UnicastProcessor<Integer> listController = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        listController
                .subscribe(marker -> POOL.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                        .flatMap(bucketInfo ->
                                POOL.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                                        .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                                        .zipWith(pool.mapToNodeInfo(bucketVnode))
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
                                    syncRequest.res.onNext(true);
                                    listController.onComplete();
                                    return;
                                }
                                AtomicInteger dealCount = new AtomicInteger();
                                AtomicInteger preCount = new AtomicInteger(listPartsResult.getParts().size());

                                for (Part part : listPartsResult.getParts()) {
                                    if (interrupt.get()) {
                                        break;
                                    }
                                    dealingAmount.incrementAndGet();
                                    PartClient.getPartInfo(bucketName, upload.getKey(), upload.getUploadId(), String.valueOf(part.getPartNumber()))
                                            .publishOn(SCAN_SCHEDULER)
                                            .doFinally(f -> dealingAmount.decrementAndGet())
                                            .subscribe(partInfo -> {
                                                UnSynchronizedRecord record = AddClusterUtils.buildPartSyncRecord(clusterIndex, bucketName, upload, partInfo);
                                                pool.mapToNodeInfo(pool.getBucketVnodeId(bucketName))
                                                        .flatMap(nodeList -> putHisSyncRecord(record, nodeList))
                                                        .doOnNext(res -> {
                                                            if (!res) {
                                                                if (interrupt.compareAndSet(false, true)) {
                                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                                                }
                                                                return;
                                                            }
                                                            // 每轮partList结束才继续下一轮
                                                            // 该分段任务下所有的碎片都写入差异记录完毕，才认为该分段任务处理完毕，res.onNext执行
                                                            if (dealCount.incrementAndGet() == preCount.get()) {
                                                                if (!listPartsResult.isTruncated()) {
                                                                    syncRequest.res.onNext(true);
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
                                log.error("listPart1 bucketName: {}, {}", bucketName, upload.getKey(), e);
                                if (interrupt.compareAndSet(false, true)) {
                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                                }
                            });
                        }, e -> {
                            log.error("listPart2 bucketName: {}, {}", bucketName, upload.getKey(), e);
                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> listController.onNext(marker));
                        }));

        listController.onNext(0);
    }

    protected static void dealUpload(int clusterIndex, String bucketName, SyncRequest<Upload> syncRequest) {
        Upload upload = syncRequest.payload;
        try {
            UnSynchronizedRecord partInitSyncRecord = AddClusterUtils.buildPartSyncRecord(clusterIndex, bucketName, upload, null);
            StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnodeId = pool.getBucketVnodeId(bucketName);
            pool.mapToNodeInfo(bucketVnodeId)
                    .publishOn(SCAN_SCHEDULER)
//                .doOnNext(vnodeList -> AddClusterUtils.preDealPartList(bucketObjectVnodeId, clusterIndex, bucketName, upload))
                    .flatMap(bucketVnodeList -> putHisSyncRecord(partInitSyncRecord, bucketVnodeList))
                    .doOnNext(b -> {
                        if (!b) {
                            syncRequest.res.onNext(false);
                            return;
                        }
                        listParts(clusterIndex, bucketName, syncRequest);
                    })
                    .doOnError(e -> {
                        log.error("dealUpload put record error, ", e);
                        syncRequest.res.onNext(false);
                    })
                    .subscribe();
        } catch (Exception e) {
            log.error("dealUpload err, {}, {}, {}", bucketName, upload.getKey(), upload.getUploadId(), e);
            syncRequest.res.onNext(false);
        }
    }

}
