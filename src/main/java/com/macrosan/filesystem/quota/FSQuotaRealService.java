package com.macrosan.filesystem.quota;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.DirInfo;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.message.xmlmsg.FsQuotaConfigAndUsedResult;
import com.macrosan.message.xmlmsg.FsQuotaConfigResult;
import com.macrosan.message.xmlmsg.FsQuotaListConfigResult;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;
import io.rsocket.Payload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.QUOTA_INFO_NOT_EXITS;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_FS_QUOTA_INFO_INDEX;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_DEL_QUOTA_INFO;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_UPDATE_QUOTA_INFO;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.filesystem.FsConstants.S_IFDIR;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.*;
import static com.macrosan.message.jsonmsg.DirInfo.ERROR_DIR_INFO;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class FSQuotaRealService {

    private static final ScheduledThreadPoolExecutor QUOTA_RECOVER_EXECUTOR = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "quota-scan-recover"));

    public static final Scheduler FS_QUOTA_EXECUTOR;

    public static final int MAX_SCAN_PRO_NUM = 10;

    protected static RedisConnPool redisConnPool = RedisConnPool.getInstance();


    private static final UnicastProcessor<FSQuotaScannerTask>[] QUOTA_SCANNER_PROCESSORS = new UnicastProcessor[MAX_SCAN_PRO_NUM];

    private static final Map<String, FSQuotaScannerTask> RUNNING_TASK_MAP = new ConcurrentHashMap<>();

    /**
     * 缓存已设置配额信息，但其中的软硬配额可能不准确，需要使用准确的软硬配额需要查询redis
     */
    public static final Map<String, Map<String, FSQuotaConfig>> QUOTA_CONFIG_CACHE = new ConcurrentHashMap<>();

    private static final long SCAN_RECOVER_DELAY = 2 * 60;

    private static final long QUOTA_ALARM_PERIOD = 5 * 60;

    private static final Map<String, String> checkKeys = new ConcurrentHashMap<String, String>();

    private static SocketSender sender = SocketSender.getInstance();


    static {
        Scheduler scheduler = null;
        try {
            ThreadFactory quotaScanTaskSchedule = new MsThreadFactory("quota-scan");
            MsExecutor executor = new MsExecutor(2 * PROC_NUM, PROC_NUM, quotaScanTaskSchedule);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        FS_QUOTA_EXECUTOR = scheduler;
    }

    public static void init() {
        initQuotaScanProcessor();
        initQuotaConfigCache();
        QUOTA_RECOVER_EXECUTOR.schedule(FSQuotaRealService::starRecover,30,TimeUnit.SECONDS);
        FS_QUOTA_EXECUTOR.schedulePeriodically(FSQuotaRealService::checkQuotaAlarm, 30, QUOTA_ALARM_PERIOD, TimeUnit.SECONDS);
    }

    public static void initQuotaConfigCache() {
        QUOTA_CONFIG_CACHE.clear();
        try {
            List<String> bucketKeys = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).keys("*_quota");
            for (String key : bucketKeys) {
                if (!key.endsWith("quota")) {
                    continue;
                }
                String bucketName = key.split("_")[0];
                Map<String, String> valMap = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hgetall(key);
                if (!valMap.isEmpty()) {
                    for (String quotaKey : valMap.keySet()) {
                        if (quotaKey.startsWith(QUOTA_PREFIX)) {
                            QUOTA_CONFIG_CACHE.compute(bucketName, (k1, v) -> {
                                if (v == null) {
                                    v = new ConcurrentHashMap<>();
                                }
                                v.computeIfAbsent(quotaKey, k2 -> Json.decodeValue(valMap.get(quotaKey), FSQuotaConfig.class));
                                fsAddQuotaCheck(bucketName, quotaKey);
                                return v;
                            });
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("init quota config cache error", e);
            QUOTA_RECOVER_EXECUTOR.schedule(FSQuotaRealService::initQuotaConfigCache, 5, TimeUnit.SECONDS);
        }

    }

    public static void fsAddQuotaCheck(String bucketName, String key) {
        checkKeys.computeIfAbsent(key, k -> bucketName);
    }

    public static void fsAddQuotaCache(String bucketName, String key, FSQuotaConfig config) {
        QUOTA_CONFIG_CACHE.computeIfAbsent(bucketName, k -> new ConcurrentHashMap<>()).computeIfAbsent(key, k -> config);
    }

    public static void fsDeleteQuotaCache(String bucketName, String key) {
        QUOTA_CONFIG_CACHE.computeIfPresent(bucketName, (k, v) -> {
            v.remove(key);
            return v;
        });
    }

    public static Map<String, FSQuotaConfig> getQuotaConfigCache(String bucketName) {
        return QUOTA_CONFIG_CACHE.get(bucketName);
    }

    public static void initQuotaScanProcessor() {
        for (int i = 0; i < MAX_SCAN_PRO_NUM; i++) {
            QUOTA_SCANNER_PROCESSORS[i] = UnicastProcessor.create(Queues.<FSQuotaScannerTask>unboundedMultiproducer().get());
            QUOTA_SCANNER_PROCESSORS[i].publishOn(FS_QUOTA_EXECUTOR)
                    .subscribe(FSQuotaScannerTask::startScan);
        }
    }

    public static void onNextQuotaScanTask(FSQuotaScannerTask task) {
        int index = (int) (task.config.nodeId % MAX_SCAN_PRO_NUM);
        RUNNING_TASK_MAP.computeIfAbsent(task.quotaTaskKey, k -> {
            QUOTA_SCANNER_PROCESSORS[index].onNext(task);
            return task;
        });
    }

    public static void removeQuotaScannerTask(FSQuotaScannerTask task) {
        RUNNING_TASK_MAP.computeIfPresent(task.quotaTaskKey, (k, v) -> null);
    }

    public static void starRecover() {
        try {
            if ("master".equals(Utils.getRoleState())) {
                ScanArgs scanArgs = new ScanArgs().match(QUOTA_PREFIX + "*").limit(10);
                KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
                keyScanCursor.setCursor("0");
                KeyScanCursor<String> scanQuotaTask = null;
                do {
                    scanQuotaTask = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).scan(keyScanCursor, scanArgs);
                    for (String key : scanQuotaTask.getKeys()) {
                        try {
                            long startStamp = Long.parseLong(redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).get(key));
                            if (System.currentTimeMillis() - startStamp - QUOTA_SCAN_EXPAND_TIME <= 0) {
                                continue;
                            }
                            String bucketName = key.split("_")[2];
                            String configStr = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(getQuotaBucketKey(bucketName), key);
                            FSQuotaConfig fsQuotaConfig = Json.decodeValue(configStr, FSQuotaConfig.class);
                            FSQuotaScannerTask task = new FSQuotaScannerTask(fsQuotaConfig);
                            onNextQuotaScanTask(task);
                        } catch (Exception e) {
                        }

                    }
                    keyScanCursor.setCursor(scanQuotaTask.getCursor());
                } while (!scanQuotaTask.isFinished());
            }
        } finally {
            QUOTA_RECOVER_EXECUTOR.schedule(FSQuotaRealService::starRecover, SCAN_RECOVER_DELAY, TimeUnit.SECONDS);
        }
    }


    public static void updateQuotaInfo(String bucket, long dirNodeId, long cap, long num, FSQuotaScannerTask task) {
        String tmpCapAndFilesKey = FSQuotaUtils.getTmpCapAndFilesKey(bucket, task.config.getNodeId(), task.config.getQuotaType(),getIdByQuotaType(task.config));
        String lastScanRes = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).get(tmpCapAndFilesKey);
        long finalCap, finalCount;
        boolean[] updateDelta = new boolean[1];
        if (StringUtils.isBlank(lastScanRes)) {
            finalCap = cap;
            finalCount = num;
        } else {
            String[] split = lastScanRes.split("-");
            long lastCount = Long.parseLong(split[0]);
            long lastCap = Long.parseLong(split[1]);
            finalCount = num - lastCount;
            finalCap = cap - lastCap;
            updateDelta[0] = true;
        }
        if (finalCap == 0 && finalCount == 0) {
            updateQuotaInfoRedisVal(bucket, dirNodeId, finalCap, finalCount, task, updateDelta[0], tmpCapAndFilesKey);
            return;
        }
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("config", Json.encode(task.config))
                        .put("vnode", bucketVnode)
                        .put("num", String.valueOf(finalCount))
                        .put("cap", String.valueOf(finalCap))
                        .put("updateDelta", String.valueOf(updateDelta[0]))
                        .put("lun", t.var2))
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo =
                ClientTemplate.oneResponse(msg, UPDATE_QUOTA_INFO, String.class, nodeList);

        responseInfo.responses.
                subscribe(res -> {
                        }
                        , e -> {
                            log.error("updateQuotaInfo error", e);
                        },
                        () -> {
                            if (responseInfo.successNum < pool.getK() + pool.getM() && responseInfo.successNum != 0) {
                                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("bucket", bucket)
                                        .put("num", String.valueOf(num))
                                        .put("cap", String.valueOf(cap))
                                        .put("dirName", String.valueOf(dirNodeId))
                                        .put("config", Json.encode(task.config))
                                        .put("poolQueueTag", poolQueueTag);
                                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_UPDATE_QUOTA_INFO);
                            }
                            if (responseInfo.successNum != 0) {
                                updateQuotaInfoRedisVal(bucket, dirNodeId, finalCap, finalCount, task, updateDelta[0], tmpCapAndFilesKey);
                            }

                            if (responseInfo.successNum == 0) {
                                updateQuotaInfo(bucket, dirNodeId, cap, num, task);
                            }
                        }
                );
    }

    public static void updateQuotaInfoRedisVal(String bucket, long dirName, long finalCap, long finalCount, FSQuotaScannerTask task, boolean updateDelta, String tmpCapAndFilesKey) {
        if (updateDelta) {
            redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(getQuotaTypeKey(task.config));
            redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(tmpCapAndFilesKey);
        } else {
            String tmpVal = finalCount + "-" + finalCap;
            redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).set(tmpCapAndFilesKey, tmpVal);
        }
        FSQuotaRealService.removeQuotaScannerTask(task);
    }

    public static Mono<Payload> updateQuotaInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        FSQuotaConfig fsQuotaConfig = Json.decodeValue(msg.get("config"), FSQuotaConfig.class);
        String bucket = fsQuotaConfig.bucket;
        long nodeId = fsQuotaConfig.getNodeId();
        int quotaType = fsQuotaConfig.getQuotaType();
        String vnode = msg.get("vnode");
        long num = Long.parseLong(msg.get("num"));
        long cap = Long.parseLong(msg.get("cap"));
        String lun = msg.get("lun");
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            if (quotaType == FS_DIR_QUOTA) {
                updateDirCap(writeBatch, bucket, vnode, num, cap, nodeId);
            } else if (quotaType == FS_USER_QUOTA) {
                updateUserQuota(writeBatch, bucket, vnode, num, cap, nodeId, fsQuotaConfig.getUid());
            } else if (quotaType == FS_GROUP_QUOTA) {
                updateGroupQuota(writeBatch, bucket, vnode, num, cap, nodeId, fsQuotaConfig.getGid());
            }
        };
        return BatchRocksDB.customizeOperateMeta(lun, String.valueOf(nodeId).hashCode(), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }


    public static Mono<Integer> setFsQuotaInfo(FSQuotaConfig fsQuotaConfig, Map<String, String> bucketInfo) {
        switch (fsQuotaConfig.getQuotaType()) {
            case FS_DIR_QUOTA:
                return setFsDirQuotaInfo(fsQuotaConfig, bucketInfo);
            case FS_USER_QUOTA:
                return setFsUerQuotaInfo(fsQuotaConfig, bucketInfo);
            case FS_GROUP_QUOTA:
                return setFsGroupQuotaInfo(fsQuotaConfig, bucketInfo);
            default:
                return Mono.just(-1);
        }
    }

    public static Mono<Integer> setFsDirQuotaInfo(FSQuotaConfig fsQuotaConfig, Map<String, String> bucketInfo) {
        return realSetFsQuota(fsQuotaConfig, bucketInfo);
    }


    public static Mono<Integer> setFsUerQuotaInfo(FSQuotaConfig fsQuotaConfig, Map<String, String> bucketInfo) {
        return realSetFsQuota(fsQuotaConfig, bucketInfo);

    }

    public static Mono<Integer> setFsGroupQuotaInfo(FSQuotaConfig fsQuotaConfigs, Map<String, String> bucketInfo) {
        return realSetFsQuota(fsQuotaConfigs, bucketInfo);
    }

    public static Mono<Integer> realSetFsQuota(FSQuotaConfig fsQuotaConfig, Map<String, String> bucketInfo) {
        String quotaBucketKey = getQuotaBucketKey(fsQuotaConfig.getBucket());
        //配额删除五分钟之内不能重复设置
        String delLockKey = getDelLockKey(fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName(), fsQuotaConfig.getQuotaType());
        String[] quotaTypeKey = new String[]{""};
        return checkDelFrequency(delLockKey, fsQuotaConfig.getDirName())
                .flatMap(dirName -> {
                    if (StringUtils.isNotBlank(fsQuotaConfig.getDirName()) && !"/".equals(fsQuotaConfig.getDirName())) {
                        if (!dirName.equals(fsQuotaConfig.getDirName())) {
                            return Mono.just(DEL_QUOTA_FREQUENCY_INODE);
                        }
                        ReqInfo reqHeader = new ReqInfo();
                        reqHeader.bucket = fsQuotaConfig.getBucket();
                        return FsUtils.lookup(fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName(), reqHeader, false, 1, null);
                    }
                    return Mono.just(new Inode().setLinkN(0));
                })
                .flatMap(i -> {
                    if (i.getNodeId() > 0) {
                        fsQuotaConfig.setNodeId(i.getNodeId());
                    }
                    return Mono.just(i);
                })
                .doOnError(e -> {
                    log.info("setFsDirQuotaInfo error.config:{}", fsQuotaConfig, e);
                })
                .flatMap(inode -> {
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        log.error("setFsDirQuotaInfo, dir not found, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-2);
                    }
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        log.error("setFsDirQuotaInfo, dir error, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-1);
                    }
                    if (inode.getLinkN() == DEL_QUOTA_FREQUENCY_INODE.getLinkN()) {
                        log.error("setFsDirQuotaInfo, dir error, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-3);
                    }
                    if (inode.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN() || inode.getLinkN() == FILES_QUOTA_EXCCED_INODE.getLinkN()) {
                        log.error("setFsDirQuotaInfo  hard quota info is not valid, dir error, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-5);
                    }
                    if (RETRY_INODE.getLinkN() == inode.getLinkN() && inode.getNodeId() == -3) {
                        log.error("setFsDirQuotaInfo  dir is renaming, can not set quota info, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-6);
                    }

                    if ((inode.getMode() & S_IFMT) != S_IFDIR) {
                        log.error("setFsDirQuotaInfo, dir not found, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        return Mono.just(-2);
                    }
                    fsQuotaConfig.setNodeId(inode.getNodeId());
                    quotaTypeKey[0] = getQuotaTypeKey(fsQuotaConfig);
                    checkQuotaConfigSet(fsQuotaConfig, quotaBucketKey, quotaTypeKey[0], bucketInfo);
                    if (fsQuotaConfig.isModify()) {
                        int _id = getIdByQuotaType(fsQuotaConfig);
                        //设置硬配额需大于已使用情况
                        return getFsQuotaInfo(fsQuotaConfig.getBucket(), fsQuotaConfig.getNodeId(), fsQuotaConfig.getQuotaType(), _id)
                                .flatMap(info -> {
                                    if (DirInfo.isErrorInfo(info)) {
                                        return Mono.just(-1);
                                    }
                                    boolean quotaAlarm = false;
                                    String capAlarmStatus = NOT_EXCEED_QUOTA;
                                    String filesAlarmStatus = NOT_EXCEED_QUOTA;
                                    long usedCap = Long.parseLong(info.getUsedCap());
                                    if (fsQuotaConfig.getCapacityHardQuota() > 0 && usedCap >= fsQuotaConfig.getCapacityHardQuota()) {
                                        quotaAlarm = true;
                                        capAlarmStatus = EXCEED_QUOTA_HARD_LIMIT;
                                    } else if ((fsQuotaConfig.getCapacitySoftQuota() > 0 && usedCap >= fsQuotaConfig.getCapacitySoftQuota())
                                            || (fsQuotaConfig.getCapacitySoftQuota() == 0 && fsQuotaConfig.getCapacityHardQuota() > 0 && usedCap >= (fsQuotaConfig.getCapacityHardQuota() * 0.8))
                                    ) {
                                        quotaAlarm = true;
                                        capAlarmStatus = EXCEED_QUOTA_SOFT_LIMIT;
                                    }
                                    long usedFiles = Long.parseLong(info.getUsedObjects());
                                    if (fsQuotaConfig.getFilesHardQuota() > 0 && usedFiles >= fsQuotaConfig.getFilesHardQuota()) {
                                        quotaAlarm = true;
                                        filesAlarmStatus = EXCEED_QUOTA_HARD_LIMIT;
                                    } else if ((fsQuotaConfig.getFilesSoftQuota() > 0 && usedFiles >= fsQuotaConfig.getFilesSoftQuota())
                                            || (fsQuotaConfig.getFilesSoftQuota() == 0 && fsQuotaConfig.getFilesHardQuota() > 0 && usedFiles >= (fsQuotaConfig.getFilesHardQuota() * 0.8))
                                    ) {
                                        quotaAlarm = true;
                                        filesAlarmStatus = EXCEED_QUOTA_SOFT_LIMIT;
                                    }
                                    //告警恢复为正常状态
                                    updateAlarmState(fsQuotaConfig, info);
                                    //从正常状态变为告警状态
                                    if (quotaAlarm) {
                                        Tuple2<String, String> capAndFilesAlarmKey = getCapAndFilesAlarmKey(fsQuotaConfig.getNodeId(), fsQuotaConfig.getBucket(), fsQuotaConfig.getQuotaType(), _id);
                                        fsQuotaChangeToAlarm(quotaBucketKey, capAndFilesAlarmKey.var1, capAlarmStatus);
                                        fsQuotaChangeToAlarm(quotaBucketKey, capAndFilesAlarmKey.var2, filesAlarmStatus);
                                    }
                                    return Mono.just(0);
                                });
                    }
                    return Mono.just(0);
                })
                .flatMap(i -> {
                    log.info("setFsDirQuotaInfo, bucket: {}, dirName: {}, quotaConfig: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName(), fsQuotaConfig);
                    if (i != 0) {
                        return Mono.just(i);
                    }
                    if (!fsQuotaConfig.isModify()) {
                        redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, quotaTypeKey[0], Json.encode(fsQuotaConfig));
                        redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).set(quotaTypeKey[0], String.valueOf(fsQuotaConfig.getStartTime()));
                        FSQuotaScannerTask task = new FSQuotaScannerTask(fsQuotaConfig);
                        onNextQuotaScanTask(task);
                    } else {
                        String quotaConfig = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, quotaTypeKey[0]);
                        if (StringUtils.isNotBlank(quotaConfig)) {
                            FSQuotaConfig last = Json.decodeValue(quotaConfig, FSQuotaConfig.class);
                            fsQuotaConfig.setStartTime(last.getStartTime());
                            redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, quotaTypeKey[0], Json.encode(fsQuotaConfig));
                        } else {
                            return Mono.just(-4);
                        }
                    }
                    return Mono.just(0);
                })
                .doOnError(e -> {
                    log.info("setFsDirQuotaInfo error.config:{}", fsQuotaConfig, e);
                })
                .onErrorResume(Mono::error)
                .doFinally(f -> {
                    tryUnLock(quotaTypeKey[0]);
                });
    }

    public static void fsQuotaChangeToAlarm(String bucketQuotaKey, String alarmKey, String alarmStatus) {
        if (!NOT_EXCEED_QUOTA.equals(alarmStatus)) {
            String lastStatus = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(bucketQuotaKey, alarmKey);
            if (StringUtils.isBlank(lastStatus)) {
                lastStatus = NOT_EXCEED_QUOTA;
            }
            redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(bucketQuotaKey, alarmKey, alarmStatus);
            if (!alarmStatus.equals(lastStatus)) {
                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(bucketQuotaKey + LAST_ALARM_SUFFIX, alarmKey + LAST_ALARM_SUFFIX, lastStatus);
            }
        }
    }

    public static void updateAlarmState(FSQuotaConfig fsQuotaConfig, DirInfo info) {
        String quotaBucketKey = getQuotaBucketKey(fsQuotaConfig.getBucket());
        int _id = 0;
        if (fsQuotaConfig.getQuotaType() == FS_USER_QUOTA) {
            _id = fsQuotaConfig.getUid();

        }
        if (fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA) {
            _id = fsQuotaConfig.getGid();
        }
        Tuple2<String, String> capAndFilesAlarmKey = getCapAndFilesAlarmKey(fsQuotaConfig.getNodeId(), fsQuotaConfig.getBucket(), fsQuotaConfig.getQuotaType(), _id);
        String capAlarmStatus = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, capAndFilesAlarmKey.var1);
        String filesAlarmStatus = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, capAndFilesAlarmKey.var2);
        if (NOT_EXCEED_QUOTA.equals(capAlarmStatus) && NOT_EXCEED_QUOTA.equals(filesAlarmStatus)) {
            return;
        }
        //软硬配额告警恢复
        updateAlarmState0(capAlarmStatus, EXCEED_QUOTA_SOFT_LIMIT, Long.parseLong(info.getUsedCap()), fsQuotaConfig.getCapacitySoftQuota(), fsQuotaConfig.getCapacityHardQuota(), quotaBucketKey, capAndFilesAlarmKey.var1);
        updateAlarmState0(capAlarmStatus, EXCEED_QUOTA_HARD_LIMIT, Long.parseLong(info.getUsedCap()), fsQuotaConfig.getCapacitySoftQuota(), fsQuotaConfig.getCapacityHardQuota(), quotaBucketKey, capAndFilesAlarmKey.var1);
        updateAlarmState0(filesAlarmStatus, EXCEED_QUOTA_SOFT_LIMIT, Long.parseLong(info.getUsedObjects()), fsQuotaConfig.getFilesSoftQuota(), fsQuotaConfig.getFilesHardQuota(), quotaBucketKey, capAndFilesAlarmKey.var2);
        updateAlarmState0(filesAlarmStatus, EXCEED_QUOTA_HARD_LIMIT, Long.parseLong(info.getUsedObjects()), fsQuotaConfig.getFilesSoftQuota(), fsQuotaConfig.getFilesHardQuota(), quotaBucketKey, capAndFilesAlarmKey.var2);
    }

    public static void updateAlarmState0(String nowStatus, String alarmStatus, long nowVal, long quotaVal, long hardQuotaVal, String quotaBucketKey, String quotaAlarmKey) {
        //从告警状态下恢复
        if (alarmStatus.equals(nowStatus)) {
            boolean updateToNormal = false;
            String updateStatus = NOT_EXCEED_QUOTA;
            if (EXCEED_QUOTA_SOFT_LIMIT.equals(alarmStatus)) {
                //现在的值
                if (nowVal < quotaVal || (quotaVal == 0 && hardQuotaVal == 0) ||
                        (nowVal < (hardQuotaVal * 0.8))) {
                    updateToNormal = true;
                }
            } else {
                if (nowVal < hardQuotaVal || hardQuotaVal == 0) {
                    updateToNormal = true;
                }
                if (quotaVal > 0 && nowVal >= quotaVal) {
                    updateStatus = EXCEED_QUOTA_SOFT_LIMIT;
                }
            }
            if (updateToNormal) {
                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, quotaAlarmKey, updateStatus);
                if (!updateStatus.equals(nowStatus)) {
                    redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey + LAST_ALARM_SUFFIX, quotaAlarmKey + LAST_ALARM_SUFFIX, nowStatus);
                }
            }
        }
    }


    public static void delFsDirQuotaInfo(String bucket, long dirNodeId, String dirName, int type, int id, MonoProcessor<Integer> delRes, String s3AccountName, boolean force) {
        String quotaTypeKey = getQuotaTypeKey(bucket, dirNodeId, type, id);
        String quotaDirValue = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).get(quotaTypeKey);
        //非强制删除的情况下，如果恢复标记存在时，不允许删除
        if (StringUtils.isNotBlank(quotaDirValue) && !force) {
            delRes.onNext(-7);
            return;
        }
        String quotaBucketKey = getQuotaBucketKey(bucket);
        String quotaConfigStr = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, quotaTypeKey);
        if (StringUtils.isBlank(quotaConfigStr)) {
            delRes.onNext(-4);
            return;
        }
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        List<Tuple3<String, String, String>> allNodeIp = NodeCache.getAllNodeIp();
        int nodeListSize = nodeList.size();
        for (Tuple3<String, String, String> t : allNodeIp) {
            boolean addFlag = true;
            for (int i = 0; i < nodeListSize; i++) {
                if (t.var1.equalsIgnoreCase(nodeList.get(i).var1)) {
                    addFlag = false;
                    break;
                }
            }
            if (addFlag) {
                nodeList.add(t);
            }
        }
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("dirNodeId", String.valueOf(dirNodeId))
                        .put("type", String.valueOf(type))
                        .put("id", String.valueOf(id))
                        .put("bucket", bucket)
                        .put("vnode", bucketVnode)
                        .put("lun", t.var2))
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo =
                ClientTemplate.oneResponse(msg, DEL_QUOTA_INFO, String.class, nodeList);

        responseInfo.responses.
                subscribe(res -> {
                        }
                        , e -> {
                            log.error("updateQuotaInfo error", e);
                            delRes.onNext(-1);
                        },
                        () -> {
                            if (responseInfo.successNum < pool.getK() + pool.getM() && responseInfo.successNum != 0) {
                                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("bucket", bucket)
                                        .put("dirNodeId", String.valueOf(dirNodeId))
                                        .put("type", String.valueOf(type))
                                        .put("id", String.valueOf(id))
                                        .put("poolQueueTag", poolQueueTag);
                                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_DEL_QUOTA_INFO);
                            }
                            if (responseInfo.successNum != 0) {

                                //删除配额配置信息
                                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hdel(quotaBucketKey, quotaTypeKey);
                                String tmpCapAndFilesKey = getTmpCapAndFilesKey(bucket, dirNodeId, type,id);
                                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(tmpCapAndFilesKey);
                                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(quotaTypeKey);
                                Tuple2<String, String> capAndFilesAlarmKey = getCapAndFilesAlarmKey(dirNodeId, bucket, type, id);
                                //删除相关的告警信息
                                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hdel(quotaBucketKey, capAndFilesAlarmKey.var2, capAndFilesAlarmKey.var1);
                                redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hdel(quotaBucketKey + LAST_ALARM_SUFFIX, capAndFilesAlarmKey.var2 + LAST_ALARM_SUFFIX, capAndFilesAlarmKey.var1 + LAST_ALARM_SUFFIX);
                                SetArgs setArgs = new SetArgs().nx().ex(QUOTA_SET_INTERVAL);
                                //主动删除指定配额信息时加锁，五分钟内该目录不能再此进行配额设置，防止重复设置导致配额信息统计不准确
                                if (!force) {
                                    String lockKey = getDelLockKey(bucket, dirName, type);
                                    redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).set(lockKey, "1", setArgs);
                                }
                                FS_QUOTA_EXECUTOR.schedule(() -> {
                                    changeFsQuotaAlarmNormal(bucket, dirNodeId, type, id, s3AccountName, dirName, false);
                                });
                                delRes.onNext(0);
                            }

                            if (responseInfo.successNum == 0) {
                                delFsDirQuotaInfo(bucket, dirNodeId, dirName, type, id, delRes, s3AccountName, force);
                            }
                        }
                );
    }

    public static void changeFsQuotaAlarmNormal(String bucket, long dirNodeId, int type, int id, String s3AccountName, String dirName, boolean needAlarm) {
        try {
            SocketReqMsg dmSocket = new SocketReqMsg("fsQuotaAlarmNormal", 0);
            dmSocket.put("bucket", bucket);
            dmSocket.put("dirNodeId", String.valueOf(dirNodeId));
            dmSocket.put("type", String.valueOf(type));
            if (type == FS_DIR_QUOTA) {
                dmSocket.put("_id", "");
            } else {
                dmSocket.put("_id", String.valueOf(id));
            }
            dmSocket.put("dirName", dirName);
            dmSocket.put("s3AccountName", s3AccountName);
            if (needAlarm) {
                dmSocket.put("needAlarm", "1");
            }
            log.info("sendAndGetResponse,dmSocket:{}", dmSocket.dataMap);
            StringResMsg response = sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
            log.info("sendAndGetResponse,response status:{}", response.getCode());
        } catch (Exception e) {
            FS_QUOTA_EXECUTOR.schedule(() -> changeFsQuotaAlarmNormal(bucket, dirNodeId, type, id, s3AccountName, dirName, needAlarm), 5, TimeUnit.SECONDS);
        }
    }

    public static Mono<DirInfo> getFsQuotaInfo(String bucket, long dirNodeId, int type, int id) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("dirNodeId", String.valueOf(dirNodeId))
                        .put("bucket", bucket)
                        .put("vnode", bucketVnode)
                        .put("id_", String.valueOf(id))
                        .put("type", String.valueOf(type))
                        .put("lun", t.var2))
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<DirInfo> responseInfo =
                ClientTemplate.oneResponse(msg, GET_QUOTA_INFO, DirInfo.class, nodeList);

        DirInfo[] dirInfo = new DirInfo[1];
        MonoProcessor<DirInfo> getResult = MonoProcessor.create();
        responseInfo.responses.
                subscribe(res -> {
                            if (res.var2 == SUCCESS) {
                                if (dirInfo[0] == null) {
                                    dirInfo[0] = res.var3;
                                } else {
                                    if (res.var3.getUsedObjects().compareTo(dirInfo[0].getUsedObjects()) > 0) {
                                        dirInfo[0] = res.var3;
                                    }
                                }
                            } else if (res.var2 != ERROR) {
                                if (DirInfo.NOT_FOUND_DIR_INFO.getFlag().equals(res.var3.getFlag()) && dirInfo[0] == null) {
                                    dirInfo[0] = DirInfo.NOT_FOUND_DIR_INFO;
                                }
                            }
                        }, e -> {
                            getResult.onNext(ERROR_DIR_INFO);
                            log.error("getDirInfo error", e);
                        }, () -> {
                            if (responseInfo.errorNum > pool.getM()) {
                                getResult.onNext(ERROR_DIR_INFO);
                            } else {
                                getResult.onNext(dirInfo[0]);
                            }
                        }
                );
        return getResult;
    }

    public static Mono<FSQuotaConfig> getFsQuotaConfig(String bucket, long dirNodeId, int quotaType, int id) {
        if (dirNodeId == 1) {
            return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                    .hgetall(bucket)
                    .flatMap(bucketInfo -> {
                        long softCap = Long.parseLong(bucketInfo.getOrDefault("soft_quota_value", "0"));
                        long hardCap = Long.parseLong(bucketInfo.getOrDefault("quota_value", "0"));
                        long softFiles = Long.parseLong(bucketInfo.getOrDefault("soft_objnum_value", "0"));
                        long hardFiles = Long.parseLong(bucketInfo.getOrDefault("objnum_value", "0"));
                        return Mono.just(new FSQuotaConfig()
                                .setCapacitySoftQuota(softCap)
                                .setCapacityHardQuota(hardCap)
                                .setFilesSoftQuota(softFiles)
                                .setFilesHardQuota(hardFiles)
                                .setUid(0)
                                .setGid(0)
                                .setQuotaType(quotaType)
                                .setBucket(bucket)
                                .setDirName("/"));
                    });
        }
        String quotaBucketKey = getQuotaBucketKey(bucket);
        String quotaTypeKey = getQuotaTypeKey(bucket, dirNodeId, quotaType, id);
        return redisConnPool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                .hget(quotaBucketKey, quotaTypeKey)
                .defaultIfEmpty("")
                .flatMap(configStr -> {
                    if (StringUtils.isBlank(configStr)) {
                        throw new MsException(QUOTA_INFO_NOT_EXITS, "No such quota info :" + dirNodeId);
                    }
                    return Mono.just(Json.decodeValue(configStr, FSQuotaConfig.class));
                });
    }

    public static Mono<FsQuotaConfigResult> realGetFsQuotaConfig(String bucket, long dirNodeId, int quotaType, int id) {
        return getFsQuotaConfig(bucket, dirNodeId, quotaType, id)
                .flatMap(quotaConfig -> {
                    FsQuotaConfigResult fsDirQuotaConfigResult = FsQuotaConfigResult.mapToFsQuotaConfigResult(quotaConfig);
                    return Mono.just(fsDirQuotaConfigResult);
                });
    }

    public static Mono<FsQuotaListConfigResult> realListFsQuotaConfig(String bucket, String dirName) {
        String quotaBucketKey = getQuotaBucketKey(bucket);
        return redisConnPool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                .hgetall(quotaBucketKey)
                .defaultIfEmpty(Collections.emptyMap())
                .flatMap(map -> {
                    Map<String, FSQuotaConfig> resultMap = new ConcurrentHashMap<>();
                    if (map.isEmpty()) {
                        return Mono.just(resultMap);
                    }
                    for (String key : map.keySet()) {
                        if (key.startsWith(QUOTA_PREFIX)) {
                            FSQuotaConfig fsQuotaConfig = Json.decodeValue(map.get(key), FSQuotaConfig.class);
                            if (StringUtils.isBlank(dirName) || fsQuotaConfig.getDirName().contains(dirName)) {
                                resultMap.put(key, fsQuotaConfig);
                            }
                        }

                    }
                    return Mono.just(resultMap);
                })
                .flatMap(quotaMap -> {
                    if (quotaMap.isEmpty()) {
                        FsQuotaListConfigResult fsQuotaListConfigResult = new FsQuotaListConfigResult(bucket, new LinkedList<>());
                        return Mono.just(fsQuotaListConfigResult);
                    }
                    AtomicInteger errorCount = new AtomicInteger(0);
                    return Flux.fromStream(quotaMap.keySet().stream())
                            .flatMap(quotaTypeKey -> {
                                FSQuotaConfig fsQuotaConfig = quotaMap.get(quotaTypeKey);
                                return getFsQuotaInfo(bucket, fsQuotaConfig.getNodeId(), fsQuotaConfig.getQuotaType(), FSQuotaUtils.getIdByQuotaType(fsQuotaConfig))
                                        .flatMap(dirInfo -> {
                                            if (ERROR_DIR_INFO.getFlag().equals(dirInfo.getFlag())) {
                                                errorCount.incrementAndGet();
                                            }
                                            FsQuotaConfigAndUsedResult fsQuotaConfigAndUsedResult = FsQuotaConfigAndUsedResult.mapToFsQuotaConfigAndUsedResult(fsQuotaConfig);
                                            fsQuotaConfigAndUsedResult.setUsedCapacity(Long.parseLong(dirInfo.getUsedCap()));
                                            fsQuotaConfigAndUsedResult.setUsedFiles(Long.parseLong(dirInfo.getUsedObjects()));
                                            return Mono.just(fsQuotaConfigAndUsedResult);
                                        });
                            }, 4)
                            .collectList()
                            .flatMap(list -> {
                                if (errorCount.get() >= quotaMap.keySet().size()) {
                                    throw new MsException(UNKNOWN_ERROR, "list quota config error.");
                                }
                                //根据list中，每一个FsQuotaConfigAndUsedResult的dirName进行排序
                                list.sort(Comparator.comparing(FsQuotaConfigAndUsedResult::getDirName));
                                FsQuotaListConfigResult fsQuotaListConfigResult = new FsQuotaListConfigResult(bucket, list);
                                return Mono.just(fsQuotaListConfigResult);
                            });
                });
    }

    public static void checkQuotaAlarm() {
        //克隆checkKeys所有元素
        Map<String, String> finalCheckKeys = new HashMap<>(checkKeys);
        checkKeys.clear();
        for (String quotaKey : finalCheckKeys.keySet()) {
            String bucketName = finalCheckKeys.get(quotaKey);
            String config = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(getQuotaBucketKey(bucketName), quotaKey);
            //简单判断此字符串是否符合预期
            if (StringUtils.isBlank(config)) {
                QUOTA_CONFIG_CACHE.compute(bucketName, (k, v) -> {
                    if (v != null) {
                        v.remove(quotaKey);
                    }
                    return v;
                });
                deleteAlarmKey(quotaKey, bucketName);
                continue;
            }
            FSQuotaConfig fsQuotaConfig;
            try {
                fsQuotaConfig = Json.decodeValue(config, FSQuotaConfig.class);
            } catch (Exception e) {
                log.info("checkQuotaAlarm error:{}", config, e);
                continue;
            }
            int[] id = new int[]{0};
            if (fsQuotaConfig.getQuotaType() == FS_USER_QUOTA) {
                id[0] = fsQuotaConfig.getUid();

            }
            if (fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA) {
                id[0] = fsQuotaConfig.getGid();
            }

            long capSoftLimit = fsQuotaConfig.getCapacitySoftQuota();
            long capHardLimit = fsQuotaConfig.getCapacityHardQuota();
            long filesSoftLimit = fsQuotaConfig.getFilesSoftQuota();
            long filesHardLimit = fsQuotaConfig.getFilesHardQuota();
            if (capHardLimit == 0 && filesHardLimit == 0 && capSoftLimit == 0 && filesSoftLimit == 0) {
                continue;
            }
            getFsQuotaInfo(bucketName, fsQuotaConfig.getNodeId(), fsQuotaConfig.getQuotaType(), id[0])
                    .publishOn(FS_QUOTA_EXECUTOR)
                    .subscribe(dirInfo -> {
                        if (DirInfo.isErrorInfo(dirInfo)) {
                            return;
                        }
                        long usedFiles = Long.parseLong(dirInfo.getUsedObjects());
                        long usedCap = Long.parseLong(dirInfo.getUsedCap());
                        Tuple2<String, String> capAndFilesAlarmKey = getCapAndFilesAlarmKey(fsQuotaConfig.getNodeId(), bucketName, fsQuotaConfig.getQuotaType(), id[0]);
                        checkQuotaLimit(bucketName, usedCap, capAndFilesAlarmKey.var1, capSoftLimit, capHardLimit);
                        checkQuotaLimit(bucketName, usedFiles, capAndFilesAlarmKey.var2, filesSoftLimit, filesHardLimit);
                    });

        }
    }
}
