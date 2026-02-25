package com.macrosan.doubleActive.archive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.xmlmsg.lifecycle.*;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.USER_META;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_ON;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_SUSPEND;
import static com.macrosan.doubleActive.archive.ArchiveHandler.*;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.ERROR_UNSYNC_RECORD;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.NOT_FOUND_UNSYNC_RECORD;
import static com.macrosan.utils.lifecycle.LifecycleUtils.*;

@Log4j2
public class ArchieveUtils {
    static RedisConnPool pool = RedisConnPool.getInstance();
    public static Map<Integer, String> START_TIME = new ConcurrentHashMap<>();
    public static Map<Integer, String> END_TIME = new ConcurrentHashMap<>();

    public static final String ARCHIVE_MAP = "archive_map";
    // 0或不存在表示本轮尚未开始扫描，需要到达start_time后从头开始扫描；
    // 1表示已开始扫描，因为可能是切换主节点，需要无视是否是START_TIME直接从marker开始扫描
    public static final String ARCHIVE_STATUS = "archive_status";
    public static final String SCANNED_BUCKETS = "scanned_buckets";
    // 判断是否到达归档日期的基准，为endtime所在的日期
    public static final String BASE_MILLIS = "base_millis";

    // 基准日期，如果startTime和endTime跨日期，则需要在扫描处理的时候将endTime所在日期作为判断归档日期是否打到的基准
    public static Map<Integer, Calendar> BASE_CALENDAR = new ConcurrentHashMap<>();

    // 开始时间戳
    public static Map<Integer, Long> BASE_STAR_TIME = new ConcurrentHashMap<>();

    public static final String ARCHIVE_SYNC_REC_MARK = "ARCHIVE_SYNC_MARK";

    static final long ONE_DAY_TIMESTAMPS = (60 * 60 * 24 * 1000);
    static final long ONE_HOUR_TIMESTAMPS = (60 * 60 * 1000);

    private static final ReentrantLock lock = new ReentrantLock();

    public static void lock() {
        lock.lock();
    }

    public static void unlock() {
        lock.unlock();
    }

    public static Calendar getCalendarByStr(String str) {
        Calendar calendar = Calendar.getInstance();
        int hour = Integer.parseInt(str.split(":")[0]);
        int minute = Integer.parseInt(str.split(":")[1]);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    public static boolean datasyncIsOn(String bucketName) {
        String datasync = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH);
        return SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync);
    }

    public static void checkArchiveTimeRotation(int clusterIndex) {
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(SYS_DATA_SYNC_TIME).defaultIfEmpty(new HashMap<>())
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(ARCHIVE_MAP).defaultIfEmpty(new HashMap<>()))
                .publishOn(SCAN_SCHEDULER)
                .doFinally(s -> DataSynChecker.SCAN_SCHEDULER.schedule(() -> checkArchiveTimeRotation(clusterIndex), 60, TimeUnit.SECONDS))
                .doOnNext(tuple2 -> {
                    Map<String, String> timeMap = tuple2.getT1();
                    Map<String, String> archiveMap = tuple2.getT2();
                    String archiveStatus = archiveMap.get(ARCHIVE_STATUS + clusterIndex);
                    // 非主节点递归，预备切主。
                    if (!MainNodeSelector.checkIfSyncNode()) {
                        return;
                    }
                    boolean needReschedule = false;
                    String start = timeMap.get(SYNC_START_TIME + clusterIndex) == null ? "00:00" : timeMap.get(SYNC_START_TIME + clusterIndex);
                    if (!start.equals(START_TIME.get(clusterIndex))) {
                        START_TIME.put(clusterIndex, start);
                        needReschedule = true;
                    }
                    String end = timeMap.get(SYNC_END_TIME + clusterIndex) == null ? "00:00" : timeMap.get(SYNC_END_TIME + clusterIndex);
                    if (!end.equals(END_TIME.get(clusterIndex))) {
                        END_TIME.put(clusterIndex, end);
                        needReschedule = true;
                    }
                    Calendar startCalendar = ArchieveUtils.getCalendarByStr(START_TIME.get(clusterIndex));
                    Calendar endCalendar = ArchieveUtils.getCalendarByStr(END_TIME.get(clusterIndex));
                    if (startCalendar.after(endCalendar) || startCalendar.equals(endCalendar)) {
                        endCalendar.add(Calendar.DAY_OF_YEAR, 1);
                    }

                    if (needReschedule) {
                        log.info("set next archive time, cluster {}: {} - {}", clusterIndex, START_TIME.get(clusterIndex), END_TIME.get(clusterIndex));
                        settleStartAndEnd(clusterIndex);
                    }

                    Calendar now = Calendar.getInstance();
                    if (now.before(startCalendar) || now.after(endCalendar)) {
                        if (archiveScanning.get(clusterIndex).get()) {
                            log.info("cluster {} not in archive time, terminate.", clusterIndex);
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARCHIVE_MAP, ARCHIVE_STATUS + clusterIndex, "0");
                            ArchiveHandler.getInstance().close(clusterIndex);
                        }
                    } else {
                        // 可能会有在启动时间内扫描不完所有数据的情况，此时需要在下次启动时继续扫描，而不是从头开始。
                        // 当前时间在start_time和end_time之间，且扫描状态为1，才可以由此启动扫描，否则都要到达start_time才可以启动扫描
                        // 或者是切换主节点后，也从这里启动
                        if ("1".equals(archiveStatus) && MainNodeSelector.checkIfSyncNode() && !archiveScanning.get(clusterIndex).get()) {
                            if (needDelayStart(clusterIndex)) {
                                log.info("end time is too close, delay archive scan1. {}", endStamp.get(clusterIndex));
                                return;
                            }
                            if (archiveScanning.get(clusterIndex).compareAndSet(false, true)) {
                                log.info("cluster {} continue former archive scan. ", clusterIndex);
                                final long lifecycleStartStamp = getLifecycleStartStamp(start);
                                BASE_STAR_TIME.put(clusterIndex, lifecycleStartStamp);
                                BASE_CALENDAR.put(clusterIndex, Calendar.getInstance());
                                if (StringUtils.isNotBlank(archiveMap.get(BASE_MILLIS + clusterIndex))) {
                                    BASE_CALENDAR.get(clusterIndex).setTimeInMillis(Long.parseLong(archiveMap.get(BASE_MILLIS + clusterIndex)));
                                } else {
                                    //  理论不会有这种情况
                                    log.error("no BASE_MILLIS in redis, please check. ");
                                    BASE_CALENDAR.get(clusterIndex).setTimeInMillis(endCalendar.getTimeInMillis());
                                }
                                int hour = Integer.parseInt(START_TIME.get(clusterIndex).split(":")[0]);
                                int minute = Integer.parseInt(START_TIME.get(clusterIndex).split(":")[1]);
                                BASE_CALENDAR.get(clusterIndex).set(Calendar.HOUR_OF_DAY, hour);
                                BASE_CALENDAR.get(clusterIndex).set(Calendar.MINUTE, minute);
                                BASE_CALENDAR.get(clusterIndex).set(Calendar.SECOND, 0);
                                BASE_CALENDAR.get(clusterIndex).set(Calendar.MILLISECOND, 0);
                                ArchiveHandler.getInstance().start(clusterIndex);
                            }
                        }
                    }
                })
                .doOnError(e -> log.error("checkArchiveTimeRotation err, ", e))
                .subscribe();
    }

    public static ScheduledFuture<?> startSchedule;
    public static ScheduledFuture<?> endSchedule;

    public static Map<Integer, Calendar> tempStartCalendar = new ConcurrentHashMap<>();

    // 注册每天开始扫描的时间。如果开始和结束时间变更则需要再次调用
    public static void settleStartAndEnd(int clusterIndex) {
        Calendar startCalendar = ArchieveUtils.getCalendarByStr(START_TIME.get(clusterIndex));
        Calendar endCalendar = ArchieveUtils.getCalendarByStr(END_TIME.get(clusterIndex));
        tempStartCalendar.put(clusterIndex, ArchieveUtils.getCalendarByStr(START_TIME.get(clusterIndex)));
        if (startCalendar.after(endCalendar) || startCalendar.equals(endCalendar)) {
            if (startCalendar.getTimeInMillis() - endCalendar.getTimeInMillis() < 5 * 60 * 60) {
                // 保证下一次开始的时间和本次结束的时间间隔在五分钟以上，提前执行end，防止end和start设置时间相同时的错乱。
                endCalendar.setTimeInMillis(startCalendar.getTimeInMillis() - 5 * 60 * 60);
            }
            endCalendar.add(Calendar.DAY_OF_YEAR, 1);
        }
        // 计算首次执行延迟时间
        long currentTimeMillis = System.currentTimeMillis();
        long initialDelay = startCalendar.getTimeInMillis() - currentTimeMillis;
        if (initialDelay < 0) {  // 如果当前时间已过目标时间，延迟到次日
            initialDelay += ONE_DAY_TIMESTAMPS;
        }

        long endDelay = endCalendar.getTimeInMillis() - currentTimeMillis;
        if (endDelay < 0) {  // 如果当前时间已过目标时间，立刻停止
            endDelay = 0;
        }

        Optional.ofNullable(startSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
        startSchedule = SCAN_TIMER.scheduleAtFixedRate(() -> {
            scheduleStart(startCalendar, clusterIndex);
        }, initialDelay, ONE_DAY_TIMESTAMPS, TimeUnit.MILLISECONDS);

        Optional.ofNullable(endSchedule).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
        endSchedule = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                if (!MainNodeSelector.checkIfSyncNode()) {
                    return;
                }
                log.info("check end archive. {} {}", clusterIndex, archiveScanning.get(clusterIndex).get());
                if (archiveScanning.get(clusterIndex).get()) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARCHIVE_MAP, ARCHIVE_STATUS + clusterIndex, "0");
                    ArchiveHandler.getInstance().close(clusterIndex);
                }
            } catch (Exception e) {
                log.error("scheduledFutureEnd err, ", e);
            }
        }, endDelay, ONE_DAY_TIMESTAMPS, TimeUnit.MILLISECONDS);
    }

    public static synchronized void scheduleStart(Calendar startCalendar, int clusterIndex) {
        try {
            if (startCalendar.getTimeInMillis() != tempStartCalendar.get(clusterIndex).getTimeInMillis()) {
                log.info("cluster {} start time changed. {} to {}", clusterIndex, startCalendar.getTime(), tempStartCalendar.get(clusterIndex).getTime());
                return;
            }

            if (archiveScanning.get(clusterIndex).get()) {
                log.info("former archive is still scanning.");
                return;
            }
            if (!MainNodeSelector.checkIfSyncNode()) {
                // 该定时器触发时本地节点不一定是扫描节点
                return;
            }
            if (needDelayStart(clusterIndex)) {
                log.info("cluster {} end time is too close, delay archive scan2. {}", clusterIndex, endStamp.get(clusterIndex));
                SCAN_SCHEDULER.schedule(() -> scheduleStart(startCalendar, clusterIndex), 60, TimeUnit.SECONDS);
                return;
            }
            String archiveStatus = pool.getCommand(REDIS_SYSINFO_INDEX).hget(ARCHIVE_MAP, ARCHIVE_STATUS + clusterIndex);
            log.info("cluster {} check start archive. status {}", clusterIndex, archiveStatus);
            if (StringUtils.isBlank(archiveStatus) || "0".equals(archiveStatus)) {
                // 0表示上一轮结束。只有这种情况才会由startSchedule来启动扫描
                BASE_CALENDAR.put(clusterIndex, Calendar.getInstance());
                BASE_STAR_TIME.put(clusterIndex, startCalendar.getTimeInMillis());
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARCHIVE_MAP, BASE_MILLIS + clusterIndex, String.valueOf(startCalendar.getTimeInMillis()));
                BASE_CALENDAR.get(clusterIndex).setTimeInMillis(startCalendar.getTimeInMillis());
                if (archiveScanning.get(clusterIndex).compareAndSet(false, true)) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARCHIVE_MAP, ARCHIVE_STATUS + clusterIndex, "1");
                    log.info("start archive scan, {}", startCalendar.getTimeInMillis());
                    ArchiveHandler.getInstance().start(clusterIndex);
                }
            }

        } catch (Exception e) {
            log.error("scheduledFutureStart err, ", e);
        }
    }

    public static boolean archiveIsEnable(String bucketName, int clusterIndex) {
        final Map<String, String> bucketMap = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketMap.isEmpty()) {
            return false;
        }
        BucketSyncSwitchCache.getInstance().check(bucketName, bucketMap);
        String archiveIndex = bucketMap.getOrDefault(ARCHIVE_INDEX, "");
        if (StringUtils.isNotEmpty(archiveIndex)) {
            try {
                Map<Integer, Set<Integer>> archiveMap = Json.decodeValue(archiveIndex, new TypeReference<Map<Integer, Set<Integer>>>() {
                });
                if (archiveMap.containsKey(LOCAL_CLUSTER_INDEX) && !archiveMap.get(LOCAL_CLUSTER_INDEX).isEmpty() && archiveMap.get(LOCAL_CLUSTER_INDEX).contains(clusterIndex)) {
                    return true;
                }

            } catch (Exception e) {
                log.error("get archiveIndex error: {}", archiveIndex, e);
            }
        }

        return false;
    }

    public static String getPrefix(Rule rule) {
        String prefix = "";
        if (rule.getFilter() != null) {
            prefix = rule.getFilter().getPrefix();
        } else {
            prefix = rule.getPrefix();
        }
        return prefix;
    }

    public static boolean putRuleMap(int clusterIndex, String bucket, LifecycleConfiguration lifecycleConfiguration) {
        boolean res = false;
        String key = bucket + "_" + clusterIndex;
        for (Rule rule : lifecycleConfiguration.getRules()) {
            if (!Rule.ENABLED.equals(rule.getStatus())) {
                continue;
            }

            for (Condition condition : rule.getConditionList()) {
                String prefix = getPrefix(rule);
                Map<String, String> tagMap = new ConcurrentSkipListMap<>();
                if (StringUtils.isEmpty(prefix)) {//当Filter中的prefix不存在 互斥
                    if (null != rule.getFilter()) {
                        List<Tag> tags = rule.getFilter().getTags();//阿里云中的规则tag可以设置多个
                        if (tags != null) {
                            for (Tag tag : tags) {
                                tagMap.put(tag.getKey(), tag.getValue());
                            }
                        }
                    }
                }
                final JSONObject json = new JSONObject();
                if (condition instanceof Replication) {
                    Replication replication = (Replication) condition;
                    Integer sourceIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getSourceSite());
                    if (LOCAL_CLUSTER_INDEX != sourceIndex) {
                        continue;
                    }
                    Integer backupIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getBackupSite());
                    if (backupIndex == null || backupIndex != clusterIndex) {
                        continue;
                    }
                    String rulesMapKey;
                    String filterType;
                    ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> rulesMap;
                    if (tagMap.isEmpty()) {
                        rulesMapKey = prefix;
                        rulesMap = PREFIX_RULES_MAP;
                        filterType = "PREFIX";
                    } else {
                        rulesMapKey = Json.encode(tagMap);
                        rulesMap = TAG_RULES_MAP;
                        filterType = "TAG";
                    }
                    json.put("version", "current");
                    json.put("filterType", filterType);
                    json.put("prefixOrTag", rulesMapKey);
                    Set<Rule> formerRules = rulesMap.computeIfAbsent(bucket + "_" + clusterIndex, k -> new ConcurrentSkipListMap<>()).getOrDefault(rulesMapKey, new ConcurrentHashSet<>());
                    if (replication.getDays() != null) {
                        //同一个prefix设置了归档天数，保留一个天数最小的归档策略
                        boolean addToRules = true;
                        for (Rule formerRule : formerRules) {
                            for (Condition condition1 : formerRule.getConditionList()) {
                                // formerRule.getConditionList()应该只有一个Replication，这里可以不用遍历
                                Replication formerReplication = (Replication) condition1;
                                if (formerReplication.getDays() != null && replication.getDays().compareTo(formerReplication.getDays()) >= 0) {
                                    addToRules = false;
                                    break;
                                }
                            }
                        }
                        if (addToRules) {
                            long timestamps = daysToDeadlineTimestamps(clusterIndex, replication.getDays());
                            final String backupKey = getBackupKey(bucket, filterType, backupIndex, rulesMapKey, false, timestamps, replication.getDays());
                            String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, false, null, replication.getDays(), sourceIndex);
                            BACK_ANALYZER_MAP.put(backupKey, analyzerKey);
                            json.put("timeStr", "days_" + replication.getDays() + "_" + timestamps);
                            BUCKET_RULES_MAP.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(backupKey, json.toJSONString());
                            res = true;
                        }
                    } else {
                        if (dateActivated(replication.getDate())) {
                            //同一个prefix设置了归档日期，在基准日期之前，保留一个日期最靠前的rule（接口也会做处理）
                            boolean addToRules = true;
                            for (Rule formerRule : formerRules) {
                                for (Condition condition1 : formerRule.getConditionList()) {
                                    Replication formerReplication = (Replication) condition1;
                                    // 存在一个formerReplication的date更早，该条replication不加入rulesMap
                                    if (StringUtils.isNotBlank(formerReplication.getDate())
                                            && parseDateToTimestamps(replication.getDate()) >= parseDateToTimestamps(formerReplication.getDate())) {
                                        addToRules = false;
                                        break;
                                    }
                                }
                            }
                            if (addToRules) {
                                final long timestamps = parseDateToTimestamps(replication.getDate());
                                final String backupKey = getBackupKey(bucket, filterType, backupIndex, rulesMapKey, false, timestamps, replication.getDays());
                                String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, false, replication.getDate(), null, sourceIndex);
                                BACK_ANALYZER_MAP.put(backupKey, analyzerKey);
                                json.put("timeStr", "date_" + timestamps);
                                BUCKET_RULES_MAP.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(backupKey, json.toJSONString());
                                res = true;
                            }
                        }
                    }

                } else if (condition instanceof NoncurrentVersionReplication) {
                    // 历史版本的情况
                    NoncurrentVersionReplication replication = (NoncurrentVersionReplication) condition;
                    Integer sourceIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getSourceSite());
                    if (LOCAL_CLUSTER_INDEX != sourceIndex) {
                        continue;
                    }
                    Integer backupIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getBackupSite());
                    if (backupIndex == null || backupIndex != clusterIndex) {
                        continue;
                    }
                    String rulesMapKey;
                    String filterType;
                    ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> rulesMap;
                    if (tagMap.isEmpty()) {
                        rulesMapKey = prefix;
                        rulesMap = NONCURRENT_PREFIX_RULES_MAP;
                        filterType = "PREFIX";
                    } else {
                        rulesMapKey = Json.encode(tagMap);
                        rulesMap = NONCURRENT_TAG_RULES_MAP;
                        filterType = "TAG";
                    }

                    Set<Rule> formerRules = rulesMap.computeIfAbsent(bucket + "_" + clusterIndex, k -> new ConcurrentSkipListMap<>()).getOrDefault(rulesMapKey, new ConcurrentHashSet<>());
                    //同一个prefix设置了归档天数，保留一个天数最小的归档策略
                    boolean addToRules = true;
                    for (Rule formerRule : formerRules) {
                        for (Condition condition1 : formerRule.getConditionList()) {
                            // formerRule.getConditionList()应该只有一个Replication，这里可以不用遍历
                            NoncurrentVersionReplication formerReplication = (NoncurrentVersionReplication) condition1;
                            if (formerReplication.getNoncurrentDays() != null && replication.getNoncurrentDays().compareTo(formerReplication.getNoncurrentDays()) >= 0) {
                                addToRules = false;
                                break;
                            }
                        }
                    }
                    if (addToRules) {
                        long timestamps = daysToDeadlineTimestamps(clusterIndex, replication.getNoncurrentDays());
                        final String backupKey = getBackupKey(bucket, filterType, backupIndex, rulesMapKey, true, timestamps, replication.getNoncurrentDays());
                        String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, true, null, replication.getNoncurrentDays(), sourceIndex);
                        BACK_ANALYZER_MAP.put(backupKey, analyzerKey);
                        json.put("filterType", filterType);
                        json.put("prefixOrTag", rulesMapKey);
                        json.put("version", "history");
                        json.put("timeStr", "days_" + replication.getNoncurrentDays() + "_" + timestamps);
                        BUCKET_RULES_MAP.computeIfAbsent(key, k -> new ConcurrentHashMap<>()).put(backupKey, json.toJSONString());
                        res = true;
                    }

                }
            }
        }
        return res;
    }

    private static long daysToDeadlineTimestamps(Integer clusterIndex, Integer days) {
        return BASE_STAR_TIME.get(clusterIndex) - ONE_DAY_TIMESTAMPS * days;
    }

    private static long daysToTimestamps(int clusterIndex, Integer days) {
        return BASE_CALENDAR.get(clusterIndex).getTimeInMillis() - ONE_DAY_TIMESTAMPS * days;
    }

    private static void addToRulesMap(int clusterIndex, String bucket, String key, Rule rule, Condition condition,
                                      ConcurrentHashMap<String, ConcurrentSkipListMap<String, ConcurrentHashSet<Rule>>> rulesMap) {
        Rule copy = rule.copy();
        ArrayList<Condition> list = new ArrayList<>();
        list.add(condition);
        copy.setConditionList(list);

        rulesMap.computeIfAbsent(bucket + "_" + clusterIndex, k -> new ConcurrentSkipListMap<>())
                .computeIfAbsent(key, k -> new ConcurrentHashSet<>())
                .add(copy);
    }

    static Mono<Boolean> putHisSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList) {
        try {
            // 当出现getObject异常或recoverPut失败的情况，需要将record落盘，通过异步复制流程恢复数据
            record.setCommited(true);
            return ECUtils.updateSyncRecord(record, nodeList, false)
                    .publishOn(SCAN_SCHEDULER)
                    .timeout(Duration.ofSeconds(30))
                    .map(res -> {
                        // 只要不是全失败就返回true，不再使用rabbitmq修复，因为修复可能在dealRecord之后导致Y重复计数，但也可能在dealRecod之前，无法处理。
                        return res != 0;
                    })
                    .onErrorResume(e -> {
                        log.error("syncErrorHandler error1, {}, {}", record.bucket, record.recordKey, e);
                        return Mono.just(false);
                    });
        } catch (Exception e) {
            log.error("syncErrorHandler error2, ", e);
            return Mono.just(false);
        }
    }

    public static Map<String, String> getUserMetaMap(MetaData metaData) {
        //map是直接从对象元数据中获取到的，用户元数据的key的前缀都是x-amz-meta，需要去除后再比较
        Map<String, String> resMap = new HashMap<>();
        Map<String, String> map = Json.decodeValue(metaData.userMetaData, new TypeReference<Map<String, String>>() {
        });
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String k = entry.getKey().toLowerCase();
            if (k.startsWith(USER_META)) {
                String key = entry.getKey().substring(USER_META.length());
                resMap.put(key, entry.getValue());
            }
        }
        return resMap;
    }

    public static boolean isContain(Map<String, String> map1, Map<String, String> map2) {
//        log.info(map1);
//        log.info(map2);
        //先判断map1中keyset是否完全包含map2中的key
        Set<String> map1Keys = map1.keySet();
        Set<String> map2Keys = map2.keySet();
        if (map1Keys.containsAll(map2Keys)) {//当map1的keyset包含map2的时，继续判断；不包含则直接返回false
            for (Map.Entry<String, String> entry1 : map1.entrySet()) {
                for (Map.Entry<String, String> entry2 : map2.entrySet()) {
                    if (entry1.getKey().equals(entry2.getKey())) {//key相同时比较映射的value
                        boolean res = entry1.getValue().equals(entry2.getValue());
                        if (!res) {//相同key对应的value如果不同则返回false
                            return false;
                        }
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static Map<Integer, Long> endStamp = new ConcurrentHashMap<>();

    public static boolean needDelayStart(int clusterIndex) {
        Long endStamp0 = endStamp.computeIfAbsent(clusterIndex, key -> -1L);
        if (endStamp0 == -1) {
            return false;
        }

        if (System.currentTimeMillis() - endStamp0 < 120 * 1000) {
            // 扫描停止后的一段时间内不允许重新开始扫描，防止上一次的信息还未清除、start()进程还未停止。
            return true;
        } else {
            return false;
        }
    }

    // 本站点如果在往任意其他站点进行归档，则返回true
    public static boolean archiveIsRunning() {
        for (Integer clusterIndex : MossHttpClient.INDEX_IPS_ENTIRE_MAP.keySet()) {
            String archiveStatus = pool.getCommand(REDIS_SYSINFO_INDEX).hget(ARCHIVE_MAP, ARCHIVE_STATUS + clusterIndex);
            if ("1".equals(archiveStatus)) {
                return true;
            }
        }
        return false;
    }

    public static String getBackupRecord(String vnode, String filterType, int backupSite, Map<String, String> tagMap, String prefix, boolean isHistoryVersion, long timestamps, Integer days) {
        String version;
        String record;
        String tagsJson = "";
        if (tagMap != null && "TAG".equals(filterType)) {
            tagsJson = JSON.toJSONString(tagMap);
        }

        if (isHistoryVersion) {
            version = "history";
        } else {
            version = "current";
        }

        if (days == null) {
            switch (filterType) {
                case "PREFIX":
                    record = vnode + File.separator + prefix + File.separator + backupSite + File.separator + version + File.separator + "date_" + timestamps;
                    break;
                case "TAG":
                    record = vnode + File.separator + tagsJson + File.separator + backupSite + File.separator + version + File.separator + "date_" + timestamps;
                    break;
                default:
                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "unknown filter type");
            }
        } else {
            switch (filterType) {
                case "PREFIX":
                    record = vnode + File.separator + prefix + File.separator + backupSite + File.separator + version + File.separator + "days_" + days;
                    break;
                case "TAG":
                    record = vnode + File.separator + tagsJson + File.separator + backupSite + File.separator + version + File.separator + "days_" + days;
                    break;
                default:
                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "unknown filter type");
            }
        }
        return record;

    }

    public static String getBackupKey(String bucketName, String filterType, int backupSite, String prefixOrTag, boolean isHistoryVersion, long timestamps, Integer days) {
        String version;
        String record;

        if (isHistoryVersion) {
            version = "history";
        } else {
            version = "current";
        }

        if (days == null) {
            record = bucketName + File.separator + backupSite + File.separator + filterType + File.separator + prefixOrTag + File.separator + version + File.separator + "date_" + timestamps;
        } else {
            record =
                    bucketName + File.separator + backupSite + File.separator + filterType + File.separator + prefixOrTag + File.separator + version + File.separator + "days_" + days + "_" + timestamps;
        }
        return record;
    }

    /**
     * @param bucketName       桶名
     * @param filterType       策略的类型，PREFIX（按前缀匹配）或TAG（对象标签）
     * @param backupSite       目标站点名
     * @param prefixOrTag      前缀名称（filterType类型为PREFIX），或Json格式的对象标签（filterType类型为TAG）
     * @param isHistoryVersion 归档策略为当前版本为current，历史版本为history
     * @param dateStr          日期
     * @param days             天数
     * @return 归档统计使用的key。
     */
    public static String getAnalyzerKey(String bucketName, String filterType, int backupSite, String prefixOrTag, boolean isHistoryVersion, String dateStr, Integer days, int sourceSite) {
        String version;
        String record;

        if (isHistoryVersion) {
            version = "history";
        } else {
            version = "current";
        }

        if (days == null) {
            record =
                    bucketName + File.separator + backupSite + File.separator + filterType + File.separator + prefixOrTag + File.separator + version + File.separator + dateStr + File.separator + sourceSite;
        } else {
            record =
                    bucketName + File.separator + backupSite + File.separator + filterType + File.separator + prefixOrTag + File.separator + version + File.separator + days + File.separator + sourceSite;
        }
        return record;
    }

    public static Tuple2<HashSet<String>, HashSet<String>> generateAnalyzerKeySet(String bucket, LifecycleConfiguration lifecycleConfiguration){
        HashSet<String> analyzerKeySet = new HashSet<>();
        HashSet<String> backupKeySet = new HashSet<>();
        for (Rule rule : lifecycleConfiguration.getRules()) {
            if (!Rule.ENABLED.equals(rule.getStatus())) {
                continue;
            }
            for (Condition condition : rule.getConditionList()) {
                String prefix = getPrefix(rule);
                Map<String, String> tagMap = new ConcurrentSkipListMap<>();
                if (StringUtils.isEmpty(prefix)) {//当Filter中的prefix不存在 互斥
                    if (null != rule.getFilter()) {
                        List<Tag> tags = rule.getFilter().getTags();//阿里云中的规则tag可以设置多个
                        if (tags != null) {
                            for (Tag tag : tags) {
                                tagMap.put(tag.getKey(), tag.getValue());
                            }
                        }
                    }
                }

                if (condition instanceof Replication) {
                    Replication replication = (Replication) condition;
                    Integer sourceIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getSourceSite());
                    Integer backupIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getBackupSite());
                    if (sourceIndex == null || backupIndex == null) {
                        continue;
                    }
                    String rulesMapKey;
                    String filterType;
                    if (tagMap.isEmpty()) {
                        rulesMapKey = prefix;
                        filterType = "PREFIX";
                    } else {
                        rulesMapKey = Json.encode(tagMap);
                        filterType = "TAG";
                    }
                    if (replication.getDays() != null) {
                        String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, false, null, replication.getDays(), sourceIndex);
                        analyzerKeySet.add(analyzerKey);
                        long timestamps = daysToDeadlineTimestamps(backupIndex, replication.getDays());
                        String backupKey = getBackupRecord(bucket, filterType, backupIndex, tagMap, rulesMapKey, false, timestamps, replication.getDays());
                        backupKeySet.add(backupKey);
                    } else {
                        if (dateActivated(replication.getDate())) {
                            String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, false, replication.getDate(), null, sourceIndex);
                            analyzerKeySet.add(analyzerKey);
                            long timestamps = parseDateToTimestamps(replication.getDate());
                            String backupKey = getBackupRecord(bucket, filterType, backupIndex, tagMap, rulesMapKey, false, timestamps, replication.getDays());
                            backupKeySet.add(backupKey);
                        }
                    }
                } else if (condition instanceof NoncurrentVersionReplication) {
                    NoncurrentVersionReplication replication = (NoncurrentVersionReplication) condition;
                    Integer sourceIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getSourceSite());
                    Integer backupIndex = MossHttpClient.NAME_INDEX_MAP.get(replication.getBackupSite());
                    if (sourceIndex == null || backupIndex == null) {
                        continue;
                    }
                    String rulesMapKey;
                    String filterType;
                    if (tagMap.isEmpty()) {
                        rulesMapKey = prefix;
                        filterType = "PREFIX";
                    } else {
                        rulesMapKey = Json.encode(tagMap);
                        filterType = "TAG";
                    }
                    String analyzerKey = getAnalyzerKey(bucket, filterType, backupIndex, rulesMapKey, true, null, replication.getNoncurrentDays(), sourceIndex);
                    analyzerKeySet.add(analyzerKey);
                    long timestamps = daysToDeadlineTimestamps(backupIndex, replication.getNoncurrentDays());
                    String backupKey = getBackupRecord(bucket, filterType, backupIndex, tagMap, rulesMapKey, false, timestamps, replication.getNoncurrentDays());
                    backupKeySet.add(backupKey);
                }
            }
        }
        return new Tuple2<>(analyzerKeySet, backupKeySet);
    }

    /**
     * 旧格式的analyzerKey加上sourceSite（必为本地站点）
     *
     * @param analyzerKey0 表4的值去掉末尾的mark(末位不是“/”)
     * @return 新格式
     */
    public static String transformAnalyzerKey(String analyzerKey0) {
        String[] split = analyzerKey0.split(File.separator);
        String s = analyzerKey0;
        if (split.length == 6) {
            s = analyzerKey0 + File.separator + LOCAL_CLUSTER_INDEX;
        }
        return s;
    }

    public static boolean isNewAnalyzerKey(String analyzerKey) {
        return analyzerKey.split(File.separator).length != 6;
    }


    public static ConcurrentHashMap<String, String> BACK_ANALYZER_MAP = new ConcurrentHashMap<>();

    /*
     * 写入归档消息记录
     */
    public static void putBackupRecord(String bucketName, String vnode, String recordKey, MetaData metaData) {
        String record = getLifeCycleMetaKey(vnode, metaData.getBucket(), metaData.key, metaData.versionId, metaData.stamp);
        Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(bucketName + BACKUP_RECORD, recordKey, record));
    }

    /**
     * 如果已经有相应的record则不写入，防止重复计数
     *
     * @return -1重试，1已存在，0不存在
     */

    public static Mono<Integer> checkIfSyncRecordAlreadyExists(UnSynchronizedRecord record, MetaData lifeMeta) {
        return ErasureClient.getUnsyncRecord(record.bucket, record.recordKey)
                .flatMap(record1 -> {
                    if (record1.equals(ERROR_UNSYNC_RECORD)) {
                        return Mono.just(-1);
                    }
                    if (record1.equals(NOT_FOUND_UNSYNC_RECORD)) {
                        Map<String, String> stringMap = Json.decodeValue(lifeMeta.sysMetaData, new TypeReference<Map<String, String>>() {
                        });
                        boolean isPartObject = StringUtils.isNotEmpty(lifeMeta.partUploadId) && stringMap.get(ETAG).contains("-");
                        if (isPartObject) {
                            return ErasureClient.getUnsyncRecord(record.bucket, record.getReWriteCompleteKey(lifeMeta.partUploadId))
                                    .flatMap(record2 -> {
                                        if (record1.equals(ERROR_UNSYNC_RECORD)) {
                                            return Mono.just(-1);
                                        }
                                        if (record1.equals(NOT_FOUND_UNSYNC_RECORD)) {
                                            return Mono.just(0);
                                        }
                                        return Mono.just(1);
                                    });
                        } else {
                            return Mono.just(0);
                        }
                    }
                    return Mono.just(1);
                });
    }

}
