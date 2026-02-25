package com.macrosan.utils.quota;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.HttpMethodTrafficStatisticsResult;
import com.macrosan.message.xmlmsg.HttpStatusCodeStatisticsResult;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListRecodeHandler;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanArgs;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_MERGE_RECORD;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_MINUTE_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static io.vertx.core.http.HttpMethod.*;

/**
 * 记录访问统计的信息
 *
 * @author gaozhiyaun
 * @date 2019.11.14
 */
@Log4j2
public class StatisticsRecorder {
    public static StoragePool STATISTIC_STORAGE_POOL;
    public static final String STATIS_ON = "1";
    public static final String STATIS_OFF = "0";
    public static final String JOINER = "&";
    public static String isMerge = "true";

    private static final long ONE_DAY = 24L * 3600 * 1000;
    private static long FIVE_MINUTE = 300 * 1000;

    public static boolean start = false;
    private static Scheduler sceduler = null;
    private static final Map<String, StatisticRecord0> statisticMap = new ConcurrentHashMap<>();
    private static final Map<String, StatisticRecord> updateMinuteMap = new ConcurrentHashMap<>();

    private static long recordTime = 0L;
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public static final String SYSTEM_RECORD_ACCOUNT_ID = "101010101010";
    public static final String SYSTEM_RECORD_BUCKET = "system";
    public static String SYSTEM_READ_RECORD_KEY;
    public static String SYSTEM_WRITE_RECORD_KEY;
    private static volatile boolean enableHttpMethodStatistic = false;
    private static volatile boolean enableSystemStatistic = false;

    private static long getDelayOfNextExec() {
        long res = redisConnPool.getCommand(REDIS_TASKINFO_INDEX).exists("nextTime");
        if (res == 1) {
            String nextTime = redisConnPool.getCommand(REDIS_TASKINFO_INDEX).get("nextTime");
            try {
                long l = Long.parseLong(nextTime);
                long thirtyMinute = 30 * 60 * 1000;
                if (l > 30 * 60 * 1000) {
                    l = thirtyMinute;
                }
                if (l < 1000) {
                    l = 1000;
                }
                FIVE_MINUTE = l;
            } catch (Exception e) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "", e);
            }
        } else {
            FIVE_MINUTE = 300 * 1000;
        }
        long curTime = System.currentTimeMillis();
        long nextTime = curTime / FIVE_MINUTE * FIVE_MINUTE + FIVE_MINUTE;
        if (nextTime - curTime < FIVE_MINUTE / 2) {
            nextTime += FIVE_MINUTE;
        }
        return nextTime - curTime;
    }

    public static void init() {
        sceduler = ErasureServer.DISK_SCHEDULER;
        String statisStatus = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("statis_swich");
        if (STATIS_OFF.equals(statisStatus)) {
            start = false;
            return;
        }
        updateStatisticConfig();
        //启动将信息刷到数据库的定时任务
        if (null == statisStatus || STATIS_ON.equals(statisStatus)) {
            //checkPowerOffData();
            start = true;
            sceduler.schedule(StatisticsRecorder::updateStatisticRecord, getDelayOfNextExec(), TimeUnit.MILLISECONDS);
        }

        STATISTIC_STORAGE_POOL = StoragePoolFactory.getStoragePool(StorageOperate.META);
        SYSTEM_READ_RECORD_KEY = SYSTEM_RECORD_ACCOUNT_ID + '&' + RequestType.READ.name() + '&' + SYSTEM_RECORD_BUCKET;
        SYSTEM_WRITE_RECORD_KEY = SYSTEM_RECORD_ACCOUNT_ID + '&' + RequestType.WRITE.name() + '&' + SYSTEM_RECORD_BUCKET;
    }

    /**
     * 每一个请求都会调用这个方法，用来记录这个请求的流量信息
     *
     * @param accountId 请求的账户id
     * @param bucket    桶名
     * @param req       本次请求
     * @param type      请求的类型
     * @param success   响应的http状态码是否为成功
     */
    public static void addStatisticRecord(String accountId, String bucket, MsHttpRequest req, RequestType type, boolean success, long time) {
        if (StringUtils.isEmpty(bucket)) {
            return;
        }
        String key = accountId + '&' + type.name() + '&' + bucket;
        long size = req.bytesRead() + req.response().bytesWritten();
        statisticMap.computeIfAbsent(key, k -> new StatisticRecord0(type)).addRecord(size, success, time, req.method(), req.response().getStatusCode());
    }

    public static void addFileStatisticRecord(String accountId, String bucket, HttpMethod method, int statusCode, RequestType type, boolean success, long time, long size) {
        if (StringUtils.isEmpty(bucket) || StringUtils.isEmpty(accountId)) {
            log.warn("Cannot add file statistic record: accountId or bucket is empty. AccountId: {}, Bucket: {}", accountId, bucket);
            return;
        }
        String key = accountId + JOINER + type.name() + JOINER + bucket;

        statisticMap.computeIfAbsent(key, k -> new StatisticRecord0(type)).addRecord(size, success, time, method, statusCode);
    }

    /**
     * 将内存中的统计信息刷入数据库的方法,会定时循环调用
     */
    private static void updateStatisticRecord() {
        long curTime = System.currentTimeMillis();
        curTime = curTime - curTime % FIVE_MINUTE;
        if (curTime != recordTime) {
            recordTime = curTime;

            // 统计系统整体的访问数据
            StatisticRecord systemReadRecord = new StatisticRecord();
            StatisticRecord systemWriteRecord = new StatisticRecord();
            if (!statisticMap.isEmpty()) {
                //将statisticMap中的数据转移到updateMinuteMap
                statisticMap.keySet().forEach(key -> {
                    StatisticRecord0 record0 = statisticMap.remove(key);
                    StatisticRecord record = new StatisticRecord();
                    record.setSuccessNum(record0.successNum.get());
                    record.setSuccessSize(record0.successSize.get());
                    record.setFailNum(record0.failNum.get());
                    record.setFailSize(record0.failSize.get());
                    record.setTime(record0.time.get());

                    if (!record0.httpMethodStatisticRecordMap.isEmpty()) {
                        Map<HttpMethod, HttpMethodStatisticRecord> recordMap = record0.httpMethodStatisticRecordMap
                                .entrySet()
                                .stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toHttpMethodStatisticRecord()));
                        record.setHttpMethodStatisticRecordMap(recordMap);
                    }
                    if (enableSystemStatistic) {
                        if (RequestType.READ == record0.requestType) {
                            systemReadRecord.mergeRecord(record);
                        } else {
                            systemWriteRecord.mergeRecord(record);
                        }
                    }
                    updateMinuteMap.put(key, record);
                });
                if (!systemReadRecord.isEmpty()) {
                    updateMinuteMap.put(SYSTEM_READ_RECORD_KEY, systemReadRecord);
                }
                if (!systemWriteRecord.isEmpty()) {
                    updateMinuteMap.put(SYSTEM_WRITE_RECORD_KEY, systemWriteRecord);
                }

                //更新分钟统计信息,同时里面也会更新账户的月统计信息
                updateMinuteRecord();
            }

            //零点的时候整合天的数据
            long res = redisConnPool.getCommand(REDIS_TASKINFO_INDEX).exists("isMerge");
            if (res == 1) {
                String isMerge = redisConnPool.getCommand(REDIS_TASKINFO_INDEX).get("isMerge");
                StatisticsRecorder.isMerge = "false".equals(isMerge) ? isMerge : "true";
            } else {
                StatisticsRecorder.isMerge = "true";
            }
            if (curTime % ONE_DAY == 0 && "true".equals(isMerge)) {
                ScanArgs scanAllArg = new ScanArgs().match("*").limit(10);
                KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
                keyScanCursor.setCursor("0");
                UnicastProcessor<KeyScanCursor<String>> processor = UnicastProcessor.create();
                UnicastProcessor<KeyScanCursor<String>> scanArgsUnicastProcessor = UnicastProcessor.create();
                scanArgsUnicastProcessor.onNext(keyScanCursor);
                scanArgsUnicastProcessor
                        .flatMap(curKeyScanCursor -> redisConnPool.getReactive(REDIS_USERINFO_INDEX).scan(curKeyScanCursor, scanAllArg))
                        .subscribe(processor::onNext, log::error, processor::onComplete);
                processor.subscribe(curKeyScanCursor -> Flux.fromIterable(curKeyScanCursor.getKeys())
                        .filter(key -> Pattern.matches("^\\d{12}$", key))
                        .flatMap(accountId -> mergeRecord(accountId, recordTime))
                        .subscribe(b -> {
                            if (b) {
                                log.debug("merge record success");
                            }
                        }, e -> log.error("merge record error.", e), () -> {
                            if (!curKeyScanCursor.isFinished()) {
                                scanArgsUnicastProcessor.onNext(curKeyScanCursor);
                            } else {
                                scanArgsUnicastProcessor.onComplete();
                            }
                        }));
                keyScanCursor.getKeys().add(SYSTEM_RECORD_ACCOUNT_ID);
                processor.onNext(keyScanCursor);
            }
        }
        //上一个定时任务结束，再启动一个定时任务
        sceduler.schedule(StatisticsRecorder::updateStatisticRecord, getDelayOfNextExec(), TimeUnit.MILLISECONDS);
    }

    private static void updateStatisticConfig() {
        try {
            List<KeyValue<String, String>> statisticsConfig = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hmget("statistic_config", "enableHttpMethodStatistic", "enableSystemStatistic");
            for (KeyValue<String, String> keyValue : statisticsConfig) {
                if ("enableHttpMethodStatistic".equals(keyValue.getKey())) {
                    enableHttpMethodStatistic = Boolean.parseBoolean(keyValue.hasValue() ? keyValue.getValue() : "false");
                }
                if ("enableSystemStatistic".equals(keyValue.getKey())) {
                    enableSystemStatistic = Boolean.parseBoolean(keyValue.hasValue() ? keyValue.getValue() : "false");
                }
            }
        } catch (Exception e) {
            log.error("updateStatisticConfig error.", e);
        } finally {
            sceduler.schedule(StatisticsRecorder::updateStatisticConfig, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * 写入分钟的统计信息
     */
    private static void updateMinuteRecord() {
        Flux.fromStream(updateMinuteMap.entrySet().stream())
                .flatMap(entry -> {
                    final String key = entry.getKey();
                    final StatisticRecord record = entry.getValue();
                    String[] keys = key.split(JOINER);
                    String accountId = keys[0];
                    RequestType type = RequestType.valueOf(keys[1]);
                    String bucket = keys[2];
                    record.setRequestType(type);
                    record.setRecordType(RecordType.MINUTE);
                    record.setBucket(bucket);
                    record.setAccountId(accountId);
                    //当前时间
                    record.setRecordTime(recordTime);
                    record.setVersionNum(VersionUtil.getVersionNum());
                    //五分钟的统计信息
                    //根据账户id映射的每个节点都写入统计信息
                    final String accountVnode = STATISTIC_STORAGE_POOL.getBucketVnodeId(accountId);
                    record.setVnode(accountVnode);
                    String curNode = ServerConfig.getInstance().getHostUuid();
                    record.setRecordSource(curNode);

                    return STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode)
                            .flatMap(nodeList -> ECUtils.putRocksKey(STATISTIC_STORAGE_POOL, record.getKey(),
                                    Json.encode(record), PUT_MINUTE_RECORD, ERROR_PUT_MINUTE_RECORD, nodeList, null));
                }).subscribe(flag -> {
                    if (flag) {
                        log.debug("put minute record success");
                    } else {
                        log.error("put minute record error.");
                    }
                }, e -> log.error("put minute record error.", e), updateMinuteMap::clear);
    }

    /**
     * 将多条分钟的统计记录合并为一条小时的记录
     * 或者将小时的记录合并为天的记录
     *
     * @param accountId 账户id
     * @return
     */
    private static Mono<Boolean> mergeRecord(String accountId, long recordTime) {
        List<StatisticRecord> statisticRecords = new ArrayList<>();
        //获取对应的间隔时间段及类型的record
        final String accountVnode = STATISTIC_STORAGE_POOL.getBucketVnodeId(accountId);
        return STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode)
                .flatMap(nodeList -> {
                    long endTime = recordTime - ONE_DAY;
                    String keyPrefix = JOINER + accountVnode + JOINER + RecordType.MINUTE.typePrefix + JOINER + accountId + JOINER;
                    List<SocketReqMsg> socketReqMsgs = ECUtils.mapToMsg(keyPrefix, String.valueOf(endTime), nodeList);
                    //查询所有待合并的
                    ClientTemplate.ResponseInfo<StatisticRecord[]> listResponseInfo =
                            ClientTemplate.oneResponse(socketReqMsgs, GET_RECORD_TO_MERGE, new TypeReference<StatisticRecord[]>() {
                            }, nodeList);
                    ListRecodeHandler listRecodeHandler = new ListRecodeHandler(listResponseInfo, nodeList, statisticRecords, "");
                    listResponseInfo.responses.subscribe(listRecodeHandler::handleResponse, e -> log.error("", e), listRecodeHandler::handleComplete);
                    return listRecodeHandler.res
                            .filter(flag -> flag)
                            .flatMap(flag -> {
                                final Map<String, StatisticRecord> recordHashMap = new HashMap<>();

                                for (StatisticRecord record : statisticRecords) {
                                    long dayTime = record.getRecordTime() / ONE_DAY * ONE_DAY;
                                    String dayKey = StatisticRecord.getKey(record.getVnode(), RecordType.DAY, record.getAccountId(),
                                            dayTime, record.getRequestType(), record.getBucket());
                                    if (recordHashMap.containsKey(dayKey)) {
                                        recordHashMap.get(dayKey).mergeRecord(record);
                                    } else {
                                        record.setRecordType(RecordType.DAY);
                                        record.setRecordTime(dayTime);
                                        record.setVersionNum(VersionUtil.getVersionNum());
                                        record.setRecordSource(ServerConfig.getInstance().getHostUuid());
                                        recordHashMap.put(dayKey, record);
                                    }
                                }
                                MonoProcessor<Boolean> res = MonoProcessor.create();
                                //执行整合操作
                                if (recordHashMap.size() != 0) {
                                    List<Map<String, StatisticRecord>> mapList = new ArrayList<>();
                                    //根据具有相同桶属性对象的聚合操作
                                    Flux.fromIterable(recordHashMap.entrySet())
                                            .groupBy(m -> m.getValue().getBucket())
                                            .flatMap(group -> group.collectList()
                                                    .map(l -> Tuples.of(Objects.requireNonNull(group.key()), l)))
                                            .subscribe(tuple -> {
//                                                String bucketName = tuple.getT1();
                                                Map<String, StatisticRecord> recordMap = tuple.getT2().stream()
                                                        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
                                                mapList.add(recordMap);
                                            });

                                    UnicastProcessor<Integer> processor = UnicastProcessor.create();
                                    processor.subscribe(l -> {
                                        if (l >= mapList.size()) {
                                            processor.onComplete();
                                            res.onNext(true);
                                            return;
                                        }
                                        Map<String, StatisticRecord> recordMap = mapList.get(l);
                                        ECUtils.putRocksKey(STATISTIC_STORAGE_POOL, "" + endTime,
                                                Json.encode(recordMap)
                                                , MERGE_RECORD, ERROR_MERGE_RECORD, nodeList, null).doFinally(d -> processor.onNext(l + 1)).subscribe();
                                    });
                                    processor.onNext(0);
                                } else {
                                    res.onNext(true);
                                }
                                return res;
                            });
                });
    }

    public static class StatisticRecord0 {
        AtomicLong successSize = new AtomicLong();
        AtomicLong successNum = new AtomicLong();
        AtomicLong failSize = new AtomicLong();
        AtomicLong failNum = new AtomicLong();
        AtomicLong time = new AtomicLong();
        RequestType requestType;
        Map<HttpMethod, AtomicHttpMethodStatisticRecord> httpMethodStatisticRecordMap = new ConcurrentHashMap<>();

        public StatisticRecord0(RequestType requestType){
            this.requestType = requestType;
        }

        void addRecord(long size, boolean success, long time, HttpMethod method, int statusCode) {
            if (!enableHttpMethodStatistic) {
                if (success) {
                    this.successSize.addAndGet(size);
                    successNum.incrementAndGet();
                    this.time.addAndGet(time);
                } else {
                    this.failSize.addAndGet(size);
                    failNum.incrementAndGet();
                    this.time.addAndGet(time);
                }
            } else {
                HttpMethod statisticHttpMethod = getStatisticHttpMethod(method);
                AtomicHttpMethodStatisticRecord httpMethodStatisticRecord = httpMethodStatisticRecordMap.computeIfAbsent(statisticHttpMethod, k -> new AtomicHttpMethodStatisticRecord());
                if (success) {
                    httpMethodStatisticRecord.getSuccessNum().incrementAndGet();
                    httpMethodStatisticRecord.getSuccessSize().addAndGet(size);
                    httpMethodStatisticRecord.getTime().addAndGet(time);
                } else {
                    httpMethodStatisticRecord.getFailNum().incrementAndGet();
                    httpMethodStatisticRecord.getFailSize().addAndGet(size);
                    httpMethodStatisticRecord.getTime().addAndGet(time);
                }
                httpMethodStatisticRecord.getHttpCodes().computeIfAbsent(statusCode, k -> new AtomicLong()).incrementAndGet();
            }
        }
    }

    @Data
    @NoArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class StatisticRecord implements Serializable, Cloneable {
        /**
         * 返回结果为成功的请求流量
         */
        private long successSize;
        /**
         * 返回结果为成功的请求次数
         */
        private long successNum;
        /**
         * 返回结果为失败的请求流量
         */
        private long failSize;
        /**
         * 返回结果为失败的请求次数
         */
        private long failNum;
        /**
         * 对应的账户
         */
        private String accountId;
        /**
         * 对应的桶
         */
        private String bucket;
        /**
         * 这条统计记录的时间戳
         */
        private long recordTime;
        /**
         * 记录类型 分钟 or 小时 or 天
         */
        private RecordType recordType;

        private RequestType requestType;

        private String vnode;

        private String versionNum;

        private long time;

        public String recordSource;

        /**
         * 根据http方法进行分类统计
         */
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private Map<HttpMethod, HttpMethodStatisticRecord> httpMethodStatisticRecordMap = new HashMap<>(5);


        public void mergeRecord(StatisticRecord statisticRecord) {
                this.successSize += statisticRecord.successSize;
            this.failSize += statisticRecord.failSize;
            this.successNum += statisticRecord.successNum;
            this.failNum += statisticRecord.failNum;
            this.time += statisticRecord.time;
            statisticRecord.httpMethodStatisticRecordMap
                    .forEach((method, record) -> this.httpMethodStatisticRecordMap.computeIfAbsent(method, k -> new HttpMethodStatisticRecord()).merge(record));
        }

        @JsonIgnore
        public String getKey() {
            String key = getKey(vnode, recordType, accountId, recordTime, requestType, bucket);
            if (recordType == RecordType.MINUTE && recordSource != null) {
                return key + JOINER + recordSource;
            }

            return key;

        }

        public static String getKey(String vnode, RecordType recordType, String accountId,
                                    long recordTime, RequestType requestType, String bucket) {
            return JOINER +
                    vnode +
                    JOINER +
                    recordType.typePrefix +
                    JOINER +
                    accountId +
                    JOINER +
                    recordTime +
                    JOINER +
                    requestType.name() +
                    JOINER +
                    bucket;
        }

        @JsonIgnore
        public boolean isEmpty() {
            return successNum == 0 && failNum == 0 && httpMethodStatisticRecordMap.isEmpty();
        }


        /**
         * 获取流量 将旧版本中的值和新版本中的值累加
         * 新版本的统计值在httpMethodStatisticRecordMap中根据httpMethod分类统计
         *
         * @return 流量
         */
        @JsonIgnore
        public long getTraffic() {
            return this.successSize + this.failSize
                    + this.httpMethodStatisticRecordMap.values().stream()
                    .mapToLong(httpMethodStatisticRecord -> httpMethodStatisticRecord.getSuccessSize() + httpMethodStatisticRecord.getFailSize())
                    .sum();
        }

        /**
         * 获取请求次数 将旧版本中的值和新版本中的值累加
         * 新版本的统计值在httpMethodStatisticRecordMap中根据httpMethod分类统计
         *
         * @return 请求次数
         */
        @JsonIgnore
        public long getCount() {
            return this.successNum + this.failNum
                    + this.httpMethodStatisticRecordMap.values().stream()
                    .mapToLong(httpMethodStatisticRecord -> httpMethodStatisticRecord.getSuccessNum() + httpMethodStatisticRecord.getFailNum())
                    .sum();
        }

        /**
         * 获取总耗时 将旧版本中的值和新版本中的值累加
         * 新版本的统计值在httpMethodStatisticRecordMap中根据httpMethod分类统计
         *
         * @return 总耗时
         */
        @JsonIgnore
        public long getTotalTime() {
            return this.time + this.httpMethodStatisticRecordMap.values().stream()
                    .mapToLong(HttpMethodStatisticRecord::getTime)
                    .sum();
        }

        @JsonIgnore
        public List<HttpMethodTrafficStatisticsResult> getAllHttpMethodTrafficStatisticsResult() {
            return this.httpMethodStatisticRecordMap.entrySet().stream()
                    .map(entry -> new HttpMethodTrafficStatisticsResult()
                            .setHttpMethod(entry.getKey().name())
                            .setSuccessCount(String.valueOf(entry.getValue().getSuccessNum()))
                            .setFailCount(String.valueOf(entry.getValue().getFailNum()))
                            .setHttpStatusCodeStatisticsResult(entry.getValue().getHttpStatusCodeStatisticsResult())
                            .setTraffic(String.valueOf(entry.getValue().getSuccessSize() + entry.getValue().getFailSize()))
                            .setTime(String.valueOf(entry.getValue().getTime()))
                    ).collect(Collectors.toList());
        }

        public List<HttpMethodTrafficStatisticsResult> getSingleHttpMethodTrafficStatisticsResult(String httpMethod) {
            try {
                HttpMethod method = HttpMethod.valueOf(httpMethod.toUpperCase());
                HttpMethodStatisticRecord httpMethodStatisticRecord = this.httpMethodStatisticRecordMap.get(method);
                if (httpMethodStatisticRecord == null) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(new HttpMethodTrafficStatisticsResult()
                        .setHttpMethod(httpMethod)
                        .setSuccessCount(String.valueOf(httpMethodStatisticRecord.getSuccessNum()))
                        .setFailCount(String.valueOf(httpMethodStatisticRecord.getFailNum()))
                        .setHttpStatusCodeStatisticsResult(httpMethodStatisticRecord.getHttpStatusCodeStatisticsResult())
                        .setTraffic(String.valueOf(httpMethodStatisticRecord.getSuccessSize() + httpMethodStatisticRecord.getFailSize()))
                        .setTime(String.valueOf(httpMethodStatisticRecord.getTime()))
                );
            } catch (Exception e) {
                return Collections.emptyList();
            }
        }

        @Override
        public StatisticRecord clone() {
            try {
                StatisticRecord clone = (StatisticRecord) super.clone();
                // 深拷贝
                clone.setHttpMethodStatisticRecordMap(this.httpMethodStatisticRecordMap.entrySet()
                        .stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clone())));
                return clone;
            } catch (CloneNotSupportedException e) {
                throw new AssertionError();
            }
        }
    }

    @Data
    @ToString
    @EqualsAndHashCode
    public static class HttpMethodStatisticRecord implements Serializable, Cloneable {
        /**
         * 返回结果为成功的请求流量
         */
        private long successSize;
        /**
         * 返回结果为成功的请求次数
         */
        private long successNum;
        /**
         * 返回结果为失败的请求流量
         */
        private long failSize;
        /**
         * 返回结果为失败的请求次数
         */
        private long failNum;
        private long time;

        public Map<Integer, Long> httpCodes = new HashMap<>();

        public void merge(AtomicHttpMethodStatisticRecord statisticRecord) {
            this.successSize += (statisticRecord.successSize.get());
            this.failSize += (statisticRecord.failSize.get());
            this.successNum += (statisticRecord.successNum.get());
            this.failNum += (statisticRecord.failNum.get());
            this.time += (statisticRecord.time.get());
            statisticRecord.httpCodes.forEach((code, count) -> {
                this.httpCodes.compute(code, (k, v) -> v == null ? count.get() : v + count.get());
            });
        }

        public void merge(HttpMethodStatisticRecord statisticRecord) {
            this.successSize += statisticRecord.successSize;
            this.failSize += statisticRecord.failSize;
            this.successNum += statisticRecord.successNum;
            this.failNum += statisticRecord.failNum;
            this.time += statisticRecord.time;
            statisticRecord.httpCodes.forEach((code, count) -> {
                this.httpCodes.compute(code, (k, v) -> v == null ? count : v + count);
            });
        }

        @JsonIgnore
        public List<HttpStatusCodeStatisticsResult> getHttpStatusCodeStatisticsResult() {
            return this.httpCodes.entrySet()
                    .stream()
                    .map(entry -> new HttpStatusCodeStatisticsResult(String.valueOf(entry.getKey()), String.valueOf(entry.getValue())))
                    .collect(Collectors.toList());
        }

        @Override
        public HttpMethodStatisticRecord clone() {
            try {
                HttpMethodStatisticRecord clone = (HttpMethodStatisticRecord) super.clone();
                // 复制可变值
                clone.setHttpCodes(new HashMap<>(this.httpCodes));
                return clone;
            } catch (CloneNotSupportedException e) {
                throw new AssertionError();
            }
        }
    }


    @Data
    public static class AtomicHttpMethodStatisticRecord {
        /**
         * 返回结果为成功的请求流量
         */
        private AtomicLong successSize = new AtomicLong();
        /**
         * 返回结果为成功的请求次数
         */
        private AtomicLong successNum = new AtomicLong();
        /**
         * 返回结果为失败的请求流量
         */
        private AtomicLong failSize = new AtomicLong();
        /**
         * 返回结果为失败的请求次数
         */
        private AtomicLong failNum = new AtomicLong();
        private AtomicLong time = new AtomicLong();

        public Map<Integer, AtomicLong> httpCodes = new ConcurrentHashMap<>();

        public HttpMethodStatisticRecord toHttpMethodStatisticRecord() {
            HttpMethodStatisticRecord record = new HttpMethodStatisticRecord();
            record.setSuccessNum(successNum.get());
            record.setSuccessSize(successSize.get());
            record.setFailNum(failNum.get());
            record.setFailSize(failSize.get());
            record.setTime(time.get());
            record.setHttpCodes(httpCodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get())));
            return record;
        }
    }

    public static HttpMethod getStatisticHttpMethod(HttpMethod method) {
        switch (method) {
            case GET:
                return GET;
            case POST:
                return POST;
            case PUT:
                return PUT;
            case DELETE:
                return DELETE;
            case HEAD:
                return HEAD;
            default:
                return OTHER;
        }
    }

    /**
     * 请求类型
     */
    public enum RequestType {
        //写请求
        WRITE(4),
        //读请求
        READ(2);
        private final int value;

        RequestType(int value) {
            this.value = value;
        }
    }

    /**
     * record数据的类型
     */
    public enum RecordType {
        MINUTE("minute"),
        DAY("day");
        public final String typePrefix;

        RecordType(String typePrefix) {
            this.typePrefix = typePrefix;
        }
    }

    private static IntObjectHashMap<RequestType> map = new IntObjectHashMap<>();

    static {
        for (RequestType type : RequestType.values()) {
            map.put(type.value, type);
        }
    }

    public static RequestType getRequestType(String method) {
        return getRequestType(HttpMethod.valueOf(method));
    }

    public static RequestType getRequestType(HttpMethod method) {
        switch (method) {
            case GET:
            case HEAD:
                return RequestType.READ;
            case POST:
            case DELETE:
            case PUT:
            default:
                return RequestType.WRITE;
        }
    }
}
