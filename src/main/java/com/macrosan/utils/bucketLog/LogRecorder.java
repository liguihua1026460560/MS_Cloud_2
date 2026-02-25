package com.macrosan.utils.bucketLog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ServerConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.httpserver.request.InternalPutRequest;
import com.macrosan.message.jsonmsg.BucketLogInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.constants.ServerConstants.X_AMX_VERSION_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_LOGGING;

/**
 * @author zhangzhixin
 */
@Log4j2
public class LogRecorder {
    private static final ExecutorService LOG_EXECUTOR = new MsExecutor(10, 10, new MsThreadFactory("bucket-log"));
    private static final Scheduler UPLOADING_SCHEDULER = Schedulers.fromExecutor(LOG_EXECUTOR);
    private static final long LOG_TIME = 600L;
    private static final long CHECK_TIME = 20L;
    private static final long CHECK_SIZE_TIME = 5L;

    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    private static final Map<String, JsonObject> requestMap = new ConcurrentHashMap<>();

    private final String node = ServerConfig.getInstance().getHostUuid();
    private final AtomicBoolean locked = new AtomicBoolean();
    private static LogRecorder instance;

    private LogRecorder() {

    }

    public static LogRecorder getInstance() {
        if (instance == null) {
            instance = new LogRecorder();
        }
        return instance;
    }

    public void start() {
        UPLOADING_SCHEDULER.schedule(this::checkTime, CHECK_TIME, TimeUnit.SECONDS);
        UPLOADING_SCHEDULER.schedule(this::checkSize, CHECK_SIZE_TIME, TimeUnit.SECONDS);
    }

    public void checkTime() {
        String result = redisConnPool.getCommand(REDIS_IAM_INDEX).get("bucket_log");
        if (StringUtils.isBlank(result)) {
            ErasureServer.DISK_SCHEDULER.schedule(() -> run());

        }else {
            long lastTime = redisConnPool.getCommand(REDIS_IAM_INDEX).ttl("bucket_log");
            if (lastTime > 600L){
                log.info("The timer failure, reset timer");
                SetArgs setArgs = SetArgs.Builder.ex(LOG_TIME);
                redisConnPool.getShortMasterCommand(REDIS_IAM_INDEX).set("bucket_log", "ready", setArgs);
            }
        }
        UPLOADING_SCHEDULER.schedule(this::checkTime, CHECK_TIME, TimeUnit.SECONDS);
    }

    private void run() {
        if (canRun() && tryGetLock()) {
            try {
                updateBucketLogging()
                        .timeout(Duration.ofSeconds(30),Mono.just(false))
                        .doFinally(s -> {
                            try {
                                ErasureServer.DISK_SCHEDULER.schedule(() -> redisConnPool.getShortMasterCommand(0).del("log"));
                            } finally {
                                locked.set(false);
                            }
                }).subscribe();
            } catch (Exception e) {
                log.error("", e);
                locked.set(false);
            }

            SetArgs setArgs = SetArgs.Builder.ex(LOG_TIME);
            redisConnPool.getShortMasterCommand(REDIS_IAM_INDEX).set("bucket_log", "ready", setArgs);
        } else {
            locked.set(false);
        }
    }

    private void checkSize() {
        long size = numMap.values().stream().mapToLong(AtomicLong::get).sum();
        if (size > 10000L) {
            try {
                log.debug("log size:{} map size:{}", size, requestMap.size());
                updateBucketLogging().doFinally(s -> {
                    UPLOADING_SCHEDULER.schedule(this::checkSize, 0, TimeUnit.SECONDS);
                }).subscribe();
            } catch (Exception e) {
                log.error("", e);
                UPLOADING_SCHEDULER.schedule(this::checkSize, 0, TimeUnit.SECONDS);
            }
        } else {
            UPLOADING_SCHEDULER.schedule(this::checkSize, CHECK_SIZE_TIME, TimeUnit.SECONDS);
        }
    }

    /**
     * 向临时的map中添加该次的访问记录
     *
     * @param req
     */
    public static void addBucketLogging(MsHttpRequest req, long curTime, long startTime, String bucketOwner) {
        BucketLogInfo info = new BucketLogInfo()
                .setBucketOwner(bucketOwner);

        info.bucket = req.getBucketName();
        String str = req.query();
        boolean queryNotNull = true;
        if (StringUtils.isBlank(str)){
            queryNotNull = false;
        }
        if (!StringUtils.isBlank(req.getHeader("X-Amz-Date"))) {
            info.time = "[" + changeDate(req.getHeader("X-Amz-date"),false) + "+0000]";
        } else if (!StringUtils.isBlank(req.getHeader("Date")) && queryNotNull && str.contains("Signature")) {
            info.time = "[" + changeDate(String.valueOf(System.currentTimeMillis()),true) + "+0000]";
        }else if (!StringUtils.isBlank(req.getHeader("Date"))) {
            info.time = "[" + changeDate(req.getHeader("Date"),false) + "+0000]";
        }

        info.remoteIp = req.connection().remoteAddress().host();
        if (!StringUtils.isBlank(req.getUserName())) {
            info.requester = "\"" + bucketOwner + " " + req.getUserName() + "\"";
        } else {
            info.requester = bucketOwner;
        }
        if (req.response().headers().get(ServerConstants.X_AMZ_REQUEST_ID) != null) {
            info.requestId = req.response().headers().get(ServerConstants.X_AMZ_REQUEST_ID);
        }

        if (str != null){
            String[] operation = str.split("&");
            if (operation.length >= 1) {
                info.operation = "REST." + req.rawMethod() + "." + operation[0].toUpperCase();
            } else {
                info.operation = "REST." + req.rawMethod();
            }

            if (str.contains("X-Amz-SignedHeaders") || str.contains("Signature")){
                info.operation = "REST." + req.rawMethod() + ".OBJECT";
            }
        }else{
            info.operation = "REST." + req.rawMethod();
        }

        if (req.getObjectName() != null) {
            if (req.getObjectName().contains(" ")){
                info.key = "\"" + req.getObjectName() + "\"";
            }else {
                info.key = req.getObjectName();
            }
        }

        if (StringUtils.isNotEmpty(req.response().headers().get(X_AMX_VERSION_ID))){
            info.versionId = req.response().headers().get(X_AMX_VERSION_ID);
        }else if (StringUtils.isNotEmpty(req.headers().get("versionId"))){
            info.versionId = req.headers().get("versionId");
        }

        switch (req.version()) {
            case HTTP_1_0:
                info.requestUri = "\"" + req.rawMethod() + " " + req.uri() + " HTTP/1.0\"";
                info.httpVersionId = "HTTP/1.0";
                break;
            case HTTP_1_1:
                info.requestUri = "\"" + req.rawMethod() + " " + req.uri() + " HTTP/1.1\"";
                info.httpVersionId = "HTTP/1.1";
                break;
            case HTTP_2:
                info.requestUri = "\"" + req.rawMethod() + " " + req.uri() + " HTTP/2.0\"";
                info.httpVersionId = "HTTP/2.0";
                break;
            default:
        }

        info.httpStatus = String.valueOf(req.response().getStatusCode());
        if ((req.response().getStatusCode() / 100) != 2) {
            if (StringUtils.isBlank(req.getMember("ErrorCode"))){
                info.errorCode = req.response().getStatusMessage();
            }else {
                info.errorCode = req.getMember("ErrorCode");
            }
        }

        if (req.getHeader("Referer") != null) {
            info.referer = req.getHeader("Referer");
        }

        if (req.getHeader(CONTENT_LENGTH) != null) {
            info.objectSize = req.getHeader(CONTENT_LENGTH);
        }

        info.bytesSent = String.valueOf(req.response().bytesWritten());
        long recodeTime = System.currentTimeMillis() - curTime;
        info.turnAroundTime = String.valueOf(recodeTime);
        long totalTime = System.currentTimeMillis() - startTime;
        info.totalTime = String.valueOf(totalTime);
        if (StringUtils.isNotEmpty(req.headers().get(ServerConstants.USER_AGENT))){
            info.userAgent = req.headers().get(ServerConstants.USER_AGENT).contains(" ")?"\"" + req.headers().get(ServerConstants.USER_AGENT) + "\"":req.headers().get(ServerConstants.USER_AGENT);
        }
        String auth = req.getHeader("Authorization");
        info.hostId = req.host();
        if (auth != null) {
            info.authType = "AuthHeader";
            if (auth.startsWith("AWS4")) {
                info.singnatureVersion = "AWSV4";
            } else {
                info.singnatureVersion = "AWSV2";
            }

        } else {
            info.authType = "QueryString";
        }


        String time = info.getTime();
        try {
            time = time.replace("[", "");
            time = time.replace("]", "");
            SimpleDateFormat s2 = new SimpleDateFormat("yyyyMMddHHmm");
            time = s2.format(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US).parse(time));
        } catch (ParseException e) {
            log.error("conversion date error:" + e);
        }
        //指定存储到map中的key
        String uuid = UUID.randomUUID().toString();
        String key = req.getBucketName() + "/" + time + "/" + req.response().headers().get(ServerConstants.X_AMZ_REQUEST_ID) + "/" + uuid;

        String tempTime = info.getTime();
        info.time = "\"" + tempTime + "\"";

        JsonObject jsonObject = new JsonObject();
        jsonObject.put("info", info.toString());
        //将本次访问的记录放到map中存储
        requestMap.put(key, jsonObject);
        increase(1);
    }

    /**
     * 获取其他节点保存的桶日志记录信息
     */
    public static Mono<Map<String, Map<String, JsonObject>>> getOtherLogging() {
        List<Tuple3<String, String, String>> list = RabbitMqUtils.HEART_IP_LIST.stream()
                .map(ip -> new Tuple3<>(ip, "0", "else"))
                .collect(Collectors.toList());

        List<SocketReqMsg> msgs = list.stream()
                .map(l -> new SocketReqMsg("", 0))
                .collect(Collectors.toList());


        TypeReference<Map<String, Map<String, Object>>> typeReference = new TypeReference<Map<String, Map<String, Object>>>() {
        };

        ClientTemplate.ResponseInfo<Map<String, Map<String, Object>>> responseInfo = ClientTemplate.oneResponse(msgs, GET_LOGGING, typeReference, list);

        Map<String, Map<String, JsonObject>> map = new HashMap<>();

        return responseInfo.responses.doOnNext(s -> {
                    if (s.getVar3() != null) {
                        Map<String, Map<String, Object>> object = s.getVar3();
                        object.forEach((k, v) -> {
                            String bucketName = k.split("/")[0];
                            Map<String, JsonObject> bucketLogging = map.computeIfAbsent(bucketName, key -> new HashMap<>());
                            bucketLogging.put(k, new JsonObject(v));
                        });
                    }
                })
                .collectList()
                .map(l -> map)
                .onErrorReturn(map);
    }

    private static final Map<Thread, AtomicLong> numMap = new ConcurrentHashMap<>();

    private static void increase(long n) {
        AtomicLong num = numMap.get(Thread.currentThread());
        if (num == null) {
            synchronized (numMap) {
                num = numMap.get(Thread.currentThread());
                if (num == null) {
                    num = new AtomicLong();
                    numMap.put(Thread.currentThread(), num);
                }
            }
        }

        num.addAndGet(n);
    }

    /**
     * 获取当前节点的数据，并数据以Map形式返回，将获取的源数据进行移除
     *
     * @return
     */
    public static Map<String, JsonObject> getLogging() {
        synchronized (requestMap) {
            Map<String, JsonObject> msg = new HashMap<>();
            Iterator<String> iterator = requestMap.keySet().iterator();
            int n = 0;
            while (iterator.hasNext() && n < 10000) {
                String key = iterator.next();
                JsonObject object = requestMap.remove(key);
                msg.put(key, object);
                n++;
            }

            increase(-n);
            return msg;
        }
    }


    /**
     * 当时为整时时将临时map中的数据传输到指定的桶中
     */
    public Mono<Boolean> updateBucketLogging() {
        return getOtherLogging()
                .flatMapMany(map -> Flux.fromStream(map.values().stream()))
                .flatMap(bucket -> updateLogging(bucket))
                .collectList()
                .map(l -> true);
    }


    /**
     * 将数据上传到目标桶中
     *
     * @param bucket
     * @return
     */
    public static Mono<Boolean> updateLogging(Map<String, JsonObject> bucket) {
        String bucketName = bucket.keySet().iterator().next().split("/")[0];
        Mono<Boolean> result = getTargetBucket(bucketName);
        return result.flatMap(res -> {
            if (res) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
                String uploadTime = simpleDateFormat.format(DateChecker.getCurrentTime());

                String objectId = RandomStringUtils.randomAlphanumeric(16);
                uploadTime = uploadTime + "-" + objectId;

                Set<String> putValue = new LinkedHashSet<>();
                bucket.keySet().forEach(value -> {
                    JsonObject text = bucket.get(value);
                    putValue.add(text.getString("info"));
                });

                AtomicReference<String> finalObjectName = new AtomicReference<>(uploadTime);
                return getInfo(bucketName).flatMap(info -> {
                    if (info == null || info.isEmpty()) {
                        log.error("The source bucket has been deleted");
                        return Mono.just(false);
                    } else {
                        String targetPrefix = info.get("targetPrefix");
                        String userId = info.get("user_id");
                        String targetBucket = info.get("address");
                        String acl = info.get("log_acl");
                        if (targetPrefix != null) {
                            finalObjectName.set(targetPrefix + finalObjectName);
                        }
                        byte[] finalValue = String.join("\n", putValue).getBytes(StandardCharsets.UTF_8);
                        return InternalPutRequest.putObject(userId, targetBucket, finalObjectName.get(), finalValue, acl)
                                .doOnNext(b -> log.debug("put bucket log {} {} {}", userId, targetBucket, finalObjectName))
                                .flatMap(b -> {
                                    if (!b){
                                        return InternalPutRequest.putObject(userId, targetBucket, finalObjectName.get(), finalValue, acl);
                                    }
                                    return Mono.just(b);
                                });
                    }
                });
            } else {
                return Mono.just(false);
            }
        });
    }


    /**
     * 在开始使用updateBucketLogging时，将获取锁防止其他节点同时开始上传桶日志记录信息
     *
     * @return
     */
    protected boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(10);
            String setKey = redisConnPool.getShortMasterCommand(0).set("log", this.node, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
                redisConnPool.getShortMasterCommand(0).set("log_run", this.node);
            }
            return res;
        } catch (Exception e) {
            return false;
        }
    }


    private void keepLock() {
        if (locked.get()) {
            SetArgs setArgs = SetArgs.Builder.xx().ex(6);
            try (StatefulRedisConnection<String, String> tmpConnection =
                         redisConnPool.getSharedConnection(0).newMaster()) {
                RedisCommands<String, String> target = tmpConnection.sync();
                target.watch("log");
                String lockNode = target.get("log");
                if (node.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set("log", this.node, setArgs);
                    UPLOADING_SCHEDULER.schedule(this::keepLock, 2, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                UPLOADING_SCHEDULER.schedule(this::keepLock, 2, TimeUnit.SECONDS);
            }
        }
    }


    protected boolean canRun() {
        try {
            String lastRunNode = redisConnPool.getShortMasterCommand(0).get("log_run");
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
                if ("0".equalsIgnoreCase(serverState)) {
                    i++;
                    if (i >= totalNodes.size()) {
                        i = 0;
                    }
                } else {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("judge create bucket log status fail ," + e);
            return false;
        }
    }

    /**
     * 获取桶的日志配置信息
     *
     * @param bucketName
     * @return
     */
    private static Mono<Map<String, String>> getInfo(String bucketName) {

        Mono<Map<String, String>> info = redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName).defaultIfEmpty(new LinkedHashMap<>());

        return info;
    }

    private static Mono<Boolean> getTargetBucket(String bucketName) {
        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName).defaultIfEmpty(new LinkedHashMap<>()).flatMap(result -> {
            if (StringUtils.isEmpty(result.get("address"))) {
                log.error("The source bucket has been deleted");
                return Mono.just(false);
            }

            return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(result.get("address")).defaultIfEmpty(new LinkedHashMap<>()).flatMap(info -> {
                if (info.isEmpty()) {
                    log.error("The target bucket has been deleted");
                    ErasureServer.DISK_SCHEDULER.schedule(() -> {
                        redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "log_acl");
                    });
                    return Mono.just(false);
                }
                return Mono.just(true);
            });

        });

    }

    private static String changeDate(String time,boolean localTime) {
        if (time.isEmpty()) {
            return null;
        }

        SimpleDateFormat s1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ", Locale.US);
        try {
            if (time.contains("GMT")) {
                Date date = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss", Locale.ENGLISH).parse(time);
                return s1.format(date);
            } else if (time.contains("T") && time.contains("Z")){
                Date date = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").parse(time);
                return s1.format(date);
            }else {
                if (localTime){
                    s1.setTimeZone(TimeZone.getTimeZone("UTC"));
                }
                return s1.format(Long.valueOf(time));
            }
        } catch (Exception e) {
            log.error(e);
        }
        return null;
    }


}
