package com.macrosan.httpserver;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.cors.CORSConfiguration;
import com.macrosan.message.xmlmsg.cors.CORSRule;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.bucketLog.LogFilter;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.cors.CORSUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.notification.BucketNotification;
import com.macrosan.utils.perf.AccountPerfLimiter;
import com.macrosan.utils.perf.BucketPerfLimiter;
import com.macrosan.utils.perf.StsPerfLimiter;
import com.macrosan.utils.quota.StatisticsRecorder;
import com.macrosan.utils.serialize.JaxbUtils;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.internal.SystemPropertyUtil;
import io.reactivex.disposables.Disposable;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.Json;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.macrosan.action.controller.DataStreamController.dataStreamRoute;
import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.AccountConstants.DEFAULT_ACCESS_KEY;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.AssignClusterHandler.ClUSTER_NAME_HEADER;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.HeartBeatChecker.*;
import static com.macrosan.doubleActive.arbitration.Arbitrator.MASTER_INDEX;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.httpserver.ResponseUtils.*;
import static com.macrosan.message.consturct.RequestBuilder.buildMsg;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.localExecutionSignTypeSet;
import static com.macrosan.utils.authorize.AuthorizeFactory.getAuthorizer;
import static com.macrosan.utils.bucketLog.LogRecorder.addBucketLogging;
import static com.macrosan.utils.msutils.MsDateUtils.nowToGMT;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.notification.BucketNotification.addMess;
import static com.macrosan.utils.quota.StatisticsRecorder.addStatisticRecord;
import static com.macrosan.utils.quota.StatisticsRecorder.getRequestType;
import static com.macrosan.utils.store.StoreManagementServer.STORE_MANAGEMENT_HEADER;
import static com.macrosan.utils.trash.TrashUtils.bucketTrash;

/**
 * RestfulVerticle
 *
 * @author liyixin
 * @date 2018/10/27
 */
public class RestfulVerticle extends AbstractVerticle {

    private static final Logger logger = LogManager.getLogger(RestfulVerticle.class.getName());

    private static final ServerConfig CONFIG = ServerConfig.getInstance();

    private static final String DNS = CONFIG.getDns();

    private static RedisConnPool pool = RedisConnPool.getInstance();

    // <bucket, <versionNum, request>>
    public static ConcurrentSkipListMap<String, ConcurrentSkipListMap<String, MsHttpRequest>> requestVersionNumMap = new ConcurrentSkipListMap<>();

    public static final String REQUEST_VERSIONNUM = "requestVersionNum";

    @Getter
    private List<Disposable> statusList = new ArrayList<>(4);

    @Override
    public void start() {
        start0(CONFIG.getBindIp1(), CONFIG.getBindIp2(), CONFIG.getBindIpV61(), CONFIG.getBindIpV62());
    }

    final void start0(String bindIp1, String bindIp2, String bindIpV61, String bindIpV62) {
        Map<String, String> nodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(CONFIG.getHostUuid());
        String heartIp1 = CONFIG.getHeartIp1();
        String syncIp = nodeMap.get(ACTIVE_SYNC_IP);

        if (ServerConfig.isUnify()) {
            if (!"0".equalsIgnoreCase(bindIp1)) {
                statusList.add(createServer(bindIp1, false));
                statusList.add(createServer(bindIp1, true));
            }

            if (!"0".equalsIgnoreCase(bindIp2)) {
                statusList.add(createServer(bindIp2, false));
                statusList.add(createServer(bindIp2, true));
            }

            statusList.add(createServer(heartIp1, true));
            statusList.add(createServer(heartIp1, false));
            if (System.getProperty("com.macrosan.takeOver") == null) {
                statusList.add(createServer("127.0.0.1", false));
                if (StringUtils.isNotBlank(syncIp)) {
                    statusList.add(createServer(syncIp, true));
                    statusList.add(createServer(syncIp, false));
                }
            }
        } else {
            statusList.add(createServer("0.0.0.0", true));
            statusList.add(createServer("0.0.0.0", false));
        }

        /*if (StringUtils.isNotEmpty(bindIpV61)) {
            statusList.add(createServer(bindIpV61, true));
            statusList.add(createServer(bindIpV61, false));
            statusList.add(createServer(bindIpV62, true));
            statusList.add(createServer(bindIpV62, false));
        }*/

        HealthChecker.getInstance().register(this);
    }

    /**
     * 鉴权流程
     * <p>
     *
     * @param req Http请求
     */
    private void authorize(MsHttpRequest req) {
        String accessKey = DEFAULT_ACCESS_KEY;
        String authType = "";
        String signature = "";
        long startTime = System.currentTimeMillis();
        req.addMember("startTime", String.valueOf(startTime));
        try {
            if (req.headers().contains(AUTHORIZATION)) {
                String[] array = req.getHeader(AUTHORIZATION).split(" ");
                authType = array[0];
            }

            String sign = getRequestSign(req, authType);
            if ("POST///".equals(sign)) {//表单上传对象
                if (req.headers().contains(CONTENT_TYPE) && req.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
                    //如果存在Content-Type为multipart/form-data，表示为表单上传对象请求
                    req.addMember("service", AuthorizeV4.SERVICE_S3);
                    //这里需要把路由转为PUT
                    sign = "PUT///";
                }
            } else if (IAM_ROUTE.equals(sign)) {
                //aws iam 会有不带 X_AMZ_CONTENT_SHA_256的请求
//                req.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256) == null &&
                if (req.getMember("body") == null) {
                    String finalSign = sign;
                    req.bodyHandler(buf -> {
                        req.addMember("body", buf.toString());
                        try {
                            QueryStringDecoder queryStringDecoder = new QueryStringDecoder("/?" + buf);
                            Map<String, List<String>> map = queryStringDecoder.parameters();
                            req.addMember("Iam_keys", Json.encode(map.keySet()));

                            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                                req.addMember("Iam_" + entry.getKey(), entry.getValue().get(0));
                            }

                            String iamSign = finalSign + "?Action=" + map.get("Action").get(0);
                            req.addMember("sign", iamSign);
                        } catch (Exception e) {
                            logger.error("decode iam payload fail", e);
                            responseError(req, ErrorNo.UNKNOWN_ERROR);
                            return;
                        }
                        authorize(req);
                    });
                    responseContinue(req);
                    req.resume();
                    return;
                } else {
                    if ("AssumeRole".equals(req.getMember("Iam_Action"))) {
                        req.addMember("service", "sts");
                        if (req.headers().contains(MULTI_SYNC) && "1".equals(req.headers().get(MULTI_SYNC))) {
                            MossHttpClient.getInstance().manageSend(req);
                            return;
                        }
                    } else {
                        req.addMember("service", AuthorizeV4.SERVICE_IAM);
                    }
                }
            } else {
                req.addMember("service", AuthorizeV4.SERVICE_S3);
            }
            if (req.headers().contains(AUTHORIZATION)) {
                String[] array = req.getHeader(AUTHORIZATION).split(" ");
                authType = array[0];
                if (authType.equals(AuthorizeFactory.AWS)) {
                    array = array[1].split(":");
                    accessKey = array[0];
                    signature = StringUtils.isBlank(accessKey) ? signature : array[1];
                } else if (authType.equals(AuthorizeFactory.SHA256)) {
                    AuthorizeV4.V4Authorization v4Authorization = AuthorizeV4.V4Authorization.from(req);
                    signature = v4Authorization.get(AuthorizeV4.V4Authorization.SIGNATURE);
                    accessKey = v4Authorization.get(AuthorizeV4.V4Authorization.CREDENTIAL).split("/")[0];
                } else {
                    responseError(req, ErrorNo.UNSUPPORTED_AUTHORIZATION_TYPE);
                    return;
                }
            } else if (req.headers().contains(X_AUTH_TOKEN)) {
                String token = req.getHeader(X_AUTH_TOKEN);
                if (StringUtils.isNotEmpty(token)) {
                    authType = "token";
                    signature = token;
                } else {
                    responseError(req, ErrorNo.MISSING_SECURITY_HEADER);
                    return;
                }
            }
            boolean[] empty = new boolean[]{true};
            boolean[] storeManagement = {false};
            String iamSign = req.getMember("sign");
            String finalSign = sign;
            getAuthorizer(authType).apply(req, iamSign == null ? sign : iamSign, accessKey, signature)
                    .doOnNext(flag -> {
                        storeManagement[0] = req.getMember(STORE_MANAGEMENT_HEADER) != null;
                        if (StatisticsRecorder.start) {
                            String account = req.getUserId();
                            String bucket = req.getBucketName();
                            StatisticsRecorder.RequestType requestType = getRequestType(req.method());
                            long time = System.currentTimeMillis();
                            long curTime = DateChecker.getCurrentTime();
                            req.addResponseEndHandler(v -> {
                                if (StringUtils.isNotBlank(req.getMember(REQUEST_VERSIONNUM))) {
                                    requestVersionNumMap.get(bucket).remove(req.getMember(REQUEST_VERSIONNUM));
                                }
                                int code = req.response().getStatusCode();
                                boolean success = code / 100 == 2 && req.response().ended();
                                if (success) {
                                    BucketNotification.checkNotification(req).subscribe(event -> {
                                        if (event != null || event.length != 0) {
                                            if (!event[0].startsWith("ObjectRemoved:multiDelete")) {
                                                ErasureServer.DISK_SCHEDULER.schedule(() -> addMess(req, event));
                                            }
                                        }
                                    });
                                }

                                String remote = "remote";
                                String local = "local";
                                try {
                                    remote = req.connection().remoteAddress().host();
                                    local = req.connection().localAddress().host();
                                } catch (Exception e) {
                                    logger.error("get connection address fail. {}", e);
                                }

                                if (!local.equals(remote)) {
                                    addStatisticRecord(account, bucket, req, requestType, success, DateChecker.getCurrentTime() - curTime);
                                }
                                //使用过滤器对请求进行过滤
                                LogFilter.judgeRequest(req).subscribe(result -> {
                                    if (result) {
                                        RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(req.getBucketName(), "user_id").subscribe(bucketOwner -> {
                                            ErasureServer.DISK_SCHEDULER.schedule(() -> addBucketLogging(req, time, startTime, bucketOwner));
                                        });
                                    }
                                });

                            });
                        }
                    })
//                    .doOnNext(b -> {
//                        if (storeManagement[0] && !StoreManagementServer.existStoreManageRoute(sign)) {
//                            //限制纳管账户只能进行对象的上传下载
//                            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "store_management account,No such permission");
//                        }
//                    })
                    .subscribe(b -> {
                        empty[0] = false;
                        if (b) {
                            final long[] timerId = new long[1];
                            Mono<Long> mono;
                            if (PASSWORD.equals(req.getHeader(SYNC_AUTH))) {
                                // 多站点相关的内部请求全部不受性能配额限制
                                mono = Mono.just(0L);
                            } else {
                                mono = AccountPerfLimiter.getInstance()
                                        .limits(req.getUserId(), THROUGHPUT_QUOTA, 1L)
                                        .flatMap(accountWait -> BucketPerfLimiter.getInstance()
                                                .limits(req.getBucketName(), THROUGHPUT_QUOTA, 1L)
                                                .map(bucketWait -> bucketWait + accountWait));
                            }
                            if (StringUtils.isNotEmpty(iamSign) && LIMIT_STS_RESOURCE.contains(iamSign.hashCode())) {
                                mono = mono
                                        .flatMap(accountAndBucketWait -> StsPerfLimiter.getInstance()
                                                .limits(STS_QUOTA, THROUGHPUT_QUOTA, 1L)
                                                .map(stsWait -> accountAndBucketWait + stsWait));
                            }
                            reactor.core.Disposable disposable = mono
                                    .subscribe(waitMillis -> {
                                        if (waitMillis == 0) {
                                            dispatchRequest(req, finalSign);
                                        } else {
                                            timerId[0] = vertx.setTimer(waitMillis, v -> dispatchRequest(req, finalSign));
                                        }
                                    });
                            req.addResponseCloseHandler(v -> {
                                disposable.dispose();
                                vertx.cancelTimer(timerId[0]);
                            });
                        } else {
                            signNotMatch(req);
                        }
                    }, e -> dealException(req, e), () -> {
                        if (empty[0]) {
                            responseError(req, ErrorNo.UNKNOWN_ERROR);
                        }
                    });
        } catch (IllegalArgumentException | UnsupportedEncodingException e) {
            logger.error("url decode fail");
            invalidArgument(req);
        } catch (MsException e) {
            logger.error(e.getMessage());
            responseError(req, e.getErrCode());
        } catch (Exception e) {
            logger.error("The Authorization was wrong : {}, exception : {}",
                    req.getHeader(AUTHORIZATION), e.getMessage());
            accessDeny(req);
        }
    }

    /**
     * 将请求分发到管理流控制器或者数据流控制器
     * <p>
     *
     * @param req Http请求
     */
    private void dispatchRequest(MsHttpRequest req, String sign) {
        if (CONFIG.existDataRoute(sign)) {
            dataStreamRoute(req.setContext(context),
                    sign.hashCode());
        } else if (CONFIG.existManageRoute(sign)) {
            final String requestId = getRequestId();
            Buffer buffer = Buffer.buffer();
            req.getDelegate()
                    .handler(buffer::appendBuffer)
                    .endHandler(v -> {
                        if (AuthorizeV4.checkManageStreamPayloadSHA256(req, buffer.getBytes()) != ErrorNo.SUCCESS_STATUS) {
                            return;
                        }

                        vertx.eventBus()
                                .<ResponseMsg>rxSend(MANAGE_STREAM_ADDRESS,
                                        buildMsg(req, buffer.toString(), sign.hashCode(), requestId), EVENT_BUS_OPTION)
                                .subscribe(message -> {
                                    ResponseMsg res = message.body();
                                    req.response().setStatusCode(res.getHttpCode());
                                    if (req.method() != HttpMethod.DELETE) {
                                        req.response().putHeader(CONTENT_LENGTH, String.valueOf(res.data.length));
                                    }
                                    if (res.getHttpCode() / 100 != 2) {
                                        logger.info("error ip:{}", req.connection().remoteAddress().host());
                                    }

                                    if (req.headers().contains("x-auth-user")) {
                                        req.response().putHeader("x-storage-url", "http://" + req.host() + ":" + req.getDelegate().localAddress().port()
                                                + "/v1/" + req.getHeader("x-auth-user"));
                                    }

                                    res.getHeaders().forEachKeyValue(addPublicHeaders(req, requestId)::putHeader);
                                    if (req.headers().contains(X_AUTH_TOKEN)) {
                                        req.response().headers().remove(X_AMZ_REQUEST_ID);
                                    }
                                    addAllowHeader(req.getDelegate().response()).end(Buffer.buffer(res.getData()));
                                }, e -> {
                                    logger.error("error :", e);
                                    requestTimeOut(req, requestId);
                                });
                    });
            responseContinue(req);
            req.resume();
        } else if (IAM_ROUTE.equals(sign)) {
            String body = req.getMember("body");

            String iamSign = req.getMember("sign");

            Set<String> iamKeys = Json.decodeValue(req.getMember("Iam_keys"), Set.class);
            for (String key : iamKeys) {
                req.params().add(key, req.getMember("Iam_" + key));
            }

            if (CONFIG.existManageRoute(iamSign)) {
                final String requestId = getRequestId();
                if ("POST/?Action=AssumeRole".equals(iamSign) && req.headers().contains(DOUBLE_FLAG)) {
                    vertx.eventBus()
                            .<ResponseMsg>rxSend(STS_MANAGE_STREAM_ADDRESS,
                                    buildMsg(req, body, iamSign.hashCode(), requestId), EVENT_BUS_OPTION)
                            .subscribe(message -> {
                                ResponseMsg res = message.body();
                                req.response().setStatusCode(res.getHttpCode());
                                if (res.getHeaders().containsKey(HANG_UP_STATUS)) {
                                    return;
                                }
                                if (req.method() != HttpMethod.DELETE) {
                                    req.response().putHeader(CONTENT_LENGTH, String.valueOf(res.data.length));
                                }
                                if (res.getHttpCode() / 100 != 2) {
                                    logger.info("error ip:{}", req.connection().remoteAddress().host());
                                }
                                res.getHeaders().forEachKeyValue(addPublicHeaders(req, requestId)::putHeader);
                                addAllowHeader(req.getDelegate().response()).end(Buffer.buffer(res.getData()));

                            }, e -> {
                                logger.error("error :", e);
                                requestTimeOut(req, requestId);
                            });
                } else {
                    vertx.eventBus()
                            .<ResponseMsg>rxSend(IAM_MANAGE_STREAM_ADDRESS,
                                    buildMsg(req, body, iamSign.hashCode(), requestId), EVENT_BUS_OPTION)
                            .subscribe(message -> {
                                ResponseMsg res = message.body();
                                req.response().setStatusCode(res.getHttpCode());
                                if (req.method() != HttpMethod.DELETE) {
                                    req.response().putHeader(CONTENT_LENGTH, String.valueOf(res.data.length));
                                }
                                if (res.getHttpCode() / 100 != 2) {
                                    logger.info("error ip:{}", req.connection().remoteAddress().host());
                                }

                                res.getHeaders().forEachKeyValue(addPublicHeaders(req, requestId)::putHeader);
                                addAllowHeader(req.getDelegate().response()).end(Buffer.buffer(res.getData()));
                            }, e -> {
                                logger.error("error :", e);
                                requestTimeOut(req, requestId);
                            });
                }
            } else {
                logger.error("no such iam route. {}", iamSign);
                invalidRequest(req);
            }
        } else {
            if (!req.method().equals(HttpMethod.OTHER)) {
                logger.error("no such route. \nversion : {}\nmethod : {}\npath : {}\nparam : {}\nheaders : {}",
                        req.version(), req.method(), req.path(), req.params(), req.headers());
            }
            invalidRequest(req);
        }
    }

    /**
     * 处理流中计算用于路由的方法签名的过程，并且保存鉴权会用到的字段
     * <p>
     * PUT/bucket1/o/b/j/1?acl   的签名是    PUT///?acl
     * 注：PUT/bucket1/o/b/j/1?acl=10 的签名也是    PUT///?acl，若有问题再改
     *
     * @param request Http请求
     * @return 请求的签名
     */
    public static String getRequestSign(MsHttpRequest request, String authType) throws UnsupportedEncodingException {
        String host = request.host();
        String path = request.path();
        HttpMethod method = request.method();
        if (request.headers().contains(X_AUTH_TOKEN)) {
            String[] pathArr = path.split("/");
            request.setUserName(pathArr[2]);
            path = "";
            for (int i = 3; i < pathArr.length; i++) {
                path = path + "/" + pathArr[i];
            }

            if (method == HttpMethod.POST) {
                method = HttpMethod.PUT;
            }
        }
        String pathStr = getSignPath(host, path);

        if (AuthorizeFactory.SHA256.equals(authType)) {
            request.setUri(path);
        } else {
            request.setUri(pathStr);
        }
        pathStr = URLDecoder.decode(pathStr, request.getCodec());
        String[] array = pathStr.split(SLASH, 3);
        String prefix = request.getParam(PREFIX);
        if (request.isTempUrlAccess() && StringUtils.isNotBlank(prefix)) {
            if (array.length != 3 || !array[2].startsWith(prefix)) {
                logger.error("object name isn't started with prefix. object name : {} prefix : {}", array[2], prefix);
                accessDeny(request);
            }
            request.setUri(SLASH + array[1] + SLASH + UrlEncoder.encode(prefix, request.getCodec()));
        }
        boolean isContainsBucket = false;
        switch (array.length) {
            /*长度为3 objectname可能为空*/
            case 3:
                isContainsBucket = true;
                request.setBucketName(array[1])
                        .setObjectName(array[2]);
                break;
            /*长度为2 说明只有bucketname*/
            case 2:
                isContainsBucket = true;
                request.setBucketName(array[1]);
                break;
            default:
                break;
        }

        /* 不是斜杠结尾则统一带上斜杠 */
        if (!pathStr.endsWith(SLASH)) {
            pathStr = pathStr + SLASH;
        }

        /* 因为ObjectName中可以带斜杠，所以签名中最多就三个斜杠 */
        StringBuilder builder = new StringBuilder(32);
        int count = StringUtils.countMatches(pathStr, SLASH);
        count = Math.min(count, 3);
        builder.append(method);
        for (int i = 0; i < count; i++) {
            builder.append(SLASH);
        }

        String paramStr = request.params().entries().stream()
                .map(Map.Entry::getKey)
                .filter(key -> INCLUDE_PARAM_LIST.contains(key.hashCode()))
                .sorted()
                .collect(Collectors.joining("&"));
        if (StringUtils.isNotBlank(paramStr)) {
            builder.append('?').append(paramStr);
        }
        boolean isCreateBucket = CONFIG.existManageRoute(builder.toString()) && "PUT//".equals(builder.toString());
        if (isContainsBucket && ServerConfig.isBucketUpper() && !isCreateBucket) {
            request.setBucketName(array[1].toLowerCase());
        }
        return builder.toString();
    }

    /**
     * 获取操作系统对应的编码方式，Window为 GB2312，Linux为UTF-8，如果没有User-Agent头则默认为 UTF-8
     * <p>
     * TODO 应该首先尝试从Content-Type的charset中获取字符集，但目前的请求都不包含charset
     * 注：此方法为首个对request进行处理的方法，需要暂停request
     *
     * @return 回调方法
     */
    private Consumer<MsHttpRequest> getCodec() {
        return request -> request.setCodec("UTF-8");
    }

    private Predicate<MsHttpRequest> tmpUrlHandler() {
        return request -> {
            try {
                MultiMap params = request.params();
                MultiMap headers = request.headers();
                if (request.headers().contains(IS_SYNCING)) {
                    //双活同步来的数据
                    request.setTempUrlAccess(false);
                    return true;
                }
                if (!request.params().contains(SIGNATURE) && !request.params().contains(AuthorizeV4.X_AMZ_SIGNATURE)) {
                    request.setTempUrlAccess(false);
                    return true;
                } else if (request.params().contains(AuthorizeV4.X_AMZ_SIGNATURE)) {
                    if (StringUtils.isNotBlank(request.getHeader(CLUSTER_ALIVE_HEADER))) {
                        Set<String> keySet = new HashSet<>();
                        headers.entries().forEach(entry -> {
                            if (entry.getKey().toLowerCase().startsWith(AUTH_HEADER)) {
                                keySet.add(entry.getKey());
                            }
                        });
                        keySet.forEach(headers::remove);
                    }
                    if (!AuthorizeV4.processTempUrl(request)) {
                        return false;
                    }
                } else {
                    //验证URL超期时间
                    final String expires = params.get(EXPIRES);
                    boolean expired = StringUtils.isBlank(expires) || Long.parseLong(expires) * 1000 < DateChecker.getCurrentTime();
                    if (expired) {
                        accessDeny(request);
                        return false;
                    }

                    final String accessKey = Optional.ofNullable(params.get(ACCESS_KEY_ID)).orElseGet(() -> params.get(AWS_ACCESS_KEY_ID));
                    final String signature = params.get(SIGNATURE).replace(' ', '+');
                    headers.set(AUTHORIZATION, "AWS " + accessKey + ':' + signature);
                    headers.set(DATE, expires);
                }
//                Optional.ofNullable(params.get(VERSIONID)).ifPresent(val -> headers.set(VERSIONID, val));
                Optional.ofNullable(params.get(CONTENT_TYPE)).ifPresent(val -> headers.set(CONTENT_TYPE, val));
                Optional.ofNullable(params.get(CONTENT_MD5)).ifPresent(val -> headers.set(CONTENT_MD5, val));
                Optional.ofNullable(params.get(CONTENT_DISPOSITION)).ifPresent(val -> headers.set(CONTENT_DISPOSITION, val));
                Optional.ofNullable(params.get(CONTENT_ENCODING)).ifPresent(val -> headers.set(CONTENT_ENCODING, val));
                Optional.ofNullable(params.get(CACHE_CONTROL)).ifPresent(val -> headers.set(CACHE_CONTROL, val));
                Optional.ofNullable(params.get(CONTENT_LANGUAGE)).ifPresent(val -> headers.set(CONTENT_LANGUAGE, val));
                Optional.ofNullable(params.get(COMPONENT_USER_ID)).ifPresent(val -> headers.set(COMPONENT_USER_ID, val));

                params.entries().stream()
                        .filter(entry -> entry.getKey().toLowerCase().startsWith(AUTH_HEADER))
                        .forEach(entry -> headers.set(new String(entry.getKey().getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1),
                                new String(entry.getValue().getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1)));
                return true;
            } catch (Exception e) {
                logger.error("error in url handler", e);
                invalidArgument(request);
                return false;
            }
        };
    }

    public static int MAX_S3_RUNNING = SystemPropertyUtil.getInt("MAX_S3_RUNNING", 800);
    public static Semaphore token = new Semaphore(MAX_S3_RUNNING, true);
    private static ConcurrentSkipListMap<Long, Tuple2<MsHttpRequest, UnicastProcessor<MsHttpRequest>>> waitMap = new ConcurrentSkipListMap<>();
    private static AtomicLong reqID = new AtomicLong();

    private static void onFinally() {
        token.release();

        if (token.tryAcquire()) {
            Map.Entry<Long, Tuple2<MsHttpRequest, UnicastProcessor<MsHttpRequest>>> entry = waitMap.pollFirstEntry();
            if (entry != null) {
                UnicastProcessor<MsHttpRequest> allRequest = entry.getValue().var2;
                allRequest.onNext(entry.getValue().var1);
            } else {
                token.release();
            }
        }
    }

    public static void onNext(MsHttpRequest request, UnicastProcessor<MsHttpRequest> allRequest) {
        long id = reqID.incrementAndGet();
        request.addResponseEndHandler(v -> {
            if (waitMap.remove(id) == null) {
                onFinally();
            }
        });

        if (!token.tryAcquire()) {
            waitMap.put(id, new Tuple2<>(request, allRequest));
        } else {
            allRequest.onNext(request);
        }
    }

    private Disposable createServer(String ip, boolean isSsl) {
        HttpServerOptions options = new HttpServerOptions()
                .setTcpFastOpen(true)
                .setTcpQuickAck(true)
                .setIdleTimeout(3600)
                .setIdleTimeoutUnit(TimeUnit.SECONDS)
                .setUsePooledBuffers(true);

        HttpServer server = isSsl ?
                vertx.createHttpServer(options
                        .setSsl(true)
                        .setPemKeyCertOptions(new PemKeyCertOptions().setKeyPath(PRIVATE_PEM)
                                .setCertPath(CERT_CRT))
                        .addEnabledCipherSuite("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
                        .addEnabledCipherSuite("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
                        .addEnabledCipherSuite("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
                        .addEnabledCipherSuite("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")) :
                vertx.createHttpServer(options);

        //处理本站点的请求
        UnicastProcessor<MsHttpRequest> normalRequest = UnicastProcessor.create(Queues.<MsHttpRequest>unboundedMultiproducer().get());
        normalRequest.subscribe(this::authorize, e -> logger.error(e.getMessage()));

        //向双活站点发送请求
        UnicastProcessor<MsHttpRequest> multiRequest = UnicastProcessor.create(Queues.<MsHttpRequest>unboundedMultiproducer().get());
        multiRequest.subscribe(req -> MossHttpClient.getInstance().send(req)
                , e -> logger.error("", e));

        UnicastProcessor<MsHttpRequest> allRequest = UnicastProcessor.create(Queues.<MsHttpRequest>unboundedMultiproducer().get());
        allRequest.flatMap(r -> Mono.just(r)
                .filter(preRequestFilter())
                //如果客户端未处理Expect头，req.pause()无法完全停住客户端请求里body的数据，os的缓存耗尽才会真的停下(ss -a查看)
//                        .map(req -> new MsHttpRequest(req.pause().getDelegate()))
                .doOnNext(getCodec())
                .filter(DateChecker.checkDateHandler())
                .filter(tmpUrlHandler())
                .doOnNext(req -> {
                    if (isSsl) {
                        req.headers().set("http-ssl", "1");
                    } else {
                        req.headers().set("http-ssl", "0");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("\nversion : {}\nmethod : {}\npath : {}\nparam : {}\nheaders : {}", req.version(), req.method(), req.path(), req.params(), req.headers());
                    }
                    try {
                        if (req.method() == HttpMethod.PUT && req.uri().equals("/?putSyncDumpFile")) {
                            // 直接调用 putSyncDumpFile
                            MossHttpClient.putSyncDumpFile(req);
                        } else {
                            if (!isMultiAliveStarted) {
                                normalRequest.onNext(req);
                            } else {
                                check(req, normalRequest, multiRequest);
                            }
                        }
                    } catch (Exception e) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "http request error", e);
                    }
                })
                .map(req -> new Tuple2<>(false, r))
                .doOnError(e -> logger.error("request error:{}", e.getMessage()))
                .onErrorReturn(new Tuple2<>(true, r)))
                .defaultIfEmpty(new Tuple2<>(false, null))
                .subscribe(tuple -> {
                    if (tuple.var1) {
                        vertx.runOnContext(v -> responseError(tuple.var2.getDelegate(), ErrorNo.INVALID_ARGUMENT));
                    }
                }, e -> logger.error("http error", e));

        Disposable serverStatus = server.requestStream()
                .toFlowable()
                .subscribe(r -> onNext(new MsHttpRequest(r.pause().getDelegate()), allRequest), e -> logger.error("http error", e));

        int port = isSsl ? Integer.parseInt(ServerConfig.getInstance().getHttpsPort())
                : Integer.parseInt(ServerConfig.getInstance().getHttpPort());
        /* 开始监听 */
        server.listen(port, ip, res -> {
            if (!res.succeeded()) {
                logger.error("Listen failed, ip : {}, the progress will exit.\ncause : {}", ip, res.cause());
                System.exit(1);
            }
        });

        return serverStatus;
    }

    private void check(MsHttpRequest request, UnicastProcessor<MsHttpRequest> normalRequest, UnicastProcessor<MsHttpRequest> multiRequest) throws UnsupportedEncodingException {
        if (!isMultiAliveStarted) {
            normalRequest.onNext(request);
            return;
        }

        String multi = request.getHeader(CLUSTER_ALIVE_HEADER);
        String pathStr = getSignPath(request.host(), request.path());
        String[] array = pathStr.split(SLASH, 3);
        HttpMethod method = request.method();
        String authType = "";
        try {
            if (StringUtils.isNotBlank(request.headers().get(AUTHORIZATION))) {
                authType = request.getHeader(AUTHORIZATION).split(" ")[0];
            }
        } catch (Exception e) {
            responseError(request, ErrorNo.UNSUPPORTED_AUTHORIZATION_TYPE);
            return;
        }

        String sign0 = getRequestSign(request, authType);

        if ("POST///".equals(sign0)) {//表单上传对象
            if (request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
                //如果存在Content-Type为multipart/form-data，表示为表单上传对象请求
                //这里需要把路由转为PUT
                sign0 = "PUT///";
            }
        }
        // 不需要转发的请求直接走正常流程
        if (localExecutionSignTypeSet.contains(sign0)) {
            normalRequest.onNext(request);
            return;
        }

        // 对站点异常时转发过来的信息直接走异步复制流程
        if (SYNC_REDIRECT.equals(request.getHeader(CLUSTER_VALUE))) {
            request.headers().remove(CLUSTER_VALUE);
            //post请求上传对象记录必要的参数信息后，其异步复制仍然按照put上传请求进行转发，这里需要改post方法为put方法
            asyncRequestPre(request, normalRequest);
            return;
        }

        // 对站点转发的管理类请求直接走正常流程
        if (isSyncMessage(request.headers())) {
            normalRequest.onNext(request);
            return;
        }

        //单站点+异构+纳管异构走单站点流程
        if (INDEX_IPS_ENTIRE_MAP.size() == 2 && !EXTRA_INDEX_IPS_ENTIRE_MAP.isEmpty() && request.headers().contains(BACK_SOURCE_REQUEST_HEADER)) {
            normalRequest.onNext(request);
            return;
        }

        boolean delfiles = sign0.equals("POST//?delete");
        final String sign = sign0;
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY)
                .zipWith(DoubleActiveUtil.datasyncIsEnabled(request.getBucketName()))
                .subscribe(tuple2 -> {
                    String syncPolicy = tuple2.getT1();
                    String bucketSyncSwitch = tuple2.getT2();
                    if (StringUtils.isNotBlank(syncPolicy) && array.length == 3 && StringUtils.isBlank(multi)
                            && !CONFIG.existManageRoute(sign) && isSwitchOn(bucketSyncSwitch) && !delfiles) {
                        if (Arbitrator.isEvaluatingMaster.get()) {
                            dealException(request, new MsException(ErrorNo.UNKNOWN_ERROR, "Master Index has not checked complete. "));
                            return;
                        }

                        // async站点不进行同步
                        if (IS_ASYNC_CLUSTER) {
                            normalRequest.onNext(request);
                            return;
                        }

                        // 指定了操作站点，在接口中单独处理。目前有删除。批删不走这里。
                        if (request.headers().contains(ClUSTER_NAME_HEADER)) {
                            normalRequest.onNext(request);
                            return;
                        }

                        //双活数据流相关操作转发
                        switch (method) {
                            case DELETE:
                            case PUT:
                            case POST:
                                if ("1".equals(syncPolicy) || SWITCH_SUSPEND.equals(bucketSyncSwitch)) {
                                    //1为异步复制
                                    String[] localClusterIps = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX);
                                    // 可用ip里无本地eth12 ip，说明本地eth12 ping不通
                                    if (!Arrays.asList(localClusterIps).contains(LOCAL_NODE_IP)) {
                                        if (DAVersionUtils.isStrictConsis() && !LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)) {
                                            //如果开启了双活优化，从节点的复制链路不通会获取不到DASyncStamp，此时会轮询eth4可用的节点转发
                                            MossHttpClient.getInstance().redirectHttpRequest(REDIRECTABLE_BACKEND_IP_SET.toArray(new String[0]), request, false);
                                            return;
                                        }
                                    }
                                    // 当前节点的复制链路通，或不通但没有开双活优化，则正常走异步复制流程
                                    if (BucketSyncSwitchCache.getSyncIndexMap(request.getBucketName(), LOCAL_CLUSTER_INDEX).isEmpty()) {
                                        normalRequest.onNext(request);
                                    } else {
                                        asyncRequestPre(request, normalRequest);
                                    }
                                } else if ("0".equals(syncPolicy) && SWITCH_ON.equals(bucketSyncSwitch)) {
                                    //异步复制或双活链路断开，记录日志走本站点写入流程
                                    //每次双活请求来此都会判断一次redis的链路状态。
                                    pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE)
                                            .subscribe(linkState -> {
                                                if ("1".equals(linkState)) {
                                                    String[] localClusterIps = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX);
                                                    // 可用ip里无本地eth12 ip，说明本地节点eth12不通，直接转发到eth12通的节点
                                                    if (!Arrays.asList(localClusterIps).contains(LOCAL_NODE_IP)) {
                                                        MossHttpClient.getInstance().redirectHttpRequest(REDIRECTABLE_BACKEND_IP_SET.toArray(new String[0]), request, false);
                                                    } else {
                                                        // 当前节点的复制链路通，则正常走双活流程
                                                        multiRequest.onNext(request);
                                                    }
                                                } else {
                                                    // 双活复制链路异常，如果本站点心跳正常则走异步复制流程。
                                                    // 如果是本站点心跳有问题(比如索引池异常，无法写预提交记录)，则将请求转发到心跳正常的站点走异步复制流程。
                                                    if (heartBeatIsNormal(LOCAL_CLUSTER_INDEX)) {
                                                        asyncRequestPre(request, normalRequest);
                                                    } else {
                                                        int availIndex = getAnAvailClusterIndex();
                                                        // 无可用的站点，走异步复制记下record。若开了双活优化会报错。
                                                        if (availIndex == -1) {
                                                            asyncRequestPre(request, normalRequest);
                                                            return;
                                                        }
                                                        String[] ips = INDEX_IPS_MAP.get(availIndex);
                                                        MossHttpClient.getInstance().redirectHttpRequest(ips, request, true);
                                                    }
//                                                    asyncRequestPre(request, normalRequest);
                                                }
                                            });
                                } else {
                                    normalRequest.onNext(request);
                                }
                                break;
                            default:
                                normalRequest.onNext(request);
                        }
                    } else {
                        try {
                            switch (method) {
                                case DELETE:
                                case PUT:
                                case POST:
                                    //管理流操作，站点连接状态检测，断开则从站点不允许进行管理流修改写入操作
                                    pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE)
                                            .defaultIfEmpty("1")
                                            .subscribe(linkState -> {
                                                if (StringUtils.isBlank(multi) && "0".equals(syncPolicy) && "1".equals(linkState)) {
                                                    String[] localClusterIps = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX);
                                                    // 可用ip里无本地eth12 ip，说明本地节点eth12不通，直接转发到eth12通的节点
                                                    if (!Arrays.asList(localClusterIps).contains(LOCAL_NODE_IP)) {
                                                        MossHttpClient.getInstance().redirectHttpRequest(REDIRECTABLE_BACKEND_IP_SET.toArray(new String[0]), request, false);
                                                    } else {
                                                        // 当前节点的复制链路通，则正常走双活流程
                                                        if (!delfiles) {
                                                            request.headers().set(MULTI_SYNC, "1");
                                                        }
                                                        if ("PUT//".equals(sign)) {
                                                            multiRequest.onNext(request);
                                                            return;
                                                        }
                                                        normalRequest.onNext(request);
                                                    }
                                                } else {
                                                    if (CONFIG.existManageRoute(sign) && !request.params().contains("multiRegionInfo")) {
                                                        Mono.just("PUT//".equals(sign) || "PUT/?createaccount".equals(sign))
                                                                .zipWith(DoubleActiveUtil.isMasterCluster())
                                                                .flatMap(tuple -> {
                                                                    if (!tuple.getT1() || !tuple.getT2()) {
                                                                        return DoubleActiveUtil.checkLinkStateReactive();
                                                                    } else {
                                                                        return Mono.just(true);
                                                                    }
                                                                })
                                                                .subscribe(b -> {
                                                                    if (b || !SWITCH_ON.equals(bucketSyncSwitch)) {
                                                                        normalRequest.onNext(request);
                                                                    } else {
                                                                        dealException(request, new MsException(ErrorNo.SITE_LIKN_BROKEN, "link state broken!"));
                                                                    }
                                                                }, e -> {
                                                                    logger.error("", e);
                                                                    responseError(request, ErrorNo.UNKNOWN_ERROR);
                                                                });
                                                    } else {
                                                        normalRequest.onNext(request);
                                                    }
                                                }
                                            });
                                    break;
                                default:
                                    normalRequest.onNext(request);
                                    break;
                            }
                        } catch (MsException e) {
                            logger.error("", e);
                            responseError(request, e.getErrCode());
                        } catch (Exception e) {
                            logger.error("", e);
                            responseError(request, ErrorNo.UNKNOWN_ERROR);
                        }
                    }
                });
    }

    /**
     * 异步数据同步预处理，记录同步日志后走normal request 流程
     *
     * @param request 请求
     */
    private void asyncRequestPre(MsHttpRequest request, UnicastProcessor<MsHttpRequest> normalRequest) {
        try {
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(request.getBucketName());
            String bucketVnode = storagePool.getBucketVnodeId(request.getBucketName());
            AtomicBoolean isRecordPut = new AtomicBoolean(false);
            final UnSynchronizedRecord[] preRecord = new UnSynchronizedRecord[1];

            storagePool.mapToNodeInfo(bucketVnode)
                    .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX)
                            .hget(request.getBucketName(), BUCKET_VERSION_STATUS)
                            .switchIfEmpty(Mono.just("NULL"))
                    )
                    .zipWith(VersionUtil.getVersionNum(request.getBucketName(), request.getObjectName()))
                    .flatMap(tuple2 -> {
                        List<Tuple3<String, String, String>> nodeList = tuple2.getT1().getT1();
                        String versionStatus = tuple2.getT1().getT2();
                        String versionNum = tuple2.getT2();

                        preRecord[0] = buildSyncRecord(request, versionStatus, versionNum);
                        preRecord[0].headers.forEach((k, v) -> request.headers().set(k, v));
                        boolean formUpload = HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data");
                        UnSynchronizedRecord.Type type = UnSynchronizedRecord.type(request.uri(), formUpload ? "PUT" : request.method().toString());
                        if (ERROR_PUT_OBJECT == type || ERROR_PART_UPLOAD == type || StringUtils.isNotEmpty(preRecord[0].headers.get("trashDir"))) {
                            // 涉及数据流的接口需要保证元数据写成功才去update预提交记录
                            request.setAllowCommit(false);
                        } else {
                            request.setAllowCommit(true);
                        }
                        request.addResponseEndHandler(v -> {
                            boolean allowCommit = request.isAllowCommit();
                            //客户端连接断开时，statusCode若未设置则默认是200.
                            if (request.response().getStatusCode() / 100 == 2) {
                                //非put请求中断则不commit。put请求如果putMetaData执行未成功就中断则不commit，其他情况都commit。
                                if (request.response().closed() && ERROR_PUT_OBJECT != type && ERROR_PART_UPLOAD != type && StringUtils.isEmpty(preRecord[0].headers.get("trashDir"))) {
                                    return;
                                }

                                if (!allowCommit) {
                                    return;
                                }
                                // 为chunked类型上传的对象，设置content-length 用于异步复制时限流
                                if (request.headers().contains(ACTUAL_OBJECT_SIZE)) {
                                    preRecord[0].getHeaders().put(CONTENT_LENGTH, request.headers().get(ACTUAL_OBJECT_SIZE));
                                }
                                preRecord[0].setCommited(true);
//                                String bucketVnode = DEFAULT_META_POOL.getBucketVnodeId(preRecord.getBucket());
                                ECUtils.updateSyncRecord(preRecord[0], nodeList, WRITE_ASYNC_RECORD)
                                        .map(resInt -> resInt != 0)
                                        .subscribe(res1 -> {
                                            if (!res1) {
                                                logger.error("commit sync preRecord error!" + preRecord[0]);
                                            }
                                        }, e -> logger.error("commit sync preRecord error, " + preRecord[0], e));
                            } else if (request.response().getStatusCode() != INTERNAL_SERVER_ERROR) {
                                if (IS_THREE_SYNC) {
                                    deleteUnsyncRecord(preRecord[0].bucket, preRecord[0].rocksKey(), null, DELETE_SYNC_RECORD);
                                } else {
                                    deleteUnsyncRecord(preRecord[0].bucket, preRecord[0].rocksKey(), null, DELETE_ASYNC_RECORD);
                                }
                            }
                        });

                        if (ERROR_COMPLETE_PART.equals(type) || ERROR_PART_UPLOAD.equals(type)) {
                            MultiMap params = RestfulVerticle.params(request.uri());
                            String uploadId = params.get(URL_PARAM_UPLOADID);
                            return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(request.getBucketName(), request.getObjectName()))
                                    .flatMap(infoList -> PartClient.getInitPartInfo(preRecord[0].bucket, preRecord[0].object, uploadId, infoList, null))
                                    .doOnNext(initPartInfo -> {
                                        if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO) || initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                            preRecord[0].setVersionId(null).headers.remove(VERSIONID);
                                        } else {
                                            preRecord[0].setVersionId(initPartInfo.metaData.getVersionId()).headers.put(VERSIONID, initPartInfo.metaData.getVersionId());
                                        }
                                    })
                                    .map(initPartInfo -> preRecord[0])
                                    .flatMap(record -> generateSourceSize(record, request, true))
                                    .zipWith(Mono.just(nodeList));
                        } else if (ERROR_PUT_OBJECT.equals(type) && request.headers().contains(X_AMZ_COPY_SOURCE)) {
                            return Mono.just(preRecord[0])
                                    .flatMap(record -> generateSourceSize(record, request, true))
                                    .zipWith(Mono.just(nodeList));
                        } else if (ERROR_PUT_OBJECT.equals(type) && !request.headers().contains(X_AMZ_COPY_SOURCE)) {
                            return Mono.just(preRecord[0]).zipWith(Mono.just(nodeList));
                        } else if (type.useCurrStamp) {
                            //同名对象获取已覆盖对象的syncStamp，服务端进行冗余删除
                            return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(request.getBucketName(), request.getObjectName()))
                                    .flatMap(infoList -> ErasureClient.getObjectMetaVersion(request.getBucketName(), request.getObjectName(), preRecord[0].versionId, infoList))
                                    .map(metaData -> {
                                        if (metaData.isAvailable()) {
                                            if (type != ERROR_DELETE_OBJECT) {
                                                preRecord[0].headers.put(VERSIONID, metaData.versionId);
                                            }
                                            if (ERROR_PUT_OBJECT.equals(type)) {
                                                preRecord[0].setLastStamp(metaData.syncStamp);
                                            } else if (ERROR_DELETE_OBJECT.equals(type) && StringUtils.isNotEmpty(bucketTrash.get(request.getBucketName()))) {
                                                String syncStamp = preRecord[0].headers.get(SYNC_STAMP);
                                                long size = metaData.endIndex - metaData.startIndex + 1;
                                                preRecord[0].headers.put(CONTENT_LENGTH, String.valueOf(size));
                                                preRecord[0].setLastStamp(metaData.syncStamp);
                                                request.headers().remove(SYNC_STAMP);
                                                request.headers().add(SYNC_STAMP, metaData.syncStamp);
                                                if (!preRecord[0].headers.containsKey("recoverObject")) {
                                                    preRecord[0].setSyncStamp(metaData.syncStamp);
                                                    preRecord[0].headers.put(SYNC_STAMP, metaData.syncStamp);
                                                } else {
                                                    request.headers().add(LAST_STAMP, syncStamp);
                                                }
                                            } else {
                                                preRecord[0].setSyncStamp(metaData.syncStamp);
                                                preRecord[0].rocksKey();
                                                preRecord[0].headers.put(SYNC_STAMP, metaData.syncStamp);
                                                request.headers().remove(SYNC_STAMP);
                                                request.headers().add(SYNC_STAMP, metaData.syncStamp);
                                            }
                                        }
                                        return preRecord[0];
                                    }).zipWith(Mono.just(nodeList));
                        } else if (BucketSyncSwitchCache.isBucketNeedFsAsync(request.getBucketName())) {
                            return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(request.getBucketName(), request.getObjectName()))
                                    .flatMap(infoList -> ErasureClient.getObjectMetaVersion(request.getBucketName(), request.getObjectName(), preRecord[0].versionId, infoList))
                                    .map(metaData -> {
                                        if (metaData.isAvailable()) {
                                            if (metaData.inode > 0) {
                                                preRecord[0].setNodeId(metaData.inode);
                                            }
                                        }
                                        return preRecord[0];
                                    }).zipWith(Mono.just(nodeList));
                        } else {
                            if (StringUtils.isNotEmpty(preRecord[0].headers.get("trashDir"))) {
                                request.headers().add(SYNC_STAMP, preRecord[0].headers.get(SYNC_STAMP));
                                return Mono.just(preRecord[0]).flatMap(record -> addRecordObjectSize(preRecord[0], request))
                                        .zipWith(Mono.just(nodeList));
                            }
                            return Mono.just(preRecord[0]).zipWith(Mono.just(nodeList));
                        }
                    })
                    .flatMap(tuple2 -> ECUtils.putSynchronizedRecord(storagePool, tuple2.getT1().rocksKey(), Json.encode(tuple2.getT1()), tuple2.getT2(), WRITE_ASYNC_RECORD, request))
                    .subscribe(res -> {
                        if (!res) {
                            logger.error("write async preRecord error! {}", preRecord[0]);
                            responseError(request, ErrorNo.UNKNOWN_ERROR);
                        } else {
                            isRecordPut.set(true);
                            request.headers().add(SYNC_RECORD_HEADER, preRecord[0].rocksKey());
                            normalRequest.onNext(request);
                        }
                    }, e -> {
                        dealException(request, e);
                        // 预提交成功才会在请求失败后去删除预提交记录
//                        if (isRecordPut.get()) {
//                            if (IS_THREE_SYNC) {
//                                deleteUnsyncRecord(preRecord[0].bucket, preRecord[0].rocksKey(), null, DELETE_SYNC_RECORD);
//                            } else {
//                                deleteUnsyncRecord(preRecord[0].bucket, preRecord[0].rocksKey(), null, DELETE_ASYNC_RECORD);
//                            }
//                        }
//                        logger.error("commit sync preRecord error", e);
//                        responseError(request, ErrorNo.UNKNOWN_ERROR);
                    });
        } catch (Exception e) {
            logger.error("asyncRequestPre error", e);
            dealException(request, e);
        }
    }

    public static String getSignPath(String host, String path) {
        host = host.split(":")[0];

        String pathStr = path;
        for (String dnsName : HOST_LIST) {
            if (host.equals(dnsName)) {
                pathStr = path;
                break;
            } else if (host.endsWith(dnsName)) {
                String[] split = host.split("\\.", 2);
                if (split[1].equals(dnsName)) {
                    pathStr = SLASH + split[0] + path;
                    break;
                }
            }
        }
        return pathStr;
    }

    public static void setBucketNameToRequest(MsHttpRequest request) throws UnsupportedEncodingException {
        String host = request.host();
        String path = request.path();
        request.setCodec("utf-8");
        HttpMethod method = request.method();
        String pathStr = getSignPath(host, path);
        request.setUri(pathStr);
        pathStr = URLDecoder.decode(pathStr, request.getCodec());
        String[] array = pathStr.split(SLASH, 3);
        switch (array.length) {
            case 3:
                request.setBucketName(array[1])
                        .setObjectName(array[2]);
                break;
            case 2:
                request.setBucketName(array[1]);
                break;
            default:
                break;
        }
    }

    private void addCORSHeaders(io.vertx.core.http.HttpServerRequest delegate, CORSConfiguration corsConfig) {
        String origin = delegate.getHeader("Origin");
        String method = delegate.getHeader("Access-Control-Request-Method");
        String allowHeaders = delegate.getHeader("Access-Control-Request-Headers");
        boolean success = false;
        for (CORSRule rule : corsConfig.getCorsRules()) {
            //匹配allowedOrigins
            boolean matchesAllowedOrigins = CORSUtils.matchesAllowedOrigins(rule.getAllowedOrigins(), origin);
            if (!matchesAllowedOrigins) {
                continue;
            }
            //匹配allowedMethods
            if (!rule.getAllowedMethods().contains(method)) {
                continue;
            }
            //匹配allowedHeaders
            boolean matchesAllowedHeaders = CORSUtils.matchesAllowedHeaders(rule.getAllowedHeaders(), allowHeaders);
            if (!matchesAllowedHeaders) {
                continue;
            }
            //设置exposeHeaders
            if (!rule.getExposeHeaders().isEmpty()) {
                String headers = String.join(",", rule.getExposeHeaders());
                delegate.response().putHeader(ACCESS_CONTROL_HEADERS, headers);
            }
            //设置maxAgeSeconds
            if (StringUtils.isNotEmpty(rule.getMaxAgeSeconds())) {
                delegate.response().putHeader(ACCESS_CONTROL_MAX_AGE, rule.getMaxAgeSeconds());
            }
            //设置ACCESS_CONTROL_ORIGIN
            delegate.response().putHeader(ACCESS_CONTROL_ORIGIN, origin);
            //设置ACCESS_CONTROL_METHODS
            String methods = String.join(",", rule.getAllowedMethods());
            delegate.response().putHeader(ACCESS_CONTROL_METHOD, methods);
            //设置ACCESS_CONTROL_ALLOW_HEADERS
            delegate.response().putHeader(ACCESS_CONTROL_ALLOW_HEADERS, allowHeaders);
            success = true;
            break;
        }
        if (success) {
            if (corsConfig.getResponseVary() != null && "true".equals(corsConfig.getResponseVary())) {
                delegate.response().putHeader(ACCESS_CONTROL_VARY, "Origin");
            }
            delegate.response()
                    .putHeader(SERVER, "MOSS")
                    .putHeader(DATE, nowToGMT())
                    .putHeader(CONTENT_LENGTH, "0")
                    .end();
        } else {
            delegate.response()
                    .putHeader(SERVER, "MOSS")
                    .putHeader(DATE, nowToGMT())
                    .putHeader(CONTENT_LENGTH, "0")
                    .end();
        }
    }

    private Set<String> getLoginIpList() {
//        String role = ServerConfig.getInstance().getState();
//        if (!"master".equals(role)){
//            return new HashSet<>();
//        }
        Set<String> loginIpSet = new HashSet<>();
        //aip
        String aip = ServerConfig.getInstance().getAccessIp();
        String aipv6 = ServerConfig.getInstance().getAccessIpV6();
        loginIpSet.add(aip);
        loginIpSet.add(aipv6);
        //所有节点ip
        String node = ServerConfig.getInstance().getHostUuid();
        loginIpSet.add(pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, "business_eth1"));
        loginIpSet.add(pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, "business_eth2"));
        loginIpSet.add(pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, "business_eth3"));
        loginIpSet.add(pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, "mgt_ip"));
        loginIpSet.add(pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, "outer_manager_ip"));
        //dns_ip
        pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(DNS_IP_KEY).forEach((k, v) -> loginIpSet.add(v));
        removeInvalidIPs(loginIpSet);
        return loginIpSet;
    }


    // 移除不合法的 IP
    public static void removeInvalidIPs(Set<String> ipSet) {
        if (ipSet == null || ipSet.isEmpty()) {
            return;
        }
        ipSet.removeIf(StringUtils::isEmpty);
        ipSet.removeIf(ip -> !(ip.contains(".") || ip.contains(":")));
    }

    /**
     * http请求的预处理
     * <p>
     * 1.Option方法直接返回
     * 2.对x-amz-开头的header的key进行解码，若解码失败（key中含有非ASCII字符）则拒绝请求
     * 3.content-length 和content-type统一大小写
     *
     * @return 用于http请求预处理的Predicate
     */
    private Predicate<MsHttpRequest> preRequestFilter() {
        return req -> {
            if (req.method().equals(HttpMethod.OPTIONS)) {
                Set<String> loginIpList = getLoginIpList();
                if (!loginIpList.isEmpty()) {
                    String origin = req.getDelegate().getHeader("Origin");
                    for (String ip : loginIpList) {
                        if (origin != null && origin.contains(ip)) {
                            addPublicHeaders(req.getDelegate(), null)
                                    .putHeader(CONTENT_LENGTH, "0")
                                    .putHeader("Access-Control-Allow-Headers", req.getHeader("Access-Control-Request-Headers"))
                                    .putHeader("Access-Control-Allow-Methods", req.getHeader("Access-Control-Request-Method"))
                                    .putHeader("Vary", "Origin,Access-Control-Request-Headers,Access-Control-Request-Method")
                                    .end();
                            return false;
                        }
                    }
                }
                MsHttpRequest msReq = new MsHttpRequest(req.getDelegate());
                try {
                    setBucketNameToRequest(msReq);
                } catch (UnsupportedEncodingException e) {
                    logger.error("setBucketNameToRequest err, ", e);
                    req.getDelegate().response()
                            .putHeader(SERVER, "MOSS")
                            .putHeader(DATE, nowToGMT())
                            .putHeader(CONTENT_LENGTH, "0")
                            .end();
                }
                String bucketName = msReq.getBucketName();
                //桶跨域检查
                if (StringUtils.isNotEmpty(bucketName) && StringUtils.isNotEmpty(pool.getCommand(REDIS_SYSINFO_INDEX).hget(CORS_CONFIG_KEY, bucketName))) {
                    CORSConfiguration corsConfig = (CORSConfiguration) JaxbUtils.toObject(pool.getCommand(REDIS_SYSINFO_INDEX).hget(CORS_CONFIG_KEY, bucketName));
                    addCORSHeaders(req.getDelegate(), corsConfig);
                } else {
                    addPublicHeaders(req.getDelegate(), null)
                            .putHeader(CONTENT_LENGTH, "0")
                            .putHeader("Access-Control-Allow-Headers", req.getHeader("Access-Control-Request-Headers"))
                            .putHeader("Access-Control-Allow-Methods", req.getHeader("Access-Control-Request-Method"))
                            .putHeader("Vary", "Origin,Access-Control-Request-Headers,Access-Control-Request-Method")
                            .end();
                }
                return false;
            }

            try {
                for (Map.Entry<String, String> entry : req.headers()) {
                    final String lowerKey = entry.getKey().toLowerCase();
                    if (lowerKey.startsWith(AUTH_HEADER)) {
                        String decodeKey;
                        try {
                            decodeKey = URLDecoder.decode(lowerKey, "UTF-8");
                        } catch (Exception e) {
                            decodeKey = lowerKey;
                        }
                        if (decodeKey.length() != lowerKey.length()) {
                            req.headers().remove(lowerKey).add(decodeKey, entry.getValue());
                        }
                    } else if (SPECIAL_HEADER.contains(lowerKey.hashCode())) {
                        req.headers().remove(lowerKey).add(kebabUpperCase(lowerKey), entry.getValue());
                    }
                }
            } catch (Exception e) {
                req.response().end();
                logger.error(e.getMessage());
                return false;
            }
            return true;
        };
    }

    public static String kebabUpperCase(final String kebabCase) {
        final int length = kebabCase.length();

        int start = 0;
        boolean firstChar = true;
        for (int i = 0; i < length; i++) {
            char c = kebabCase.charAt(i);
            if (firstChar) {
                if (isUpperCase(c)) {
                    start++;
                } else {
                    return kebabUpperCase(kebabCase, true, start, length);
                }
                firstChar = false;
            } else if (c == '-') {
                firstChar = true;
                start++;
            } else {
                if (isLowerCase(c)) {
                    start++;
                } else {
                    return kebabUpperCase(kebabCase, false, start, length);
                }
            }
        }

        return kebabCase;
    }

    private static boolean isUpperCase(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static boolean isLowerCase(char c) {
        return c >= 'a' && c <= 'z';
    }

    private static String kebabUpperCase(final CharSequence kebabCase, boolean firstChar, int start, int end) {
        StringBuilder builder = new StringBuilder(end);
        builder.append(kebabCase, 0, start);
        for (int i = start; i < end; i++) {
            char c = kebabCase.charAt(i);
            if (firstChar) {
                firstChar = false;
                c = ((char) (c & 0xdf));
            } else if (c == '-') {
                firstChar = true;
            } else {
                c = ((char) (c | 0x20));
            }

            builder.append(c);
        }

        return builder.toString();
    }

    /**
     * 根据url获取各参数的集合。
     * 详见 io.vertx.core.http.impl.HttpUtils#params(java.lang.String)
     */
    public static MultiMap params(String uri) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        Map<String, List<String>> prms = queryStringDecoder.parameters();
        MultiMap params = new CaseInsensitiveHeaders();
        if (!prms.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : prms.entrySet()) {
                params.add(entry.getKey(), entry.getValue());
            }
        }
        return params;
    }

    public static void responseContinue(io.vertx.core.http.HttpServerRequest request) {
        String expect = request.getHeader(EXPECT);
        if (expect != null
                && HttpVersion.HTTP_1_0.compareTo(request.version()) < 0
                && expect.toLowerCase().startsWith(EXPECT_100_CONTINUE)) {
            request.response().writeContinue();
        }
    }
}
