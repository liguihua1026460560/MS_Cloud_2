package com.macrosan.utils.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ResponseUtils;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.authorize.AuthorizeFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.streams.Pump;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple3;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.authorize.AuthorizeV4.SERVICE_S3;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_CONTENT_SHA_256;
import static com.macrosan.utils.store.AWSV2Auth.getCanonicalizedHeaders;
import static com.macrosan.utils.store.AWSV2Auth.getCanonicalizedResources;
import static com.macrosan.utils.store.StoreUtils.generateHmacSignature;

/**
 * @Description: 纳管服务
 * @Author wanhao
 * @Date 2023/5/4 下午 4:21
 */
@Log4j2
public class StoreManagementServer {
    private static RedisConnPool pool = RedisConnPool.getInstance();
    private final static ThreadFactory STORA_MANAGEMENT_THREAD_FACTORY = new MsThreadFactory("store-management");
    public static final Scheduler STORA_MANAGEMENT_SCHEDULER;

    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(PROC_NUM, PROC_NUM, STORA_MANAGEMENT_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }

        STORA_MANAGEMENT_SCHEDULER = scheduler;
    }

    /**
     * 保存纳管账户可以访问的moss接口
     */
    private static final Set<String> ROUTE_SET = ConcurrentHashMap.newKeySet(16);

    public static final String STORE_MANAGEMENT_HEADER = "store_management";
    public static final String TARGET_IP_HEADER = "target_ip";
    public static final String HTTP_PORT = "httpPort";
    public static final String HTTPS_PORT = "httpsPort";
    public static final String DEFAULT_HTTP_PORT = "80";
    public static final String DEFAULT_HTTPS_PORT = "443";
    public static final String LOCALHOST = "127.0.0.1";
    public static final int FORWARD_ERROR_CODE = 2808;
    public static HttpClient HTTP_CLIENT;
    public static HttpClient HTTPS_CLIENT;
    public static final String BUCKETS = "buckets";
    public static final String EXTRA_SITE_COMPLETE_FLAG = "extra_site_complete_flag";
    private static final String nodeName = ServerConfig.getInstance().getHostUuid();
    public static Set<String> OTHER_CLUSTER_IP_SET = new ConcurrentSkipListSet<>();
    public static ConcurrentSkipListSet<String> requestSet = new ConcurrentSkipListSet<>();

    static {
        ROUTE_SET.add("PUT///");
        ROUTE_SET.add("GET///");
        ROUTE_SET.add("DELETE///");
        ROUTE_SET.add("POST//?delete");
        ROUTE_SET.add("POST///?uploads");
        ROUTE_SET.add("PUT///?partNumber&uploadId");
        ROUTE_SET.add("GET///?uploadId");

        ROUTE_SET.add("GET/?account-name&accountusedcapacity");
        ROUTE_SET.add("GET/?ObjectStatistics");
        ROUTE_SET.add("GET//?objectsInfo");
        ROUTE_SET.add("PUT//");
        ROUTE_SET.add("DELETE//");

    }

    public static void init() {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(PROC_NUM)
                .setPreferNativeTransport(true));
        HttpClientOptions httpOptions = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                //并发数远大于2000时，转发的http请求可能超时
                .setMaxPoolSize(2000)
                .setMaxWaitQueueSize(10000)
                .setConnectTimeout(3000)
                .setIdleTimeout(300);

        HttpClientOptions httpsOptions = new HttpClientOptions(httpOptions)
                .setSsl(true)
                .setTrustAll(true)
                .setVerifyHost(false);
        HTTP_CLIENT = vertx.createHttpClient(httpOptions);
        HTTPS_CLIENT = vertx.createHttpClient(httpsOptions);
        Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
        for (String otherCluster : otherClusters) {
            JSONObject cluster = JSON.parseObject(otherCluster);
            if (StringUtils.isNotEmpty(cluster.getString(CLUSTER_IPS))){
                OTHER_CLUSTER_IP_SET.add(cluster.getString(CLUSTER_IPS));
            }
        }
        ErasureServer.DISK_SCHEDULER.schedule(StoreManagementServer::addOtherClusterIp, 10, TimeUnit.SECONDS);
        log.info("init store_management client successful");
    }
    private static void addOtherClusterIp() {
        if ("1".equals(pool.getCommand(REDIS_NODEINFO_INDEX).hget(nodeName, EXTRA_SITE_COMPLETE_FLAG))){
            Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
            for (String otherCluster : otherClusters) {
                JSONObject cluster = JSON.parseObject(otherCluster);
                if (StringUtils.isNotEmpty(cluster.getString(CLUSTER_IPS))){
                    OTHER_CLUSTER_IP_SET.add(cluster.getString(CLUSTER_IPS));
                }
            }
            pool.getShortMasterCommand(REDIS_NODEINFO_INDEX).hdel(nodeName, EXTRA_SITE_COMPLETE_FLAG);
        }
        ErasureServer.DISK_SCHEDULER.schedule(StoreManagementServer::addOtherClusterIp, 10, TimeUnit.SECONDS);
    }


    public static boolean existStoreManageRoute(String sign) {
        return ROUTE_SET.contains(sign);
    }

    public static Mono<Boolean> forwardRequest(MsHttpRequest request, boolean toResponse, byte[] data) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        STORA_MANAGEMENT_SCHEDULER.schedule(() -> preMono(request, request.getBucketName())
                .subscribe(tuple3 -> {
                    int port = Integer.parseInt(tuple3.getT1());
                    boolean existBucket = tuple3.getT2();
                    if (!existBucket) {
                        res.onNext(false);
                        return;
                    }
                    HttpClientRequest forwardRequest;
                    String targetIp = request.getMember(TARGET_IP_HEADER);
                    String uri = request.uri();
                    String signPath = RestfulVerticle.getSignPath(request.host(), request.path());
                    boolean isHost = !Objects.equals(signPath, request.path())
                            || ((request.getParam(EXPIRES) != null && request.getHeader(DATE) != null)
                            && !request.params().contains(AuthorizeV4.X_AMZ_SIGNATURE) && request.params().contains(SIGNATURE));
                    if (isHost) {
                        uri = getUri(request);
                    }
                    if (request.isSSL()) {
                        forwardRequest = HTTPS_CLIENT.request(request.method(), port, targetIp, uri);
                    } else {
                        forwardRequest = HTTP_CLIENT.request(request.method(), port, targetIp, uri);
                    }
                    //使用host访问路径改变，由于访问纳管是path方式，所以需要重新构造签名
                    if (!isHost) {
                        request.headers().forEach(entry -> {
                            if (!entry.getKey().equalsIgnoreCase(SYNC_STAMP) && !entry.getKey().equalsIgnoreCase(IS_SYNCING)) {
                                forwardRequest.putHeader(entry.getKey(), entry.getValue());
                            }
                        });
                    } else {
                        preHeaders(request, forwardRequest, tuple3, targetIp, false);
                    }

                    forwardRequest.exceptionHandler(e -> {
                                res.onNext(false);
                                if (!isConnectionClosed(e)) {
                                    ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                                }
                            })
                            .handler(response -> {
                                res.onNext(true);
                                if (toResponse) {
                                    request.response().setStatusCode(response.statusCode());
                                    response.headers().forEach(header -> request.response().putHeader(header.getKey(), header.getValue()));
                                    Pump pump = Pump.pump(response.getDelegate(), request.response());
                                    pump.start();
                                    response.exceptionHandler(e -> pump.stop());
                                    response.endHandler(end -> request.response().end());
                                }
                            })
                            .putHeader(CONTENT_LENGTH, String.valueOf(Objects.nonNull(data) ? data.length : 0))
                            .write(Buffer.buffer(Objects.nonNull(data) ? data : new byte[0]))
                            .end();
                }));
        return res;
    }

    public static Mono<Boolean> forwardPutRequest(MsHttpRequest request, boolean toResponse, byte[] data) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        STORA_MANAGEMENT_SCHEDULER.schedule(() -> preMono(request, request.getBucketName())
                .subscribe(tuple3 -> {
                    int port = Integer.parseInt(tuple3.getT1());
                    boolean existBucket = tuple3.getT2();
                    if (!existBucket) {
                        res.onNext(false);
                        return;
                    }
                    HttpClientRequest forwardRequestForStored;
                    HttpClientRequest putRequest;
                    String targetIp = request.getMember(TARGET_IP_HEADER);
                    String uri = request.uri();
                    String signPath = RestfulVerticle.getSignPath(request.host(), request.path());
                    boolean isHost = !Objects.equals(signPath, request.path())
                            || ((request.getParam(EXPIRES) != null && request.getHeader(DATE) != null)
                            && !request.params().contains(AuthorizeV4.X_AMZ_SIGNATURE) && request.params().contains(SIGNATURE));
                    if (isHost) {
                        uri = getUri(request);
                    }
                    if (request.host().contains(":")){
                        String putIp = request.host().split(":")[0];
                        int putPort = 0;
                        try {
                            putPort = Integer.parseInt(request.host().split(":")[1]);
                        } catch (NumberFormatException e) {
                            throw new RuntimeException(e);
                        }
                        if (request.isSSL()){
                            putRequest = HTTPS_CLIENT.request(HttpMethod.PUT, putPort, LOCALHOST, signPath);
                        }else {
                            putRequest = HTTP_CLIENT.request(HttpMethod.PUT, putPort, LOCALHOST, signPath);
                        }
                    }else {
                        if (request.isSSL()){
                            putRequest = HTTPS_CLIENT.request(HttpMethod.PUT,443, LOCALHOST, signPath);
                        }else {
                            putRequest = HTTP_CLIENT.request(HttpMethod.PUT,80, LOCALHOST, signPath);
                        }
                    }
                    if (request.isSSL()) {
                        forwardRequestForStored = HTTPS_CLIENT.request(request.method(), port, targetIp, uri);
                    } else {
                        forwardRequestForStored = HTTP_CLIENT.request(request.method(), port, targetIp, uri);
                    }

                    //使用host访问路径改变，由于访问纳管是path方式，所以需要重新构造签名
                    if (!isHost) {
                        request.headers().forEach(entry -> {
                            if (!entry.getKey().equalsIgnoreCase(SYNC_STAMP) && !entry.getKey().equalsIgnoreCase(IS_SYNCING)) {
                                forwardRequestForStored.putHeader(entry.getKey(), entry.getValue());
                            }
                        });
                    } else {
                        preHeaders(request, forwardRequestForStored, tuple3, targetIp, false);
                    }

                    forwardRequestForStored.exceptionHandler(e -> {
                                res.onNext(false);
                                log.debug("the site is forward request for stored! the error is ", e);
                            })
                            .handler(response -> {
                                if (toResponse) {
                                    if (response.statusCode() == 200 || response.statusCode() == 206){
                                        //构造put请求
                                        if (Long.parseLong(response.getHeader(CONTENT_LENGTH)) > MAX_PUT_SIZE){
                                            // 1. 生成时间戳（秒级）
                                            long timestamp = Instant.now().getEpochSecond();
                                            // 2. 生成HMAC签名
                                            String signature = generateHmacSignature(timestamp);
                                            // 3. 构造请求头
                                            String requestHeaderValue = timestamp + ":" + signature;
                                            putRequest.putHeader(CONTENT_LENGTH, response.getHeader(CONTENT_LENGTH))
                                                    .putHeader(CONTENT_TYPE, response.getHeader(CONTENT_TYPE))
                                                    .putHeader(X_AMZ_STORE_MANAGEMENT_SERVER_HEADER,requestHeaderValue);

                                        }else {
                                            putRequest.putHeader(CONTENT_LENGTH, response.getHeader(CONTENT_LENGTH))
                                                    .putHeader(CONTENT_TYPE, response.getHeader(CONTENT_TYPE));
                                        }

                                        prePutHeaders(request, putRequest, tuple3, request.host(), putRequest.method(), targetIp);
                                        putRequest.exceptionHandler(e -> {
                                                    res.onNext(false);
                                                    putRequest.reset();
                                                    log.debug("put request error:{}", e);
                                                })
                                                .handler(putResponse -> {
                                                    res.onNext(true);
                                                    log.debug("the put request send success");
                                                    // 显式消费响应体
                                                    putResponse.bodyHandler(buffer -> {})
                                                            .endHandler(v -> {
                                                                log.debug("have consume the put body");
                                                            });
                                                    putRequest.end();
                                                });

                                        Pump pump = Pump.pump(response.getDelegate(), putRequest.getDelegate());
                                        pump.start();

                                        response.exceptionHandler(e -> {
                                            res.onNext(false);
                                            log.debug("response error:", e);
                                            pump.stop();
                                        });
                                        response.endHandler(end -> {
                                            log.debug("the get object request end");
                                        });
                                    }else {
                                        res.onNext(false);
                                        log.debug("the get object request return code is not 200 or 206");
                                    }
                                }
                            })
                            .putHeader(CONTENT_LENGTH, String.valueOf(Objects.nonNull(data) ? data.length : 0))
                            .write(Buffer.buffer(Objects.nonNull(data) ? data : new byte[0]))
                            .end();
                }));
        return res;
    }

    public static Mono<Boolean> forwardGetRequest(MsHttpRequest request, boolean toResponse, byte[] data) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        STORA_MANAGEMENT_SCHEDULER.schedule(() -> preMono(request, request.getBucketName())
                .subscribe(tuple3 -> {
                    int port = Integer.parseInt(tuple3.getT1());
                    boolean existBucket = tuple3.getT2();
                    if (!existBucket) {
                        res.onNext(false);
                        return;
                    }
                    HttpClientRequest forwardRequestForReturn;
                    HttpClientRequest forwardRequestForStored;
                    HttpClientRequest putRequest;
                    String targetIp = request.getMember(TARGET_IP_HEADER);
                    String uri = request.uri();
                    String signPath = RestfulVerticle.getSignPath(request.host(), request.path());
                    boolean isHost = !Objects.equals(signPath, request.path())
                            || ((request.getParam(EXPIRES) != null && request.getHeader(DATE) != null)
                            && !request.params().contains(AuthorizeV4.X_AMZ_SIGNATURE) && request.params().contains(SIGNATURE));
                    if (isHost) {
                        uri = getUri(request);
                    }
                    if (request.host().contains(":")){
                        String putIp = request.host().split(":")[0];
                        int putPort = 0;
                        try {
                            putPort = Integer.parseInt(request.host().split(":")[1]);
                        } catch (NumberFormatException e) {
                            throw new RuntimeException(e);
                        }
                        if (request.isSSL()){
                            putRequest = HTTPS_CLIENT.request(HttpMethod.PUT, putPort, LOCALHOST, signPath);
                        }else {
                            putRequest = HTTP_CLIENT.request(HttpMethod.PUT, putPort, LOCALHOST, signPath);
                        }
                    }else {
                        if (request.isSSL()){
                            putRequest = HTTPS_CLIENT.request(HttpMethod.PUT,443, LOCALHOST, signPath);
                        }else {
                            putRequest = HTTP_CLIENT.request(HttpMethod.PUT,80, LOCALHOST, signPath);
                        }
                    }
                    if (request.isSSL()) {
                        forwardRequestForReturn = HTTPS_CLIENT.request(request.method(), port, targetIp, uri);
                        forwardRequestForStored = HTTPS_CLIENT.request(request.method(), port, targetIp, uri);
                    } else {
                        forwardRequestForReturn = HTTP_CLIENT.request(request.method(), port, targetIp, uri);
                        forwardRequestForStored = HTTP_CLIENT.request(request.method(), port, targetIp, uri);
                    }

                    //使用host访问路径改变，由于访问纳管是path方式，所以需要重新构造签名
                    if (!isHost) {
                        request.headers().forEach(entry -> {
                            if (!entry.getKey().equalsIgnoreCase(SYNC_STAMP) && !entry.getKey().equalsIgnoreCase(IS_SYNCING)) {
                                forwardRequestForReturn.putHeader(entry.getKey(), entry.getValue());
                            }
                        });
                        request.headers().forEach(entry -> {
                            if (!entry.getKey().equalsIgnoreCase(SYNC_STAMP) && !entry.getKey().equalsIgnoreCase(IS_SYNCING)) {
                                forwardRequestForStored.putHeader(entry.getKey(), entry.getValue());
                            }
                        });
                    } else {
                        preHeaders(request, forwardRequestForReturn, tuple3, targetIp, false);
                        preHeaders(request, forwardRequestForStored, tuple3, targetIp, false);
                    }

                    forwardRequestForReturn.exceptionHandler(e -> {
                                log.debug("error:", e);
                                res.onNext(false);
                                if (!isConnectionClosed(e)) {
                                    ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                                }
                            })
                            .handler(response -> {
                                if (toResponse) {
                                    request.response().setStatusCode(response.statusCode());
                                    response.headers().forEach(header -> {
                                        request.response().putHeader(header.getKey(), header.getValue());
                                    });
                                    Pump pump = Pump.pump(response.getDelegate(), request.response());
                                    pump.start();
                                    response.exceptionHandler(e -> {
                                        pump.stop();
                                        log.debug("pump error",e);
                                    });
                                    response.endHandler(end -> {
                                        request.response().end();
                                    });
                                }
                            })
                            .putHeader(CONTENT_LENGTH, String.valueOf(Objects.nonNull(data) ? data.length : 0))
                            .write(Buffer.buffer(Objects.nonNull(data) ? data : new byte[0]))
                            .end();

                    forwardRequestForStored.exceptionHandler(e -> {
                                res.onNext(true);
                                log.debug("the site is forward request for stored! the error is ", e);
                            })
                            .handler(response -> {
                                if (toResponse) {
                                    if (response.statusCode() == 200 || response.statusCode() == 206){
                                        //构造put请求
                                        if (Long.parseLong(response.getHeader(CONTENT_LENGTH)) > MAX_PUT_SIZE){
                                            // 1. 生成时间戳（秒级）
                                            long timestamp = Instant.now().getEpochSecond();
                                            // 2. 生成HMAC签名
                                            String signature = generateHmacSignature(timestamp);
                                            // 3. 构造请求头
                                            String requestHeaderValue = timestamp + ":" + signature;
                                            putRequest.putHeader(CONTENT_LENGTH, response.getHeader(CONTENT_LENGTH))
                                                    .putHeader(CONTENT_TYPE, response.getHeader(CONTENT_TYPE))
                                                    .putHeader(X_AMZ_STORE_MANAGEMENT_SERVER_HEADER,requestHeaderValue);

                                        }else {
                                            putRequest.putHeader(CONTENT_LENGTH, response.getHeader(CONTENT_LENGTH))
                                                    .putHeader(CONTENT_TYPE, response.getHeader(CONTENT_TYPE));
                                        }

                                        prePutHeaders(request, putRequest, tuple3, request.host(), putRequest.method(), targetIp);
                                        putRequest.exceptionHandler(e -> {
                                                    res.onNext(true);
                                                    putRequest.reset();
                                                    log.debug("put request error:{}", e);
                                                })
                                                .handler(putResponse -> {
                                                    res.onNext(true);
                                                    log.debug("the put request send success");
                                                    // 显式消费响应体
                                                    putResponse.bodyHandler(buffer -> {})
                                                            .endHandler(v -> {
                                                                log.debug("have consume the put body");
                                                            });
                                                    putRequest.end();
                                                });

                                        Pump pump = Pump.pump(response.getDelegate(), putRequest.getDelegate());
                                        pump.start();

                                        response.exceptionHandler(e -> {
                                            res.onNext(true);
                                            log.debug("response error:", e);
                                            pump.stop();
                                        });
                                        response.endHandler(end -> {
                                            log.debug("the get object request end");
                                        });
                                    }else {
                                        res.onNext(true);
                                        log.debug("the get object request return code is not 200 or 206");
                                    }
                                }
                            })
                            .putHeader(CONTENT_LENGTH, String.valueOf(Objects.nonNull(data) ? data.length : 0))
                            .write(Buffer.buffer(Objects.nonNull(data) ? data : new byte[0]))
                            .end();
                }));
        return res;
    }



    public static void prePutHeaders(MsHttpRequest request, HttpClientRequest forwardRequest, Tuple3<String, Boolean, String> tuple3, String host,HttpMethod method, String targetIp){
        if (host.contains(":")){
            String[] ipAndPort = host.split(":");
            if (DEFAULT_HTTP_PORT.equals(ipAndPort[1]) || DEFAULT_HTTPS_PORT.equals(ipAndPort[1])){
                host = ipAndPort[0];
            }
        }
        if (checkTargetIpIsOtherClusterIp(targetIp)){
            forwardRequest.putHeader(BACK_SOURCE_REQUEST_HEADER, "true");
        }
        //添加SK
        request.addMember(SECRET_KEY, tuple3.getT3());
        String uri;
        // 多个/连续情况下sdk发请求时会对部分进行编码，需要解码一次再编码得到未对/编码的uri
        try {
            uri = UrlEncoder.encode(URLDecoder.decode(forwardRequest.uri(), "UTF-8"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            uri = forwardRequest.uri();
        }
        dealPutSignature(request, forwardRequest, host, uri, method, new TreeMap<>(), new TreeMap<>());
    }

    public static boolean checkTargetIpIsOtherClusterIp(String targetIp) {
        for (String ip : OTHER_CLUSTER_IP_SET){
            if (ip.contains(targetIp)){
                return true;
            }
        }
        return false;
    }

    public static void dealPutSignature(MsHttpRequest request, HttpClientRequest forwardRequest, String targetIp,
                                     String uri, HttpMethod method, TreeMap<String, String> headMap, TreeMap<String, String> queryMap) {
        String authorization = request.getHeader(AUTHORIZATION);
        if (authorization.startsWith(AuthorizeFactory.SHA256)) {
            buildV4Request(request, forwardRequest, queryMap, headMap, uri, method, targetIp);
        } else if (authorization.startsWith(AuthorizeFactory.AWS)) {
            buildV2PutRequest(request, forwardRequest, request.getBucketName(), request.getObjectName(), queryMap, headMap, uri, method, false);
        } else {
            log.error("authorization type not supported!");
        }
    }

    public static HttpClientRequest buildV2PutRequest(MsHttpRequest request, HttpClientRequest forwardRequest, String bucketName, String objectName,
                                                   TreeMap<String, String> resources, Map<String, String> map, String uri, HttpMethod method, boolean isV2) {
        String date = MsDateUtils.nowToGMT();
        AWSV2Auth awsv2Auth = new AWSV2Auth.Builder(request.getAccessKey(), request.getMember(SECRET_KEY))
                .HTTPVerb(method.name())
                .ContentMD5(Objects.nonNull(forwardRequest.headers().get(CONTENT_MD5)) ? forwardRequest.headers().get(CONTENT_MD5) : "")
                .ContentType(Objects.nonNull(forwardRequest.headers().get(CONTENT_TYPE)) ? forwardRequest.headers().get(CONTENT_TYPE) : "")
                .Date(date)
                .CanonicalizedHeaders(getCanonicalizedHeaders(map))
                .CanonicalizedResources(uri)
                .build();
        String authorization = awsv2Auth.getAuthorization();
        forwardRequest.putHeader(AUTHORIZATION, authorization)
                .putHeader("Date", date);
        return forwardRequest;
    }

    public static Mono<Tuple2<Boolean, String>> newForwardHead(MsHttpRequest request, String objName) {
        MonoProcessor<Tuple2<Boolean, String>> res = MonoProcessor.create();
        STORA_MANAGEMENT_SCHEDULER.schedule(() -> preMono(request, request.getBucketName())
                .subscribe(tuple3 -> {
                    int port = Integer.parseInt(tuple3.getT1());
                    boolean existBucket = tuple3.getT2();
                    if (!existBucket) {
                        res.onNext(new Tuple2<>(false, ""));
                        return;
                    }
                    String targetIp = request.getMember(TARGET_IP_HEADER);
                    HttpClientRequest forwardRequest;
                    String uri = SLASH + request.getBucketName() + SLASH + objName;
                    if (request.isSSL()) {
                        forwardRequest = HTTPS_CLIENT.request(HttpMethod.HEAD, port, targetIp, uri);
                    } else {
                        forwardRequest = HTTP_CLIENT.request(HttpMethod.HEAD, port, targetIp, uri);
                    }
                    request.addMember(SECRET_KEY, tuple3.getT3());
                    dealSignature(request, forwardRequest, targetIp, true, uri, HttpMethod.HEAD, new TreeMap<>(), new TreeMap<>());
                    forwardRequest.exceptionHandler(e -> {
                                if (!isConnectionClosed(e)) {
                                    ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                                }
                            })
                            .handler(response -> response.bodyHandler(buf -> {
                                if (response.getHeader(X_AMX_VERSION_ID) != null) {
                                    res.onNext(new Tuple2<>(true, response.getHeader(X_AMX_VERSION_ID)));
                                    return;
                                }
                                res.onNext(new Tuple2<>(false, new String(buf.getBytes())));
                            })).end();
                }));
        return res;
    }

    public static Mono<byte[]> forwardRequestListObject(MsHttpRequest request, String realValue) {
        MonoProcessor<byte[]> res = MonoProcessor.create();
        STORA_MANAGEMENT_SCHEDULER.schedule(() -> preMono(request, request.getBucketName())
                .subscribe(tuple3 -> {
                    int port = Integer.parseInt(tuple3.getT1());
                    boolean existBucket = tuple3.getT2();
                    if (!existBucket) {
                        res.onNext(new byte[0]);
                        return;
                    }
                    String targetIp = request.getMember(TARGET_IP_HEADER);
                    String uri = request.uri();
                    String signPath = RestfulVerticle.getSignPath(request.host(), request.path());
                    boolean isHost = !Objects.equals(signPath, request.path());
                    if (isHost) {
                        uri = getUri(request);
                    }
                    String newUri = uri;
                    if(request.params().contains(LIST_TYPE)){
                        StringBuilder result = new StringBuilder();
                        String[] params = uri.split("&");
                        String marker = "";
                        for (String param : params) {
                            if (!param.contains(LIST_TYPE + "=") && !param.contains(FETCH_OWNER + "=") && !param.contains(CONTINUATION_TOKEN + "=") && !param.contains(START_AFTER + "=")) {
                                if (result.length() > 0) {
                                    result.append("&");
                                }
                                result.append(param);
                            }
                            if (param.contains(CONTINUATION_TOKEN)){
                                marker = realValue;
                            }
                            if (param.contains(START_AFTER)){
                                if (marker.compareTo(param.substring(param.indexOf("=") + 1)) <= 0){
                                    marker = param.substring(param.indexOf("=") + 1);
                                }
                            }
                        }
                        if (StringUtils.isNotBlank(marker)) {
                            if (result.length() > 0) {
                                result.append("&");
                            }
                            result.append(MARKER + "=").append(marker);
                            request.params().add(MARKER, marker);
                        }
                        String[] strings = uri.split("\\?")[0].split(SLASH);
                        newUri  = SLASH + strings[1] + "?" + result;
                    }
                    log.debug("uri: {}", newUri);
                    HttpClientRequest forwardRequest;
                    if (request.isSSL()) {
                        forwardRequest = HTTPS_CLIENT.request(request.method(), port, targetIp, newUri);
                    } else {
                        forwardRequest = HTTP_CLIENT.request(request.method(), port, targetIp, newUri);
                    }

                    boolean isTrue = isHost || request.params().contains(LIST_TYPE);
                    if (isTrue) {
                        preHeaders(request, forwardRequest, tuple3, targetIp, true);
                    } else {
                        request.headers().forEach(entry -> forwardRequest.putHeader(entry.getKey(), entry.getValue()));
                    }
                    forwardRequest.exceptionHandler(e -> {
                                if (!isConnectionClosed(e)) {
                                    ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                                }
                            })
                            .handler(response -> response.bodyHandler(buf -> res.onNext(buf.getBytes()))).end();
                }));
        return res;
    }

    private static void preHeaders(MsHttpRequest request, HttpClientRequest forwardRequest, Tuple3<String, Boolean, String> tuple3, String targetIp, Boolean flag) {
        for (Map.Entry<String, String> entry : request.headers()) {
            if (!entry.getKey().equalsIgnoreCase(AUTHORIZATION) && !entry.getKey().equalsIgnoreCase(HOST)
                    && !entry.getKey().equalsIgnoreCase(X_AMZ_CONTENT_SHA_256) && !entry.getKey().equalsIgnoreCase(X_AMZ_DATE)
                    && !entry.getKey().equalsIgnoreCase(SYNC_STAMP) && !entry.getKey().equalsIgnoreCase(IS_SYNCING)) {
                forwardRequest.putHeader(entry.getKey(), entry.getValue());
            }
        }
        request.addMember(SECRET_KEY, tuple3.getT3());
        dealSignature(request, forwardRequest, targetIp, flag);
    }

    public static String getUri(MsHttpRequest request) {
        String uri = SLASH;
        try {
            String requestUri = request.uri();
            int paramIndex = requestUri.indexOf("?");
            String params = "";
            if (paramIndex != -1) {
                params = requestUri.substring(paramIndex);
            }
            if (StringUtils.isNotBlank(request.getBucketName())) {
                uri +=URLEncoder.encode(request.getBucketName(), StandardCharsets.UTF_8.name())
                        .replace("+", "%20");
            }
            if (StringUtils.isNotBlank(request.getObjectName())) {
                uri += SLASH + URLEncoder.encode(request.getObjectName(), StandardCharsets.UTF_8.name())
                        .replace("+", "%20");
            }
            return uri + params;
        } catch (UnsupportedEncodingException e) {
            log.info("storeManagement getURI error,{}",e.getMessage(),e);
        }
        return uri;
    }

    public static void dealSignature(MsHttpRequest request, HttpClientRequest forwardRequest, String targetIp, boolean flag) {
        TreeMap<String, String> headMap = new TreeMap<>();
        List<Map.Entry<String, String>> entries = request.headers().entries();
        entries.forEach(stringEntry -> {
            if (stringEntry.getKey().toLowerCase().startsWith("x-amz-") &&
                    !stringEntry.getKey().equalsIgnoreCase(X_AMZ_CONTENT_SHA_256)
                    && !stringEntry.getKey().equalsIgnoreCase(X_AMZ_DATE)) {
                headMap.put(stringEntry.getKey(), stringEntry.getValue());
            }
        });
        TreeMap<String, String> queryMap = new TreeMap<>();
        boolean isV2 = false;
        if (request.params().contains(LIST_TYPE)) {
            isV2 = true;
            Set<String> keysToRemove = new HashSet<>();
            request.params().forEach(key -> {
                if (key.getKey().contains(LIST_TYPE) || key.getKey().contains(FETCH_OWNER) || key.getKey().contains(CONTINUATION_TOKEN) || key.getKey().contains(START_AFTER)) {
                    keysToRemove.add(key.getKey());
                }
            });
            keysToRemove.forEach(request.params()::remove);
        }
        request.params().forEach(entry -> queryMap.put(entry.getKey(), entry.getValue()));
        String authorization = request.getHeader(AUTHORIZATION);
        // 前端包host访问使用v2签名时会报签名错误，到达不了当前代码,暂未验证v2
        if (authorization.startsWith(AuthorizeFactory.SHA256)) {
            String canonicalURI = SLASH;
            if (StringUtils.isNotBlank(request.getBucketName())) {
//                if (isV2){
//                    canonicalURI += request.getBucketName() + SLASH;
//                } else {
                    canonicalURI += request.getBucketName();
//                }
            }

            if (StringUtils.isNotBlank(request.getObjectName())) {
                canonicalURI += SLASH + request.getObjectName();
            }
            buildV4Request(request, forwardRequest, queryMap, headMap, canonicalURI, request.method(), targetIp);
        } else if (authorization.startsWith(AuthorizeFactory.AWS)) {
            TreeMap<String, String> queryMap1 = queryMap;
            if (flag) {
                queryMap1 = new TreeMap<>();
            }
            buildV2Request(request, forwardRequest, request.getBucketName(), request.getObjectName(), queryMap1, headMap, request.method(), isV2, false);
        } else {
            log.error("authorization type not supported!");
        }
    }

    public static void dealSignature(MsHttpRequest request, HttpClientRequest forwardRequest, String targetIp, boolean flag,
                                     String uri, HttpMethod method, TreeMap<String, String> headMap, TreeMap<String, String> queryMap) {
        String authorization = request.getHeader(AUTHORIZATION);
        if (authorization.startsWith(AuthorizeFactory.SHA256)) {
            String[] split = uri.split("\\?");
            String canonicalURI = split[0];
            buildV4Request(request, forwardRequest, queryMap, headMap, canonicalURI, method, targetIp);
        } else if (authorization.startsWith(AuthorizeFactory.AWS)) {
            TreeMap<String, String> queryMap1 = queryMap;
            if (flag) {
                queryMap1 = new TreeMap<>();
            }
            buildV2Request(request, forwardRequest, request.getBucketName(), request.getObjectName(), queryMap1, headMap, method, false, true);
        } else {
            log.error("authorization type not supported!");
        }
    }

    public static HttpClientRequest buildV2Request(MsHttpRequest request, HttpClientRequest forwardRequest, String bucketName, String objectName,
                                                   TreeMap<String, String> resources, Map<String, String> map, HttpMethod method, boolean isV2, boolean isNew) {
        String date = MsDateUtils.nowToGMT();
        String canonicalizedResources = getCanonicalizedResources(bucketName, objectName, resources, isV2, request);
        if (!isNew && getUri(request).contains("?delete")) {
            canonicalizedResources = getUri(request);
        }
        AWSV2Auth awsv2Auth = new AWSV2Auth.Builder(request.getAccessKey(), request.getMember(SECRET_KEY))
                .HTTPVerb(method.name())
                .ContentMD5(!isNew && Objects.nonNull(request.getHeader(CONTENT_MD5)) ? request.getHeader(CONTENT_MD5) : "")
                .ContentType(!isNew && Objects.nonNull(request.getHeader(CONTENT_TYPE)) ? request.getHeader(CONTENT_TYPE) : "")
                .Date(date)
                .CanonicalizedHeaders(getCanonicalizedHeaders(map))
                .CanonicalizedResources(canonicalizedResources)
                .build();
        String authorization = awsv2Auth.getAuthorization();
        forwardRequest.putHeader(AUTHORIZATION, authorization)
                .putHeader("Date", date)
                .exceptionHandler(e -> {
                    log.error("{}", e.getMessage());
                    if (!isConnectionClosed(e)) {
                        ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                    }
                });
        return forwardRequest;
    }

    public static HttpClientRequest buildV4Request(MsHttpRequest request, HttpClientRequest forwardRequest, TreeMap<String, String> queryMap,
                                                   TreeMap<String, String> headMap, String canonicalURI, HttpMethod method, String ip) {

        String date = AWSV4Auth.getTimeStamp();
        headMap.put(HOST.toLowerCase(), ip);
        headMap.put(X_AMZ_DATE, date);
        headMap.put(X_AMZ_CONTENT_SHA_256, "UNSIGNED-PAYLOAD");
        AWSV4Auth v4 = new AWSV4Auth.Builder(request.getAccessKey(), request.getMember(SECRET_KEY))
                .httpMethodName(method.name())
                .regionName(ServerConfig.getInstance().getRegion())
                .serviceName(SERVICE_S3)
                .queryParametes(queryMap)
                .canonicalURI(canonicalURI)
                .awsHeaders(headMap)
                .xAmzDate(date)
                .toChunked(false)
                .build();
        String authorization = v4.getHeaders().get(AUTHORIZATION);
        forwardRequest.putHeader(AUTHORIZATION, authorization)
                .putHeader(X_AMZ_CONTENT_SHA_256, "UNSIGNED-PAYLOAD")
                .putHeader(X_AMZ_DATE, date)
                .putHeader(HOST.toLowerCase(), ip)
                .exceptionHandler(e -> {
                    log.error("{}", e.getMessage());
                    if (!isConnectionClosed(e)) {
                        ResponseUtils.responseError(request, FORWARD_ERROR_CODE);
                    }
                });
        return forwardRequest;
    }

    public static Mono<Tuple3<String, Boolean, String>> preMono(MsHttpRequest request, String bucketName) {
        String accessKey = request.getAccessKey();
        return Mono.zip(Mono.just(request.isSSL())
                        .flatMap(b -> b ? pool.getReactive(REDIS_USERINFO_INDEX).hget(accessKey, HTTPS_PORT).defaultIfEmpty("443")
                                : pool.getReactive(REDIS_USERINFO_INDEX).hget(accessKey, HTTP_PORT).defaultIfEmpty("80")),
                pool.getReactive(REDIS_USERINFO_INDEX).hget(request.getUserId(), BUCKETS)
                        .flatMap(bucketJson -> Mono.just(Arrays.asList(JSONObject.parseObject(bucketJson, String[].class))))
                        .flatMap(buckets -> Mono.just(buckets.contains(bucketName))),
                pool.getReactive(REDIS_USERINFO_INDEX).hget(request.getAccessKey(), SECRET_KEY)
        );
    }

    private static boolean isConnectionClosed(Throwable e) {
        return e.getMessage() != null && e.getMessage().contains("Connection was closed");
    }


    private static class RouteSet {
        private static final String PUT = "PUT///";
        private static final String GET = "GET///";
        private static final String DELETE = "DELETE///";
        private static final String MUITI_DELETE = "POST//?delete";

        private static final String PART_INIT = "POST///?uploads";
        private static final String PART_UPLOAD = "PUT///?partNumber&uploadId";
        private static final String PART_MERGE = "GET///?uploadId";

        private static final String GET_OBJECT_NUM = "GET/?ObjectStatistics";
        private static final String GET_USED_CAPACITY = "GET/?account-name&accountusedcapacity";
    }
}