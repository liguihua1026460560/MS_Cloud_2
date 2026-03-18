package com.macrosan.doubleActive;

import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.utils.authorize.AuthorizeV2;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.http.HttpClient;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.HeartBeatChecker.TIMEOUT_RECHECK_IP_SET;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.utils.authorize.AuthorizeV4.*;

@Data
@Accessors(chain = true)
@Log4j2
public class PoolingRequest {
    /**
     * 指定发起该请求的httpClient
     */
    @NonNull
    HttpClient httpClient;
    /**
     * http请求类型
     */
    @NonNull
    HttpMethod method;
    /**
     * 请求送往的ip。
     */
    @NonNull
    String ip;
    /**
     * 目标站点的索引。若ip不为null，该变量无效。
     */
    Integer clusterIndex = 0;
    /**
     * 要访问的接口的uri
     */
    String uri;
    /**
     * 控制连接重试。每次接收到1都将重新创建轮询的连接
     */
    UnicastProcessor<Integer> retryProcessor;
//        DirectProcessor<Integer> retryProcessor = DirectProcessor.create();
    /**
     * 热发布，可同时接收多个subscriber，用于链式发起请求后重试。
     */
    Flux<Integer> retryHotFlux;
    /**
     * 记录重连次数
     */
    AtomicInteger tryNum = new AtomicInteger();
    /**
     * 用于签名。record为空时使用
     */
    String bucket;
    /**
     * 用于签名。record为空时使用
     */
    String object;
    /**
     * 用于签名。record为空时使用
     */
    String authString;

    public PoolingRequest() {
    }

    public PoolingRequest(HttpMethod method, Integer clusterIndex, String uri, HttpClient client) {
        setMethod(method).setClusterIndex(clusterIndex).setUri(uri).setHttpClient(client);
    }

    public PoolingRequest(HttpMethod method, String ip, String uri) {
        setMethod(method).setIp(ip).setUri(uri).setHttpClient(MossHttpClient.getClient());
    }

    /**
     * true表示需要轮询所有ip，不考虑其是否可用
     */
    private boolean isEntire = false;

    private AtomicInteger ipIndexCur = new AtomicInteger(-1);

    /**
     * 访问指定ip或轮询指定站点下的ip，生成http请求。
     * retryProcessor.onNext(1)可实现轮询。
     */
    public Flux<MsClientRequest> request() {
        return request(null);
    }

    public DataSyncSignHandler signHandler;

    public Mono<MsClientRequest> request0(UnSynchronizedRecord record) {
        String[] ips;
        if (record != null) {
            ips = INDEX_IPS_MAP.get(this.clusterIndex);
        } else {
            isEntire = true;
            ips = INDEX_IPS_ENTIRE_MAP.get(this.clusterIndex);
        }
        int ipIndex = ThreadLocalRandom.current().nextInt(0, ips.length);
        String ip;
        if (this.ip != null) {
            ip = this.ip;
        } else {
            if (ipIndex >= 0) {
                ip = ips[ipIndex];
            } else {
                throw new RuntimeException("try " + this.tryNum.get() + " times. send sync request to {} failed.");
            }
        }
        boolean isExtra = EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(this.clusterIndex);

        MsClientRequest request = new MsClientRequest(this.httpClient.request(this.method, isExtra ? EXTRA_PORT : DA_PORT, ip, this.uri)
                //设置10s可能导致限制流量时一直超时
                .setTimeout(30_000)
                .setHost(ip + ":" + (isExtra ? EXTRA_PORT : DA_PORT)));
        request.putHeader(SYNC_AUTH, PASSWORD);
        record.headers.entrySet().stream()
                .filter(entry -> !escapeHeaders.contains(entry.getKey().toLowerCase()))
                .forEach(entry -> request.putHeader(entry.getKey(), entry.getValue()));
        if (isExtra) {
            String authString = "AWS " + EXTRA_AK_SK_MAP.get(record.bucket).var1() + ":0000";
            record.headers.put(AUTHORIZATION, authString);
            request.headers().remove(IS_SYNCING);
        }
        request.putHeader(HOST, ip);
        return Mono.just(request);
    }

    /**
     * 所有请求头都put完毕,再调用此方法算签名。V4单块校验不在此计算
     */
    public Mono<Boolean> sign(UnSynchronizedRecord record, MsClientRequest request, boolean isExtra) {
        signHandler = new DataSyncSignHandler(record.bucket, record.index, request);
        return signHandler.getSignType()
                .flatMap(signType -> {
                    switch (signType) {
                        case AWS_V2:
                            return AuthorizeV2.getAuthorizationHeader(record, request)
                                    .doOnNext(authHeader -> DoubleActiveUtil.encodeAmzHeaders(request))
                                    .map(authHeader -> {
                                        request.putHeader(AUTHORIZATION, authHeader);
                                        return true;
                                    });
                        case AWS_V4_NONE:
                            String payloadHash = "";
                            if ("0".equals(record.headers.get(CONTENT_LENGTH))) {
                                payloadHash = EMPTY_SHA256;
                            }
                            return AuthorizeV4.getAuthorizationHeader(record.bucket, record.headers.get(AUTHORIZATION), request, payloadHash, request.headers().contains(IS_SYNCING), isExtra)
                                    .doOnNext(authHeader -> DoubleActiveUtil.encodeAmzHeaders(request))
                                    .map(authHeader -> {
                                        request.putHeader(AUTHORIZATION, authHeader);
                                        return true;
                                    });
                        case AWS_V4_SINGLE:
                            return Mono.just(true);
                        case AWS_V4_CHUNK:
                            // 暂不支持
                        default:
                            log.error("no such signType. {} {}", bucket, signType);
                            throw new RuntimeException("no such signType");
                    }
                });
    }

    public Flux<MsClientRequest> request(UnSynchronizedRecord record) {
        retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        retryHotFlux = retryProcessor.publish().autoConnect();

        String[] ips;
        if (record != null) {
            ips = INDEX_IPS_MAP.get(this.clusterIndex);
        } else {
            isEntire = true;
            ips = INDEX_IPS_ENTIRE_MAP.get(this.clusterIndex);
        }
        boolean isExtra = EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(this.clusterIndex);
        //遍历目标站点ip
        Flux<MsClientRequest> map = this.retryHotFlux.publishOn(SCAN_SCHEDULER).map(count -> {
//            log.info("----------{} ,try: {}", this.method.name(), this.tryNum.get());
            int ipIndex;
            if (ipIndexCur.get() == -1) {
                ipIndex = ThreadLocalRandom.current().nextInt(0, ips.length);
                ipIndexCur.set(ipIndex);
            } else {
                ipIndex = ipIndexCur.addAndGet(count) % ips.length;
            }

            if (this.tryNum.addAndGet(count) <= ips.length) {
                return ipIndex;
            } else {
                this.retryProcessor.onComplete();
                return -1;
            }
        }).map(ipIndex -> {
            String ip;
            if (this.ip != null) {
                ip = this.ip;
            } else {
                if (ipIndex >= 0) {
                    ip = ips[ipIndex];
                } else {
                    throw new RuntimeException("try " + this.tryNum.get() + " times. send sync request to {} failed.");
                }
            }
//            log.info("create {}, ip: {}", this.method, ip);
            return new MsClientRequest(this.httpClient.request(this.method, isExtra ? EXTRA_PORT : DA_PORT, ip, this.uri)
                    //设置10s可能导致限制流量时一直超时
                    .setTimeout(30_000)
                    .setHost(ip + ":" + (isExtra ? EXTRA_PORT : DA_PORT)));
        }).flatMap(request -> {
            request.putHeader(SYNC_AUTH, PASSWORD);
            if (record != null) {
                record.headers.entrySet().stream()
                        .filter(entry -> !escapeHeaders.contains(entry.getKey().toLowerCase()))
                        .forEach(entry -> request.putHeader(entry.getKey(), entry.getValue()));
                if (isExtra) {
                    String authString = "AWS " + EXTRA_AK_SK_MAP.get(record.bucket).var1 + ":0000";
                    record.headers.put(AUTHORIZATION, authString);
                    request.headers().remove(IS_SYNCING);
                }
                return AuthorizeV2.getAuthorizationHeader(record, request).zipWith(Mono.just(request));
            } else {
                return AuthorizeV2.getAuthorizationHeader(this.bucket, this.object, this.authString, request, isExtra).zipWith(Mono.just(request));
            }
        }).map(tuple2 -> {
            if (record != null && StringUtils.isNotBlank(record.headers.get(AUTHORIZATION)) && StringUtils.isNotEmpty(tuple2.getT1())) {
                tuple2.getT2().putHeader(AUTHORIZATION, tuple2.getT1());
            }
            return tuple2.getT2();
        });


        this.retryProcessor.onNext(1);
        return map;
    }

    public MsClientRequest request1(int port) {
        String[] ips;
        isEntire = true;
        ips = INDEX_IPS_ENTIRE_MAP.get(this.clusterIndex);
        int ipIndex = ThreadLocalRandom.current().nextInt(0, ips.length);
        String ip;
        if (ipIndex >= 0) {
            ip = ips[ipIndex];
        } else {
            throw new RuntimeException("try " + this.tryNum.get() + " times. send sync request to {} failed.");
        }

        return new MsClientRequest(this.httpClient.request(this.method, port, ip, this.uri)
                .setTimeout(30_000)
                .setHost(ip + ":" + port));
    }

    public final static Map<Integer, AtomicInteger> IP_COUNT_MAP = new HashMap<>();

    private int unitCount;


    // todo f 节点很多且总超时时间超过遍历时间可能存在问题。比如15个节点，前14个都是1s超时，最后一个正常。
    public Flux<MsClientRequest> requestHeartBeat(String[] ips, long timeoutMillis, int unitCount) {
        retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        retryHotFlux = retryProcessor.publish().autoConnect();
        this.unitCount = unitCount;
        Flux<MsClientRequest> map = this.retryHotFlux.publishOn(SCAN_SCHEDULER).map(i -> {
            AtomicInteger count1 = IP_COUNT_MAP.computeIfAbsent(this.clusterIndex, k -> new AtomicInteger());
            Tuple2<String, Long> tuple2_ = DoubleActiveUtil.getRequestTimeout(ips, timeoutMillis, unitCount, count1);
            String ip = tuple2_.var1;
            Long timeout = tuple2_.var2;

            tryNum.incrementAndGet();
//            log.info("create {}, ip: {}", this.method, ip);
            return new MsClientRequest(this.httpClient.request(this.method, DA_PORT, ip, this.uri)
                    //设置10s可能导致限制流量时一直超时
                    .setTimeout(timeout)
                    .setHost(ip + ":" + DA_PORT));
        });

        this.retryProcessor.onNext(1);
        return map;
    }

    public boolean needRetry() {
        return tryNum.get() < unitCount;
    }

    public static Set<String> escapeHeaders;

    static {
        escapeHeaders = new HashSet<>();
        escapeHeaders.add(X_AMZ_DATE.toLowerCase());
        escapeHeaders.add(X_AMZ_CONTENT_SHA_256.toLowerCase());
        escapeHeaders.add("X-Amz-Algorithm".toLowerCase());
        escapeHeaders.add(X_AMZ_SIGNATURE.toLowerCase());
        escapeHeaders.add("X-Amz-Credential".toLowerCase());
        escapeHeaders.add("X-Amz-SignedHeaders".toLowerCase());
        escapeHeaders.add("X-Amz-Expires".toLowerCase());
        escapeHeaders.add("syncHistory".toLowerCase());
    }

}
