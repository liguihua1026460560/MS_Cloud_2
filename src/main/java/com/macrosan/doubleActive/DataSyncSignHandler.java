package com.macrosan.doubleActive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.utils.authorize.AuthorizeV4;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.function.Function;

import static com.macrosan.constants.ServerConstants.AUTHORIZATION;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;

@Log4j2
public class DataSyncSignHandler {

    public static RedisConnPool pool = RedisConnPool.getInstance();
    public static final String BUCKET_SIGN_TYPE = "bucket_sign_type";

    @AllArgsConstructor
    public enum SignType {
        AWS_V2(0),
        AWS_V4_NONE(1),
        AWS_V4_SINGLE(2),
        AWS_V4_CHUNK(3);

        public int code;

        public static SignType parse(int i) {
            for (SignType value : values()) {
                if (value.code == i) {
                    return value;
                }
            }
            throw new RuntimeException("no such SignType");
        }
    }

    public String bucket;
    public Integer toIndex;
    public SignType signType;
    public HttpClientRequest request;
    private MessageDigest sha256;

    public DataSyncSignHandler(String bucket, int clusterIndex, HttpClientRequest request) {
        this.bucket = bucket;
        this.toIndex = clusterIndex;
        this.request = request;
    }

    /**
     * 桶为单位指定往index站点的签名格式。
     * 表7中数据结构为map[srcIndex-dstIndex, SignType.code]
     */
    public Mono<SignType> getSignType() {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_SIGN_TYPE)
                .defaultIfEmpty("")
                .map(s -> {
                    if (StringUtils.isBlank(s)) {
                        this.signType = SignType.AWS_V2;
                    } else {
                        HashMap<String, Integer> hashMap = Json.decodeValue(s, new TypeReference<HashMap<String, Integer>>() {
                        });
                        Integer signTypeCode = hashMap.get(LOCAL_CLUSTER_INDEX + "-" + toIndex);
                        if (signTypeCode == null) {
                            this.signType = SignType.AWS_V2;
                        } else {
                            this.signType = SignType.parse(signTypeCode);
                        }
                    }
                    return this.signType;
                })
                .doOnNext(signType1 -> {
                    if (signType1 != SignType.AWS_V2) {
                        try {
                            sha256 = MessageDigest.getInstance("SHA-256");
                        } catch (NoSuchAlgorithmException e) {
                            log.error("", e);
                            throw new RuntimeException("no s uch SignType");
                        }
                    }
                });
    }

    // v4单块校验先计算整个对象的sha256
    public Mono<Boolean> singleCheckSHA256Flux(String authString, Flux<byte[]> flux, Function<Void, Boolean> function, boolean defaultAk, boolean isExtra) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        flux.publishOn(DataSynChecker.SCAN_SCHEDULER)
                .timeout(Duration.ofSeconds(30))
                .doOnNext(bytes -> {
                    sha256.update(bytes);
                    function.apply(null);
                })
                .doOnComplete(() -> {
                    String contentSHA256 = Hex.encodeHexString(sha256.digest());
                    AuthorizeV4.getAuthorizationHeader(bucket, authString, request, contentSHA256, defaultAk, isExtra)
                            .doOnNext(authHeader -> DoubleActiveUtil.encodeAmzHeaders(request))
                            .subscribe(authHeader -> {
                                request.putHeader(AUTHORIZATION, authHeader);
                                res.onNext(true);
                            }, e -> {
                                log.error("singleCheckSHA256 err1, ", e);
                                res.onError(e);
                            });
                })
                .doOnError(e -> {
                    log.error("singleCheckSHA256 err2, ", e);
                    res.onError(e);
                })
                .doFinally(s -> sha256.reset())
                .subscribe();
        return res;
    }

    public Mono<String> singleCheckSHA256(String authString, byte[] bytes, boolean defaultAk, boolean isExtra) {
        String contentSHA256 = DigestUtils.sha256Hex(bytes);
        return AuthorizeV4.getAuthorizationHeader(bucket, authString, request, contentSHA256, defaultAk, isExtra);
    }

}
