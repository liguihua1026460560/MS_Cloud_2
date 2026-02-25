package com.macrosan.utils.perf;

import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.SampleConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Fuseable;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

/**
 * 基础的限流类
 *
 * @author gaozhiyuan
 * @date 2019.10.25
 */
public abstract class BasePerfLimiter {
    protected static final Logger logger = LogManager.getLogger(BasePerfLimiter.class);

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    protected static Redis6380ConnPool iamPool = Redis6380ConnPool.getInstance();

    private static final int BUCKET_SCALING = 10;

    /**
     * 生产令牌，更新redis令牌桶
     *
     * @param commands 与redis表的连接
     * @param key      要更新的key
     * @param type     要更新的quota类型
     * @param addValue 缓存中的需要添加的令牌数
     * @param quota    账户配额大小
     * @return Long ，后转为true
     */
    private Mono<Long> produceToken(RedisReactiveCommands<String, String> commands, String key, String type, long addValue, long quota) {
        return commands.watch(key)
                .flatMapMany(s -> commands.time())
                .collectList()
                .map(timeList -> timeList.get(0) + timeList.get(1))
                .map(str -> str + '0' * (16 - str.length()))
                .map(str -> Long.parseLong(str) / 10000)
                .flatMap(currentTime ->
                        commands.hget(key, type).map(Long::parseLong).flatMap(nowToken ->
                                commands.hget(key, "last_modified_" + type)
                                        .map(Long::parseLong)
                                        .flatMap(lastModified -> {
                                            if (currentTime - lastModified < 100) {
                                                return commands.hincrby(key, type, addValue);
                                            } else {
                                                long producedToken = (currentTime - lastModified) * quota / 1000;
                                                long shouldAdd;
                                                if (nowToken + producedToken + addValue > quota * BUCKET_SCALING || producedToken < 0) {
                                                    shouldAdd = quota * BUCKET_SCALING - nowToken;
                                                } else {
                                                    shouldAdd = producedToken + addValue;
                                                }

                                                return commands.multi()
                                                        .doOnSuccess(s -> {
                                                            commands.hincrby(key, type, shouldAdd).subscribe();
                                                            commands.hset(key, "last_modified_" + type, String.valueOf(currentTime)).subscribe();
                                                        }).flatMap(s -> commands.exec())
                                                        .flatMap(transactionResult -> {
                                                            if (transactionResult.wasDiscarded()) {
                                                                return produceToken(commands, key, type, addValue, quota);
                                                            } else {
                                                                return Mono.just(0L);
                                                            }
                                                        });
                                            }
                                        }))
                );
    }

    private Mono<Long> getTokenHandler(String k) {
        String[] tmp = k.split("_token");
        String key = tmp[0] + "_token";
        String type = tmp[1];
        return iamPool.getThreadLocalConnReactive().hget(key, type).map(Long::parseLong);
    }

    /**
     * k为空时的处理（如初始化令牌桶）
     *
     * @param k key
     * @return Mono
     */
    private Mono<Long> getEmptyTokenHandler(String k) {
        String[] tmp = k.split("_token");
        String key = tmp[0] + "_token";
        String type = tmp[1];
        Map<String, String> keyMap = new HashMap<>(2);
        keyMap.put(type, "0");
        keyMap.put("last_modified_" + type, "0");
        return iamPool.getThreadLocalConnReactive().hmset(key, keyMap).map(l -> 0L);
    }

    private Mono<Boolean> updateTokenHandler(SampleConnection connection, String k, long addValue) {
        String[] tmp = k.split("_token");
        String key = tmp[0] + "_token";
        String type = tmp[1];

        return getQuotaHandler(tmp[0], type)
                .defaultIfEmpty(0L)
                .flatMap(quota -> produceToken(connection.reactive(), key, type, addValue, quota))
                .map(l -> true);
    }

    /**
     * 获得性能配额的方法，不同类型的限流获得性能配额方法不同
     *
     * @param key   性能配额的key
     * @param value 性能配额的类型，带宽或吞吐量
     * @return quota
     */
    protected abstract Mono<Long> getQuotaHandler(String key, String value);

    /**
     * 缓存令牌信息
     */
    private LongCacheMap tokenMap;

    /**
     * 缓存性能配额信息
     */
    @Getter
    private LongCacheMap quotaMap;

    public void init() {
        tokenMap = new LongCacheMap(1000,
                this::getTokenHandler,
                this::getEmptyTokenHandler,
                this::updateTokenHandler);

        quotaMap = new LongCacheMap(5000,
                k -> {
                    String[] tmp = k.split("_token");
                    String key = tmp[0];
                    String type = tmp[1];
                    return getQuotaHandler(key, type);
                },
                k -> Mono.just(0L),
                (c, k, v) -> Mono.just(true));
    }

    public Mono<Long> getQuota(String key, String type) {
        if (null == key) {
            return Mono.just(0L);
        }

        String mapKey = key + "_token" + type;

        return quotaMap.get(mapKey).onErrorReturn(0L);
    }

    public Mono<Long> limits(String key, String type, long token) {
        if (null == key) {
            return Mono.just(0L);
        }

        String mapKey = key + "_token" + type;
        Mono<Long> quotaMono = quotaMap.get(mapKey);
        if (quotaMono instanceof Fuseable.ScalarCallable && quotaMono.block() == 0L) {
            return Mono.just(0L);
        }

        return quotaMap.get(mapKey).onErrorReturn(0L).flatMap(quota -> {
            if (0L == quota) {
                return Mono.just(0L);
            }

            return tokenMap.get(mapKey).flatMap(tokenInBucket -> {
                tokenMap.add(mapKey, -token);
                if (tokenInBucket >= token) {
                    return Mono.just(0L);
                } else {
                    long waitTime = (token - tokenInBucket) * 1000 / quota;
                    waitTime = waitTime < 100 ? 100 : waitTime;
                    return Mono.just(waitTime);
                }
            }).onErrorReturn(0L);
        });
    }
}
