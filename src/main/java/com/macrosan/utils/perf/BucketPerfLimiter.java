package com.macrosan.utils.perf;

import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;

/**
 * 桶性能配额
 *
 * @author gaozhiyuan
 * @date 2019.10.25
 */
public class BucketPerfLimiter extends BasePerfLimiter {
    @Override
    protected Mono<Long> getQuotaHandler(String key, String type) {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(key, type).map(Long::parseLong);
    }

    private BucketPerfLimiter() {
    }

    private static BucketPerfLimiter instance = new BucketPerfLimiter();

    public static BucketPerfLimiter getInstance() {
        return instance;
    }
}
