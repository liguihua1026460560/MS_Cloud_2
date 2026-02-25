package com.macrosan.utils.perf;

import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;

public class BucketFSPerfLimiter extends BasePerfLimiter {
    private BucketFSPerfLimiter() {
    }

    private static BucketFSPerfLimiter instance = new BucketFSPerfLimiter();

    public static BucketFSPerfLimiter getInstance() {
        return instance;
    }

    /**
     * 查询配额值。
     *
     * @param key   桶名
     * @param value 接口名称-性能配额类型，如nfs3_create-throughput_quota
     * @return
     */
    @Override
    protected Mono<Long> getQuotaHandler(String key, String value) {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(key, value).map(Long::parseLong);
    }

}
