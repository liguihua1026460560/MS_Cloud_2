package com.macrosan.utils.perf;

import com.macrosan.constants.SysConstants;
import reactor.core.publisher.Mono;

public class StsPerfLimiter extends BasePerfLimiter{
    @Override
    protected Mono<Long> getQuotaHandler(String key, String value) {
        return pool.getReactive(SysConstants.REDIS_SYSINFO_INDEX).hget(key,value).defaultIfEmpty("600").map(Long::parseLong);
    }

    private static StsPerfLimiter stsPerfLimiter;
    public static StsPerfLimiter getInstance() {
        if (stsPerfLimiter == null) {
            stsPerfLimiter = new StsPerfLimiter();
        }
        return stsPerfLimiter;
    }
    private StsPerfLimiter() {}
}
