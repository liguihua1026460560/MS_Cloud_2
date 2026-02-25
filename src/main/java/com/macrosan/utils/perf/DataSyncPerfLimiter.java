package com.macrosan.utils.perf;

import com.macrosan.constants.SysConstants;
import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className DataSyncPerfLimiter
 * @date 2021/9/8 16:12
 */
public class DataSyncPerfLimiter extends BasePerfLimiter{
    @Override
    protected Mono<Long> getQuotaHandler(String key, String value) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(key,value).map(Long::parseLong);
    }

    private DataSyncPerfLimiter() {}

    private static final DataSyncPerfLimiter instance = new DataSyncPerfLimiter();

    public static DataSyncPerfLimiter getInstance() {
        return instance;
    }

}
