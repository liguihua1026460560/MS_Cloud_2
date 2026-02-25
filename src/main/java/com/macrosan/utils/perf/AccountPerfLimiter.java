package com.macrosan.utils.perf;

import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_USERINFO_INDEX;


/**
 * 账户性能配额
 *
 * @author gaozhiyuan
 * @date 2019.10.25
 */
public class AccountPerfLimiter extends BasePerfLimiter {
    @Override
    protected Mono<Long> getQuotaHandler(String key, String type) {
        return pool.getReactive(REDIS_USERINFO_INDEX).hget(key, type).map(Long::parseLong);
    }

    private AccountPerfLimiter() {
    }

    private static AccountPerfLimiter instance = new AccountPerfLimiter();

    public static AccountPerfLimiter getInstance() {
        return instance;
    }
}
