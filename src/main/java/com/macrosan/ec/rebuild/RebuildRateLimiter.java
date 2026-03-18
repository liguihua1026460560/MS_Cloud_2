package com.macrosan.ec.rebuild;

import com.google.common.util.concurrent.RateLimiter;
import com.macrosan.database.redis.RedisConnPool;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.*;

import static com.macrosan.constants.SysConstants.REDIS_MIGING_V_INDEX;
import static com.macrosan.ec.rebuild.RebuildCache.REBUILD_CACHE_SCHEDULER;

/**
 * @author zhaoyang
 * @date 2026/01/06
 * @description 重构限流器
 **/
@Log4j2
public class RebuildRateLimiter {
    private static final RebuildRateLimiter INSTANCE = new RebuildRateLimiter();
    private static double rate = 8000.0;
    private static final double MAX_RATE = 10000.0;
    public static RateLimiter metaLimiter = RateLimiter.create(rate);
    static {
        REBUILD_CACHE_SCHEDULER.schedulePeriodically(() -> {
            try {
                String metaRate = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).get("rebuild_meta_rate");
                if (null == metaRate) {
                    return;
                }
                double newMetaRate = Double.parseDouble(metaRate);
                if (rate != newMetaRate && newMetaRate <= MAX_RATE) {
                    rate = newMetaRate;
                    adjustRate();
                }
            } catch (Exception e) {
                log.error("get rebuild_meta_rate failed", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    public static RebuildRateLimiter getInstance() {
        return INSTANCE;
    }

    private static void adjustRate() {
        metaLimiter.setRate(rate);
        log.info("adjust rebuild meat rate: {}", rate);
    }

    public boolean tryAcquire() {
        return metaLimiter.tryAcquire();
    }

}
