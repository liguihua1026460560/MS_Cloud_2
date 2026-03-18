package com.macrosan.database.redis;

import io.lettuce.core.ScriptOutputType;
import lombok.extern.log4j.Log4j2;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_RATE_LIMIT_INDEX;

/**
 * @author zhaoyang
 * @date 2026/02/26
 * @description: 基于Redis的zset和string实现的分布式信号量
 **/
@Log4j2
public class RedisSemaphore {
    private final String permitsKey;
    private final String holdersKey;

    private final String[] keys;

    private static final RedisConnPool REDIS_POOL = RedisConnPool.getInstance();

    public RedisSemaphore(String resourceId, int maxConcurrent) {
        permitsKey = resourceId + ":permits";
        holdersKey = resourceId + ":holders";
        keys = new String[]{permitsKey, holdersKey};
        boolean trySetPermits = trySetPermits(maxConcurrent);
        if (!trySetPermits) {
            // 没有设置成功，说明key已存在，则判断是否需要更新permits
            if (getPermits() != maxConcurrent) {
                updateMaxPermits(maxConcurrent);
            }
        }
    }

    private boolean trySetPermits(int maxConcurrent) {
        return REDIS_POOL.getShortMasterCommand(REDIS_RATE_LIMIT_INDEX).setnx(permitsKey, String.valueOf(maxConcurrent));
    }


    /**
     * 尝试获取并发许可
     *
     * @return true: 获取成功，可以执行任务；false: 并发已满，拒绝执行
     */
    public String tryAcquire(long leaseTime, TimeUnit unit) {
        return tryAcquire(1, leaseTime, unit);
    }

    /**
     * 尝试获取多个并发许可
     *
     * @param permits 需要的许可数
     */
    public String tryAcquire(int permits, long leaseTime, TimeUnit timeUnit) {
        if (permits <= 0) return null;


        String permitId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        long expireTime = now + timeUnit.toMillis(leaseTime);
        String[] values = new String[4];
        values[0] = permitId;
        values[1] = String.valueOf(now);
        values[2] = String.valueOf(expireTime);
        values[3] = String.valueOf(permits);
        String script =
                "local expired = redis.call('zrangebyscore', KEYS[2], 0, ARGV[2], 'limit', 0, ARGV[4]); " +
                        "if #expired > 0 then " +
                        "  redis.call('zrem', KEYS[2], unpack(expired)); " +
                        "  redis.call('incrby', KEYS[1], #expired); " +
                        "end; " +
                        "local current = tonumber(redis.call('get', KEYS[1]) or '0'); " +
                        "if current >= 1 then " +
                        "  redis.call('decrby', KEYS[1], 1); " +
                        "  redis.call('zadd', KEYS[2], ARGV[3], ARGV[1]); " +
                        "  return 'OK'; " +
                        "end; " +
                        "return nil;";

        try {
            String result = REDIS_POOL.getShortMasterCommand(REDIS_RATE_LIMIT_INDEX).eval(script, ScriptOutputType.VALUE,
                    keys, values);

            if ("OK".equals(result)) {
                return permitId;
            }
            return null;

        } catch (Exception e) {
            log.error("Failed to acquire concurrent permit from Redis for {}", permitsKey, e);
            return null;
        }
    }

    public void release(String permitId) {
        String script =
                "local removed = redis.call('zrem', KEYS[2], ARGV[1])\n" +
                        "if removed == 1 then\n" +
                        "    redis.call('incrby', KEYS[1], 1)\n" +
                        "    return 1\n" +
                        "end\n" +
                        "return 0";
        try {
            REDIS_POOL.getShortMasterCommand(REDIS_RATE_LIMIT_INDEX).eval(
                    script,
                    ScriptOutputType.INTEGER,
                    keys,
                    permitId
            );
        } catch (Exception e) {
            log.error("Failed to release concurrent permit from Redis for {}", permitsKey, e);
        }
    }


    public long getPermits() {
        String script =
                "local permit = tonumber(redis.call('get', KEYS[1]) or '0')\n" +
                        "local used = redis.call('zcard', KEYS[2])\n" +
                        "return permit + used";
        Object permits = REDIS_POOL.getShortMasterCommand(REDIS_RATE_LIMIT_INDEX).eval(
                script,
                ScriptOutputType.INTEGER,
                keys);
        if (permits == null) {
            return 0;
        }
        return (Long) permits;
    }


    public void updateMaxPermits(int maxPermits) {
        try {
            String script =
                    "local permit = tonumber(redis.call('get', KEYS[1]) or '0')\n" +
                            "local used = redis.call('zcard', KEYS[2])\n" +
                            "local currentMax = permit + used\n" +
                            "local newMax = tonumber(ARGV[1])\n" +
                            "local delta = newMax - currentMax\n" +
                            "redis.call('incrby', KEYS[1], delta)\n" +
                            "return permit + used";
            REDIS_POOL.getShortMasterCommand(REDIS_RATE_LIMIT_INDEX).eval(
                    script,
                    ScriptOutputType.INTEGER,
                    keys,
                    String.valueOf(maxPermits)
            );
            log.info("Update max permits {} for {}", maxPermits, permitsKey);
        } catch (Exception e) {
            log.error("Failed to release concurrent permit from Redis for {}", permitsKey, e);
        }
    }

}
