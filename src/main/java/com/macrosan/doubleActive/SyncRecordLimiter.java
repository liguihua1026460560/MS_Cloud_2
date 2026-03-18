package com.macrosan.doubleActive;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_INDEX_IPS_ENTIRE_MAP;


/**
 * 对不同类型的差异记录设置更精确的单次扫描并发限制。目前只有对所有同步请求总的限制.
 */
@Log4j2
public class SyncRecordLimiter {

    static Map<String, SyncRecordLimiter> map = new ConcurrentHashMap<>();

    public static int DEFAULT_EXTRA_MAX_COUNT = 1000;
    public static int DEFAULT_EXTRA_MAX_SIZE = 500 * 1024;

    /**
     * 异步复制每秒处理的记录数限制。默认大小10000。更改6379表2：hset sync_limit count 1000
     */
    public static final int DEFAULT_MAX_COUNT = 10000;
    public static volatile SyncRecordLimiter countLimiter;

    /**
     * 异步复制每秒处理的对象总大小限制。单位KB。默认大小1GB。更改6379表2：hset sync_limit size 1048576
     */
    public static final int DEFAULT_MAX_SIZE = 1048576;
    public static volatile SyncRecordLimiter sizeLimiter;

    /**
     * 对小分段但是分段数很多的差异记录设置更精确的单次扫描并发限制。
     */
    public static int DEFAULT_PART_MAX_COUNT = 1;

    static ScheduledFuture<?> scheduledFuture1;

    static ScheduledFuture<?> scheduledFuture2;

    static final int freshSec = 1;

    public static final String SYNC_LIMIT = "sync_limit";

    public static final AtomicLong LAST_UPDATE_TOKEN_TIME = new AtomicLong(System.currentTimeMillis());

    private static void initLimiters() {
        String type = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(EXTRA_CLUSTER_INFO, EXTRA_ASYNC_TYPE);
        if (TAPE_LIBRARY.equals(type)) {
            DEFAULT_EXTRA_MAX_COUNT = 10000;
            DEFAULT_EXTRA_MAX_SIZE = 614400;
        }
        for (Integer extraIndex : EXTRA_INDEX_IPS_ENTIRE_MAP.keySet()) {
            String extraCountStr = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(EXTRA_ASYNC, "extraCount_" + extraIndex);
            if (StringUtils.isBlank(extraCountStr)) {
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(EXTRA_ASYNC, "extraCount_" + extraIndex, String.valueOf(DEFAULT_EXTRA_MAX_COUNT));
            }
            long extraCount = StringUtils.isNotBlank(extraCountStr) ? Long.parseLong(extraCountStr) : DEFAULT_EXTRA_MAX_COUNT;
            SyncRecordLimiter extraCountLimiter = new SyncRecordLimiter("extraCount_" + extraIndex, extraCount);
            map.put("extraCount_" + extraIndex, extraCountLimiter);

            String extraSizeStr = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(EXTRA_ASYNC, "extraSize_" + extraIndex);
            if (StringUtils.isBlank(extraSizeStr)) {
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(EXTRA_ASYNC, "extraSize_" + extraIndex, String.valueOf(DEFAULT_EXTRA_MAX_SIZE));
            }
            long extraSize = StringUtils.isNoneBlank(extraSizeStr) ? Long.parseLong(extraSizeStr) : DEFAULT_EXTRA_MAX_SIZE;
            SyncRecordLimiter extraSizeLimiter = new SyncRecordLimiter("extraSize_" + extraIndex, extraSize);
            map.put("extraSize_" + extraIndex, extraSizeLimiter);
        }

        Map<String, String> syncLimitMap = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall(SYNC_LIMIT);
        if (syncLimitMap.isEmpty()) {
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(SYNC_LIMIT, "count", String.valueOf(DEFAULT_MAX_COUNT));
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(SYNC_LIMIT, "size", String.valueOf(DEFAULT_MAX_SIZE));
            log.info("init sync_limit");
        }
        long count = Long.parseLong(syncLimitMap.getOrDefault("count", String.valueOf(DEFAULT_MAX_COUNT)));
        long size = Long.parseLong(syncLimitMap.getOrDefault("size", String.valueOf(DEFAULT_MAX_SIZE)));

        countLimiter = new SyncRecordLimiter("count", count);
        map.put("count", countLimiter);
        sizeLimiter = new SyncRecordLimiter("size", size);
        map.put("size", sizeLimiter);
    }

    public static void start() {
        initLimiters();
        scheduledFuture1 = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                for (Integer extraIndex : EXTRA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                    RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).hget(EXTRA_ASYNC, "extraCount_" + extraIndex)
                            .doOnNext(extraCount -> {
                                int i = DEFAULT_EXTRA_MAX_COUNT;
                                if (StringUtils.isNotBlank(extraCount)) {
                                    i = Integer.parseInt(extraCount);
                                }
                                if (getExtraLimiter(extraIndex, DATASYNC_THROUGHPUT_QUOTA).getlimit() != i) {
                                    SyncRecordLimiter extraCountLimiter = new SyncRecordLimiter("extraCount_" + extraIndex, i);
                                    map.put("extraCount_" + extraIndex, extraCountLimiter);
                                    log.info("----------extraCount {} {} {}", extraIndex, extraCountLimiter.getlimit(), extraCountLimiter.getTokensAmount());
                                }
                            })
                            .flatMap(l -> RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).hget(EXTRA_ASYNC, "extraSize_" + extraIndex))
                            .doOnNext(extraSize -> {
                                int i = DEFAULT_EXTRA_MAX_SIZE;
                                if (StringUtils.isNotBlank(extraSize)) {
                                    i = Integer.parseInt(extraSize);
                                }
                                if (getExtraLimiter(extraIndex, DATASYNC_BAND_WIDTH_QUOTA).getlimit() != i) {
                                    SyncRecordLimiter extraSizeLimiter = new SyncRecordLimiter("extraSize_" + extraIndex, i);
                                    map.put("extraSize_" + extraIndex, extraSizeLimiter);
                                    log.info("----------extraSize {} {} {}", extraIndex, extraSizeLimiter.getlimit(), extraSizeLimiter.getTokensAmount());
                                }
                            })
                            .doOnError(e -> log.error("extraTotalLimit get error", e))
                            .subscribe();
                }

                RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).hgetall(SYNC_LIMIT)
                        .doOnNext(syncLimitMap -> {
                            long count = Long.parseLong(syncLimitMap.getOrDefault("count", String.valueOf(DEFAULT_MAX_COUNT)));
                            long size = Long.parseLong(syncLimitMap.getOrDefault("size", String.valueOf(DEFAULT_MAX_SIZE)));
                            if (countLimiter.getlimit() != count) {
                                countLimiter = new SyncRecordLimiter("count", count);
                                map.put("count", countLimiter);
                            }
                            if (sizeLimiter.getlimit() != size) {
                                sizeLimiter = new SyncRecordLimiter("size", size);
                                map.put("size", sizeLimiter);
                            }
                            log.debug("sync limit: {}, {}", count, size);
                            log.debug("--------count {} {}", countLimiter.getlimit(), countLimiter.getTokensAmount());
                            log.debug("--------size {} {}", sizeLimiter.getlimit(), sizeLimiter.getTokensAmount());

                        })
                        .doOnError(e -> log.error("syncLimit get error", e))
                        .subscribe();
            } catch (Exception e) {
                log.error("timer1 error,", e);
            }

        }, 10, 10, TimeUnit.SECONDS);
        // 启动 更新令牌定时器
        startUpdateTokenTimer();
        // 定时检测更新令牌的定时器是否正常
        SCAN_TIMER.scheduleAtFixedRate(() -> {
            // 上一次更新的时间小于10s，则重启定时器
            if (LAST_UPDATE_TOKEN_TIME.get() < System.currentTimeMillis() - 10000L) {
                Optional.ofNullable(scheduledFuture2).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
                startUpdateTokenTimer();
                log.info("restart update token timer");
            }
        }, 30, 60, TimeUnit.SECONDS);

    }

    private static void startUpdateTokenTimer() {
        // 固定时间间隔，当无可用的令牌数，则加上该时间段生产的令牌。
        scheduledFuture2 = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                LAST_UPDATE_TOKEN_TIME.set(System.currentTimeMillis());
                for (SyncRecordLimiter limiter : map.values()) {
                    if (limiter.getlimit() > 0) {
                        if (limiter.getTokensAmount() > (limiter.getlimit() * freshSec * 1.2)) {
                            limiter.tokensAmount.set(limiter.getlimit() * freshSec);
                        }
                        // 可用令牌数降到配额的五分之一时可以生成下一批令牌，让流量更平滑
                        if (limiter.getTokensAmount() <= (limiter.getlimit() * freshSec / 5)) {
                            limiter.tokensAmount.addAndGet(limiter.getlimit() * freshSec);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("timer2 error,", e);
            }
        }, 10, freshSec, TimeUnit.SECONDS);

    }

    /**
     * 桶设置了性能配额的情况下，扫描时也需要进行限制
     */
    public static void addNewBucketSyncLimiter(Integer clusterIndex, String bucket, String type, long quota) {
        String key = getBucketLimiterKey(clusterIndex, bucket, type);
        if (quota == 0L) {
            if (map.get(key) == null || map.get(key) != null && map.get(key).getlimit() != -1) {
                log.debug("add new bucket limiter for sync, {}, {}, {}", bucket, type, quota);
                map.put(key, new SyncRecordLimiter(key, -1L));
            }
            return;
        }

        if (map.get(key) != null && map.get(key).getlimit() == quota) {
            return;
        }
        log.debug("add new bucket limiter for sync, {}, {}, {}", bucket, type, quota);
        SyncRecordLimiter limiter = new SyncRecordLimiter(key, quota);
        map.put(key, limiter);
    }


    public static void updateBucketSyncLimiter(Integer clusterIndex, String bucket, String type, long quota) {
        String key = getBucketLimiterKey(clusterIndex, bucket, type);
        if (quota == 0L) {
            if (map.get(key) == null || map.get(key) != null && map.get(key).getlimit() != -1) {
                log.debug("add new bucket limiter for sync, {}, {}, {}", bucket, type, quota);
                map.put(key, new SyncRecordLimiter(key, -1L));
            }
            return;
        }

        if (map.get(key) == null || map.get(key).getlimit() == quota) {
            return;
        }

        SyncRecordLimiter limiter = new SyncRecordLimiter(key, quota);
        limiter.tokensAmount.set(map.get( key).getTokensAmount());
        map.put(key, limiter);
        log.debug(" update bucket limiter ------- {} {} {}", key, limiter.limit, limiter.tokensAmount);
    }

    public static SyncRecordLimiter getBucketLimiter(Integer clusterIndex, String bucket, String type) {
        return map.get(getBucketLimiterKey(clusterIndex, bucket, type));
    }

    private static String getBucketLimiterKey(Integer clusterIndex, String bucket, String type) {
        return bucket + "_" + type + "_" + clusterIndex;
    }

    /**
     * 返回一个更"严格"的limiter
     *
     * @param syncLimiter   扫描本身的limiter
     * @param bucketLimiter 桶配额相关的limiter
     * @return 当前扫描使用的limiter
     */
    public static SyncRecordLimiter getTrueLimiter(SyncRecordLimiter syncLimiter, SyncRecordLimiter bucketLimiter) {
        if (bucketLimiter == null) {
            return syncLimiter;
        }

        // getLimit小于0表示无限制
        if (bucketLimiter.getlimit() < 0) {
            return syncLimiter;
        }
        if (syncLimiter.getlimit() < 0) {
            return bucketLimiter;
        }

        return syncLimiter.getlimit() <= bucketLimiter.getlimit() ? syncLimiter : bucketLimiter;
    }

    /**
     * 异构复制有无限流。
     */
    public static boolean hasExtraLimiter(int index, String type) {
        String s = "";
        if (type.equals(DATASYNC_THROUGHPUT_QUOTA)) {
            s = "extraCount_" + index;
        } else if (type.equals(DATASYNC_BAND_WIDTH_QUOTA)) {
            s = "extraSize_" + index;
        }
        return EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(index) && map.get(s).getlimit() > 0;
    }

    public static SyncRecordLimiter getExtraLimiter(int index, String type) {
        String s = "";
        if (type.equals(DATASYNC_THROUGHPUT_QUOTA)) {
            s = "extraCount_" + index;
        } else if (type.equals(DATASYNC_BAND_WIDTH_QUOTA)) {
            s = "extraSize_" + index;
        }
        return map.get(s);
    }

    public static void rollBackTokens(UnSynchronizedRecord record) {
        if (hasExtraLimiter(record.index, DATASYNC_THROUGHPUT_QUOTA)) {
            getExtraLimiter(record.index, DATASYNC_THROUGHPUT_QUOTA).acquireToken(-1L);
        }
        if (hasExtraLimiter(record.index, DATASYNC_BAND_WIDTH_QUOTA)) {
            long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
            getExtraLimiter(record.index, DATASYNC_BAND_WIDTH_QUOTA).acquireToken(-dataSize);
        }
        if (hasSyncLimitation(countLimiter)) {
            countLimiter.acquireToken(-1L);
        }
        if (hasSyncLimitation(sizeLimiter)) {
            long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
            sizeLimiter.acquireToken(-dataSize);
        }

        SyncRecordLimiter bucketTpLimiter = getBucketLimiter(record.index, record.bucket, DATASYNC_THROUGHPUT_QUOTA);
        SyncRecordLimiter bucketBwLimiter = getBucketLimiter(record.index, record.bucket, DATASYNC_BAND_WIDTH_QUOTA);
        if (hasSyncLimitation(bucketTpLimiter)) {
            bucketTpLimiter.acquireToken(-1L);
        }
        if (hasSyncLimitation(bucketBwLimiter)) {
            long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
            bucketBwLimiter.acquireToken(-dataSize);
        }
    }

    /**
     * 有无常规的异步复制限流
     */
    public static boolean hasSyncLimitation(SyncRecordLimiter limiter) {
        return limiter != null && limiter.getlimit() > 0;
    }


    private long limit = 0;

    private AtomicLong tokensAmount = new AtomicLong();

    private String name;


    public SyncRecordLimiter(String name, long limitCount) {
        this.name = name;
        this.limit = limitCount;
    }

    public long getlimit() {
        return this.limit;
    }

    public String getName() {
        return this.name;
    }

    public long acquireToken(long amount) {
        return tokensAmount.addAndGet(-amount);
    }

    public long getTokensAmount() {
        return tokensAmount.get();
    }

    public boolean reachLimit() {
        if (getlimit() < 0) {
            return false;
        }
        if (getTokensAmount() <= 0) {
            log.debug("reachLimit. {}", this.name);
            return true;
        } else {
            return false;
        }
    }
}
