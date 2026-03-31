package com.macrosan.storage.strategy;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.xmlmsg.CacheFlushConfig;
import com.macrosan.storage.PoolHealth;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.move.CacheFlushConfigRefresher;
import com.macrosan.storage.strategy.select.SelectStrategy;
import com.macrosan.utils.ratelimiter.LimitStrategy;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.xmlmsg.CacheFlushConfig.DEFAULT_CONFIG;
import static com.macrosan.storage.strategy.select.SelectStrategy.NO;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class CacheSmallObjectStrategy extends SelectorStrategy {
    public static final int HIGH_RUN_NUM;
    public static final int LOW_RUN_NUM;
    public static final int RUN_ONCE;
    public static final AtomicLong USED = new AtomicLong(Long.MAX_VALUE);

    static {
        HIGH_RUN_NUM = Integer.parseInt(System.getProperty("com.macrosan.storage.cache.run.high", "200"));
        LOW_RUN_NUM = Integer.parseInt(System.getProperty("com.macrosan.storage.cache.run.low", "100"));
        RUN_ONCE = Integer.parseInt(System.getProperty("com.macrosan.storage.cache.run.once", "10000"));
    }


    private final StorageStrategy strategy;
    private final StoragePool[] cache;
    private final long boundary;
    private final AtomicBoolean cacheMigrate = new AtomicBoolean(false);


    CacheSmallObjectStrategy(long boundary, StoragePool[] cache, SelectStrategy selectStrategy, StorageStrategy strategy) {
        super(selectStrategy);
        this.strategy = strategy;
        this.boundary = boundary;
        this.cache = cache;
        check();
        existCacheMigrate();
    }

    CacheSmallObjectStrategy(long boundary, StoragePool cache, StorageStrategy strategy) {
        this(boundary, new StoragePool[]{cache}, NO, strategy);
    }

    private static ThreadLocal<AtomicLong> num = ThreadLocal.withInitial(AtomicLong::new);

    @Override
    public StoragePool getStoragePool(StorageOperate operate) {
        if (operate.getPoolType() == StorageOperate.PoolType.DATA) {
            if (operate.getDataSize() >= 0 && operate.getDataSize() <= boundary) {
                if (LimitStrategy.LIMIT == RecoverLimiter.getInstance().getDataLimiter().getStrategy()
                    && cacheMigrate.get()) {
                    //业务优先时且缓存池存在迁移时跳过缓存池直接返回数据池
                    log.debug("use data pool");
                    return strategy.getStoragePool(operate);
                }
//                long used = USED.get();
                StoragePool pool0 = selector.select(cache);
                StoragePool pool = null;
                float used = 100.0f;
                long singleUsed = 0L;
                long singleTotal = 0L;
                singleUsed = pool0.getCache().size;
                singleTotal = pool0.getCache().totalSize;
                if (singleTotal != 0L) {
                    used = singleUsed * 100.0f / singleTotal;
                }
                CacheFlushConfig flushConfig = CacheFlushConfigRefresher.getInstance().getFlushConfig(pool0.getVnodePrefix());
                int low = flushConfig.getLow();
                int high = flushConfig.getHigh();
                int full = flushConfig.getFull();

                if (used > high && used < full) {
                    if (num.get().incrementAndGet() % 10 == 0) {
                        pool = pool0;
                    }
                } else if (used <= high && used >= low) {
                    if (num.get().incrementAndGet() % 2 == 0) {
                        pool = pool0;
                    }
                } else if (used < low) {
                    pool = pool0;
                }

                if (pool != null
                        && pool.getState() != PoolHealth.HealthState.Fault
                        && pool.getState() != PoolHealth.HealthState.Broken) {
                    return pool;
                }
            }
        } else if (operate.getPoolType() == StorageOperate.PoolType.CACHE) {
            //直接获取一个缓存池
            return selector.select(cache);
        }

        return strategy.getStoragePool(operate);
    }

    @Override
    public StoragePool[] getStoragePools(StorageOperate operate) {
        if (operate.getPoolType() == StorageOperate.PoolType.DATA) {
            if (operate.getDataSize() >= 0 && operate.getDataSize() <= boundary) {
                long used = USED.get();
                StoragePool[] pool = null;
                CacheFlushConfig flushConfig = DEFAULT_CONFIG;
                int low = flushConfig.getLow();
                int high = flushConfig.getHigh();
                int full = flushConfig.getFull();
                if (used > high && used < full) {
                    if (num.get().incrementAndGet() % 10 == 0) {
                        pool = selector.sort(cache);
                    }
                } else if (used <= high && used >= low) {
                    if (num.get().incrementAndGet() % 2 == 0) {
                        pool = selector.sort(cache);
                    }
                } else if (used < low) {
                    pool = selector.sort(cache);
                }

                if (pool != null) {
                    return pool;
                }
            }
        }

        return strategy.getStoragePools(operate);
    }

    private void check() {
        long used = 0L;
        long total = 0L;
        for (StoragePool pool : cache) {
            used += pool.getCache().size;
            total += pool.getCache().totalSize;
        }

        if (total != 0L) {
            long precent = used * 100 / total;
            USED.set(precent);
        }


        ErasureServer.DISK_SCHEDULER.schedule(this::check, 30, TimeUnit.SECONDS);
    }

    public long getBoundary() {
        return boundary;
    }

    private void existCacheMigrate() {
        RedisCommands<String, String> command = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX);
        ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("running_*"));
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (!"hash".equals(command.type(key))) {
                continue;
            }
            String poolName = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).hget(key , "poolName");
            String role = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolName, "role");
            if ("cache".equals(role)) {
                log.debug("existCacheMigrate!!!");
                cacheMigrate.set(true);
                ErasureServer.DISK_SCHEDULER.schedule(this::existCacheMigrate, 10, TimeUnit.SECONDS);
                return;
            }
        }
        cacheMigrate.set(false);
        ErasureServer.DISK_SCHEDULER.schedule(this::existCacheMigrate, 10, TimeUnit.SECONDS);
    }

    @Override
    public boolean isCache(StoragePool pool) {
        for (StoragePool p : cache) {
            if (p.getVnodePrefix().equalsIgnoreCase(pool.getVnodePrefix())) {
                return true;
            }
        }

        return false;
    }
}
