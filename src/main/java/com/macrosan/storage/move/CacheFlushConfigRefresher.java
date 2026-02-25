package com.macrosan.storage.move;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.xmlmsg.CacheFlushConfig;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.message.xmlmsg.CacheFlushConfig.DEFAULT_CONFIG;

/**
 * @author zhaoyang
 * @date 2025/06/25
 **/
@Log4j2
public class CacheFlushConfigRefresher {

    public static final String CACHE_FLUSH_CONFIG_KEY_SUFFIX = "_flush_config";
    public static final String ENABLE_DELAYED_FLUSH_KEY = "enable_delayed_flush";
    public static final String ENABLE_ORDERED_FLUSH_KEY = "enable_ordered_flush";
    public static final String DELAYED_FLUSH_WATER_MARK_KEY = "delayed_flush_water_mark";
    public static final String LOW_WATER_MARK_KEY = "low";
    public static final String HIGH_WATER_MARK_KEY = "high";
    public static final String FULL_WATER_MARK_KEY = "full";

    public static final String DEFAULT_ENABLE_DELAYED_FLUSH = "false";
    public static final String DEFAULT_ENABLE_ORDERED_FLUSH = "false";
    public static final String DEFAULT_DELAYED_FLUSH_WATER_MARK = "20";

    public static final String DEFAULT_LOW_WATER_MARK;
    public static final String DEFAULT_HIGH_WATER_MARK;
    public static final String DEFAULT_FULL_WATER_MARK;


    static {
        DEFAULT_LOW_WATER_MARK = System.getProperty("com.macrosan.storage.cache.low", "40");
        DEFAULT_HIGH_WATER_MARK = System.getProperty("com.macrosan.storage.cache.high", "60");
        DEFAULT_FULL_WATER_MARK = System.getProperty("com.macrosan.storage.cache.full", "80");
    }
    private static final ScanArgs SCAN_ARGS = new ScanArgs().match("storage_cache*").limit(100);
    private static final int LENGTH = "storage_".length();

    private final Set<String> existCachePoolSet = new HashSet<>();

    private final Scheduler scheduler = ErasureServer.DISK_SCHEDULER;

    private final Map<String, CacheFlushConfig> configMap = new ConcurrentHashMap<>();


    public static CacheFlushConfigRefresher getInstance() {
        return SingletonWrapper.INSTANCE;
    }

    private CacheFlushConfigRefresher() {
        startPeriodicRefresh();
    }



    /**
     * 从 Redis 加载配置
     */
    private void startPeriodicRefresh() {
        try {
            RedisCommands<String, String> command = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX);
            KeyScanCursor<String> scanCursor = null;
            do {
                if (scanCursor == null) {
                    scanCursor = command.scan(SCAN_ARGS);
                } else {
                    scanCursor = command.scan(scanCursor, SCAN_ARGS);
                }
                for (String key : scanCursor.getKeys()) {
                    String cachePrefix = key.substring(LENGTH);
                    String flushConfigKey = cachePrefix + CACHE_FLUSH_CONFIG_KEY_SUFFIX;
                    Map<String, String> redisConfig = command.hgetall(flushConfigKey);
                    CacheFlushConfig tmpConfig;
                    if (redisConfig != null && !redisConfig.isEmpty()) {
                        tmpConfig = new CacheFlushConfig(redisConfig);
                    } else {
                        tmpConfig = DEFAULT_CONFIG;
                    }
                    if (!tmpConfig.equals(configMap.get(cachePrefix))) {
                        configMap.put(cachePrefix, tmpConfig);
                        log.info("{} flush config change! {}", key, tmpConfig);
                    }
                    existCachePoolSet.add(cachePrefix);
                }
            } while (!scanCursor.isFinished());
            // 删除不存在的缓存池的配置
            configMap.entrySet().removeIf(entry -> !existCachePoolSet.contains(entry.getKey()));
            existCachePoolSet.clear();
        } catch (Exception e) {
            log.error("load cache flush config error!", e);
        } finally {
            scheduler.schedule(this::startPeriodicRefresh, 10, TimeUnit.SECONDS);
        }
    }

    public CacheFlushConfig getFlushConfig(String cache) {
        return configMap.getOrDefault(cache, DEFAULT_CONFIG);
    }

    public static class SingletonWrapper {
        private static final CacheFlushConfigRefresher INSTANCE = new CacheFlushConfigRefresher();
    }

}
