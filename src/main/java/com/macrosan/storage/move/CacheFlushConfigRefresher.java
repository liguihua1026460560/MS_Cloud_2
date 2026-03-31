package com.macrosan.storage.move;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.xmlmsg.CacheFlushConfig;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    public static final String ENABLE_ACCESS_FLUSH_KEY = "enable_access_flush";
    public static final String LOW_FREQUENCY_ACCESS_DAYS_KEY = "low_frequency_access_days";
    public static final String ENABLE_LAYERING_STAMP_KEY = "enable_layering_stamp";
    public static final String DEL_DATA_WHEN_BACK_STORE_KEY = "del_data_when_back_store";
    public static final String DELAYED_FLUSH_WATER_MARK_KEY = "delayed_flush_water_mark";
    public static final String LOW_WATER_MARK_KEY = "low";
    public static final String HIGH_WATER_MARK_KEY = "high";
    public static final String FULL_WATER_MARK_KEY = "full";

    public static final String DEFAULT_ENABLE_DELAYED_FLUSH = "false";
    public static final String DEFAULT_ENABLE_ORDERED_FLUSH = "false";
    public static final String DEFAULT_ENABLE_ACCESS_FLUSH = "false";
    public static final String DEFAULT_DELAYED_FLUSH_WATER_MARK = "20";
    public static final String DEFAULT_LOW_FREQUENCY_ACCESS_DAYS = "30";

    public static final String DEFAULT_LOW_WATER_MARK;
    public static final String DEFAULT_HIGH_WATER_MARK;
    public static final String DEFAULT_FULL_WATER_MARK;

    public static final Map<String, AtomicInteger> enableAccessFlushMap = new ConcurrentHashMap<>();

    public static final Map<String, StrategyFlushConfig> strategyAccessFlush = new ConcurrentHashMap<>();


    static {
        DEFAULT_LOW_WATER_MARK = System.getProperty("com.macrosan.storage.cache.low", "40");
        DEFAULT_HIGH_WATER_MARK = System.getProperty("com.macrosan.storage.cache.high", "60");
        DEFAULT_FULL_WATER_MARK = System.getProperty("com.macrosan.storage.cache.full", "80");
    }
    private static final ScanArgs SCAN_ARGS = new ScanArgs().match("storage_cache*").limit(100);
    private static final ScanArgs SCAN_ARGS_STRATEGY = new ScanArgs().match("strategy_*").limit(100);
    private static final int LENGTH = "storage_".length();

    private final Set<String> existCachePoolSet = new HashSet<>();
    private final Set<String> existStrategySet = new HashSet<>();

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
                    scanCursor = command.scan(SCAN_ARGS_STRATEGY);
                } else {
                    scanCursor = command.scan(scanCursor, SCAN_ARGS_STRATEGY);
                }
                for (String key : scanCursor.getKeys()) {
                    Map<String, String> strategyConfig = command.hgetall(key);

                    if (strategyConfig != null && "true".equals(strategyConfig.get(ENABLE_ACCESS_FLUSH_KEY))) {
                        StrategyFlushConfig strategyFlushConfig = new StrategyFlushConfig(strategyConfig);
                        if (!strategyFlushConfig.equals(strategyAccessFlush.get(key))) {
                            strategyAccessFlush.put(key, strategyFlushConfig);
                            log.info("{} Layered config change! {}", key, strategyFlushConfig);
                        }
                    } else {
                        strategyAccessFlush.remove(key);
                    }
                    existStrategySet.add(key);
                }
            } while (!scanCursor.isFinished());

            scanCursor = null;

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
                        if (tmpConfig.isEnableAccessTimeFlush()) {//这里开启分层的同时也会记录设置的规则
                            //当前缓存池开启分层，记录其所关联的存储策略开启分层
                            String pool = command.hget(key, "pool");
                            String storageStrategy = command.hget(pool, "storage_strategy");
                            if (storageStrategy != null && !"[]".equals(storageStrategy)) {
                                String[] strategies = Json.decodeValue(storageStrategy, String[].class);
                                for (String strategy : strategies) {
                                    enableAccessFlushMap.computeIfAbsent(strategy, k -> new AtomicInteger(0)).incrementAndGet();//供访问数据池中数据时转回缓存池，数据池数据被访问时通过数据池前缀查找所属的存储策略然后再确认策略是否分层,可以直接通过DATA_POOL_STRATEGY_MAP根据数据池前缀获取所属的存储策略
                                }
                            }
                        } else {//这里应该在改变缓存池分层配置时判断其所属策略下所有缓存池的分层配置，确定当前策略是否关闭分层
                            String pool = command.hget(key, "pool");
                            String storageStrategy = command.hget(pool, "storage_strategy");
                            if (storageStrategy != null && !"[]".equals(storageStrategy)) {
                                String[] strategies = Json.decodeValue(storageStrategy, String[].class);
                                for (String strategy : strategies) {
                                    enableAccessFlushMap.computeIfPresent(strategy, (k, v) -> {
//                                        if (v.get() > 0) {
                                        v.decrementAndGet();//减到0表示此策略下缓存池均关闭分层，不再进行分层
//                                        }
                                        return v;
                                    });//供访问数据池中数据时转回缓存池，数据池数据被访问时通过数据池前缀查找所属的存储策略然后再确认策略是否分层,可以直接通过DATA_POOL_STRATEGY_MAP根据数据池前缀获取所属的存储策略
                                }
                            }
                        }
                        log.info("{} flush config change! {}", key, tmpConfig);
                    }
                    existCachePoolSet.add(cachePrefix);
                }
            } while (!scanCursor.isFinished());
            // 删除不存在的缓存池的配置
            configMap.entrySet().removeIf(entry -> !existCachePoolSet.contains(entry.getKey()));
            enableAccessFlushMap.entrySet().removeIf(entry -> !existStrategySet.contains(entry.getKey()));
            log.debug("cache flush config refresh complete! configMap:{}", configMap);
            log.debug("strategyAccessFlush:{}", strategyAccessFlush);
            log.debug("enableAccessFlushMap:{}", enableAccessFlushMap);
            existCachePoolSet.clear();
            existStrategySet.clear();
        } catch (Exception e) {
            log.error("load cache flush config error!", e);
        } finally {
            scheduler.schedule(this::startPeriodicRefresh, 10, TimeUnit.SECONDS);
        }
    }

    public CacheFlushConfig getFlushConfig(String cache) {
        return configMap.getOrDefault(cache, DEFAULT_CONFIG);
    }

    public StrategyFlushConfig getStrategyFlushConfig(String strategy) {
        return strategyAccessFlush.getOrDefault(strategy, StrategyFlushConfig.DEFAULT_STRATEGY_CONFIG);
    }

    public static class SingletonWrapper {
        private static final CacheFlushConfigRefresher INSTANCE = new CacheFlushConfigRefresher();
    }

    public static boolean isStrategyEnableAccessTimeFlush(String strategy) {
        return enableAccessFlushMap.containsKey(strategy) && enableAccessFlushMap.get(strategy).get() > 0;
    }

    @Data
    public static class StrategyFlushConfig {
        boolean enableAccessTimeFlush;
        String enableLayeringStamp;
        int lowFrequencyAccessDays;
        boolean delDataWhenBackStore;

        public static final StrategyFlushConfig DEFAULT_STRATEGY_CONFIG = new StrategyFlushConfig(false, String.valueOf(System.currentTimeMillis()), 30, false);

        public StrategyFlushConfig(boolean enableAccessTimeFlush, String enableLayeringStamp, int lowFrequencyAccessDays, boolean delDataWhenBackStore) {
            this.enableAccessTimeFlush = enableAccessTimeFlush;
            this.enableLayeringStamp = enableLayeringStamp;
            this.lowFrequencyAccessDays = lowFrequencyAccessDays;
            this.delDataWhenBackStore = delDataWhenBackStore;
        }

        public StrategyFlushConfig(Map<String, String> map) {
            this.enableAccessTimeFlush = Boolean.parseBoolean(map.get(ENABLE_ACCESS_FLUSH_KEY));
            this.enableLayeringStamp = map.getOrDefault(ENABLE_LAYERING_STAMP_KEY, String.valueOf(System.currentTimeMillis()));
            this.lowFrequencyAccessDays = Integer.parseInt(map.getOrDefault(LOW_FREQUENCY_ACCESS_DAYS_KEY, "30"));
            this.delDataWhenBackStore = Boolean.parseBoolean(map.getOrDefault(DEL_DATA_WHEN_BACK_STORE_KEY, "false"));
        }
    }

}
