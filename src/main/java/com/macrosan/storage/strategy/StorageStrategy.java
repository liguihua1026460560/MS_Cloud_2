package com.macrosan.storage.strategy;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.move.CacheMove;
import com.macrosan.storage.strategy.select.SelectStrategy;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.storage.strategy.select.SelectStrategy.SIZE;

/**
 * @author gaozhiyuan
 */
@Log4j2
public abstract class StorageStrategy {
    public static StorageStrategy DEFAULT;
    public static String DEFAULT_STRATEGY_NAME;
    public Map<String, String> redisMap;
    public Set<String> dataPool = new HashSet<>();
    // 存储策略下的全体缓存池集合
    public Set<String> cachePool = new HashSet<>();

    public static final String POOL_STRATEGY_PREFIX = "strategy_";
    public static final Map<String, StorageStrategy> POOL_STRATEGY_MAP = new ConcurrentHashMap<>();
    public static final Map<String, String> BUCKET_STRATEGY_NAME_MAP = new ConcurrentHashMap<>();

    public static final Map<String, NameSpace> STRATEGY_NAMESPACE_MAP = new ConcurrentHashMap<>();

    public abstract StoragePool getStoragePool(StorageOperate operate);

    public abstract StoragePool[] getStoragePools(StorageOperate operate);

    public static StorageStrategy loadStrategy(String strategyName, Map<String, StoragePool> poolMap) {
        Map<String, String> strategyMap = RedisConnPool
                .getInstance()
                .getCommand(REDIS_POOL_INDEX)
                .hgetall(strategyName);

        String metaStr = strategyMap.get("meta");
        String cacheStr = strategyMap.get("cache");
        String dataStr = strategyMap.get("data");

        if (checkPoolsEmpty(metaStr) || checkPoolsEmpty(dataStr)) {
            throw new UnsupportedOperationException("load strategy " + strategyName + " fail");
        }

        StorageStrategy strategy;
        String[] meta = Json.decodeValue(metaStr, String[].class);
        String[] data = Json.decodeValue(dataStr, String[].class);

        if (!poolMap.keySet().containsAll(Arrays.asList(data)) || !poolMap.keySet().containsAll(Arrays.asList(meta))) {
            throw new UnsupportedOperationException("waiting for the next pool reload");
        }

        if (meta.length == 1 && data.length == 1) {
            strategy = new SimpleStrategy(poolMap.get(meta[0]), poolMap.get(data[0]));
        } else {
            StoragePool[] metaPools = mapToPools(meta, poolMap);
            StoragePool[] dataPools = mapToPools(data, poolMap);
            String select = strategyMap.get("select_strategy");
            SelectStrategy selectStrategy = StringUtils.isBlank(select) ? SIZE : SelectStrategy.valueOf(select.toUpperCase());
            String maxRate = strategyMap.getOrDefault("max_rate", "0.98");
            log.debug("reload maxRate {}", maxRate);
            String minRemainSize = strategyMap.getOrDefault("min_remain_size", "0");
            strategy = new DomainStrategy(metaPools, dataPools, selectStrategy, maxRate, minRemainSize);
        }


        if (StringUtils.isNotBlank(cacheStr)) {
            String cacheBoundary = strategyMap.get("cache_boundary");
            long size = Long.parseLong(cacheBoundary);
            String[] cache = Json.decodeValue(cacheStr, String[].class);

            if (!poolMap.keySet().containsAll(Arrays.asList(cache))) {
                throw new UnsupportedOperationException("waiting for the next pool reload");
            }
            if (cache.length == 1) {
                strategy = new CacheSmallObjectStrategy(size, poolMap.get(cache[0]), strategy);
                CacheMove.dealPools(strategyName, poolMap.get(cache[0]));
            } else if (cache.length > 1) {
                StoragePool[] cachePool = mapToPools(cache, poolMap);
                String select = strategyMap.get("cache_select_strategy");
                SelectStrategy selectStrategy = StringUtils.isBlank(select) ? SIZE : SelectStrategy.valueOf(select.toUpperCase());
                strategy = new CacheSmallObjectStrategy(size, cachePool, selectStrategy, strategy);
                CacheMove.dealPools(strategyName, cachePool);
            } else {
                CacheMove.dealPools(strategyName);
            }
            if (cache.length > 0) {
                Collections.addAll(strategy.cachePool, cache);
            }
        }

        strategy.redisMap = strategyMap;
        String dataListStr = strategyMap.get("data");
        if (StringUtils.isNotEmpty(dataListStr)) {
            strategy.dataPool.addAll(Arrays.asList(Json.decodeValue(dataStr, String[].class)));
        }
        return strategy;

    }

    private static StoragePool[] mapToPools(String[] str, Map<String, StoragePool> poolMap) {
        return Arrays.stream(str).map(s -> poolMap.get(s.trim())).toArray(StoragePool[]::new);
    }

    public boolean isCache(StoragePool pool) {
        return false;
    }

    public static boolean checkPoolsEmpty(String poolStr) {
        return poolStr == null || "[]".equals(poolStr);
    }

    public static StorageStrategy getStorageStrategy(String bucketName) {
        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(POOL_STRATEGY_MAP::get)
                .orElseGet(() -> DEFAULT);
    }

}
