package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.CacheFlushConfig;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.storage.move.CacheFlushConfigRefresher.*;

/**
 * @author zhaoyang
 * @date 2025/06/25
 **/
public class CacheFlushConfigService extends BaseService {
    private CacheFlushConfigService() {
        super();
    }

    private static CacheFlushConfigService instance = null;

    public static CacheFlushConfigService getInstance() {
        if (instance == null) {
            instance = new CacheFlushConfigService();
        }
        return instance;
    }

    public ResponseMsg putCacheFlushConfig(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        boolean updateWaterMarker = false;
        Map<String, String> cacheFlushConfig = new HashMap<>();
        String cachePrefix = paramMap.get("cache_prefix");

        String enableDelayedFlush = paramMap.get(ENABLE_DELAYED_FLUSH_KEY);
        if (isBoolean(enableDelayedFlush)) {
            cacheFlushConfig.put(ENABLE_DELAYED_FLUSH_KEY, enableDelayedFlush);
        }
        String enableOrderedFlush = paramMap.get(ENABLE_ORDERED_FLUSH_KEY);
        if (isBoolean(enableOrderedFlush)) {
            cacheFlushConfig.put(ENABLE_ORDERED_FLUSH_KEY, enableOrderedFlush);
        }
        String delayedFlushWaterMark = paramMap.get(DELAYED_FLUSH_WATER_MARK_KEY);
        if (isInRange(delayedFlushWaterMark)) {
            cacheFlushConfig.put(DELAYED_FLUSH_WATER_MARK_KEY, delayedFlushWaterMark);
        }
        String low = paramMap.get(LOW_WATER_MARK_KEY);
        if (isInRange(low)) {
            updateWaterMarker = true;
            cacheFlushConfig.put(LOW_WATER_MARK_KEY, low);
        }
        String high = paramMap.get(HIGH_WATER_MARK_KEY);
        if (isInRange(high)) {
            updateWaterMarker = true;
            cacheFlushConfig.put(HIGH_WATER_MARK_KEY, high);
        }
        String full = paramMap.get(FULL_WATER_MARK_KEY);
        if (isInRange(full)) {
            updateWaterMarker = true;
            cacheFlushConfig.put(FULL_WATER_MARK_KEY, full);
        }
        if (cacheFlushConfig.isEmpty()) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
        }
        Long exists = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).exists("storage_" + cachePrefix);
        if (exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_CACHE_POOL, "no such cache pool");
        }
        String cacheFlushConfigKey = cachePrefix + CACHE_FLUSH_CONFIG_KEY_SUFFIX;
        Map<String, String> currentConfig = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hgetall(cacheFlushConfigKey);
        if (currentConfig.isEmpty() && !updateWaterMarker) {
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_POOL_INDEX).hmset(cacheFlushConfigKey, cacheFlushConfig);
        } else {
            // 合并配置
            currentConfig.putAll(cacheFlushConfig);
            // 判断新配置是否合法
            CacheFlushConfig configView = new CacheFlushConfig(currentConfig);
            int lowWaterMark = configView.getLow();
            int highWaterMark = configView.getHigh();
            int fullWaterMark = configView.getFull();
            if (lowWaterMark >= highWaterMark || lowWaterMark >= fullWaterMark || highWaterMark >= fullWaterMark) {
                throw new MsException(ErrorNo.INVALID_CACHE_WATER_MARK_CONFIG, "Invalid water mark config");
            }
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_POOL_INDEX).hmset(cacheFlushConfigKey, currentConfig);
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getCacheFlushConfig(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String cachePrefix = paramMap.get("cache_prefix");
        Long exists = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).exists("storage_" + cachePrefix);
        if (exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_CACHE_POOL, "no such cache pool");
        }
        Map<String, String> configMap = RedisConnPool.getInstance().getShortMasterCommand(REDIS_POOL_INDEX).hgetall(cachePrefix + CACHE_FLUSH_CONFIG_KEY_SUFFIX);
        CacheFlushConfig cacheFlushConfig;
        if (configMap == null || configMap.isEmpty()) {
            cacheFlushConfig = CacheFlushConfig.DEFAULT_CONFIG;
        } else {
            cacheFlushConfig = new CacheFlushConfig(configMap);
        }
        return new ResponseMsg()
                .setData(cacheFlushConfig)
                .addHeader(CONTENT_TYPE, "application/xml");
    }


    public static boolean isBoolean(String str) {
        return "true".equals(str) || "false".equals(str);
    }

    public static boolean isInRange(String value) {
        try {
            int num = Integer.parseInt(value);
            return num >= 1 && num <= 100;
        } catch (Exception e) {
            return false;
        }
    }
}
