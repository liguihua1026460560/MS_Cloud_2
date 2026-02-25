package com.macrosan.utils.worm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.datastream.ObjectRetentionService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.action.managestream.ObjectLockService.VALID_RETENTION_MODE;
import static com.macrosan.constants.ErrorNo.INVALID_ARGUMENT;
import static com.macrosan.constants.ErrorNo.WORM_STATE_CONFLICT;
import static com.macrosan.constants.ServerConstants.IS_SYNCING;

public class WormUtils {
    public static final long ONE_DAY = 24 * 60 * 60 * 1000L;
    public static final long ONE_YEAR = 365L * ONE_DAY;
    public static final String WORM_STAMP = "wormStamp";
    public static final String WORM_CLOCK = "worm_time";
    public static final String EXPIRATION = "expiration";
    public static final String SYNC_WORM_EXPIRE= "syncWormExpire";
    public static final RedisConnPool pool = RedisConnPool.getInstance();

    /**
     * 上传对象流程中，设置对象的到期时间
     * @param bucketInfo 桶信息
     * @param sysMeta 系统元数据
     * @param currentWormTime 当前worm时钟时间
     * @param wormExpire 设置的到期时间
     * @param mode 保护模式
     */
    public static void setObjectRetention(Map<String, String> bucketInfo, JsonObject sysMeta, long currentWormTime, String wormExpire, String mode) {
        if (!StringUtils.isEmpty(mode) && !VALID_RETENTION_MODE.equals(mode)) {
            throw new MsException(INVALID_ARGUMENT, "invalid object lock mode.");
        }

        if (StringUtils.isEmpty(wormExpire)) {
            // 获取桶的保护持续时间
            long lockTimeCommand = getBucketLockTimeCommand(bucketInfo);
            // 根据持续保护时间计算对象的到期时间
            long retention = currentWormTime + lockTimeCommand;
            if (lockTimeCommand != 0) {
                sysMeta.put(EXPIRATION, String.valueOf(retention));
            }
            return;
        }

        // 校验wormExpire参数是否符合时间格式
        if (!ObjectRetentionService.pattern.matcher(wormExpire).matches()) {
            throw new MsException(INVALID_ARGUMENT,"Invalid retain util date format!");
        }

        // 桶未开启worm保护，不允许设置对象的worm
        if (StringUtils.isEmpty(bucketInfo.get("lock_periodic")) || "Disabled".equals(bucketInfo.get("lock_status"))) {
            throw new MsException(ErrorNo.BUCKET_MISSING_OBJECT_LOCK_CONFIG, "Bucket is missing Object Lock Configuration.");
        }

        // 根据对象的到期时间
        long expiration = Long.parseLong(MsDateUtils.tzToStamp(wormExpire));
        // 到期时间从当前时间计算，不得大于100年
        long limit = currentWormTime + ONE_YEAR * 100 + 30 * ONE_DAY;
        if (expiration <= currentWormTime || expiration > limit) {
            throw new MsException(INVALID_ARGUMENT, "Invalid retain util date!");
        }
        // 设置依法保留到期时间(对象级别的过期时间)
        sysMeta.put(EXPIRATION, String.valueOf(expiration));
    }

    public static void setObjectRetention(Map<String, String> bucketInfo, Map<String, String> sysMeta, long currentWormTime, String wormExpire, String mode) {
        // 移除之前的worm属性
        JsonObject tmp = new JsonObject();
        setObjectRetention(bucketInfo, tmp, currentWormTime, wormExpire, mode);
        tmp.forEach(entry -> sysMeta.put(entry.getKey(), String.valueOf(entry.getValue())));
    }

    /**
     * 获取当前worm时间
     * @return 当前worm时钟时间
     */
    public static Mono<Long> currentWormTimeMillis() {
        // 如果worm时钟没有初始化或者初始化失败依然财通当前系统时间
        return pool.getReactive(SysConstants.REDIS_SYSINFO_INDEX).hgetall("worm_conf")
                .defaultIfEmpty(new HashMap<>())
                .flatMap(wormInfo -> {
                    if (!"running".equals(wormInfo.get("worm_state"))) {
                        return Mono.just(System.currentTimeMillis());
                    }
                    return Mono.just(Long.parseLong(wormInfo.getOrDefault(WORM_CLOCK, String.valueOf(System.currentTimeMillis()))));
                });
    }

    /**
     * 检查桶是否开启了worm功能
     * @param bucketName 桶名
     */
    public static Boolean checkBucketWormEnable(String bucketName) {
        return pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hexists(bucketName, "lock_periodic");
    }

    public static Mono<Boolean> checkBucketWormEnableReactive (String bucketName) {
        return pool.getReactive(SysConstants.REDIS_BUCKETINFO_INDEX).hexists(bucketName, "lock_periodic");
    }

    public static long getWormExpire(MetaData metaData) {
        if (metaData.sysMetaData == null) {
            return 0;
        }
        Map<String, String> map = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        String expiration = map.getOrDefault(EXPIRATION, "0");
        return Long.parseLong(expiration);
    }


    /**
     * 检查对象是否在worm保护期内
     * @param request 多站点请求
     * @param expiration 过期时间
     * @param currentWormMills 当前worm时钟的时间
     */
    public static void checkWormRetention(MsHttpRequest request, long expiration, long currentWormMills) {
        if (request != null && request.headers().contains(IS_SYNCING)) {
            return;
        }
        if (expiration >= currentWormMills) {
            throw new MsException(ErrorNo.OBJECT_IMMUTABLE, "The current object is protected by WORM of the object.");
        }
    }

    /**
     * 获取桶的保护时间 响应式
     * @param bucketName 同名
     * @return 桶的保护时间
     */
    public static Mono<Long> getBucketLockTimeReactive(String bucketName) {
        return pool.getReactive(SysConstants.REDIS_BUCKETINFO_INDEX).hget(bucketName, "lock_periodic")
                .defaultIfEmpty("")
                .flatMap(periodic -> {
                    if (StringUtils.isEmpty(periodic)) {
                        return Mono.just(0L);
                    }
                    String[] strings = periodic.split(":");
                    if ("day".equals(strings[0])) {
                        int day = Integer.parseInt(strings[1]);
                        return Mono.just((long)day * ONE_DAY);
                    } else if ("year".equals(strings[0])) {
                        int year = Integer.parseInt(strings[1]);
                        return Mono.just((long)year * ONE_YEAR);
                    }
                    return Mono.just(0L);
                });
    }

    /**
     * 从桶信息中获取桶的保护时间
     * @param bucketInfo 桶信息
     * @return 桶的保护时间
     */
    public static long getBucketLockTimeCommand(Map<String, String> bucketInfo) {
        String lockPeriodic = bucketInfo.get("lock_periodic");
        if (StringUtils.isEmpty(lockPeriodic)) {
            return 0;
        }
        String[] periodic = lockPeriodic.split(":");
        String type = periodic[0];
        String value = periodic[1];
        if ("day".equals(type)) {
            int day = Integer.parseInt(value);
            return ONE_DAY * day;
        } else if ("year".equals(type)) {
            int year = Integer.parseInt(value);
            return ONE_YEAR * year;
        }
        return 0;
    }
}
