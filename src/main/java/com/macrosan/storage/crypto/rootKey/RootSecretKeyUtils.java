package com.macrosan.storage.crypto.rootKey;

import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import io.lettuce.core.KeyValue;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Calendar;
import java.util.List;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.storage.crypto.rootKey.RootSecretKeyConstants.*;
import static com.macrosan.storage.crypto.rootKey.RootSecretKeyManager.*;

/**
 * @Description: 用于外部调用根密钥加解密的工具类
 * @Author wanhao
 * @Date 2023/4/6 0006 下午 1:55
 */
@Log4j2
public class RootSecretKeyUtils {
    public static Tuple2<String, String> rootKeyEncrypt(String secretKey) {
        try {
            if (!INIT_FLAG.get() || KEY_VERSION_NUM.get() == 0) {
                return new Tuple2<>("0", secretKey);
            }
            String versionNum = String.valueOf(KEY_VERSION_NUM.get());
            String rootKey = ROOT_KEY_CACHE.get(versionNum);
            byte[] encrypt = CryptoUtils.encrypt(ROOT_KEY_ALGORTHM, rootKey, CryptoUtils.stringToBytes(secretKey));
            return new Tuple2<>(versionNum, CryptoUtils.bytesToString(encrypt));
        } catch (Exception e) {
            log.error("", e);
        }
        return new Tuple2<>("0", secretKey);
    }

    public static String rootKeyDecrypt(String secretKey, String versionNum) {
        try {
            if (StringUtils.isBlank(versionNum) || "0".equals(versionNum)) {
                return secretKey;
            }
            if (!INIT_FLAG.get()) {
                if (!ROOT_KEY_CACHE.containsKey(versionNum)) {
                    return secretKey;
                }
            }
            String rootKey = ROOT_KEY_CACHE.get(versionNum);
            byte[] decrypt = CryptoUtils.rootDecrypt(ROOT_KEY_ALGORTHM, rootKey, CryptoUtils.stringToBytes(secretKey));
            return CryptoUtils.bytesToString(decrypt);
        } catch (Exception e) {
            log.error("", e);
        }
        return secretKey;
    }

    /**
     * 根据开始时间和以及轮换周期，计算密钥的结束时间
     */
    static long getEndTime(long startTime, String rotationUnit, String rotationInterval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(startTime);
        if (!StringUtils.isNumeric(rotationInterval)) {
            log.info("roration interval error,use default roration interval");
            return startTime + DEFAULT_ROTATION_INTERVAL;
        }
        int l = Integer.parseInt(rotationInterval);
        switch (rotationUnit.toUpperCase()) {
            case "SECONDS":
                calendar.add(Calendar.SECOND, l);
                break;
            case "MINUTES":
                calendar.add(Calendar.MINUTE, l);
                break;
            case "HOURS":
                calendar.add(Calendar.HOUR, l);
                break;
            case "DAYS":
                calendar.add(Calendar.DAY_OF_YEAR, l);
                break;
            case "MONTHS":
                calendar.add(Calendar.MONTH, l);
                break;
            case "YEARS":
                calendar.add(Calendar.YEAR, l);
                break;
            default:
                calendar.add(Calendar.MILLISECOND, l);
                break;
        }
        long endTime = calendar.getTimeInMillis();
//        if (endTime - startTime < DEFAULT_MIN_ROTATION_INTERVAL || endTime - startTime > DEFAULT_MAX_ROTATION_INTERVAL) {
//            return startTime + DEFAULT_ROTATION_INTERVAL;
//        }
        return endTime;
    }

    /**
     * redis查询密钥的开始时间和轮换周期，计算密钥的结束时间
     */
    static long getEndTimeFromRedis() {
        //redis主节点离线后，发生主从切换，此阶段其他节点已经更新主redis的密钥信息时，可能还未同步到从节点，此时节点去从redis获取数据后会重新触
        List<KeyValue<String, String>> values = POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmget(REDIS_SECRET_KEY, "startTime", "rotationUnit", "rotationInterval");
        String startTimeStr = String.valueOf(System.currentTimeMillis());
        String rotationUnit = "DAYS";
        String rotationInterval = "365";
        for (KeyValue<String, String> value : values) {
            if ("startTime".equals(value.getKey())) {
                startTimeStr = value.getValue();
            } else if ("rotationUnit".equals(value.getKey())) {
                rotationUnit = value.getValue();
            } else {
                rotationInterval = value.getValue();
            }
        }
        long startTime = Long.parseLong(startTimeStr);
        return getEndTime(startTime, rotationUnit, rotationInterval);
    }

    static long getEndTimeFromRedis(long startTime) {
        List<KeyValue<String, String>> values = POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmget(REDIS_SECRET_KEY, "rotationUnit", "rotationInterval");
        String rotationUnit = "DAYS";
        String rotationInterval = "365";
        for (KeyValue<String, String> value : values) {
            if ("rotationUnit".equals(value.getKey())) {
                rotationUnit = value.getValue();
            } else {
                rotationInterval = value.getValue();
            }
        }
        return getEndTime(startTime, rotationUnit, rotationInterval);
    }
}
