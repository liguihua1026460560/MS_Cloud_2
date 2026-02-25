package com.macrosan.utils.lifecycle;

import com.alibaba.fastjson.JSON;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.lifecycle.LifecycleConfiguration;
import com.macrosan.message.xmlmsg.lifecycle.Rule;
import com.macrosan.utils.msutils.MsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.macrosan.constants.SysConstants.LIFECYCLE_RECORD;
import static com.macrosan.constants.SysConstants.REDIS_TASKINFO_INDEX;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.lifecycle.LifecycleService.getStartStamp;

public class LifecycleUtils {
    public static final Logger logger = LogManager.getLogger(LifecycleUtils.class.getName());
    private static final long ONE_DAY_TIMESTAMPS = (60 * 60 * 24 * 1000);
    private static final long ONE_HOUR_TIMESTAMPS = (60 * 60 * 1000);
    private static final long ONE_MIN_TIMESTAMPS = (60 * 1000);
    private static List<DateTimeFormatter> pattrenList = new ArrayList<>();

    static {
        pattrenList.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm:ss"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm:ss'Z'"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm:ss.SSS"));
        pattrenList.add(DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm:ss.SSS'Z'"));
    }

    private static Map<String, Rule> parsePrefixRuleMap(LifecycleConfiguration lifecycleConfiguration) {
        Map<String, Rule> map = new HashMap<>();
        for (Rule rule : lifecycleConfiguration.getRules()) {
            if (map.containsKey(rule.getFilter().getPrefix())) {
                map.replace(rule.getFilter().getPrefix(), rule);
            } else {
                map.put(rule.getFilter().getPrefix(), rule);
            }
        }
        return map;
    }

    private static Map<String, Rule> parseIdRuleMap(LifecycleConfiguration lifecycleConfiguration) {
        Map<String, Rule> map = new HashMap<>();
        for (Rule rule : lifecycleConfiguration.getRules()) {
            if (rule.getId() != null) {
                if (map.containsKey(rule.getId())) {
                    map.replace(rule.getId(), rule);
                } else {
                    map.put(rule.getId(), rule);
                }
            }
        }
        return map;
    }

    public static boolean dateActivated(String date) {
        try {
            Objects.requireNonNull(date);
            long ruleTimestamps = parseDateToTimestamps(date);
            return ruleTimestamps <= System.currentTimeMillis();
        } catch (Exception e) {
            logger.error("unsupported date format:" + date);
            return false;
        }
    }

    public static LifecycleConfiguration mergeLifecycleConfiguration(LifecycleConfiguration oldConfig, LifecycleConfiguration newConfig) {
        Map<String, Rule> prefixRuleMap = parsePrefixRuleMap(oldConfig);
        Map<String, Rule> idRuleMap = parseIdRuleMap(oldConfig);
        for (Rule rule : newConfig.getRules()) {
            if (prefixRuleMap.containsKey(rule.getFilter().getPrefix())) {
                prefixRuleMap.replace(rule.getFilter().getPrefix(), rule);
            } else {
                if (rule.getId() != null) {
                    if (idRuleMap.containsKey(rule.getId())) {
                        Rule sameIdRule = idRuleMap.get(rule.getId());
                        prefixRuleMap.replace(sameIdRule.getFilter().getPrefix(), rule);
                    } else {
                        prefixRuleMap.put(rule.getFilter().getPrefix(), rule);
                    }
                } else {
                    prefixRuleMap.put(rule.getFilter().getPrefix(), rule);
                }
            }
        }

        LifecycleConfiguration lifecycleConfiguration = new LifecycleConfiguration(new LinkedList<>());
        for (Rule rule : prefixRuleMap.values()) {
            lifecycleConfiguration.getRules().add(rule);
        }
        return lifecycleConfiguration;
    }

    public static long parseDateToTimestamps(String dateStr) {
        LocalDateTime dateTime = null;
        logger.debug("LifecycleConfDate: " + dateStr);
        for (DateTimeFormatter dateTimeFormatter : pattrenList) {
            try {
                dateTime = LocalDateTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception ignore) {

            }
        }
        Objects.requireNonNull(dateTime);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli() - 8 * ONE_HOUR_TIMESTAMPS;
    }

    /**
     * 获得“今天”零点时间戳 获得2点的加上2个小时的毫秒数就行
     * 北京时间八点之前获取的是当天零点的时间戳，八点之后获取的是第二天零点的时间戳
     */
    private static long getTodayZeroPointTimestamps() {
        long currentTimestamps = System.currentTimeMillis();
        // 时间是否是当天时间戳
        boolean flag = currentTimestamps % ONE_DAY_TIMESTAMPS < 16 * ONE_HOUR_TIMESTAMPS;
        if (flag) {
            return currentTimestamps - (currentTimestamps) % ONE_DAY_TIMESTAMPS - 8 * ONE_HOUR_TIMESTAMPS;
        } else {
            return currentTimestamps - (currentTimestamps) % ONE_DAY_TIMESTAMPS + 16 * ONE_HOUR_TIMESTAMPS;
        }
    }

    public static long daysToDeadlineTimestamps(Integer days) {
        return getStartStamp() - ONE_DAY_TIMESTAMPS * days;
    }

    public static long getLifecycleStartStamp(String startDate) {
        String[] startDateSplit = startDate.split(":");
        int startHour = Integer.parseInt(startDateSplit[0]);
        int startMin = Integer.parseInt(startDateSplit[1]);
        return getTodayZeroPointTimestamps() + startHour * ONE_HOUR_TIMESTAMPS + startMin * ONE_MIN_TIMESTAMPS;
    }

    public static long getLifecycleEndStamp(String startDate, String endDate) {
        String[] startDateSplit = startDate.split(":");
        int startHour = Integer.parseInt(startDateSplit[0]);
        int startMin = Integer.parseInt(startDateSplit[1]);
        String[] endDateSplit = endDate.split(":");
        int endHour = Integer.parseInt(endDateSplit[0]);
        int endMin = Integer.parseInt(endDateSplit[1]);
        if (startHour < endHour) {
            return getTodayZeroPointTimestamps() + endHour * ONE_HOUR_TIMESTAMPS + endMin * ONE_MIN_TIMESTAMPS;
        }
        if (startHour == endHour) {
            if (startMin < endMin) {
                return getTodayZeroPointTimestamps() + endHour * ONE_HOUR_TIMESTAMPS + endMin * ONE_MIN_TIMESTAMPS;
            }
        }
        return getTodayZeroPointTimestamps() + ONE_DAY_TIMESTAMPS + endHour * ONE_HOUR_TIMESTAMPS + endMin * ONE_MIN_TIMESTAMPS;
    }

    /**
     * 日期和表达式的转化
     *
     * @param date 日期
     * @return
     */
    public static String makeDailyTrigger(String date) {
        String[] dateSplit = date.split(":");
        int hour = Integer.parseInt(dateSplit[0]);
        int min = Integer.parseInt(dateSplit[1]);
        return "0 " + min + " " + hour + " 1/1 * ? ";
    }


    public static String getLifecycleRecord(String bucketName, String vnode, String filterType, Map<String, String> tagMap, String prefix, boolean isMove, boolean isHistoryVersion, long timestamps, Integer days) {
        String method;
        String version;
        String record;
        String tagsJson = "";
        if (tagMap != null && "TAG".equals(filterType)) {
            tagsJson = JSON.toJSONString(tagMap);
        }
        if (isMove) {
            method = "move";
        } else {
            method = "del";
        }
        if (isHistoryVersion) {
            version = "history";
        } else {
            version = "current";
        }

        if (days == null) {
            switch (filterType) {
                case "PREFIX":
                    record = bucketName + File.separator + vnode + File.separator + prefix + File.separator + method + File.separator + version + File.separator + "date_" + timestamps;
                    break;
                case "TAG":
                    record = bucketName + File.separator + vnode + File.separator + tagsJson + File.separator + method + File.separator + version + File.separator + "date_" + timestamps;
                    break;
                default:
                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "unknown filter type");
            }
//            return bucketName + File.separator + vnode + File.separator + prefix + File.separator + method + File.separator + version + File.separator + "date_" + timestamps;
        } else {
            switch (filterType) {
                case "PREFIX":
                    record = bucketName + File.separator + vnode + File.separator + prefix + File.separator + method + File.separator + version + File.separator + "days_" + days;
                    break;
                case "TAG":
                    record = bucketName + File.separator + vnode + File.separator + tagsJson + File.separator + method + File.separator + version + File.separator + "days_" + days;
                    break;
                default:
                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "unknown filter type");
            }
//            return bucketName + File.separator + vnode + File.separator + prefix + File.separator + method + File.separator + version + File.separator + "days_" + days;
        }
        return record;

    }

    /*
     * 写入生命周期生产消息记录
     */
    public static void putLifecycleRecord(String bucketName, String vnode, String recordKey, MetaData metaData, RedisConnPool pool) {
        String record = getLifeCycleMetaKey(vnode, metaData.getBucket(), metaData.key, metaData.versionId, metaData.stamp);
        Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(bucketName + LIFECYCLE_RECORD, recordKey, record));
    }

}
