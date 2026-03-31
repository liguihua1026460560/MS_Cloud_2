package com.macrosan.utils.lifecycle;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.xmlmsg.lifecycle.*;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.MossHttpClient.ASYNC_INDEX_IPS_MAP;
import static com.macrosan.httpserver.MossHttpClient.INDEX_NAME_MAP;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;
import static com.macrosan.utils.regex.PatternConst.*;

public class LifecycleValidation {
    private static final Logger logger = LogManager.getLogger(LifecycleValidation.class.getName());
    private static RedisConnPool pool = RedisConnPool.getInstance();
    private static final long ONE_DAY_TIMESTAMPS = (long) (60 * 60 * 24 * 1000);
    private static final long TEN_MINITE_TIMESTAMPS = (long) (10 * 60 * 1000);
    private static final int MAX_RULES_COUNT = 1000;
    private static final int MAX_ID_LENGHT = 255;
    private static final String EXPIRATION = "Expiration";
    private static final String TRANSITION = "Transition";
    private static final String REPLICATION = "Replication";
    private static final String NON_EXPIRATION = "NoncurrentVersionExpiration";
    private static final String NON_TRANSITION = "NoncurrentVersionTransition";
    private static final String NON_REPLICATION = "NoncurrentVersionReplication";
    private static final String DAYS = "Days>";
    private static final String NON_CURRENT_DAYS = "NoncurrentDays>";
    private static List<DateTimeFormatter> pattrenList = new ArrayList<>();
    protected static Map<String, List<Long>> time_map;
    private static Set<String> idSet;
    protected static Boolean days_sign;
    protected static Boolean date_sign;
    protected static String lifecycleStr = "";
    protected static String prefixStr = "";
    protected static Map<String, String> tagMap;
    protected static final String SPLIT = "_";
    protected static String expTagFlag = "";
    protected static Map<String, List<Long>> transDatePrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Long>> expDatePrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Long>> repDatePrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> transDaysPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> expDaysPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> repDaysPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> noncurrentTransPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> noncurrentExpPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, List<Integer>> noncurrentRepPrefixMap = new ConcurrentHashMap<>();
    protected static Map<String, Map<String, String>> dayAndDateTagMap = new ConcurrentHashMap<>();
    protected static Map<String, Map<String, Map<String, String>>> dayAndDateTagRepMap = new ConcurrentHashMap<>();
    protected static Map<String, String> prefixRepMap = new ConcurrentHashMap<>();

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

    public static void validateLifecycleConfig(String body, LifecycleConfiguration lifecycleConfiguration, boolean backup) {
        try {
            Objects.requireNonNull(lifecycleConfiguration);
            lifecycleStr = body;
            transDatePrefixMap.clear();
            expDatePrefixMap.clear();
            repDatePrefixMap.clear();
            transDaysPrefixMap.clear();
            repDaysPrefixMap.clear();
            expDaysPrefixMap.clear();
            noncurrentTransPrefixMap.clear();
            noncurrentExpPrefixMap.clear();
            noncurrentRepPrefixMap.clear();
            dayAndDateTagMap.clear();
            dayAndDateTagRepMap.clear();
            prefixRepMap.clear();
            validateRules(lifecycleConfiguration.getRules(), backup);
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("LifecycleConfiguration");
        }
    }


    private static void validateRules(List<Rule> rules, boolean backup) {

        try {
            Objects.requireNonNull(rules);
            if (rules.size() > MAX_RULES_COUNT) {
                throwInvalidBucketStoragePolicyException("Too much rules.");
            }
            idSet = new HashSet<>();
            boolean[] existsPrefix = new boolean[]{false};
            boolean[] existsTag = new boolean[]{false};
            rules.forEach(rule -> {
                validateRule(rule, backup);
                if (rule.getFilter() != null) {
                    if (null != rule.getFilter().getPrefix()) {
                        existsPrefix[0] = true;
                    }
                    if (null != rule.getFilter().getTags()) {
                        existsTag[0] = true;
                    }
                    if (null == rule.getFilter().getPrefix() && null == rule.getFilter().getTags()) {
                        existsPrefix[0] = true;
                    }
                } else {
                    existsPrefix[0] = true;
                }
            });
            if (!backup && existsPrefix[0] && existsTag[0]) {
                throw new MsException(RULE_CONFLICT, "The set rules conflicts with the setting rules");
            }
            if (backup && !dayAndDateTagRepMap.keySet().isEmpty()) {
                for (String key : dayAndDateTagRepMap.keySet()) {
                    if (prefixRepMap.containsKey(key)) {
                        throw new MsException(RULE_CONFLICT, "The set rules conflicts with the setting rules");
                    }
                }
            }
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("Rules");
        }

    }

    private static void validateRule(Rule rule, boolean backup) {
        try {
            Objects.requireNonNull(rule);
            if (rule.getId() != null) {
                if (rule.getId().length() > MAX_ID_LENGHT) {
                    throw new MsException(ErrorNo.ID_LENGTH_EXCEED, "ID length should not exceed allowed limit of 255");
                }
                if (!idSet.add(rule.getId())) {
                    throw new MsException(ErrorNo.LIFECYCLE_INVALID_ID, "Rule ID must be unique. Found same ID for more than one rule");
                }
                if (!LIFECYCLE_RULE_ID.matcher(rule.getId()).matches()) {
                    throw new MsException(LIFECYCLE_INVALID_ID_CHAR, "The ID contains illegal characters.");
                }
            } else {
                String id = getRandomChat();
                while (!idSet.add(id)) {
                    id = getRandomChat();
                }
                rule.setId(id);
            }
            if (rule.getFilter() != null && rule.getPrefix() != null) {
                throw new MsException(ErrorNo.LIFECYCLE_PREFIX_CONFICT, "Prefix cannot be used with Filter.");
            }
            String prefix;
            if (rule.getFilter() != null) {
                prefix = rule.getFilter().getPrefix();
            } else {
                prefix = rule.getPrefix();
            }
            prefixStr = prefix;
            if (StringUtils.isNotEmpty(prefix) && prefix.length() > 1024) {
                throw new MsException(INVALID_LIFECYCLE_PREFIX, "The maximum size of a prefix is 1024.");
            }

            if (StringUtils.isNotEmpty(prefix) && !OBJECT_NAME_PATTERN.matcher(prefix).matches()) {
                throw new MsException(INVALID_LIFECYCLE_PREFIX_CHAR, "The prefix contains illegal characters.");
            }
            if (rule.getFilter() != null) {
                List<Tag> tags = rule.getFilter().getTags();

                if (tags != null && StringUtils.isNotEmpty(prefix)) {
                    throw new MsException(INVALID_LIFECYCLE_RULE, "prefix and tag cannot exist simultaneously");
                }
                if (tags != null) {
                    for (Tag tag : tags) {
                        if (StringUtils.isEmpty(tag.getKey()) || StringUtils.isEmpty(tag.getValue())) {
                            throw new MsException(INVALID_LIFECYCLE_TAG, "the key or value of tags cannot be empty");
                        }
                    }

                    for (int i = 0; i < tags.size() - 1; i++) {
                        for (int j = i + 1; j < tags.size(); j++) {
                            if (tags.get(i).getKey().equals(tags.get(j).getKey())) {
                                //存在重复设置相同的key
                                throw new MsException(INVALID_LIFECYCLE_TAG, "There are duplicate tags");
                            }
                        }
                    }
                }
                if (tags != null) {
                    tagMap = new HashMap<>();
                    for (Tag tag : tags) {
                        String key = tag.getKey();
                        String value = tag.getValue();
                        tagMap.put(key, value);
                    }
                }
            }

            Objects.requireNonNull(rule.getStatus());
            if (!Rule.ENABLED.equals(rule.getStatus()) && !Rule.DISABLED.equals(rule.getStatus())) {
                throwInvalidBucketStoragePolicyException("Status");
            }
            validateConditions(rule.getConditionList(), backup);
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("Rule");
        }
    }

    // 判断交叉前缀是否有相同日期
    private static void validatePrefix(Long date, String type) {
        if (TRANSITION.equals(type)) {
            for (String key : transDatePrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (transDatePrefixMap.get(key).contains(date)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            transDatePrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(date);
            for (String key : expDatePrefixMap.keySet()) {
                if (prefixStr.startsWith(key)) {
                    List<Long> prefixList = expDatePrefixMap.get(key);
                    for (Long time : prefixList) {
                        if (date.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else if (time < date) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                        }
                    }
                }
            }
        } else if (EXPIRATION.equals(type)) {
            for (String key : expDatePrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (expDatePrefixMap.get(key).contains(date)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Expiration with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            expDatePrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(date);
            for (String key : transDatePrefixMap.keySet()) {
                if (key.startsWith(prefixStr)) {
                    List<Long> prefixList = transDatePrefixMap.get(key);
                    for (Long time : prefixList) {
                        if (date.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else if (time > date) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                        }
                    }
                }
            }
        }
    }

    private static void validateBackUpPrefix(Long date, String sourceSite, String backupSite) {
        String prefix = sourceSite + "_" + backupSite + "_" + prefixStr;
        String noPrefix = backupSite + "_" + sourceSite + "_" + prefixStr;
        for (String key : repDatePrefixMap.keySet()) {
            if (key.startsWith(prefix) || prefix.startsWith(key)) {
                if (repDatePrefixMap.get(key).contains(date)) {
                    throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Replication with cross prefix cannot be set to occur at the same time");
                }
            }
            if (key.startsWith(noPrefix) || noPrefix.startsWith(key)) {
                throw new MsException(ErrorNo.INVALID_BACKUP_RULE, "Replication with cross prefix cannot be set to occur at the opposite of sourceSite and backupSite");
            }
        }
        repDatePrefixMap.computeIfAbsent(prefix, i -> new ArrayList<>()).add(date);
    }

    private static void validatePrefix(Integer day, String type) {
        if (TRANSITION.equals(type)) {
            for (String key : transDaysPrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (transDaysPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            transDaysPrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(day);
            for (String key : expDaysPrefixMap.keySet()) {
                if (prefixStr.startsWith(key)) {
                    List<Integer> prefixList = expDaysPrefixMap.get(key);
                    for (Integer time : prefixList) {
                        if (day.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else if (time < day) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                        }
                    }
                }
            }
        } else if (EXPIRATION.equals(type)) {
            for (String key : expDaysPrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (expDaysPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Expiration with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            expDaysPrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(day);
            for (String key : transDaysPrefixMap.keySet()) {
                if (key.startsWith(prefixStr)) {
                    List<Integer> prefixList = transDaysPrefixMap.get(key);
                    for (Integer time : prefixList) {
                        if (day.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else if (time > day) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                        }
                    }
                }
            }
        }
    }

    private static void validateBackUpPrefix(Integer day, String type, String sourceSite, String backupSite) {
        String prefix = sourceSite + "_" + backupSite + "_" + prefixStr;
        String noPrefix = backupSite + "_" + sourceSite + "_" + prefixStr;
        if (REPLICATION.equals(type)) {
            for (String key : repDaysPrefixMap.keySet()) {
                if (key.startsWith(prefix) || prefix.startsWith(key)) {
                    if (repDaysPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Replication with cross prefix cannot be set to occur at the same time");
                    }
                }
                if (key.startsWith(noPrefix) || noPrefix.startsWith(key)) {
                    throw new MsException(ErrorNo.INVALID_BACKUP_RULE, "Replication with cross prefix cannot be set to occur at the opposite of sourceSite and backupSite");
                }
            }
            repDaysPrefixMap.computeIfAbsent(prefix, i -> new ArrayList<>()).add(day);
        } else if (NON_REPLICATION.equals(type)) {
            for (String key : noncurrentRepPrefixMap.keySet()) {
                if (key.startsWith(prefix) || prefix.startsWith(key)) {
                    if (noncurrentRepPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Noncurrent Replication with cross prefix cannot be set to occur at the same time");
                    }
                }
                if (key.startsWith(noPrefix) || noPrefix.startsWith(key)) {
                    throw new MsException(ErrorNo.INVALID_BACKUP_RULE, "Replication with cross prefix cannot be set to occur at the opposite of sourceSite and backupSite");
                }
            }
            noncurrentRepPrefixMap.computeIfAbsent(prefix, i -> new ArrayList<>()).add(day);
        }
    }

    // 判断前缀是否有交叉
    private static void validateNoncurrentVersionPrefix(Integer day, String type) {
        if (TRANSITION.equals(type)) {
            for (String key : noncurrentTransPrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (noncurrentTransPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Noncurrent Transition with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            noncurrentTransPrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(day);
            for (String key : noncurrentExpPrefixMap.keySet()) {
                if (prefixStr.startsWith(key)) {
                    List<Integer> prefixList = noncurrentExpPrefixMap.get(key);
                    for (Integer time : prefixList) {
                        if (day.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Noncurrent Transition and Expiration cannot be set to occur at the same time");
                        } else if (time < day) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Noncurrent Expiration time cannot be earlier than the Noncurrent Transition time");
                        }
                    }
                }
            }
        } else if (EXPIRATION.equals(type)) {
            for (String key : noncurrentExpPrefixMap.keySet()) {
                if (key.startsWith(prefixStr) || prefixStr.startsWith(key)) {
                    if (noncurrentExpPrefixMap.get(key).contains(day)) {
                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Noncurrent Expiration with cross prefix cannot be set to occur at the same time");
                    }
                }
            }
            noncurrentExpPrefixMap.computeIfAbsent(prefixStr, i -> new ArrayList<>()).add(day);
            for (String key : noncurrentTransPrefixMap.keySet()) {
                if (key.startsWith(prefixStr)) {
                    List<Integer> prefixList = noncurrentTransPrefixMap.get(key);
                    for (Integer time : prefixList) {
                        if (day.equals(time)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Noncurrent Transition and Expiration cannot be set to occur at the same time");
                        } else if (time > day) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Noncurrent Expiration time cannot be earlier than the Noncurrent Transition time");
                        }
                    }
                }
            }
        }
    }

    //判断相同tag是否存在相同过期时间
    private static void validateTag(String key) {
        dayAndDateTagMap.compute(key, (k, v) -> {
            if (v == null) {
                v = tagMap;
            } else {
                if (mapsEqual(tagMap, v)) {
                    throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition with same tag cannot be set to occur at the same time");
                }
            }
            return v;
        });
        //判断相同tag迁移是否在过期之前
        String dayOrDate = key.split(SPLIT)[0];
        int day = 0;
        long date = 0;
        if (Pattern.compile("[0-9]*").matcher(dayOrDate).matches()) {
            day = Integer.parseInt(dayOrDate);
        } else {
            date = LifecycleUtils.parseDateToTimestamps(dayOrDate);
        }
        String type = key.split(SPLIT)[1];
        validateExpAndTransition(dayOrDate, day, date, type);
        validateNoExpAndTransition(dayOrDate, day, type);
    }

    private static void validateBackupTag(String dayOrDate, String type, String sourceSite, String backupSite) {
        String prefix = sourceSite + "_" + backupSite;
        String noPrefix = backupSite + "_" + sourceSite;
        String key = dayOrDate + SPLIT + type;
        dayAndDateTagRepMap.compute(prefix, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
                v.put(key, tagMap);
            } else {
                v.compute(key, (k1, v1) -> {
                    if (v1 == null) {
                        v1 = tagMap;
                    } else {
                        if (mapsEqual(tagMap, v1)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, type + " with cross tag cannot be set to occur at the same time");
                        }
                    }
                    return v1;
                });
            }
            return v;
        });
        if (dayAndDateTagRepMap.containsKey(noPrefix)) {
            dayAndDateTagRepMap.get(noPrefix).forEach((k, v) -> {
                String kType = k.split(SPLIT)[1];
                if (type.equals(kType) && mapsEqual(tagMap, v)) {
                    throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, type + "with cross tag cannot be set to occur at the opposite of sourceSite and backupSite");
                }
            });
        }
    }

    private static void validateExpAndTransition(String dayOrDate, int day, long date, String type) {
        if (type.equals(LifecycleValidation.EXPIRATION)) {
            for (Map.Entry<String, Map<String, String>> entry : dayAndDateTagMap.entrySet()) {
                String expType = entry.getKey().split(SPLIT)[1];
                if (expType.equals(LifecycleValidation.TRANSITION)) {
                    String tranTime = entry.getKey().split(SPLIT)[0];
                    Map<String, String> map1 = entry.getValue();
                    if (mapsEqual(tagMap, map1)) {
                        if (dayOrDate.equals(tranTime)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else {
                            if (Pattern.compile("[0-9]*").matcher(tranTime).matches()) {
                                int tranDay = Integer.parseInt(tranTime);
                                if (date == 0) {
                                    if (day < tranDay) {
                                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                                    }
                                }
                            } else {
                                long tranDate = LifecycleUtils.parseDateToTimestamps(tranTime);
                                if (day == 0) {
                                    if (date < tranDate) {
                                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if (type.equals(LifecycleValidation.TRANSITION)) {
            for (Map.Entry<String, Map<String, String>> entry : dayAndDateTagMap.entrySet()) {
                String expType = entry.getKey().split(SPLIT)[1];
                if (expType.equals(LifecycleValidation.EXPIRATION)) {
                    String expTime = entry.getKey().split(SPLIT)[0];
                    Map<String, String> map1 = entry.getValue();
                    if (mapsEqual(tagMap, map1)) {
                        if (dayOrDate.equals(expTime)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else {
                            if (Pattern.compile("[0-9]*").matcher(expTime).matches()) {
                                int expDay = Integer.parseInt(expTime);
                                if (date == 0) {
                                    if (expDay < day) {
                                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                                    }
                                }
                            } else {
                                if (day == 0) {
                                    long expDate = LifecycleUtils.parseDateToTimestamps(expTime);
                                    if (expDate < date) {
                                        throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static void validateNoExpAndTransition(String tranDayOrDate, int tranDay, String type) {
        if (type.equals(LifecycleValidation.NON_TRANSITION)) {
            for (Map.Entry<String, Map<String, String>> entry : dayAndDateTagMap.entrySet()) {
                String expType = entry.getKey().split(SPLIT)[1];
                if (expType.equals(LifecycleValidation.NON_EXPIRATION)) {
                    String expTime = entry.getKey().split(SPLIT)[0];
                    Map<String, String> map1 = entry.getValue();
                    if (mapsEqual(tagMap, map1)) {
                        if (tranDayOrDate.equals(expTime)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else {
                            if (Integer.parseInt(expTime) < tranDay) {
                                throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                            }
                        }
                    }
                }
            }
        }

        if (type.equals(LifecycleValidation.NON_EXPIRATION)) {
            for (Map.Entry<String, Map<String, String>> entry : dayAndDateTagMap.entrySet()) {
                String expType = entry.getKey().split(SPLIT)[1];
                if (expType.equals(LifecycleValidation.NON_TRANSITION)) {
                    String expTime = entry.getKey().split(SPLIT)[0];
                    Map<String, String> map1 = entry.getValue();
                    if (mapsEqual(tagMap, map1)) {
                        if (tranDayOrDate.equals(expTime)) {
                            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
                        } else {
                            if (Integer.parseInt(expTime) > tranDay) {
                                throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
                            }
                        }
                    }
                }
            }
        }
    }

    private static boolean mapsEqual(Map<String, String> map1, Map<String, String> map2) {
        boolean flag = true;
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            String k1 = entry.getKey();
            String v1 = entry.getValue();
            if (!map2.containsKey(k1) || !Objects.equals(map2.get(k1), v1)) {
                flag = false;
                break;
            }
        }
        return flag;
    }

    private static void validateConditions(List<Condition> conditionList, boolean backup) {
        try {
            Objects.requireNonNull(conditionList);
            if (time_map != null) {
                time_map.clear();
            } else {
                time_map = new HashMap<>();
            }
            date_sign = false;
            days_sign = false;
            conditionList.forEach(list -> LifecycleValidation.validateCondition(list, backup));
            if (days_sign && date_sign) {
                throw new MsException(ErrorNo.LIFECYCLE_MIX_DATE_AND_DAYS, "Found mixed 'Date' and 'Days' based Expiration or Transition actions in lifecycle rule for prefix " + prefixStr);
            }
            if (time_map.get(EXPIRATION) != null && time_map.get(TRANSITION) != null) {
                checkListTime(time_map.get(EXPIRATION), time_map.get(TRANSITION));
            }
            if (time_map.get(NON_EXPIRATION) != null && time_map.get(NON_TRANSITION) != null) {
                checkListTime(time_map.get(NON_EXPIRATION), time_map.get(NON_TRANSITION));
            }
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("Conditions");
        }
    }

    private static void validateCondition(Condition condition, boolean backup) {
        try {
            Objects.requireNonNull(condition);
            if (backup) {
                if (condition instanceof Replication) {
                    validateReplication((Replication) condition);
                } else if (condition instanceof NoncurrentVersionReplication) {
                    validateNoncurrentVersionReplication((NoncurrentVersionReplication) condition);
                } else {
                    throwInvalidBucketStoragePolicyException("Condition");
                }
            } else {
                if (condition instanceof Expiration) {
                    validateExpiration((Expiration) condition);
                } else if (condition instanceof Transition) {
                    validateTransition((Transition) condition);
                } else if (condition instanceof NoncurrentVersionTransition) {
                    validateNoncurrentVersionTransition((NoncurrentVersionTransition) condition);
                } else if (condition instanceof NoncurrentVersionExpiration) {
                    validateNoncurrentVersionExpiration((NoncurrentVersionExpiration) condition);
                } else {
                    throwInvalidBucketStoragePolicyException("Condition");
                }
            }
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("Condition");
        }
    }

    private static void validateNoncurrentVersionExpiration(NoncurrentVersionExpiration condition) {
        Objects.requireNonNull(condition.getNoncurrentDays());
        checkDaysAndNoncurrentDays(condition.getNoncurrentDays(), false);
        if (prefixStr == null) {
            addTimestampsToMap(condition.getNoncurrentDays(), NON_EXPIRATION);
            if (tagMap != null) {
                expTagFlag = SPLIT + NON_EXPIRATION;
                validateTag(condition.getNoncurrentDays() + expTagFlag);
            }
        } else {
            validateNoncurrentVersionPrefix(condition.getNoncurrentDays(), EXPIRATION);
        }
    }

    private static void validateNoncurrentVersionTransition(NoncurrentVersionTransition condition) {
        Objects.requireNonNull(condition.getNoncurrentDays());
        Objects.requireNonNull(condition.getStorageClass());
        checkDaysAndNoncurrentDays(condition.getNoncurrentDays(), false);
        checkStorageClass(condition.getStorageClass());
        if (prefixStr == null) {
            addTimestampsToMap(condition.getNoncurrentDays(), NON_TRANSITION);
            if (tagMap != null) {
                expTagFlag = SPLIT + NON_TRANSITION;
                validateTag(condition.getNoncurrentDays() + expTagFlag);
            }
        } else {
            validateNoncurrentVersionPrefix(condition.getNoncurrentDays(), TRANSITION);
        }
    }

    private static void validateTransition(Transition condition) {
        Objects.requireNonNull(condition.getStorageClass());
        checkStorageClass(condition.getStorageClass());
        expTagFlag = SPLIT + TRANSITION;
        if (condition.getDays() == null) {
            date_sign = true;
            Objects.requireNonNull(condition.getDate());
            validateDate(condition.getDate());
            String formatedDate = formatDate(condition.getDate());
            condition.setDate(formatedDate);
            long timestamps = LifecycleUtils.parseDateToTimestamps(formatedDate);
            if (prefixStr == null) {
                addTimestampsToMap(condition.getDate(), TRANSITION);
                if (tagMap != null) {
                    validateTag(condition.getDate() + expTagFlag);
                }
            } else {
                validatePrefix(timestamps, TRANSITION);
            }
        } else if (condition.getDate() == null) {
            days_sign = true;
            Objects.requireNonNull(condition.getDays());
            if (!BUCKET_LIFEDAYS_PATTERN.matcher(condition.getDays() + "").matches()) {
                throwInvalidBucketStoragePolicyException(TRANSITION);
            }
            checkDaysAndNoncurrentDays(condition.getDays(), true);
            if (prefixStr == null) {
                addTimestampsToMap(condition.getDays(), TRANSITION);
                if (tagMap != null) {
                    validateTag(condition.getDays() + expTagFlag);
                }
            } else {
                validatePrefix(condition.getDays(), TRANSITION);
            }
        } else {
            throwInvalidBucketStoragePolicyException(TRANSITION);
        }
    }

    private static void validateExpiration(Expiration condition) {
        try {
            expTagFlag = SPLIT + EXPIRATION;
            if (condition.getDate() == null) {
                days_sign = true;
                Objects.requireNonNull(condition.getDays());
                if (!BUCKET_LIFEDAYS_PATTERN.matcher(condition.getDays() + "").matches()) {
                    throwInvalidBucketStoragePolicyException(EXPIRATION);
                }
                checkDaysAndNoncurrentDays(condition.getDays(), true);
                if (prefixStr == null) {
                    addTimestampsToMap(condition.getDays(), EXPIRATION);
                    if (tagMap != null) {
                        validateTag(condition.getDays() + expTagFlag);
                    }
                } else {
                    validatePrefix(condition.getDays(), EXPIRATION);
                }
            } else if (condition.getDays() == null) {
                date_sign = true;
                Objects.requireNonNull(condition.getDate());
                validateDate(condition.getDate());
                String formatedDate = formatDate(condition.getDate());
                condition.setDate(formatedDate);
                long timestamps = LifecycleUtils.parseDateToTimestamps(formatedDate);
                if (prefixStr == null) {
                    addTimestampsToMap(condition.getDate(), EXPIRATION);
                    if (tagMap != null) {
                        validateTag(condition.getDate() + expTagFlag);
                    }
                } else {
                    validatePrefix(timestamps, EXPIRATION);
                }
            } else {
                throwInvalidBucketStoragePolicyException(EXPIRATION);
            }
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException(EXPIRATION);
        }
    }

    // 检测天数是否超出范围
    private static void checkDaysAndNoncurrentDays(Integer days, Boolean flag) {
        String sign = flag ? DAYS : NON_CURRENT_DAYS;
        int first = lifecycleStr.indexOf(sign);
        String substring = lifecycleStr.substring(first + sign.length());
        int second = substring.indexOf(sign);
        System.out.println(second);
        String daysStr = lifecycleStr.substring(first + sign.length(), first + sign.length() + second - 2);
        if (days < 1) {
            throwInvalidBucketStoragePolicyException(sign);
        } else if (!daysStr.equals(days + "")) {
            throwInvalidBucketStoragePolicyException(sign);
        }
        lifecycleStr = lifecycleStr.substring(first + second + sign.length() * 2);
    }

    // 检测过期时间是否大于迁移时间
    private static void checkListTime(List<Long> expirationList, List<Long> transitionList) {
        if (expirationList.size() > 0 && transitionList.size() > 0) {
            Long min = Collections.min(expirationList);
            Long max = Collections.max(transitionList);
            if (min < max) {
                throw new MsException(ErrorNo.LIFECYCLE_INVALID_ARGUMENT, "The Expiration time cannot be earlier than the Transition time");
            }
            disjointTime(expirationList, transitionList);
        }
    }

    // 判断两个集合是否有日期重复
    private static void disjointTime(List<Long> list1, List<Long> list2) {
        if (list1.size() > 0 && list2.size() > 0) {
            if (!Collections.disjoint(list1, list2)) {
                throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, "Transition and Expiration cannot be set to occur at the same time");
            }
        }
    }

    public static String formatDate(String date) {
        long timestamps = parseDateToTimestamps(date);
        Instant instant = Instant.ofEpochMilli(timestamps);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    }

    public static void validateDate(String date) {
        try {
            long timestamps = parseDateToTimestamps(date);
            if (timestamps % ONE_DAY_TIMESTAMPS != 0) {
                throw new MsException(INVALID_DATE, "Date must be at midnight GMT.");
            }
        } catch (NullPointerException e) {
            throwInvalidBucketStoragePolicyException("Date");
        }
    }

    public static long parseDateToTimestamps(String dateStr) {
        LocalDateTime dateTime = null;
        for (DateTimeFormatter dateTimeFormatter : pattrenList) {
            try {
                dateTime = LocalDateTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception e) {
                //ignore
            }
        }
        Objects.requireNonNull(dateTime);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    // 检查存储策略是否存在
    private static void checkStorageClass(String storageStrategy) {
        Set<String> keySet = POOL_STRATEGY_MAP.keySet();
        boolean flag = true;
        for (String key : keySet) {
            if (key.equals(storageStrategy)) {
                flag = false;
                break;
            }
        }
        if (flag) {
            throw new MsException(ErrorNo.INVALID_STORAGE_CLASS, "Invalid lifecycle storage strategy");
        }
    }

    private static void checkSourceClusterName(String clusterName) {
        boolean flag = true;
        final Set<Integer> indexSet = INDEX_NAME_MAP.keySet();
        for (Integer index : indexSet) {
            if (INDEX_NAME_MAP.get(index).equals(clusterName)) {
                if (!ASYNC_INDEX_IPS_MAP.containsKey(index)) {
                    flag = false;
                    break;
                }
            }
        }

        if (flag) {
            throw new MsException(ErrorNo.INVALID_SOURCE_CLUSTER, "Invalid lifecycle source cluster name");
        }
    }

    private static void checkBackUpClusterName(String clusterName) {
        boolean flag = false;
        final String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        if (localCluster.equals(clusterName)) {
            flag = true;
        } else {
            final Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
            for (String otherCluster : otherClusters) {
                JSONObject cluster = JSON.parseObject(otherCluster);
                if (cluster.getString(CLUSTER_NAME).equals(clusterName)) {
                    flag = true;
                    break;
                }
            }
        }
        if (!flag) {
            throw new MsException(ErrorNo.INVALID_BACKUP_CLUSTER, "Invalid lifecycle backup cluster name");
        }
    }

    private static void addTimestampsToMap(Integer days, String key) {
        Long daysL = Long.valueOf(days.toString());
        if (time_map.get(key) != null && time_map.get(key).contains(daysL)) {
            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, key + " cannot be set to occur at the same days");
        }
        time_map.computeIfAbsent(key, l -> new ArrayList<>());
        time_map.get(key).add(daysL);
    }

    private static void addTimestampsToMap(String date, String key) {
        long timestamps = LifecycleUtils.parseDateToTimestamps(date);
        if (time_map.get(key) != null && time_map.get(key).contains(timestamps)) {
            throw new MsException(ErrorNo.LIFECYCLE_INVALID_REQUEST, key + " cannot be set to occur at the same date");
        }
        time_map.computeIfAbsent(key, l -> new ArrayList<>());
        time_map.get(key).add(timestamps);
    }

    private static void throwInvalidBucketStoragePolicyException(String msg) {
        throw new MsException(ErrorNo.MALFORMED_ERROR, msg);
    }

    public static HashSet getTransitionStrategys(LifecycleConfiguration lifecycleConfiguration) {
        HashSet<String> transitionStrategySet = new HashSet<>();
        if (lifecycleConfiguration == null) {
            return transitionStrategySet;
        }
        for (Rule rule : lifecycleConfiguration.getRules()) {
            if (!Rule.ENABLED.equals(rule.getStatus())) {
                continue;
            }
            for (Condition condition : rule.getConditionList()) {
                if (condition instanceof Transition) {
                    transitionStrategySet.add(((Transition) condition).getStorageClass());
                } else if (condition instanceof NoncurrentVersionTransition) {
                    transitionStrategySet.add(((NoncurrentVersionTransition) condition).getStorageClass());
                }
            }
        }
        return transitionStrategySet;
    }

    // 生成随机10位字母
    public static String getRandomChat() {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 10; ++i) {
            long round = Math.round(Math.random() * +65);
            System.out.println(round);
            String s = String.valueOf((char) round);
            System.out.println(s);
            buffer.append(s);
        }
        return buffer.append(System.currentTimeMillis()).toString();
    }

    private static void validateReplication(Replication condition) {
        final String sourceSite = condition.getSourceSite();
        final String backupSite = condition.getBackupSite();
        Objects.requireNonNull(sourceSite);
        checkSourceClusterName(sourceSite);
        Objects.requireNonNull(backupSite);
        checkBackUpClusterName(backupSite);
        if (sourceSite.equals(backupSite)) {
            throw new MsException(ErrorNo.INVALID_BACKUP_RULE, "Replication with same sourceSite and backupSite");
        }
        if (condition.getDays() == null) {
            date_sign = true;
            Objects.requireNonNull(condition.getDate());
            validateDate(condition.getDate());
            String formatedDate = formatDate(condition.getDate());
            condition.setDate(formatedDate);
            long timestamps = LifecycleUtils.parseDateToTimestamps(formatedDate);
            if (prefixStr == null) {
                addTimestampsToMap(condition.getDate(), REPLICATION);
                if (tagMap != null) {
                    validateBackupTag(condition.getDate(), REPLICATION, sourceSite, backupSite);
                }
            } else {
                String prefix = sourceSite + "_" + backupSite;
                prefixRepMap.put(prefix, prefixStr);
                validateBackUpPrefix(timestamps, sourceSite, backupSite);
            }
        } else if (condition.getDate() == null) {
            days_sign = true;
            Objects.requireNonNull(condition.getDays());
            if (!BUCKET_LIFEDAYS_PATTERN.matcher(condition.getDays() + "").matches()) {
                throwInvalidBucketStoragePolicyException(REPLICATION);
            }
            checkDaysAndNoncurrentDays(condition.getDays(), true);
            if (prefixStr == null) {
                addTimestampsToMap(condition.getDays(), REPLICATION);
                if (tagMap != null) {
                    validateBackupTag(condition.getDays() + "", REPLICATION, sourceSite, backupSite);
                }
            } else {
                String prefix = sourceSite + "_" + backupSite;
                prefixRepMap.put(prefix, prefixStr);
                validateBackUpPrefix(condition.getDays(), REPLICATION, sourceSite, backupSite);
            }
        } else {
            throwInvalidBucketStoragePolicyException(REPLICATION);
        }
    }


    private static void validateNoncurrentVersionReplication(NoncurrentVersionReplication condition) {
        final String sourceSite = condition.getSourceSite();
        final String backupSite = condition.getBackupSite();
        Objects.requireNonNull(condition.getNoncurrentDays());
        Objects.requireNonNull(sourceSite);
        checkSourceClusterName(sourceSite);
        Objects.requireNonNull(backupSite);
        checkBackUpClusterName(backupSite);
        if (sourceSite.equals(backupSite)) {
            throw new MsException(ErrorNo.INVALID_BACKUP_RULE, "Replication of Noncurrent Version with same sourceSite and backupSite");
        }
        checkDaysAndNoncurrentDays(condition.getNoncurrentDays(), false);
        if (prefixStr == null) {
            addTimestampsToMap(condition.getNoncurrentDays(), NON_REPLICATION);
            if (tagMap != null) {
                validateBackupTag(condition.getNoncurrentDays() + "", NON_REPLICATION, sourceSite, backupSite);
            }
        } else {
            String prefix = sourceSite + "_" + backupSite;
            prefixRepMap.put(prefix, prefixStr);
            validateBackUpPrefix(condition.getNoncurrentDays(), NON_REPLICATION, sourceSite, backupSite);
        }
    }

}
