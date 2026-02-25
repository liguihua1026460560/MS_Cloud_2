package com.macrosan.utils.policy;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.BucketPolicy;
import com.macrosan.message.jsonmsg.Principal;
import com.macrosan.message.jsonmsg.Statement;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.PatternConst;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.msutils.MsDateUtils.*;
import static com.macrosan.utils.regex.PatternConst.ACCOUNTID_PATTERN;

public class BucketPolicyUtils {
    static final String ALLOW = "Allow";
    static final String DENY = "Deny";
    private static final String RESOUREPREFIX = "mrn::moss:::";
    private static final String PRINCIPALPREFIX = "mrn::iam::";
    /**
     * iam redis表 用来判断策略中的用户名
     */
    private static IamRedisConnPool iamPool = IamRedisConnPool.getInstance();
    private static RedisConnPool pool = RedisConnPool.getInstance();
    private static final Logger logger = LoggerFactory.getLogger(BucketPolicyUtils.class);

    /**
     * 判断策略json转化成的PolicyJsonObj是否非法
     *
     * @param policyJsonObj
     * @return 若非法，返true
     */
    public static boolean isPolicyJsonObjError(BucketPolicy policyJsonObj, String bucketName) {
        if (policyJsonObj == null) {
            logger.debug("policyJsonObj is null");
            return true;
        }

        List<Statement> statements = policyJsonObj.getStatement();
        Set<String> sidSet = new ConcurrentHashSet<>();

        for (Statement statement : statements) {

            //需要判断action 与not action都不为空,sameLabelCheck已判断都为空的情况,这里一定存在一个元素
            List<String> actions = statement.getAction();

            if (actions == null) {
                actions = statement.getNotAction();
            }

            if (actions == null || actions.size() == 0) {
                throw new MsException(ErrorNo.INVALID_POLICY_ACTION, "The policy is invalid in action");
            }
            for (String action : actions) {
                if (!checkActionAuth(action)) {
                    logger.info("check action " + action + " fail");
                    throw new MsException(ErrorNo.INVALID_POLICY_ACTION, "The policy is invalid in action");
                }
            }

            //todo 校驗principal中的數據
            Principal principal = statement.getPrincipal();
            if (statement.getNotPrincipal() != null) {
                principal = statement.getNotPrincipal();
            }
            if (principal == null) {
                throw new MsException(ErrorNo.INVALID_POLICY_PRINCIPAL, "The policy is invalid in principal");
            }
            List<String> service = principal.service;
            if (service == null) {
                throw new MsException(ErrorNo.INVALID_POLICY_PRINCIPAL, "The policy is invalid in principal");
            }
            for (String serviceValue : service) {
                if (principalCheck(serviceValue)) {
                    logger.info("check principal " + serviceValue + " fail");
                    throw new MsException(ErrorNo.INVALID_POLICY_PRINCIPAL, "The policy is invalid in principal");
                }
            }

            String effect = statement.getEffect();
            if (!(DENY.equals(effect) || ALLOW.equals(effect))) {
                logger.info("check effect " + effect + " fail");
                throw new MsException(ErrorNo.INVALID_POLICY_EFFECT, "The policy is invalid in effect");
            }

            //与action 标签情况类似
            List<String> resources = statement.getResource();

            if (resources == null) {
                resources = statement.getNotResource();
            }

            if (resources == null || resources.size() == 0) {
                throw new MsException(ErrorNo.INVALID_POLICY_RESOURCE, "The policy is invalid in resource");
            }
            for (String resource : resources) {
                // 增加resource参数校验
                if (resource == null || "".equals(resource)) {
                    throw new MsException(ErrorNo.INVALID_POLICY_RESOURCE, "The policy is invalid in resource");
                }
                if (resourceCheck(resource, bucketName)) {
                    logger.info("check resource " + resource + " fail");
                    throw new MsException(ErrorNo.INVALID_POLICY_RESOURCE, "The policy is invalid in resource");
                }
            }
        }
        return false;
    }

    /**
     * 判断策略元素是否合法
     *
     * @param policy
     */
    public static void policyCheck(JSONObject policy) {
        for (String key : policy.keySet()) {
            if (!key.equals(POLICY_VERSION) && !key.equals(POLICY_Statement)) {
                throw new MsException(ErrorNo.INVALID_POLICY, "The element is invalid: " + key);
            }
        }
    }

    /**
     * 判断策略的action是否合法
     *
     * @param action
     * @return 合法返回true，不合法返回false
     */
    public static boolean checkActionAuth(String action) {
        if (StringUtils.isBlank(action)) {
            return false;
        }

        //不支持的
        //moss:GetSysCapacityInfo
        //moss:GetCapacityQuota
        //moss:PutCapacityQuota
        String[] notSupport = {"moss:ListBuckets", "moss:PutBucket", "moss:GetObjectStatistics",
                "moss:GetTrafficStatistics", "moss:GetAccountUsedCap", "moss:GetSysCapacityInfo",
                "moss:GetCapacityQuota", "moss:PutCapacityQuota", "moss:PutPerformanceQuota",
                "moss:GetPerformanceQuota", "moss:GetAccountObjectsQuota", "moss:PutAccountObjectsQuota",
                "moss:GetBucketDataSynchronizationSwitch", "moss:GetClusterName"};
        if (action.contains("iam")) {
            return false;
        }

        for (String noSupport : notSupport) {
            if (isMatch(action.toLowerCase(), noSupport.toLowerCase())) {
                return false;
            }
        }

        for (IamUtils.ActionType type : IamUtils.ActionType.values()) {
            if (isMatch(type.getActionType().toLowerCase(), action.toLowerCase())) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断包含?和*的字符串和常规字符串是否匹配
     *
     * @param s 被比较的字符串()
     * @param p 表示包含?和*的字符串(策略中字段)
     * @return
     */
    public static boolean isMatch(String s, String p) {
        // 字符串s的下标
        int i = 0;
        // 字符串p的下标
        int j = 0;
        int starIndex = -1;
        int iIndex = -1;

        while (i < s.length()) {
            if (j < p.length() && (p.charAt(j) == '?' || p.charAt(j) == s.charAt(i))) {
                ++i;
                ++j;
            } else if (j < p.length() && p.charAt(j) == '*') {
                starIndex = j;
                iIndex = i;
                j++;
            } else if (starIndex != -1) {
                j = starIndex + 1;
                i = iIndex + 1;
                iIndex++;
            } else {
                return false;
            }
        }
        while (j < p.length() && p.charAt(j) == '*') {
            ++j;
        }
        return j == p.length();
    }

    /***
     * @Description :获取JSON数据 还原策略中被省略json中的元素部分
     * @param objJson
     * @param nodeKey
     * @param prefix 资源前缀
     * @return java.lang.Object
     */
    public static Object reductionJson(Object objJson, String nodeKey, String prefix) {
        if (objJson instanceof JSONArray) {
            JSONArray objArray = (JSONArray) objJson;
            for (int i = 0; i < objArray.size(); i++) {
                reductionJson(objArray.get(i), nodeKey, prefix);
            }
        } else if (objJson instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) objJson;
            Iterator iterator = jsonObj.entrySet().iterator();

            while (iterator.hasNext()) {
                String key = iterator.next().toString().split("=")[0];

                if (key.equals(nodeKey) || key.equals(antonym(nodeKey))) {
                    Object next = jsonObj.get(key);
                    if (next instanceof JSONArray) {
                        JSONArray newArray = new JSONArray();
                        JSONArray resources = (JSONArray) next;
                        for (int i = 0; i < resources.size(); i++) {
                            String source = String.valueOf(resources.get(i));
                            newArray.add(reductionPrefix(source, prefix));
                        }
                        jsonObj.put(key, newArray);
                    }
                }

                //将effect的值还原 ALLOW = 1, Deny = 2
                if (POLICY_EFFECT.equals(key)) {
                    String effect = jsonObj.getString(key);
                    if ("2".equals(effect)) {
                        jsonObj.put(key, DENY);
                    } else if ("1".equals(effect)) {
                        jsonObj.put(key, ALLOW);
                    }
                }

            }

        }
        return objJson;
    }

    public static String reductionPrefix(String source, String prefix) {
        if (!"*".equals(source)) {
            source = prefix + source;
        }
        return source;
    }

    /***
     * @Description :更新JSON数据 去除前缀,并校验json中的元素
     * @param objJson
     * @param nodeKey
     * @param prefix 资源前缀
     * @return java.lang.Object
     */
    public static Object updateJson(Object objJson, String nodeKey, String prefix) {

        if (objJson instanceof JSONArray) {
            JSONArray objArray = (JSONArray) objJson;
            for (int i = 0; i < objArray.size(); i++) {
                updateJson(objArray.get(i), nodeKey, prefix);
            }
        } else if (objJson instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) objJson;
            Iterator iterator = jsonObj.entrySet().iterator();

            while (iterator.hasNext()) {
                String key = iterator.next().toString().split("=")[0];

                //判断condition
                if (POLICY_CONDITION.equals(key)) {
                    if (checkCondition(jsonObj.getJSONObject(POLICY_CONDITION))) {
                        throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "the filed condition in policy is error");
                    }
                }

                if (key.equals(nodeKey) || key.equals(antonym(nodeKey))) {
                    Object next = jsonObj.get(key);
                    if (next instanceof JSONArray) {
                        JSONArray newArray = new JSONArray();
                        JSONArray resources = (JSONArray) next;
                        if (resources == null || resources.size() == 0) {
                            throw new MsException(ErrorNo.INVALID_POLICY_RESOURCE, "the filed in policy is null");
                        }
                        for (int i = 0; i < resources.size(); i++) {
                            String source = String.valueOf(resources.get(i));
                            if (source.split(":").length == 6) {
                                source = source.split(":")[5];
                                String[] split = source.split("/");
                                if (split.length >= 2) {
                                    String bucket = split[0].toLowerCase();
                                    source = bucket + source.substring(bucket.length());
                                } else {
                                    source = source.toLowerCase();
                                }
                            }
                            newArray.add(source.replaceFirst(prefix, ""));
                        }
                        jsonObj.put(key, newArray);
                    } else if (next instanceof JSONObject && key.contains(SysConstants.POLICY_PRINCIPAL)) {
                        JSONObject principals = (JSONObject) next;
                        JSONArray principalsJSONArray = principals.getJSONArray("MOSS");
                        if (principalsJSONArray == null || principalsJSONArray.size() == 0) {
                            throw new MsException(ErrorNo.INVALID_POLICY_PRINCIPAL, "the filed in policy is null");
                        }
                    }
                }

                //将effect的值简化为ALLOW = 1, Deny = 2
                if (POLICY_EFFECT.equals(key)) {
                    String effect = jsonObj.getString(key);
                    if (DENY.equals(effect)) {
                        jsonObj.put(key, 2);
                    } else if (ALLOW.equals(effect)) {
                        jsonObj.put(key, 1);
                    }
                }
            }

        }

        return objJson;
    }

    /**
     * 判断condition条件是否符合标准 暂时只支持ip类型
     */
    public static boolean checkCondition(JSONObject condition) {
        if (condition == null) {
            logger.debug("condition is null");
            return true;
        }
        logger.info("check condition: " + condition.toJSONString());
        Set<String> elementSet = condition.keySet();
        for (String element : elementSet) {
            JSONObject elementJson = condition.getJSONObject(element);
            if (elementJson == null || elementJson.size() == 0) {
                continue;
            }

            // IP类型
            if (element.endsWith("IpAddress")) {
                int count = 0;
                String sourceIp = elementJson.getString("SourceIp");
                if (sourceIp != null) {
                    if (checkSourceIp(sourceIp)) {
                        return true;
                    }
                    ++count;
                }

                String moss_sourceIp = elementJson.getString("moss:SourceIp");
                if (moss_sourceIp != null) {
                    if (checkSourceIp(moss_sourceIp)) {
                        return true;
                    }
                    ++count;
                }

                if (count != elementJson.size()) {
                    throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The condition is invalid, condition: " + condition);
                }
                continue;
            }

            // 布尔类型
            if (element.equals("Bool")) {
                int count = 0;
                String secureTransport = elementJson.getString("SecureTransport");
                if (secureTransport != null) {
                    if (checkBoolean(secureTransport)) {
                        return true;
                    }
                    ++count;
                }

                String mossSecureTransport = elementJson.getString("moss:SecureTransport");

                if (mossSecureTransport != null) {
                    if (checkBoolean(mossSecureTransport)) {
                        return true;
                    }
                    ++count;
                }

                if (count != elementJson.size()) {
                    throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The condition is invalid, condition: " + condition);
                }
                continue;
            }

            // 日期类型
            if (element.startsWith("Date")) {
                Set<String> keySet = elementJson.keySet();
                ArrayList<String> keyList = new ArrayList<>();
                for (String key : keySet) {
                    String keySub = key.startsWith("moss:") ? key.substring("moss:".length()) : key;
                    if (keyList.contains(keySub) || !CONDITION_ELEMENT_DATA_LIST.contains(keySub)) {
                        throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The condition is invalid, condition: " + condition);
                    }
                    String string = elementJson.getString(key);
                    if (checkDate(string)) {
                        return true;
                    }
                    keyList.add(keySub);
                }
            }

            // 数值类型
            if (element.startsWith("Numeric")) {
                int count = 0;
                String maxKeys = elementJson.getString("max-keys");
                if (maxKeys != null) {
                    if (checkNumeric(maxKeys)) {
                        return true;
                    }
                    ++count;
                }

                String mossMaxKeys = elementJson.getString("moss:max-keys");

                if (mossMaxKeys != null) {
                    if (checkNumeric(mossMaxKeys)) {
                        return true;
                    }
                    ++count;
                }

                if (count != elementJson.size()) {
                    throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The condition is invalid, condition: " + condition);
                }
                continue;
            }

            // 字符串类型
            if (element.startsWith("String")) {
                Set<String> keySet = elementJson.keySet();
                ArrayList<String> keyList = new ArrayList<>();
                for (String key : keySet) {
                    String keySub = key.startsWith("moss:") ? key.substring("moss:".length()) : key;
                    if (keyList.contains(keySub) || !CONDITION_ELEMENT_STRING_LIST.contains(keySub)) {
                        throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The condition is invalid, condition: " + condition);
                    }
                    String string = elementJson.getString(key);
                    if (checkString(keySub, string)) {
                        return true;
                    }
                    keyList.add(keySub);
                }
            }
        }

        return false;
    }

    private static boolean checkString(String stringElement, String string) {
//        if (string.contains("[")) {
//            if (POLICY_STRING_SIGNATURE_VERSION.equals(stringElement)) {
//                String[] strings = Json.decodeValue(string, String[].class);
//                boolean b = false;
//                if (strings.length > 0) {
//                    for (String s : strings) {
//                        if (!AuthorizeFactory.AWS.equals(s) && !AuthorizeFactory.SHA256.equals(s)) {
//                            b = true;
//                            break;
//                        }
//                    }
//                }
//                return b;
//            }
//        } else {
//            if (POLICY_STRING_SIGNATURE_VERSION.equals(stringElement)) {
//                return !AuthorizeFactory.AWS.equals(string) && !AuthorizeFactory.SHA256.equals(string);
//            }
//        }
        return false;
    }

    private static boolean checkNumeric(String numeric) {
        try {
            if (numeric.contains("[")) {
                String[] strings = Json.decodeValue(numeric, String[].class);
                if (strings.length > 0) {
                    for (String s : strings) {
                        Integer.valueOf(s);
                    }
                }
            } else {
                Integer.valueOf(numeric);
            }
        } catch (Exception e) {
            logger.error("The max-keys of Condition format error");
            return true;
        }
        return false;
    }

    private static boolean checkDate(String time) {
        try {
            if (time.contains("[")) {
                String[] strings = Json.decodeValue(time, String[].class);
                if (strings.length > 0) {
                    for (String s : strings) {
                        DateUtils.parseDate(s, Locale.ENGLISH, POLICY_ISO8601, STANDARD_ISO8601, STANDARD_TZ);
                    }
                }
            } else {
                DateUtils.parseDate(time, Locale.ENGLISH, POLICY_ISO8601, STANDARD_ISO8601, STANDARD_TZ);
            }
        } catch (Exception e) {
            logger.error("The CurrentTime of Condition does not meet the ISO 8601 date format");
            return true;
        }
        return false;
    }

    private static boolean checkBoolean(String b) {
        return !"True".equals(b) && !"true".equals(b) && !"False".equals(b) && !"false".equals(b);
    }

    public static boolean checkSourceIp(String sourceIp) {
        if (sourceIp.contains("[")) {
            String[] strings = Json.decodeValue(sourceIp, String[].class);
            if (strings.length > 0) {
                for (String ip : strings) {
                    if (!ip.contains(":")) {
                        if (!ip.contains("/")) {
                            ip += "/32";
                        }
                        if (!PatternConst.IP_CIDR_PATTERN.matcher(ip).matches()) {
                            return true;
                        }
                    } else {
                        if (!ip.contains("/")) {
                            ip += "/128";
                        }
                        if (!PatternConst.IPV6_CIDR_PATTERN.matcher(ip).matches()) {
                            return true;
                        }
                    }
                }
            }
        } else {
            if (!sourceIp.contains(":")) {
                if (!sourceIp.contains("/")) {
                    sourceIp += "/32";
                }
                if (!PatternConst.IP_CIDR_PATTERN.matcher(sourceIp).matches()) {
                    return true;
                }
            } else {
                if (!sourceIp.contains("/")) {
                    sourceIp += "/128";
                }
                if (!PatternConst.IPV6_CIDR_PATTERN.matcher(sourceIp).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 元素取反
     */
    public static String antonym(String source) {
        return "Not" + source;
    }

    /**
     * mrn::iam::100570189158:user/userkdd
     * mrn::iam::100570189158:root 或 100570189158
     * 校验principle中value 是否合法
     * true 表示不合法
     */
    public static boolean principalCheck(String pinValue) {
        //空不合法
        if (pinValue == null) {
            return true;
        }

        //匿名对象
        if ("*".equals(pinValue)) {
            return false;
        }

        String[] split = pinValue.split(":");

        if (split.length != 1) {

            if (split.length != 6) {
                return true;
            }
            String mrn = split[0];
            String partition = split[1];
            String service = split[2];
            String region = split[3];
            String accountId = split[4];
            String resource = split[5];

            if (!MRN.equals(mrn)) {
                return true;
            }


            if (StringUtils.isNotEmpty(partition)) {
                return true;
            }

            String localRegion = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
            String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
            boolean belongsRegion = localRegion.equals(region);
            if (StringUtils.isNotEmpty(defaultRegion)) {
                belongsRegion = (localRegion.equals(region)) || (defaultRegion.equals(region));
            }
            if (!belongsRegion && StringUtils.isNotEmpty(region)) {
                return true;
            }

            if (!IAM.equals(service)) {
                return true;
            }
            //判断账户是否存在
            if (pool.getCommand(REDIS_USERINFO_INDEX).exists(accountId) == 0) {
                return true;
            }
            //判斷id是否符合要求
            if (!ACCOUNTID_PATTERN.matcher(accountId).matches()) {
                return true;
            }

            if (resource == null || resource.contains("*") || resource.contains("?")) {
                return true;
            }

            // 判断用户情况
            if (!ROOT.equals(resource)) {
                if (!resource.startsWith(USERPREFIX) || resource.length() <= 5) {
                    return true;
                }

                String[] split1 = resource.split("/");
                if (split1.length != 2) {
                    return true;
                }

                // 账户下是否有用户
                if (iamPool.getCommand(REDIS_IAM_INDEX).scard(accountId) == 0) {
                    return true;
                }

                String userName = split1[1];
                return !iamPool.getCommand(REDIS_IAM_INDEX).sismember(accountId, userName);
            }
            return false;
        }

        String accountId = split[0];

        //判斷id是否符合要求
        if (!ACCOUNTID_PATTERN.matcher(accountId).matches()) {
            return true;
        }

        //判断账户是否存在
        if (pool.getCommand(REDIS_USERINFO_INDEX).exists(accountId) == 0) {
            return true;
        }

        return false;
    }

    /**
     * 校验resource 中value是否合法
     * true 表示不合法
     */
    public static boolean resourceCheck(String resources, String bucketName) {
        String bucketKey = "";
        if (resources.startsWith(RESOUREPREFIX)) {
            String[] sourceSpilt = resources.split(RESOUREPREFIX);
            if (sourceSpilt.length < 2) {
                return true;
            }

            bucketKey = ServerConfig.isBucketUpper() ? sourceSpilt[1].toLowerCase() : sourceSpilt[1];

        } else {
            String[] split = resources.split(":");
            if (split.length != 6) {
                return true;
            }

            String mrn = split[0];
            String partition = split[1];
            String service = split[2];
            String region = split[3];
            String account = split[4];
            bucketKey = ServerConfig.isBucketUpper() ? split[5].toLowerCase() : split[5];

            if (!MRN.equals(mrn)) {
                return true;
            }

            if (StringUtils.isNotEmpty(partition)) {
                return true;
            }

            // 区域暂不支持
            if (region != null) {
                return true;
            }

//            String localRegion = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
//            String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
//            boolean belongsRegion = localRegion.equals(region);
//            if(StringUtils.isNotEmpty(defaultRegion)){
//                belongsRegion = (localRegion.equals(region)) || (defaultRegion.equals(region));
//            }
//            if (!belongsRegion && StringUtils.isNotEmpty(region)) {
//                return true;
//            }

            //service z只能是moss
            if (!MOSS.equals(service)) {
                return true;
            }

            //accountId 只能为空
            if (account.length() > 0) {
                return true;
            }
        }

        if (bucketName.equals(bucketKey)) {
            return false;
        }
        //判断bucket/key
        String[] split = bucketKey.split(SLASH);
        if (split.length < 2) {
            return true;
        }
        String bucket = split[0];
        String keys = split[1];

        if (!bucketName.equals(bucket) || "".equals(keys)) {
            return true;
        }
        return false;
    }

    /**
     * 校验同一个意义元素是否同时存在 或者同时不存在
     */
    public static void sameLabelCheck(String convertPolicyToJsonStr, Object objJson) {
        if (objJson instanceof JSONArray) {
            JSONArray objArray = (JSONArray) objJson;
            for (int i = 0; i < objArray.size(); i++) {
                sameLabelCheck(convertPolicyToJsonStr, objArray.get(i));
            }
        } else if (objJson instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) objJson;
            if (jsonObj.getString("Effect") == null) {
                throw new MsException(ErrorNo.INVALID_POLICY_EFFECT, "The Effect in the policy is invalid");
            }
            String[] labels = {POLICY_PRINCIPAL, POLICY_ACTION, POLICY_RESOURCE, POLICY_EFFECT};
            // 不存在的策略元素和同一元素重复出现进行报错
            Set<String> keys = jsonObj.keySet();
            ArrayList<String> memList = new ArrayList<>();
            for (String key : keys) {
                boolean contains = STATEMENT_ELEMENT_LIST.contains(key);
                if (!contains) {
                    throw new MsException(ErrorNo.INVALID_POLICY_STATEMENT, "The " + key + " in the policy is invalid");
                }
                if (memList.contains(key)) {
                    throw new MsException(ErrorNo.INVALID_POLICY_STATEMENT, "The " + key + " in the policy is invalid");
                }
                memList.add(key);
            }
            int[] errors = {ErrorNo.INVALID_POLICY_PRINCIPAL, ErrorNo.INVALID_POLICY_ACTION, ErrorNo.INVALID_POLICY_RESOURCE};
            for (int i = 0; i < labels.length; i++) {
                String nodeKey = labels[i];
                boolean first = jsonObj.get(nodeKey) == null;
                boolean second = jsonObj.get(antonym(nodeKey)) == null;

                if (!(first ^ second)) {
                    logger.error("policy has invalid of Action in label {}", nodeKey);
                    throw new MsException(errors[i], "there are elements that contain the same or all not");
                }
            }
            if (jsonObj.getJSONObject("Condition") != null) {
                JSONObject conJson = jsonObj.getJSONObject("Condition");
                Set<String> conKeys = conJson.keySet();
                ArrayList<String> conList = new ArrayList<>();
                for (String conKey : conKeys) {
                    boolean contains = CONDITION_ELEMENT_LIST.contains(conKey);
                    if (!contains) {
                        throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The " + conKey + " in the condition of policy is invalid");
                    }
                    if (conList.contains(conKey)) {
                        throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "The " + conKey + " in the condition of policy is invalid");
                    }
                    JSONObject elementJson = conJson.getJSONObject(conKey);
                    Set<String> elementSet = elementJson.keySet();
                    for (String element : elementSet) {
                        if (sameElementCheck(convertPolicyToJsonStr, conKey, element + "\"")) {
                            throw new MsException(ErrorNo.INVALID_POLICY_CONDITION, "Duplicate condition element: " + element);
                        }
                    }
                    conList.add(conKey);
                }
            }
        }
    }

    /**
     * 判断一个statement内各个元素是否重复出现
     */
    public static void statementCheck(String policyStr, BucketPolicy bucketPolicy) {
        int statementSize = bucketPolicy.getStatement().size();
        List<Statement> statementList = bucketPolicy.getStatement();
        int count = 0;

        for (String member : STATEMENT_ELEMENT_LIST) {
            if (sameMemberCheck(policyStr, member, statementSize)) {
                throw new MsException(ErrorNo.INVALID_POLICY_STATEMENT, "The " + member + " in the policy is invalid");
            }
        }

        // 获取Condition的数量
        for (Statement statement : statementList) {
            if (statement.getCondition() != null) {
                ++count;
            }
        }
    }

    /**
     * 判断一个标签是否重复出现
     */
    public static boolean sameMemberCheck(String str, String member, int size) {
        int count = 0;
        int start = 0;
        while (str.indexOf(member, start) >= 0 && start < str.length()) {
            count++;
            start = str.indexOf(member, start) + member.length();
        }
        return count > size;
    }

    public static boolean sameElementCheck(String str, String member, String key) {
        String result = "";
        int start = str.indexOf(member);
        while (str.indexOf(member, start) >= 0 && start < str.length()) {
            int openBraceIndex = str.indexOf("{", start);
            int closeBraceIndex = str.indexOf("}", openBraceIndex);
            if (openBraceIndex != -1 && closeBraceIndex != -1) {
                result = str.substring(openBraceIndex + 1, closeBraceIndex);
                if (sameMemberCheck(result, key, 1)) {
                    return true;
                }
            }
            start = str.indexOf(member, start) + member.length();
        }
        return false;
    }


    /**
     * 判断Ip是否在CIDR格式里面
     */
    public static boolean isInRange(String ip, String cidr) {
        try {
            // 解析 CIDR 地址和掩码
            String[] parts = cidr.split("/");
            if (parts.length != 2) return false;

            InetAddress ipAddress = InetAddress.getByName(ip);
            InetAddress networkAddress = InetAddress.getByName(parts[0]);
            int prefixLength = Integer.parseInt(parts[1]);

            byte[] ipBytes = ipAddress.getAddress();
            byte[] networkBytes = networkAddress.getAddress();

            // IPv4 和 IPv6 长度不同，必须匹配
            if (ipBytes.length != networkBytes.length) return false;

            // 计算要比较的字节数和剩余的比特位
            int fullBytes = prefixLength / 8; // 需要完全匹配的字节
            int remainingBits = prefixLength % 8; // 需要部分匹配的比特

            // 比较完整字节
            for (int i = 0; i < fullBytes; i++) {
                if (ipBytes[i] != networkBytes[i]) return false;
            }

            // 比较剩余比特
            if (remainingBits > 0) {
                int mask = 0xFF << (8 - remainingBits);
                return (ipBytes[fullBytes] & mask) == (networkBytes[fullBytes] & mask);
            }

            return true;
        } catch (UnknownHostException | NumberFormatException e) {
            return false;
        }
    }

}
