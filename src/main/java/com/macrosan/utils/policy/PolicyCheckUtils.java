package com.macrosan.utils.policy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.utils.authorize.AuthorizeFactory;
import com.macrosan.utils.functional.Entry;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.authorize.AuthorizeV4.*;
import static com.macrosan.utils.policy.BucketPolicyUtils.isMatch;


/**
 * @author zp
 */
public class PolicyCheckUtils {
    public static final Logger logger = LogManager.getLogger(PolicyCheckUtils.class.getName());
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    private static final ServerConfig config = ServerConfig.getInstance();
    private static final UnifiedMap<String, String> sourceAction = config.getSourceAction();

    /**
     * 校验桶策略并输出结果
     *
     * @param bucketName 桶名
     * @param userId     账户id
     * @param method     接口名
     */
    public static int getPolicyResult1(UnifiedMap<String, String> paramMap, String bucketName, String userId, String method) {
        String userName = paramMap.getOrDefault(USERNAME, "");
        Long exists = pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).exists(bucketName + "_meta");
        if (0 == exists) {
            return 0;
        }
        List<List<Integer>> policyMeta = getPolicyMeta(bucketName, userId, userName);
        int statement = 0;
        if (policyMeta != null && policyMeta.size() > 0) {
            statement = getStatement(policyMeta, paramMap, method);
        }
        if (statement == 2) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such action permission.");
        }
        return statement;
    }

    public static int getPolicyResult(UnifiedMap<String, String> paramMap, String bucketName, String userId, String method) {
        return getPolicyResult(paramMap, bucketName, "", userId, method);
    }

    public static int getPolicyAclResult(UnifiedMap<String, String> paramMap, String bucketName, String userId, String method) {
        String acl = paramMap.get(X_AMZ_ACL);
        Map<String, String> conditionMap = new HashMap<>();
        conditionMap.put(POLICY_STRING_X_AMZ_ACL, acl);
        return getPolicyResult(paramMap, bucketName, "", method, conditionMap);
    }

    public static int getPolicyGrantAclResult(UnifiedMap<String, String> paramMap, String bucketName, String userId, String method, List<Entry<String, String>> xmlList) {
        Map<String, String> conditionMap = new HashMap<>();
        for (Entry<String, String> map : xmlList) {
            String targetUserId = map.getKey();
            String aclType = map.getValue();
            if (PERMISSION_READ_LONG.equals(aclType)) {
                conditionMap.put(POLICY_STRING_X_AMZ_READ, POLICY_X_AMZ_PREFIX + targetUserId);
            } else if (PERMISSION_WRITE_LONG.equals(aclType)) {
                conditionMap.put(POLICY_STRING_X_AMZ_WRITE, POLICY_X_AMZ_PREFIX + targetUserId);
            } else if (PERMISSION_READ_CAP_LONG.equals(aclType)) {
                conditionMap.put(POLICY_STRING_X_AMZ_READ_ACP, POLICY_X_AMZ_PREFIX + targetUserId);
            } else if (PERMISSION_WRITE_CAP_LONG.equals(aclType)) {
                conditionMap.put(POLICY_STRING_X_AMZ_WRITE_ACP, POLICY_X_AMZ_PREFIX + targetUserId);
            } else if (PERMISSION_FULL_CON_LONG.equals(aclType)) {
                conditionMap.put(POLICY_STRING_X_AMZ_FULL, POLICY_X_AMZ_PREFIX + targetUserId);
            }
        }
        return getPolicyResult(paramMap, bucketName, "", method, conditionMap);
    }

    public static int getPolicyResult(UnifiedMap<String, String> paramMap, String bucketName, String keyName, String userId, String method) {
        Map<String, String> conditionMap = new HashMap<>();
        return getPolicyResult(paramMap, bucketName, keyName, method, conditionMap);
    }

    public static int getPolicyResult(UnifiedMap<String, String> paramMap, String bucketName, String keyName, String method, Map<String, String> conditionMap) {
        if (paramMap.containsKey(IS_SYNCING)) {
            return 0;
        }
        addPolicyPublicCondition(paramMap, conditionMap);
        String policy = bucketName + POLICY_SUFFIX;
        Long exists = pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).exists(policy);
        if (0 == exists) {
            return 0;
        }
        String policy0 = pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hget(policy, STATEMENTS_POLICES[0]);
        List<Statement> policyValues = JSON.parseObject(policy0,
                new TypeReference<List<Statement>>() {
                });
        int[] result = {0};
        policyValues.forEach(statement -> {
            int check = policyCheck(statement, paramMap, keyName, method, conditionMap);
            if (check == 2) {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such action permission.");
            }
            if (check > result[0]) {
                result[0] = check;
            }
        });
        return result[0];
    }

    /**
     * 获取请求通用字段
     */
    private static void addPolicyPublicCondition(UnifiedMap<String, String> paramMap, Map<String, String> conditionMap) {
        // CurrentTime
        long currentTimeMillis = System.currentTimeMillis();
        conditionMap.put(POLICY_DATE_CURRENT_TIME, currentTimeMillis + "");
        // HTTPS
        if ("1".equals(paramMap.get("http-ssl"))) {
            conditionMap.put(POLICY_BOOL_SECURE_TRANSPORT, "true");
        } else {
            conditionMap.put(POLICY_BOOL_SECURE_TRANSPORT, "false");
        }
        // signatureVersion: v2 or v4
        if (paramMap.contains(AUTHORIZATION)) {
            String[] array = paramMap.get(AUTHORIZATION).split(" ");
            String authType = array[0];
            if (authType.equals(AuthorizeFactory.AWS)) {
                conditionMap.put(POLICY_STRING_SIGNATURE_VERSION, AuthorizeFactory.AWS);
            } else if (authType.equals(AuthorizeFactory.SHA256)) {
                conditionMap.put(POLICY_STRING_SIGNATURE_VERSION, AuthorizeFactory.SHA256);
                String contentSHA256 = paramMap.get(X_AMZ_CONTENT_SHA_256);
                if (CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(contentSHA256) || CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(contentSHA256)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_SHA256, contentSHA256);
                }
                conditionMap.put(POLICY_STRING_X_AMZ_SHA256, contentSHA256);
            }
        } else {
            conditionMap.put(POLICY_STRING_SIGNATURE_VERSION, "");
        }
        // User-Agent
        if (paramMap.contains(USER_AGENT)) {
            String userAgent = paramMap.get(USER_AGENT);
            conditionMap.put(POLICY_STRING_USER_AGENT, userAgent);
        }
    }

    /**
     * 从bucket_meta中取出策略的位置信息
     */
    public static List<List<Integer>> getPolicyMeta(String bucket, String accountId, String userName) {
        List<List<Integer>> metas = new ArrayList<>();

        String metaKey = bucket + "_meta";
        String root = accountId + ":root";
        String user = accountId + ":user/" + userName;

        Set<String> principals = pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hgetall(metaKey).keySet();
        principals.forEach(principal -> {
            List<List<Integer>> tempValues;
            if (isMatch(root, principal) || isMatch(user, principal)) {
                String metaValues = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hget(metaKey, principal);
                tempValues = JSON.parseObject(metaValues,
                        new TypeReference<List<List<Integer>>>() {
                        });
                metas.addAll(tempValues);
            }
        });
        return metas;
    }

    /**
     * 根据元数据位置拿到statement并校验权限
     * Deny = 2;Allow = 1; 默认 = 0
     */
    public static int getStatement(List<List<Integer>> metas, UnifiedMap<String, String> paramMap, String method) {
        final int[] policyNum = {-1};
        String bucket = paramMap.get(BUCKET_NAME);
        //存放策略信息
        String policys = bucket + "_policys";
        AtomicReference<List<Statement>> statements = new AtomicReference<>();
        int[] result = {0};
        Map<String, String> conditionMap = new HashMap<>();
        addPolicyPublicCondition(paramMap, conditionMap);
        metas.forEach(meta -> {
            if (meta.get(0) != policyNum[0]) {
                policyNum[0] = meta.get(0);
                String policy = pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hget(policys, STATEMENTS_POLICES[policyNum[0]]);
                List<Statement> lists;
                lists = JSON.parseObject(policy,
                        new TypeReference<List<Statement>>() {
                        });
                statements.getAndSet(lists);
            }

            if (result[0] < 2 && statements.get() != null) {
                Statement statement = statements.get().get(meta.get(1));

                int check = policyCheck(statement, paramMap, "", method, conditionMap);

                if (check > result[0]) {
                    result[0] = check;
                }
            }

        });
        return result[0];
    }

    /**
     * 校验一条statement 的结果
     * 需要方法名, 资源名, 先要匹配资源,在匹配方法,最后看effect
     */
    public static int policyCheck(Statement statement, UnifiedMap<String, String> paramMap, String keyName, String method, Map<String, String> conditionMap) {
        String bucket = paramMap.get(BUCKET_NAME);
        String source = StringUtils.isEmpty(keyName) ? bucket : bucket + SLASH + keyName;
        String sourceIp = paramMap.get(SOURCEIP);
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(USERNAME, "");
        String root = accountId + ":root";
        String user = accountId + ":user/" + userName;
        method = "moss:" + method;

        Condition condition = statement.getCondition();

        if (statement.Principal != null) {
            List<String> service = statement.getPrincipal().service;
            List<String> principal = updatePrincipal(service);
            if (!sourceActionMatch(principal, root) && !sourceActionMatch(principal, user) && !sourceActionMatch(principal, accountId)) {
                return 0;
            }
        }

        //授权人不匹配,直接进行下一条策略
        if (statement.NotPrincipal != null) {
            List<String> service = statement.getNotPrincipal().service;
            List<String> principal = updatePrincipal(service);
            if (sourceActionMatch(principal, root) || sourceActionMatch(principal, user) || sourceActionMatch(principal, accountId)) {
                return 0;
            }
        }

        if (statement.NotResource != null) {
            if (sourceActionMatch(statement.getNotResource(), source)) {
                return 0;
            }
        }

        if (statement.getResource() != null && !sourceActionMatch(statement.getResource(), source)) {
            return 0;
        }

        if (statement.NotAction != null) {
            if (actionMatch(statement.getNotAction(), method)) {
                return 0;
            }
        }

        if (statement.getAction() != null && !actionMatch(statement.getAction(), method)) {
            return 0;
        }

        if (condition != null) {
            // IP类型
            if (condition.getIp() != null) {
                if (checkIp(sourceIp, condition.getIp())) {
                    return 0;
                }
            }

            if (condition.getNotIp() != null) {
                if (checkNotIp(sourceIp, condition.getNotIp())) {
                    return 0;
                }
            }

            // 布尔类型
            if (checkBoolList(condition, conditionMap)) {
                return 0;
            }

            // 日期类型
            if (checkDateList(condition, conditionMap)) {
                return 0;
            }

            // 数值类型
            if (checkNumericList(condition, conditionMap)) {
                return 0;
            }

            // 字符串类型
            if (checkStringList(condition, conditionMap, method)) {
                return 0;
            }
        }

        return Integer.parseInt(statement.Effect);
    }

    public static boolean checkIp(String sourceIp, Address address) {
        // 判断策略中 ip 是数组还是字符串
        if (address.getSourceIp() != null) {
            String ip = address.getSourceIp().toString();
            if (ip.contains("[")) {
                String[] sourceIpStr = Json.decodeValue(ip, String[].class);
                boolean b = true;
                for (String ips : sourceIpStr) {
                    if (!ips.contains(":")) {
                        if (!ips.contains("/")) {
                            ips += "/32";
                        }
                    } else {
                        if (!ips.contains("/")) {
                            ips += "/128";
                        }
                    }
                    if (BucketPolicyUtils.isInRange(sourceIp, ips)) {
                        b = false;
                    }
                }
                // 不包含ip，策略不生效
                if (b) {
                    return true;
                }
            } else {
                if (!ip.contains(":")) {
                    if (!ip.contains("/")) {
                        ip += "/32";
                    }
                } else {
                    if (!ip.contains("/")) {
                        ip += "/128";
                    }
                }
                if (!BucketPolicyUtils.isInRange(sourceIp, ip)) {
                    return true;
                }
            }
        }

        if (address.getMossSourceIp() != null) {
            String ip = address.getMossSourceIp().toString();
            if (ip.contains("[")) {
                String[] sourceIpStr = Json.decodeValue(ip, String[].class);
                boolean b = true;
                for (String ips : sourceIpStr) {
                    if (!ips.contains(":")) {
                        if (!ips.contains("/")) {
                            ips += "/32";
                        }
                    } else {
                        if (!ips.contains("/")) {
                            ips += "/128";
                        }
                    }
                    if (BucketPolicyUtils.isInRange(sourceIp, ips)) {
                        b = false;
                    }
                }
                return b;
            } else {
                if (!ip.contains(":")) {
                    if (!ip.contains("/")) {
                        ip += "/32";
                    }
                } else {
                    if (!ip.contains("/")) {
                        ip += "/128";
                    }
                }
                return !BucketPolicyUtils.isInRange(sourceIp, ip);
            }
        }
        return false;
    }

    public static boolean checkNotIp(String sourceIp, Address address) {
        if (address.getSourceIp() != null) {
            String ip = address.getSourceIp().toString();
            if (ip.contains("[")) {
                String[] sourceIpStr = Json.decodeValue(ip, String[].class);
                boolean b = false;
                for (String ips : sourceIpStr) {
                    if (!ips.contains(":")) {
                        if (!ips.contains("/")) {
                            ips += "/32";
                        }
                    } else {
                        if (!ips.contains("/")) {
                            ips += "/128";
                        }
                    }
                    if (BucketPolicyUtils.isInRange(sourceIp, ips)) {
                        b = true;
                    }
                    // 包含 ip，策略不生效
                    if (b) {
                        return true;
                    }
                }
            } else {
                if (!ip.contains(":")) {
                    if (!ip.contains("/")) {
                        ip += "/32";
                    }
                } else {
                    if (!ip.contains("/")) {
                        ip += "/128";
                    }
                }
                if (BucketPolicyUtils.isInRange(sourceIp, ip)) {
                    return true;
                }
            }
        }

        if (address.getMossSourceIp() != null) {
            String ip = address.getMossSourceIp().toString();
            if (ip.contains("[")) {
                String[] sourceIpStr = Json.decodeValue(ip, String[].class);
                boolean b = false;
                for (String ips : sourceIpStr) {
                    if (!ips.contains(":")) {
                        if (!ips.contains("/")) {
                            ips += "/32";
                        }
                    } else {
                        if (!ips.contains("/")) {
                            ips += "/128";
                        }
                    }
                    if (BucketPolicyUtils.isInRange(sourceIp, ips)) {
                        b = true;
                    }
                    if (b) {
                        return true;
                    }
                }
            } else {
                if (!ip.contains(":")) {
                    if (!ip.contains("/")) {
                        ip += "/32";
                    }
                } else {
                    if (!ip.contains("/")) {
                        ip += "/128";
                    }
                }
                return BucketPolicyUtils.isInRange(sourceIp, ip);
            }
        }
        return false;
    }

    public static boolean checkBoolList(Condition condition, Map<String, String> conditionMap) {
        if (condition.getBool() != null) {
            PolicyBool bool = condition.getBool();
            if (bool.getSecureTransport() != null) {
                boolean sourceSsl = bool.getSecureTransport();
                boolean ssl = Boolean.parseBoolean(conditionMap.get(POLICY_BOOL_SECURE_TRANSPORT));
                return sourceSsl != ssl;
            }
            if (bool.getMossSecureTransport() != null) {
                boolean sourceSsl = bool.getMossSecureTransport();
                boolean ssl = Boolean.parseBoolean(conditionMap.get(POLICY_BOOL_SECURE_TRANSPORT));
                return sourceSsl != ssl;
            }
        }
        return false;
    }

    public static boolean checkDateList(Condition condition, Map<String, String> conditionMap) {
        if (condition.getDateEquals() != null) {
            if (checkDateType(condition.getDateEquals(), conditionMap, POLICY_DATE_EQUALS)) {
                return true;
            }
        }
        if (condition.getDateNotEquals() != null) {
            if (checkDateType(condition.getDateNotEquals(), conditionMap, POLICY_DATE_NOT_EQUALS)) {
                return true;
            }
        }
        if (condition.getDateGreaterThan() != null) {
            if (checkDateType(condition.getDateGreaterThan(), conditionMap, POLICY_DATE_GREATER_THAN)) {
                return true;
            }
        }
        if (condition.getDateLessThan() != null) {
            if (checkDateType(condition.getDateLessThan(), conditionMap, POLICY_DATE_LESS_THAN)) {
                return true;
            }
        }
        if (condition.getDateGreaterThanEquals() != null) {
            if (checkDateType(condition.getDateGreaterThanEquals(), conditionMap, POLICY_DATE_GREATER_THAN)) {
                return true;
            }
        }
        if (condition.getDateLessThanEquals() != null) {
            return checkDateType(condition.getDateLessThanEquals(), conditionMap, POLICY_DATE_LESS_THAN_EQUALS);
        }
        return false;
    }

    public static boolean checkNumericList(Condition condition, Map<String, String> conditionMap) {
        if (condition.getNumericEquals() != null) {
            if (checkNumericType(condition.getNumericEquals(), conditionMap, POLICY_NUMERIC_EQUALS)) {
                return true;
            }
        }
        if (condition.getNumericNotEquals() != null) {
            if (checkNumericType(condition.getNumericNotEquals(), conditionMap, POLICY_NUMERIC_NOT_EQUALS)) {
                return true;
            }
        }
        if (condition.getNumericLessThan() != null) {
            if (checkNumericType(condition.getNumericLessThan(), conditionMap, POLICY_NUMERIC_LESS_THAN)) {
                return true;
            }
        }
        if (condition.getNumericGreaterThan() != null) {
            if (checkNumericType(condition.getNumericGreaterThan(), conditionMap, POLICY_NUMERIC_GREATER_THAN)) {
                return true;
            }
        }
        if (condition.getNumericLessThanEquals() != null) {
            if (checkNumericType(condition.getNumericLessThanEquals(), conditionMap, POLICY_NUMERIC_LESS_THAN_EQUALS)) {
                return true;
            }
        }
        if (condition.getNumericGreaterThanEquals() != null) {
            return checkNumericType(condition.getNumericGreaterThanEquals(), conditionMap, POLICY_NUMERIC_GREATER_THAN_EQUALS);
        }
        return false;
    }

    public static boolean checkStringList(Condition condition, Map<String, String> conditionMap, String method) {
        if (condition.getStringEquals() != null) {
            if (checkStringType(method, condition.getStringEquals(), conditionMap, false, POLICY_STRING_EQUALS)) {
                return true;
            }
        }
        if (condition.getStringEqualsIgnoreCase() != null) {
            if (checkStringType(method, condition.getStringEqualsIgnoreCase(), conditionMap, true, POLICY_STRING_EQUALS)) {
                return true;
            }
        }
        if (condition.getStringNotEquals() != null) {
            if (checkStringType(method, condition.getStringNotEquals(), conditionMap, false, POLICY_STRING_NOT_EQUALS)) {
                return true;
            }
        }
        if (condition.getStringNotEqualsIgnoreCase() != null) {
            if (checkStringType(method, condition.getStringNotEqualsIgnoreCase(), conditionMap, true, POLICY_STRING_NOT_EQUALS)) {
                return true;
            }
        }
        if (condition.getStringLike() != null) {
            if (checkStringType(method, condition.getStringLike(), conditionMap, false, POLICY_STRING_LIKE)) {
                return true;
            }
        }
        if (condition.getStringNotLike() != null) {
            return checkStringType(method, condition.getStringNotLike(), conditionMap, true, POLICY_STRING_NOT_LIKE);
        }
        // 策略未设置，默认生效
        return false;
    }


    public static boolean checkStringType(String method, PolicyString policyString, Map<String, String> conditionMap, boolean ignoreCase, String stringType) {
        // f默认的返回结果
        boolean f = false;
        // b标记当前条件键匹配结果
        boolean b = false;
        // c标记策略是否设置该条件键
        boolean c = false;
        if (POLICY_STRING_EQUALS.equals(stringType) || POLICY_STRING_LIKE.equals(stringType)) {
            f = true;
        }
        // 公共方法
        if (conditionMap.containsKey(POLICY_STRING_SIGNATURE_VERSION)) {
            String sourceSignature = conditionMap.get(POLICY_STRING_SIGNATURE_VERSION);
            if (ignoreCase) {
                sourceSignature = sourceSignature.trim().toLowerCase();
            }
            if (policyString.getSignatureVersion() != null) {
                String signature = policyString.getSignatureVersion();
                if (ignoreCase) {
                    signature = signature.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceSignature, signature, stringType);
                c = true;
            }

            if (policyString.getMossSignatureVersion() != null) {
                String signature = policyString.getMossSignatureVersion();
                if (ignoreCase) {
                    signature = signature.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceSignature, signature, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        } else {
            if (f && (policyString.getSignatureVersion() != null || policyString.getMossSignatureVersion() != null)) {
                return true;
            }
        }
        if (conditionMap.containsKey(POLICY_STRING_USER_AGENT)) {
            c = false;
            String sourceUserAgent = conditionMap.get(POLICY_STRING_USER_AGENT);
            if (ignoreCase) {
                sourceUserAgent = sourceUserAgent.trim().toLowerCase();
            }
            if (policyString.getUserAgent() != null) {
                String userAgent = policyString.getUserAgent();
                if (ignoreCase) {
                    userAgent = userAgent.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceUserAgent, userAgent, stringType);
                c = true;
            }

            if (policyString.getMossUserAgent() != null) {
                String userAgent = policyString.getMossUserAgent();
                if (ignoreCase) {
                    userAgent = userAgent.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceUserAgent, userAgent, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        } else {
            if (f && (policyString.getUserAgent() != null || policyString.getMossUserAgent() != null)) {
                return true;
            }
        }
        if (conditionMap.containsKey(POLICY_STRING_X_AMZ_SHA256)) {
            c = false;
            String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_SHA256);
            if (ignoreCase) {
                sourceStr = sourceStr.trim().toLowerCase();
            }
            if (policyString.getXAmzContentSha256() != null) {
                String policyStr = policyString.getXAmzContentSha256();
                if (!CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(policyStr) && !CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(policyStr)) {
                    return false;
                }
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }

            if (policyString.getMossXAmzContentSha256() != null) {
                String policyStr = policyString.getMossXAmzContentSha256();
                if (!CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(policyStr) && !CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(policyStr)) {
                    return false;
                }
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        } else {
            if (f && (policyString.getXAmzContentSha256() != null || policyString.getMossXAmzContentSha256() != null)) {
                return true;
            }
        }
        // Put/GetObjectVersionAcl, Get/DeleteObjectVersion使用
        if (conditionMap.containsKey(POLICY_STRING_VERSION_ID)) {
            c = false;
            String sourceVersionId = conditionMap.get(POLICY_STRING_VERSION_ID);
            if (ignoreCase) {
                sourceVersionId = sourceVersionId.trim().toLowerCase();
            }
            if (policyString.getVersionId() != null) {
                String versionId = policyString.getVersionId();
                if (ignoreCase) {
                    versionId = versionId.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceVersionId, versionId, stringType);
                c = true;
            }

            if (policyString.getMossVersionId() != null) {
                String versionId = policyString.getMossVersionId();
                if (ignoreCase) {
                    versionId = versionId.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceVersionId, versionId, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        } else {
            if (f && (policyString.getVersionId() != null || policyString.getMossVersionId() != null)) {
                return true;
            }
        }

        // ListObject使用，都为ture，策略不生效
        if (method.equals("moss:ListObjects") || method.equals("ListObjectsVersions")) {
            c = false;
            if (conditionMap.containsKey(POLICY_STRING_PREFIX)) {
                String sourcePrefix = conditionMap.get(POLICY_STRING_PREFIX);
                if (ignoreCase) {
                    sourcePrefix = sourcePrefix.trim().toLowerCase();
                }
                if (policyString.getPrefix() != null) {
                    String prefix = policyString.getPrefix();
                    if (ignoreCase) {
                        prefix = prefix.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourcePrefix, prefix, stringType);
                    c = true;
                }

                if (policyString.getMossPrefix() != null) {
                    String prefix = policyString.getMossPrefix();
                    if (ignoreCase) {
                        prefix = prefix.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourcePrefix, prefix, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getPrefix() != null || policyString.getMossPrefix() != null)) {
                    return true;
                }
            }

            if (conditionMap.containsKey(POLICY_STRING_DELIMITER)) {
                c = false;
                String sourceDelimiter = conditionMap.get(POLICY_STRING_DELIMITER);
                if (ignoreCase) {
                    sourceDelimiter = sourceDelimiter.trim().toLowerCase();
                }
                if (policyString.getDelimiter() != null) {
                    String delimiter = policyString.getDelimiter();
                    if (ignoreCase) {
                        delimiter = delimiter.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceDelimiter, delimiter, stringType);
                    c = true;
                }

                if (policyString.getMossDelimiter() != null) {
                    String delimiter = policyString.getMossDelimiter();
                    if (ignoreCase) {
                        delimiter = delimiter.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceDelimiter, delimiter, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getDelimiter() != null || policyString.getMossDelimiter() != null)) {
                    return true;
                }
            }
        }

        // PutObjectRetention使用
        if (conditionMap.containsKey(POLICY_STRING_LOCK_MODE)) {
            c = false;
            String sourceStr = conditionMap.get(POLICY_STRING_LOCK_MODE);
            if (ignoreCase) {
                sourceStr = sourceStr.trim().toLowerCase();
            }
            if (policyString.getObjectLockMode() != null) {
                String policyStr = policyString.getObjectLockMode();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }

            if (policyString.getMossDelimiter() != null) {
                String policyStr = policyString.getMossDelimiter();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        }

        // PutObjectAcl使用
        if (method.equals("moss:PutObjectAcl") || method.equals("PutObjectVersionAcl")) {
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_ACL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_ACL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzAcl() != null) {
                    String policyStr = policyString.getXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzAcl() != null) {
                    String policyStr = policyString.getMossXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzAcl() != null || policyString.getMossXAmzAcl() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_FULL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_FULL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getMossXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantFullControl() != null || policyString.getMossXAmzGrantFullControl() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_WRITE)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_WRITE);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantWrite() != null) {
                    String policyStr = policyString.getXAmzGrantWrite();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantWrite() != null) {
                    String policyStr = policyString.getMossXAmzGrantWrite();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantWrite() != null || policyString.getMossXAmzGrantWrite() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantRead() != null) {
                    String policyStr = policyString.getXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantRead() != null) {
                    String policyStr = policyString.getMossXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantRead() != null || policyString.getMossXAmzGrantRead() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_WRITE_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_WRITE_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantWriteAcp() != null) {
                    String policyStr = policyString.getXAmzGrantWriteAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantWriteAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantWriteAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantWriteAcp() != null || policyString.getMossXAmzGrantWriteAcp() != null)) {
                    return true;
                }
            }

            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantReadAcp() != null || policyString.getMossXAmzGrantReadAcp() != null)) {
                    return true;
                }
            }
        }

        // CopyObject使用
        if (conditionMap.containsKey(POLICY_STRING_X_AMZ_COPY_SOURCE)) {
            c = false;
            String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_COPY_SOURCE);
            if (ignoreCase) {
                sourceStr = sourceStr.trim().toLowerCase();
            }
            if (policyString.getXAmzCopySource() != null) {
                String policyStr = policyString.getXAmzCopySource();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }

            if (policyString.getMossXAmzCopySource() != null) {
                String policyStr = policyString.getMossXAmzCopySource();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceStr, policyStr, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }

            String sourceDirective = conditionMap.get(POLICY_STRING_X_AMZ_DIRECTIVE);
            c = false;
            if (ignoreCase) {
                sourceDirective = sourceDirective.trim().toLowerCase();
            }
            if (policyString.getXAmzMetadataDirective() != null) {
                String policyStr = policyString.getXAmzMetadataDirective();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceDirective, policyStr, stringType);
                c = true;
            }

            if (policyString.getMossXAmzMetadataDirective() != null) {
                String policyStr = policyString.getMossXAmzMetadataDirective();
                if (ignoreCase) {
                    policyStr = policyStr.trim().toLowerCase();
                }
                b = checkStringCompareType(sourceDirective, policyStr, stringType);
                c = true;
            }
            if (c && b) {
                return true;
            }
        }

        // PutObject使用
        if (conditionMap.containsKey("PutObjectSign")) {
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_ACL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_ACL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzAcl() != null) {
                    String policyStr = policyString.getXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzAcl() != null) {
                    String policyStr = policyString.getMossXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzAcl() != null || policyString.getMossXAmzAcl() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_FULL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_FULL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getMossXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantFullControl() != null || policyString.getMossXAmzGrantFullControl() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantRead() != null) {
                    String policyStr = policyString.getXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantRead() != null) {
                    String policyStr = policyString.getMossXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantRead() != null || policyString.getMossXAmzGrantRead() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_WRITE_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_WRITE_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantWriteAcp() != null) {
                    String policyStr = policyString.getXAmzGrantWriteAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantWriteAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantWriteAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantWriteAcp() != null || policyString.getMossXAmzGrantWriteAcp() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantReadAcp() != null || policyString.getMossXAmzGrantReadAcp() != null)) {
                    return true;
                }
            }

            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_ENCRYPTION)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_ENCRYPTION);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzServerSideEncryption() != null) {
                    String policyStr = policyString.getXAmzServerSideEncryption();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzServerSideEncryption() != null) {
                    String policyStr = policyString.getMossXAmzServerSideEncryption();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzServerSideEncryption() != null || policyString.getMossXAmzServerSideEncryption() != null)) {
                    return true;
                }
            }
        }

        // PutBucketAcl使用
        if (method.equals("moss:PutBucketAcl")) {
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_ACL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_ACL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzAcl() != null) {
                    String policyStr = policyString.getXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzAcl() != null) {
                    String policyStr = policyString.getMossXAmzAcl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzAcl() != null || policyString.getMossXAmzAcl() != null)) {
                    return true;
                }
            }

            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_FULL)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_FULL);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantFullControl() != null) {
                    String policyStr = policyString.getMossXAmzGrantFullControl();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantFullControl() != null || policyString.getMossXAmzGrantFullControl() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_WRITE)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_WRITE);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantWrite() != null) {
                    String policyStr = policyString.getXAmzGrantWrite();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantWrite() != null) {
                    String policyStr = policyString.getMossXAmzGrantWrite();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantWrite() != null || policyString.getMossXAmzGrantWrite() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantRead() != null) {
                    String policyStr = policyString.getXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantRead() != null) {
                    String policyStr = policyString.getMossXAmzGrantRead();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantRead() != null || policyString.getMossXAmzGrantRead() != null)) {
                    return true;
                }
            }
            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_WRITE_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_WRITE_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantWriteAcp() != null) {
                    String policyStr = policyString.getXAmzGrantWriteAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantWriteAcp() != null || policyString.getMossXAmzGrantReadAcp() != null)) {
                    return true;
                }
            }

            if (conditionMap.containsKey(POLICY_STRING_X_AMZ_READ_ACP)) {
                c = false;
                String sourceStr = conditionMap.get(POLICY_STRING_X_AMZ_READ_ACP);
                if (ignoreCase) {
                    sourceStr = sourceStr.trim().toLowerCase();
                }
                if (policyString.getXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }

                if (policyString.getMossXAmzGrantReadAcp() != null) {
                    String policyStr = policyString.getMossXAmzGrantReadAcp();
                    if (ignoreCase) {
                        policyStr = policyStr.trim().toLowerCase();
                    }
                    b = checkStringCompareType(sourceStr, policyStr, stringType);
                    c = true;
                }
                if (c && b) {
                    return true;
                }
            } else {
                if (f && (policyString.getXAmzGrantReadAcp() != null || policyString.getMossXAmzGrantReadAcp() != null)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean checkNumericType(PolicyNumeric numeric, Map<String, String> conditionMap, String numericType) {
        if (conditionMap != null) {
            if (conditionMap.containsKey(POLICY_NUMERIC_MAX_KEYS)) {
                String maxKeys = conditionMap.get(POLICY_NUMERIC_MAX_KEYS);
                int sourceMaxKeys = Integer.parseInt(maxKeys);
                if (numeric.getMaxKeys() != null) {
                    String maxKeys1 = numeric.getMaxKeys();
                    int policyMaxKeys = Integer.parseInt(maxKeys1);
                    return checkNumericCompareType(sourceMaxKeys, policyMaxKeys, numericType);
                }

                if (numeric.getMossMaxKeys() != null) {
                    String maxKeys1 = numeric.getMossMaxKeys();
                    int policyMaxKeys = Integer.parseInt(maxKeys1);
                    return checkNumericCompareType(sourceMaxKeys, policyMaxKeys, numericType);
                }
            }
        }
        return false;
    }

    public static boolean checkDateType(PolicyDate date, Map<String, String> conditionMap, String dateType) {
        // b标记综合匹配结果
        boolean b = false;
        if (conditionMap.containsKey(POLICY_DATE_CURRENT_TIME)) {
            long sourceDate = Long.parseLong(conditionMap.get(POLICY_DATE_CURRENT_TIME));
            if (date.getCurrentTime() != null) {
                String currentTime = date.getCurrentTime();
                long policyDate = Long.parseLong(MsDateUtils.policyToStamp(currentTime));
                b = checkDateCompareType(sourceDate, policyDate, dateType);
            }
            if (date.getMossCurrentTime() != null) {
                String currentTime = date.getMossCurrentTime();
                long policyDate = Long.parseLong(MsDateUtils.policyToStamp(currentTime));
                b = checkDateCompareType(sourceDate, policyDate, dateType);
            }
            if (b) {
                return true;
            }
        }
        if (conditionMap.containsKey(POLICY_DATE_WORM_DATE)) {
            String wormDate = conditionMap.get(POLICY_DATE_WORM_DATE);
            if (date.getRetentionDate() != null) {
                String retentionDate = date.getRetentionDate();
                long sourceDate = Long.parseLong(MsDateUtils.policyToStamp(wormDate));
                long policyDate = Long.parseLong(MsDateUtils.policyToStamp(retentionDate));
                return checkDateCompareType(sourceDate, policyDate, dateType);
            }
            if (date.getMossRetentionDate() != null) {
                String retentionDate = date.getMossRetentionDate();
                long sourceDate = Long.parseLong(MsDateUtils.policyToStamp(wormDate));
                long policyDate = Long.parseLong(MsDateUtils.policyToStamp(retentionDate));
                return checkDateCompareType(sourceDate, policyDate, dateType);
            }
        }
        return false;
    }

    private static boolean checkStringCompareType(String sourceStr, String policyStr, String stringType) {
        if (policyStr.contains("[")) {
            String[] prefixStr = Json.decodeValue(policyStr, String[].class);
            boolean b = false;
            if (POLICY_STRING_EQUALS.equals(stringType)) {
                for (String str : prefixStr) {
                    if (sourceStr.equals(str)) {
                        // 有一个相等生效
                        return false;
                    } else {
                        b = true;
                    }
                }
            } else if (POLICY_STRING_NOT_EQUALS.equals(stringType)) {
                for (String str : prefixStr) {
                    if (sourceStr.equals(str)) {
                        // 有一个相等不生效
                        b = true;
                        break;
                    }
                }
            } else if (POLICY_STRING_LIKE.equals(stringType)) {
                List<String> list = Collections.singletonList(policyStr);
                if (policyStr.contains("[")) {
                    list = Arrays.asList(prefixStr);
                }
                // 匹配上生效
                return !sourceActionMatch(list, sourceStr);
            } else if (POLICY_STRING_NOT_LIKE.equals(stringType)) {
                List<String> list = Collections.singletonList(policyStr);
                if (policyStr.contains("[")) {
                    list = Arrays.asList(prefixStr);
                }
                // 匹配上不生效
                return sourceActionMatch(list, sourceStr);
            }
            // 返回true策略不生效，默认生效
            return b;
        } else {
            if (POLICY_STRING_EQUALS.equals(stringType)) {
                // 有一个相等生效
                return !sourceStr.equals(policyStr);
            } else if (POLICY_STRING_NOT_EQUALS.equals(stringType)) {
                // 有一个相等不生效
                return sourceStr.equals(policyStr);
            } else if (POLICY_STRING_LIKE.equals(stringType)) {
                List<String> list = Collections.singletonList(policyStr);
                // 无法匹配不生效
                return !sourceActionMatch(list, sourceStr);
            } else if (POLICY_STRING_NOT_LIKE.equals(stringType)) {
                List<String> list = Collections.singletonList(policyStr);
                // 匹配上不生效
                return sourceActionMatch(list, sourceStr);
            }
        }
        return false;
    }

    private static boolean checkNumericCompareType(int sourceNumeric, int policyNumeric, String numericType) {
        if (POLICY_NUMERIC_EQUALS.equals(numericType)) {
            return sourceNumeric != policyNumeric;
        } else if (POLICY_NUMERIC_NOT_EQUALS.equals(numericType)) {
            return sourceNumeric == policyNumeric;
        } else if (POLICY_NUMERIC_LESS_THAN.equals(numericType)) {
            return sourceNumeric >= policyNumeric;
        } else if (POLICY_NUMERIC_LESS_THAN_EQUALS.equals(numericType)) {
            return sourceNumeric > policyNumeric;
        } else if (POLICY_NUMERIC_GREATER_THAN.equals(numericType)) {
            return sourceNumeric <= policyNumeric;
        } else if (POLICY_NUMERIC_GREATER_THAN_EQUALS.equals(numericType)) {
            return sourceNumeric < policyNumeric;
        }
        return false;
    }

    private static boolean checkDateCompareType(long sourceDate, long policyDate, String dateType) {
        if (POLICY_DATE_EQUALS.equals(dateType)) {
            return sourceDate != policyDate;
        } else if (POLICY_DATE_NOT_EQUALS.equals(dateType)) {
            return sourceDate == policyDate;
        } else if (POLICY_DATE_LESS_THAN.equals(dateType)) {
            return sourceDate >= policyDate;
        } else if (POLICY_DATE_LESS_THAN_EQUALS.equals(dateType)) {
            return sourceDate > policyDate;
        } else if (POLICY_DATE_GREATER_THAN.equals(dateType)) {
            return sourceDate <= policyDate;
        } else if (POLICY_DATE_GREATER_THAN_EQUALS.equals(dateType)) {
            return sourceDate < policyDate;
        }
        return false;
    }

    //判断 Action 是否匹配，不区分大小写
    public static boolean actionMatch(List<String> resource, String action) {

        for (String s : resource) {
            boolean match = false;
            if (sourceAction.get(s) != null) {
                match = isMatch(action.trim().toLowerCase(), sourceAction.get(s).trim().toLowerCase());
            }
            if (match || isMatch(action.trim().toLowerCase(), s.trim().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    //判断是否匹配
    public static boolean sourceActionMatch(List<String> resource, String action) {
        for (String s : resource) {
            boolean match = false;
            if (sourceAction.get(s) != null) {
                match = isMatch(action.trim(), sourceAction.get(s).trim());
            }
            if (match || isMatch(action.trim(), s.trim())) {
                return true;
            }
        }
        return false;
    }

    // 简化 Principal 前缀校验
    public static List<String> updatePrincipal(List<String> service) {
        List<String> principal = new ArrayList<>();
        for (String source : service) {
            if (source.split(":").length == 6) {
                source = source.split(":", 5)[4];
                logger.debug(source);
            }
            principal.add(source);
        }
        return principal;
    }


}
