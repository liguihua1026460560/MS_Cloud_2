package com.macrosan.utils.policy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.xmlmsg.worm.Retention;
import com.macrosan.utils.authorize.AuthorizeFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.functional.Entry;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.ServerConstants.X_AMZ_ACL;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.authorize.AuthorizeV4.*;
import static com.macrosan.utils.policy.PolicyCheckUtils.*;

public class ReactorPolicyCheckUtils {
    public static final Logger logger = LogManager.getLogger(ReactorPolicyCheckUtils.class.getName());

    private static RedisConnPool pool = RedisConnPool.getInstance();

    /**
     * 根据请求和不同方法进行桶级别校验
     */
    public static Mono<Integer> getPolicyCheckResult(MsHttpRequest request, String bucketName, String method) {
        Map<String, String> conditionMap = new HashMap<>();
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        return getPolicyCheckResult(request, bucketName, "", method, sourceIp, conditionMap);
    }

    /**
     * 根据请求和不同方法进行对象级别校验
     */
    public static Mono<Integer> getPolicyCheckResult(MsHttpRequest request, String bucketName, String object, String method) {
        Map<String, String> conditionMap = new HashMap<>();
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * DeleteObject 根据请求和不同方法进行对象级别校验(特殊处理批量删除时的策略判断)
     */
    public static Mono<Integer> getPolicyCheckResult(MsHttpRequest request, String bucketName, String object, String version, String method, String sourceIp) {
        Map<String, String> conditionMap = new HashMap<>();
        if ("DeleteObjectVersion".equals(method)) {
            conditionMap.put(POLICY_STRING_VERSION_ID, version);
        }
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * GetObject, GetObjectAcl请求使用
     */
    public static Mono<Integer> getPolicyGetCheckResult(MsHttpRequest request, String bucketName, String object, String version, String method) {
        Map<String, String> conditionMap = new HashMap<>();
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        if ("GetObjectVersion".equals(method) || "GetObjectVersionAcl".equals(method)) {
            conditionMap.put(POLICY_STRING_VERSION_ID, version);
        }
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * PutObject请求使用
     */
    public static Mono<Integer> getPolicyPutCheckResult(MsHttpRequest request, String bucketName, String object, String method, JsonObject aclJson, String crypto) {
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        HashMap<String, String> conditionMap = new HashMap<>();
        String acl = "private";
        if (request.headers().contains("x-amz-acl")) {
            acl = request.headers().get("x-amz-acl");
        }
        conditionMap.put(POLICY_STRING_X_AMZ_ACL, acl);
        if (StringUtils.isNotBlank(crypto)) {
            conditionMap.put(POLICY_STRING_X_AMZ_ENCRYPTION, crypto);
        }
        for (Map.Entry<String, Object> entry : aclJson) {
            String k = entry.getKey();
            if (k.contains("-")) {
                String[] split = k.split("-");
                String aclType = split[0];
                String id = split[1];
                if (OBJECT_PERMISSION_READ.equals(aclType)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_READ, POLICY_X_AMZ_PREFIX + id);
                } else if (OBJECT_PERMISSION_READ_CAP.equals(aclType)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_READ_ACP, POLICY_X_AMZ_PREFIX + id);
                } else if (OBJECT_PERMISSION_WRITE_CAP.equals(aclType)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_WRITE_ACP, POLICY_X_AMZ_PREFIX + id);
                } else if (OBJECT_PERMISSION_FULL_CON.equals(aclType)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_FULL, POLICY_X_AMZ_PREFIX + id);
                }
            }
        }
        conditionMap.put("PutObjectSign", "1");
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * PutObjectACL请求使用
     */
    public static Mono<Integer> getPolicyObjectAclCheckResult(MsHttpRequest request, String bucketName, String object, String version, String method, List<Entry<String, String>> aclList) {
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        Map<String, String> conditionMap = new HashMap<>();
        final String sourceIp = request.remoteAddress().host();
        if (aclList.size() > 0) {
            for (Entry<String, String> map : aclList) {
                String aclType = map.getValue();
                String targetUserId = map.getKey();
                if (aclType.equalsIgnoreCase(PERMISSION_READ_LONG)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_READ, POLICY_X_AMZ_PREFIX + targetUserId);
                } else if (aclType.equalsIgnoreCase(PERMISSION_READ_CAP_LONG)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_READ_ACP, POLICY_X_AMZ_PREFIX + targetUserId);
                } else if (aclType.equalsIgnoreCase(PERMISSION_WRITE_CAP_LONG)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_WRITE_ACP, POLICY_X_AMZ_PREFIX + targetUserId);
                } else {
                    conditionMap.put(POLICY_STRING_X_AMZ_FULL, POLICY_X_AMZ_PREFIX + targetUserId);
                }
            }
        } else if (request.headers().contains("x-amz-acl")) {
            String acl = request.headers().get("x-amz-acl");
            conditionMap.put(X_AMZ_ACL, acl);
        }
        if ("PutObjectVersionAcl".equals(method)) {
            conditionMap.put(POLICY_STRING_VERSION_ID, version);
        }
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * CopyObject请求使用
     */
    public static Mono<Integer> getPolicyCopyCheckResult(MsHttpRequest request, String sourceBucket, String sourceObject, String targetBucket, String targetObject, String method) {
        final String sourceIp = request.remoteAddress().host();
        HashMap<String, String> conditionMap = new HashMap<>();
        conditionMap.put(POLICY_STRING_X_AMZ_COPY_SOURCE, sourceBucket + "/" + sourceObject);
        String directive = request.getHeader(X_AMZ_METADATA_DIRECTIVE);
        directive = StringUtils.isEmpty(directive) ? "COPY" : directive;
        conditionMap.put(POLICY_STRING_X_AMZ_DIRECTIVE, directive);
        return getPolicyCheckResult(request, targetBucket, targetObject, method, sourceIp, conditionMap);
    }

    /**
     * ListObject和ListObjectVersion请求使用
     */
    public static Mono<Integer> getPolicyListCheckResult(MsHttpRequest request, String bucketName, String method, int maxKey) {
        return getPolicyListCheckResult(request, bucketName, "", method, maxKey);
    }

    public static Mono<Integer> getPolicyListCheckResult(MsHttpRequest request, String bucketName, String object, String method, int maxKey) {
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        String prefix = request.getParam(PREFIX, "");
        String delimiter = request.getParam(DELIMITER, "");
        Map<String, String> conditionMap = new HashMap<>();
        conditionMap.put(POLICY_NUMERIC_MAX_KEYS, maxKey + "");
        conditionMap.put(POLICY_STRING_PREFIX, prefix);
        conditionMap.put(POLICY_STRING_DELIMITER, delimiter);
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }


    /**
     * putObjectRetention请求使用
     */
    public static Mono<Integer> getPolicyWormCheckResult(MsHttpRequest request, String bucketName, String object, String method, Retention retention) {
        if (request.headers().contains(COMPONENT_USER_ID)) {
            return Mono.just(1);
        }
        final String sourceIp = request.remoteAddress().host();
        HashMap<String, String> conditionMap = new HashMap<>();
        String wormDate = retention.getRetainUntilDate();
        String mode = retention.getMode();
        conditionMap.put(POLICY_DATE_WORM_DATE, wormDate);
        conditionMap.put(POLICY_STRING_LOCK_MODE, mode);
        return getPolicyCheckResult(request, bucketName, object, method, sourceIp, conditionMap);
    }

    /**
     * 获取请求通用字段
     */
    private static void addPolicyPublicCondition(MsHttpRequest request, Map<String, String> conditionMap) {
        // CurrentTime
        long currentTimeMillis = System.currentTimeMillis();
        conditionMap.put(POLICY_DATE_CURRENT_TIME, currentTimeMillis + "");
        // HTTPS
        if ("1".equals(request.headers().get("http-ssl"))) {
            conditionMap.put(POLICY_BOOL_SECURE_TRANSPORT, "true");
        } else {
            conditionMap.put(POLICY_BOOL_SECURE_TRANSPORT, "false");
        }
        // signatureVersion: v2 or v4
        if (request.headers().contains(AUTHORIZATION)) {
            String[] array = request.getHeader(AUTHORIZATION).split(" ");
            String authType = array[0];
            if (authType.equals(AuthorizeFactory.AWS)) {
                conditionMap.put(POLICY_STRING_SIGNATURE_VERSION, AuthorizeFactory.AWS);
            } else if (authType.equals(AuthorizeFactory.SHA256)) {
                conditionMap.put(POLICY_STRING_SIGNATURE_VERSION, AuthorizeFactory.SHA256);
                String contentSHA256 = request.getHeader(X_AMZ_CONTENT_SHA_256);
                if (CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(contentSHA256) || CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(contentSHA256)) {
                    conditionMap.put(POLICY_STRING_X_AMZ_SHA256, contentSHA256);
                }
                conditionMap.put(POLICY_STRING_X_AMZ_SHA256, contentSHA256);
            }
        }
        // User-Agent
        if (request.headers().contains(USER_AGENT)) {
            String userAgent = request.getHeader(USER_AGENT);
            conditionMap.put(POLICY_STRING_USER_AGENT, userAgent);
        }
    }

    public static Mono<Integer> getPolicyCheckResult(MsHttpRequest request, String bucketName, String object, String method, String sourceIp, Map<String, String> conditionMap) {
        addPolicyPublicCondition(request, conditionMap);
        if (request.headers().contains(IS_SYNCING)) {
            return Mono.just(0);
        }
        final String bucketPolicy = bucketName + POLICY_SUFFIX;
        final String userName = request.getUserName() == null ? "" : request.getUserName();
        return pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketPolicy)
                .flatMap(exist -> {
                    if (0 == exist) {
                        return Mono.just(0);
                    }
                    return ReactorPolicyCheckUtils.getPolicyMetaReactor(bucketName, object, request.getUserId(), userName, method, sourceIp, conditionMap)
                            .onErrorMap(e -> new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such action permission"))
                            .flatMap(access -> {
                                logger.debug("access = {}", access);
                                return Mono.just(access);
                            });
                });
    }

    public static Mono<Integer> getPolicyCheckResult(String userId, String userName, String bucketName, String object, String method, String sourceIp) {
        HashMap<String, String> conditionMap = new HashMap<>();
        final String bucketPolicy = bucketName + POLICY_SUFFIX;
        return pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketPolicy)
                .flatMap(exist -> {
                    if (0 == exist) {
                        return Mono.just(0);
                    }
                    return ReactorPolicyCheckUtils.getPolicyMetaReactor(bucketName, object, userId, userName, method, sourceIp, conditionMap)
                            .onErrorMap(e -> new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such action permission"))
                            .flatMap(access -> {
                                logger.debug("access = {}", access);
                                return Mono.just(access);
                            });
                });

    }

    /**
     * 响应式获取redis中策略并返回校验结果,资源是对象级别,上传、删除对象只需要判断到桶级别
     */
    public static Mono<Integer> getPolicyMetaReactor(String bucketName, String keyName, String accountId, String userName, String method, String sourceIp, Map<String, String> conditionMap) {
        //存放策略信息
        String policys = bucketName + POLICY_SUFFIX;
        final String root = accountId + ":root";
        final String userSource = "".equals(userName) ? root : accountId + ":user/" + userName;
        final int[] res = {0};
        return pool.getReactive(REDIS_TASKINFO_INDEX).hget(policys, "policy0")
                .flatMapMany(policyInfo -> {
                    List<Statement> tempValues = JSON.parseObject(policyInfo,
                            new TypeReference<List<Statement>>() {
                            });
                    return Flux.fromStream(Arrays.stream(tempValues.toArray()));
                }).flatMap(stat -> {
                    Statement statement = (Statement) stat;
                    return policyCheckReactor(statement, bucketName, keyName, sourceIp, method, accountId, root, userSource, conditionMap);
                }).collect(Collectors.toSet())
                .flatMap(deny -> {
                    if (deny.contains(2)) {
                        return Mono.error(new MsException(ErrorNo.NO_BUCKET_PERMISSION, "no such action permission"));
                    }

                    if (deny.contains(1)) {
                        return Mono.just(1);
                    }

                    return Mono.just(res[0]);
                });

    }

    /**
     * 授权人是否匹配
     * true 表示匹配
     */
    public static boolean principalMatch(String principal, String root, String user) {

        if (BucketPolicyUtils.isMatch(root, principal)) {
            return true;
        }

        if (BucketPolicyUtils.isMatch(user, principal)) {
            return true;
        }

        if (BucketPolicyUtils.isMatch("*", principal)) {
            return true;
        }
        return false;
    }

    /**
     * 校验一条statement 的结果
     * 需要方法名, 资源名, 先要匹配资源,在匹配方法,最后看effect
     */
    public static Mono<Integer> policyCheckReactor(Statement statement, String bucketName, String keyName, String sourceIp, String method, String account, String root, String sourcePrincipal, Map<String, String> conditionMap) {
        String source = "".equals(keyName) ? bucketName : bucketName + SLASH + keyName;
        Condition condition = statement.getCondition();
        method = "moss:" + method;

        //授权人不匹配,直接进行下一条策略
        if (statement.NotPrincipal != null) {
            List<String> service = statement.getNotPrincipal().service;
            List<String> principal = updatePrincipal(service);
            if (sourceActionMatch(principal, account) || sourceActionMatch(principal, root) || sourceActionMatch(principal, sourcePrincipal)) {
                return Mono.just(0);
            }
        }

        if (statement.Principal != null) {
            List<String> service = statement.getPrincipal().service;
            List<String> principal = updatePrincipal(service);
            if (!sourceActionMatch(principal, account) && !sourceActionMatch(principal, root) && !sourceActionMatch(principal, sourcePrincipal)) {
                return Mono.just(0);
            }
        }

        if (statement.NotResource != null) {
            if (sourceActionMatch(statement.getNotResource(), source)) {
                return Mono.just(0);
            }
        }

        if (statement.getResource() != null && !sourceActionMatch(statement.getResource(), source)) {
            return Mono.just(0);
        }

        if (statement.NotAction != null) {
            if (actionMatch(statement.getNotAction(), method)) {
                return Mono.just(0);
            }
        }

        if (statement.getAction() != null && !actionMatch(statement.getAction(), method)) {
            return Mono.just(0);
        }

        if (condition != null) {
            // IP类型
            if (condition.getIp() != null) {
                if (checkIp(sourceIp, condition.getIp())) {
                    return Mono.just(0);
                }
            }
            if (condition.getNotIp() != null) {
                if (checkNotIp(sourceIp, condition.getNotIp())) {
                    return Mono.just(0);
                }
            }

            // 布尔类型
            if (checkBoolList(condition, conditionMap)) {
                return Mono.just(0);
            }

            // 日期类型
            if (checkDateList(condition, conditionMap)) {
                return Mono.just(0);
            }

            // 数值类型 (目前ListObject才会调用)
            if (checkNumericList(condition, conditionMap)) {
                return Mono.just(0);
            }

            // 字符串类型
            if (checkStringList(condition, conditionMap, method)) {
                return Mono.just(0);
            }
        }

        return Mono.just(Integer.valueOf(statement.Effect));
    }
}
