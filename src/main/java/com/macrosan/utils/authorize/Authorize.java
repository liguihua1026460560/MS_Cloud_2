package com.macrosan.utils.authorize;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.AccessKeyVO;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.iam.IamUtils.ActionType;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.property.PropertyReader;
import com.macrosan.utils.serialize.JsonUtils;
import com.macrosan.utils.sts.StsCredentialSyncTask;
import io.vertx.core.http.HttpMethod;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.jsonmsg.Credential.ERROR_STS_TOKEN;
import static com.macrosan.message.jsonmsg.Credential.NOT_FOUND_STS_TOKEN;
import static com.macrosan.utils.iam.IamUtils.*;
import static com.macrosan.utils.sts.RoleUtils.ARN_ROLE_PREFIX;
import static com.macrosan.utils.sts.RoleUtils.ASSUME_PREFIX;

/**
 * Authorize
 *
 * @author liyixin
 * @date 2018/12/4
 */
class Authorize {

    private static RedisConnPool pool = RedisConnPool.getInstance();
    private static IamRedisConnPool iamPool = IamRedisConnPool.getInstance();
    private static Redis6380ConnPool pool_6380 = Redis6380ConnPool.getInstance();
    private static final Map<String, String> SIGN_MAP;

    static {
        PropertyReader reader = new PropertyReader(DATA_ROUTE);
        Map<String, String> dataMap = reader.getPropertyAsMap();

        reader = new PropertyReader(MANAGE_ROUTE);
        Map<String, String> manageMap = reader.getPropertyAsMap();

        SIGN_MAP = new UnifiedMap<>();
        dataMap.forEach((k, v) -> {
            String value = v.contains("?")
                    ? v.split("\\?")[0].replaceAll("bucket", "").replaceAll("object", "")
                    + "?" + v.split("\\?")[1]
                    : v.replaceAll("bucket", "").replaceAll("object", "");
//            v = v.replaceAll("bucket", "").replaceAll("object", "");
            SIGN_MAP.put(value, k);
        });

        manageMap.forEach((k, v) -> {
            String value = v.contains("?")
                    ? v.split("\\?")[0].replaceAll("bucket", "").replaceAll("object", "")
                    + "?" + v.split("\\?")[1]
                    : v.replaceAll("bucket", "").replaceAll("object", "");
//            v = v.replaceAll("bucket", "").replaceAll("object", "");
            SIGN_MAP.put(value, k);
        });
    }

    static Mono<Map<String, String>> findSecretAccessKeyByIam(MsHttpRequest request, String sign,
                                                              String accessKey) {
        String bucket = request.getBucketName();
        String object = request.getObjectName();

        String[] accId = new String[1];
        accId[0] = DEFAULT_ACCOUNTID;
        Map<String, String> map = new HashMap<>(4);
        ActionType actionType = ActionType.valueOf(
                SIGN_MAP.get(sign)
                        .split("-")[1].toUpperCase()
        );

        //首先去6381表0中获取
        return iamPool.getReactive(SysConstants.REDIS_IAM_INDEX).exists(accessKey)
                .flatMap(l -> {
                    if (1 == l) {
                        return iamPool.getReactive(SysConstants.REDIS_IAM_INDEX)
                                .get(accessKey)
                                .flatMap(accessKeyValue -> {
                                    AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                                    String userId = accessKeyVO.getUserId();
                                    accId[0] = accessKeyVO.getAccountId();//获取账户ID
                                    if ("admin".equalsIgnoreCase(accId[0]) && !"PUT/?createadminaccesskey".equals(sign) && StringUtils.isNotEmpty(accessKeyVO.getAcl())) {
                                        if (request.method() != HttpMethod.GET && request.method() != HttpMethod.HEAD) {
                                            throw new MsException(ErrorNo.ACCESS_DENY, "ak can read only.");
                                        }
                                    }
                                    map.put(SECRET_KEY, accessKeyVO.getSecretKey());
                                    map.put(USERNAME, accessKeyVO.getUserName());

                                    if ("admin".equalsIgnoreCase(accId[0]) && request.headers().contains("accountId") &&
                                            ("moss:GetTrafficStatistics".equals(actionType.getActionType()) || "moss:GetTrafficStatisticsList".equals(actionType.getActionType()))) {
                                        return Mono.just("admin");
                                    }

                                    if ((actionType.getActionType().startsWith("iam:") || actionType.getActionType().startsWith("sts:")) && request.getMember("Iam_Action") != null) {
                                        String accountId = accessKeyVO.accountId;
                                        String action = request.getMember("Iam_Action");
                                        String userName = request.getMember("Iam_UserName", "");
                                        String groupName = request.getMember("Iam_GroupName", "");
                                        String policyName = request.getMember("Iam_PolicyName", "");
                                        String roleName = request.getMember("Iam_RoleName", "");

                                        if (StringUtils.isBlank(userName) && IamAuth.curUserAction.contains(action)) {
                                            userName = accessKeyVO.userName;
                                            request.addMember("Iam_UserName", userName);
                                        }
                                        if (StringUtils.isBlank(policyName)) {
                                            String policyArn = request.getMember("Iam_PolicyArn", "");
                                            if (StringUtils.isNotBlank(policyArn)) {
                                                int start = policyArn.indexOf("policy/");
                                                policyName = policyArn.substring(start + "policy/".length());
                                            }
                                        }
                                        String roleArn = request.getMember("Iam_RoleArn", "");
                                        if (StringUtils.isNotBlank(roleArn)) {
                                            String[] split = roleArn.split(":");
                                            if (split.length == 6 && split[5].startsWith(ARN_ROLE_PREFIX) && split[5].split("/").length == 2) {
                                                accountId = split[4];//获取角色arn中的账户id
                                                roleName = split[5].split("/")[1];//获取角色名
                                            } else {
                                                accountId = "";
                                            }
                                        }
                                        return IamAuth.auth(action, userId, accountId, userName, groupName, policyName, roleName, request);
                                    } else {
                                        String resource = "mrn::moss:::";
                                        if (StringUtils.isNotBlank(bucket)) {
                                            resource += bucket;
                                        }

                                        if (StringUtils.isNotBlank(object)) {
                                            resource += "/" + object;
                                        }

                                        return IamUtils.checkAuth(userId, actionType, resource);
                                    }
                                })
                                .onErrorMap(e -> {
                                    if (e.getMessage() != null && e.getMessage().startsWith("ak can read only")) {
                                        return new MsException(ErrorNo.ACCESS_FORBIDDEN, "ak can read only, " + accessKey);
                                    }
                                    if ("admin".equalsIgnoreCase(accId[0])) {
                                        return new MsException(ErrorNo.ACCESS_DENY, "manage user has no permission to access." + actionType.getActionType());
                                    }
                                    if (MANAGER_ACTION_TYPE_LIST.contains(actionType.name())) {
                                        return new MsException(ErrorNo.ACCESS_DENY, "not manage user, no permission.");
                                    }

                                    return new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail");
                                })
                                .switchIfEmpty(Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey)))
                                .flatMap(accountId -> {
                                    map.put(ID, accountId);
                                    return Mono.just(map);
                                });
                    } else {
                        //如果6381表0中不存在，则去6380表0中获取，此时只校验临时凭证
                        return pool_6380.getReactive(REDIS_IAM_INDEX).exists(accessKey)
                                .flatMap(l0 -> {
                                    if (1 == l0) {
                                        return pool_6380.getReactive(SysConstants.REDIS_IAM_INDEX)
                                                .get(accessKey)
                                                .flatMap(accessKeyValue -> {
                                                    AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                                                    String userId = accessKeyVO.getUserId();
                                                    accId[0] = accessKeyVO.getAccountId();
                                                    map.put(SECRET_KEY, accessKeyVO.getSecretKey());
                                                    map.put(USERNAME, accessKeyVO.getUserName());
                                                    if ((actionType.getActionType().startsWith("iam:") || actionType.getActionType().startsWith("sts:")) && request.getMember("Iam_Action") != null) {
                                                        String accountId = accessKeyVO.accountId;
                                                        String action = request.getMember("Iam_Action");
                                                        String userName = request.getMember("Iam_UserName", "");
                                                        String groupName = request.getMember("Iam_GroupName", "");
                                                        String policyName = request.getMember("Iam_PolicyName", "");
                                                        String roleName = request.getMember("Iam_RoleName", "");

                                                        if (StringUtils.isBlank(userName) && IamAuth.curUserAction.contains(action)) {
                                                            userName = accessKeyVO.userName;
                                                            request.addMember("Iam_UserName", userName);
                                                        }
                                                        if (StringUtils.isBlank(policyName)) {
                                                            String policyArn = request.getMember("Iam_PolicyArn", "");
                                                            if (StringUtils.isNotBlank(policyArn)) {
                                                                int start = policyArn.indexOf("policy/");
                                                                policyName = policyArn.substring(start + "policy/".length());
                                                            }
                                                        }
                                                        String roleArn = request.getMember("Iam_RoleArn", "");
                                                        if (StringUtils.isNotBlank(roleArn)) {
                                                            String[] split = roleArn.split(":");
                                                            if (split.length == 6 && split[5].startsWith(ARN_ROLE_PREFIX) && split[5].split("/").length == 2) {
                                                                accountId = split[4];
                                                                roleName = split[5].split("/")[1];
                                                            } else {
                                                                accountId = "";
                                                            }
                                                        }
                                                        return IamAuth.auth(action, userId, accountId, userName, groupName, policyName, roleName, request);
                                                    } else {
                                                        String resource = "mrn::moss:::";
                                                        if (StringUtils.isNotBlank(bucket)) {
                                                            resource += bucket;
                                                        }

                                                        if (StringUtils.isNotBlank(object)) {
                                                            resource += "/" + object;
                                                        }
                                                        if (userId != null && userId.startsWith(ASSUME_PREFIX)) {
                                                            return checkTempUserAuth(request, userId, actionType, resource);
                                                        }
                                                        return IamUtils.checkAuth(userId, actionType, resource);
                                                    }
                                                })
                                                .onErrorMap(e -> {
                                                    if (MANAGER_ACTION_TYPE_LIST.contains(actionType.name())) {
                                                        return new MsException(ErrorNo.ACCESS_DENY, "not manage user, no permission.");
                                                    }

                                                    return new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail");
                                                })
                                                .switchIfEmpty(Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey)))
                                                .flatMap(accountId -> {
                                                    map.put(ID, accountId);
                                                    return Mono.just(map);
                                                });
                                    } else {
                                        //redis中不存在直接去rocksdb中获取,这时只会对临时凭证进行校验
                                        Credential cred = StsCredentialSyncTask.credentialCache.get(accessKey);
                                        return (cred != null ? Mono.just(cred) : ErasureClient.getCredential(accessKey))
                                                .flatMap(credential -> {
                                                    if (ERROR_STS_TOKEN.equals(credential)) {
                                                        return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                                                    }
                                                    if (NOT_FOUND_STS_TOKEN.equals(credential)) {
                                                        return Mono.empty();
                                                    }
                                                    long cur = System.currentTimeMillis() / 1000;
                                                    if (cur > credential.deadline) {
                                                        return Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey));
                                                    }
                                                    String userId = credential.getAssumeId();
                                                    accId[0] = credential.getAccountId();
                                                    map.put(SECRET_KEY, credential.getSecretKey());
                                                    map.put(USERNAME, credential.getUseName());

                                                    if ((actionType.getActionType().startsWith("iam:") || actionType.getActionType().startsWith("sts:")) && request.getMember("Iam_Action") != null) {
                                                        String accountId = credential.getAccountId();
                                                        String action = request.getMember("Iam_Action");
                                                        String userName = request.getMember("Iam_UserName", "");
                                                        String groupName = request.getMember("Iam_GroupName", "");
                                                        String policyName = request.getMember("Iam_PolicyName", "");
                                                        String roleName = request.getMember("Iam_RoleName", "");

                                                        if (StringUtils.isBlank(userName) && IamAuth.curUserAction.contains(action)) {
                                                            userName = credential.getUseName();
                                                            request.addMember("Iam_UserName", userName);
                                                        }
                                                        if (StringUtils.isBlank(policyName)) {
                                                            String policyArn = request.getMember("Iam_PolicyArn", "");
                                                            if (StringUtils.isNotBlank(policyArn)) {
                                                                int start = policyArn.indexOf("policy/");
                                                                policyName = policyArn.substring(start + "policy/".length());
                                                            }
                                                        }
                                                        String roleArn = request.getMember("Iam_RoleArn", "");
                                                        if (StringUtils.isNotBlank(roleArn)) {
                                                            String[] split = roleArn.split(":");
                                                            if (split.length == 6 && split[5].startsWith(ARN_ROLE_PREFIX) && split[5].split("/").length == 2) {
                                                                accountId = split[4];
                                                                roleName = split[5].split("/")[1];
                                                            } else {
                                                                accountId = "";
                                                            }
                                                        }
                                                        return IamAuth.authFromRocksDb(action, userId, credential, accountId, userName, groupName, policyName, roleName, request);
                                                    } else {
                                                        String resource = "mrn::moss:::";
                                                        if (StringUtils.isNotBlank(bucket)) {
                                                            resource += bucket;
                                                        }

                                                        if (StringUtils.isNotBlank(object)) {
                                                            resource += "/" + object;
                                                        }
                                                        if (userId != null && userId.startsWith(ASSUME_PREFIX)) {
                                                            return checkTempUserAuthFromRocksDb(request, credential, actionType, resource);
                                                        }

                                                        return IamUtils.checkAuth(userId, actionType, resource);
                                                    }
                                                })
                                                .onErrorMap(e -> {
                                                    if (accId[0].equalsIgnoreCase("admin")) {
                                                        return new MsException(ErrorNo.ACCESS_DENY, "manage user has no permission to access." + actionType.getActionType());
                                                    }
                                                    if (MANAGER_ACTION_TYPE_LIST.contains(actionType.name())) {
                                                        return new MsException(ErrorNo.ACCESS_DENY, "not manage user, no permission.");
                                                    }
                                                    return e;
                                                })
                                                .switchIfEmpty(Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey)))
                                                .flatMap(accountId -> {
                                                    map.put(ID, accountId);
                                                    return Mono.just(map);
                                                });
                                    }
                                });
                    }
                });
    }

    static Mono<Map<String, String>> findComponentSecretAccessKey(MsHttpRequest request, String accessKey) {
        Map<String, String> map = new HashMap<>(4);

        return iamPool.getReactive(SysConstants.REDIS_IAM_INDEX)
                .get(accessKey)
                .doOnNext(accessKeyValue -> {
                    AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                    map.put(SECRET_KEY, accessKeyVO.getSecretKey());
                    map.put(USERNAME, accessKeyVO.getUserName());
                    map.put(ID, request.getHeader(COMPONENT_USER_ID));
                })
                .flatMap(ak -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(request.getHeader(COMPONENT_USER_ID)))
                .doOnNext(userMap -> map.put(USERNAME, userMap.get("name")))
                .onErrorMap(e -> new MsException(ErrorNo.ACCESS_FORBIDDEN, "Component IAM check fail"))
                .switchIfEmpty(Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey)))
                .flatMap(accountId -> Mono.just(map));
    }

    static Mono<Map<String, String>> findSecretAccessKeyByIam(String bucket, String object, String sign, String accessKey) {
        String[] accId = new String[1];
        accId[0] = DEFAULT_ACCOUNTID;
        Map<String, String> map = new HashMap<>(4);
        ActionType actionType = ActionType.valueOf(
                SIGN_MAP.get(sign)
                        .split("-")[1].toUpperCase()
        );

        return iamPool.getReactive(SysConstants.REDIS_IAM_INDEX)
                .get(accessKey)
                .flatMap(accessKeyValue -> {
                    AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                    String userId = accessKeyVO.getUserId();
                    accId[0] = accessKeyVO.getAccountId();
                    map.put(SECRET_KEY, accessKeyVO.getSecretKey());
                    map.put(USERNAME, accessKeyVO.getUserName());
                    String resource = "mrn::moss:::";
                    if (StringUtils.isNotBlank(bucket)) {
                        resource += bucket;
                    }

                    if (StringUtils.isNotBlank(object)) {
                        resource += "/" + object;
                    }

                    return IamUtils.checkAuth(userId, actionType, resource);
                })
                .onErrorMap(e -> {
                    if (accId[0].equalsIgnoreCase("admin")) {
                        return new MsException(ErrorNo.ACCESS_DENY, "manage user has no permission to access." + actionType.getActionType());
                    }
                    if (MANAGER_ACTION_TYPE_LIST.contains(actionType.name())) {
                        return new MsException(ErrorNo.ACCESS_DENY, "not manage user, no permission.");
                    }

                    return new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail");
                })
                .switchIfEmpty(Mono.error(new MsException(ErrorNo.AK_NOT_MATCH, "signature failed, access_key doesn't exist. access_key:" + accessKey)))
                .flatMap(accountId -> {
                    map.put(ID, accountId);
                    return Mono.just(map);
                });
    }

    static Mono<Map<String, String>> findSecretAccessKey(String accessKey) {
        return pool.getSharedConnection(3).reactive().hgetall(accessKey);
    }
}
