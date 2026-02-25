package com.macrosan.utils.iam;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.AbstractConnection;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.utils.authorize.IamAuth;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JsonUtils;
import com.macrosan.utils.sts.LRUCache;
import com.macrosan.utils.sts.StsCredentialSyncTask;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.sts.RoleUtils.getAssumeInfo;

/**
 * IamUtils
 *
 * @author gaozhiyuan
 * @date 2019.5.21
 */
public class IamUtils {
    private static IamRedisConnPool pool = IamRedisConnPool.getInstance();
    private static Redis6380ConnPool pool_6380 = Redis6380ConnPool.getInstance();
    private static final Logger logger = LogManager.getLogger(IamUtils.class.getName());
    private static final int FIND_DENY = 1;
    private static final int FIND_ALLOW = 2;
    private static final int FIND_NOTHING = 3;
    private static final int FIND_FS_ERROR = 4;
    public static boolean iamDebug = false;

    private static Flux<String> getGroupPolicyId(String groupValue) {
        if (StringUtils.isNotBlank(groupValue)) {
            GroupVO groupJsonObj = JsonUtils.toObject(GroupVO.class, groupValue.getBytes());
            return Flux.fromStream(groupJsonObj.getPolicyIds().stream());
        } else {
            return Flux.empty();
        }
    }

    private static Flux<StatementVO> getStatement(String policyValue) {
        if (StringUtils.isNotBlank(policyValue)) {
            PolicyVO policyVO = JsonUtils.toObject(PolicyVO.class, policyValue.getBytes());
            return Flux.fromStream(policyVO.getStatement().stream());
        } else {
            return Flux.empty();
        }
    }

    public static Mono<String> checkAuth(String userId, ActionType actionType, String resource) {
        String[] accountId = new String[1];
        accountId[0] = DEFAULT_ACCOUNTID;
        return pool.getReactive(REDIS_IAM_INDEX)
                .get(userId)
                .flatMapMany(userValue -> {
                    if (StringUtils.isNotBlank(userValue)) {
                        UserVO userVO = JsonUtils.toObject(UserVO.class, userValue.getBytes());
                        accountId[0] = userVO.getAccountId();
                        return Flux.fromStream(userVO.getPolicyIds().stream())
                                .concatWith(Flux.fromStream((userVO.getGroupIds().stream()))
                                        .flatMap(groupId -> pool.getReactive(REDIS_IAM_INDEX).get(groupId))
                                        .flatMap(IamUtils::getGroupPolicyId));
                    } else {
                        return Flux.empty();
                    }
                })
                .distinct()
                .flatMap(policyId -> pool.getReactive(REDIS_IAM_INDEX).get(policyId))
                .flatMap(IamUtils::getStatement)
                .flatMap(statement -> {
                    if (PolicyUtil.isActionTypeInActionList(actionType, statement.getAction()) &&
                            PolicyUtil.isResourceInResourceList(resource, statement.getResource())) {
                        if (PolicyUtil.DENY.equalsIgnoreCase(statement.getEffect())) {
                            return Flux.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                        }

                        if (PolicyUtil.ALLOW.equalsIgnoreCase(statement.getEffect())) {
                            return Mono.just(FIND_ALLOW);
                        }
                    }

                    return Mono.just(FIND_NOTHING);
                })
                .collect(Collectors.toSet())
                .flatMap(set -> {
                    if (set.contains(FIND_ALLOW)) {
                        return Mono.just(accountId[0]);
                    }
                    return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                });
    }

    /**
     * 用于文件系统 IAM 权限判断
     **/
    public static Mono<Integer> checkFsAuth(String userId, String action, String resource) {
        String[] accountId = new String[1];
        accountId[0] = DEFAULT_ACCOUNTID;
        return pool.getReactive(REDIS_IAM_INDEX)
                .get(userId)
                .flatMapMany(userValue -> {
                    if (iamDebug) {
                        logger.info("checkAuth: userId: {}, actionType: {}, resource: {}, userValue: {}", userId, action, resource, userValue);
                    }
                    if (StringUtils.isNotBlank(userValue)) {
                        UserVO userVO = JsonUtils.toObject(UserVO.class, userValue.getBytes());
                        accountId[0] = userVO.getAccountId();
                        return Flux.fromStream(userVO.getPolicyIds().stream())
                                .concatWith(Flux.fromStream((userVO.getGroupIds().stream()))
                                        .flatMap(groupId -> pool.getReactive(REDIS_IAM_INDEX).get(groupId))
                                        .flatMap(IamUtils::getGroupPolicyId));
                    } else {
                        return Flux.empty();
                    }
                })
                .distinct()
                .flatMap(policyId -> pool.getReactive(REDIS_IAM_INDEX).get(policyId))
                .flatMap(IamUtils::getStatement)
                .flatMap(statement -> {
                    if (iamDebug) {
                        logger.info("statement: {}", statement);
                    }
                    if (PolicyUtil.isMethodInFsActionList(action, statement.getAction()) &&
                            PolicyUtil.isResourceInResourceList(resource, statement.getResource())) {
                        if (PolicyUtil.DENY.equalsIgnoreCase(statement.getEffect())) {
                            return Mono.just(FIND_FS_ERROR);
                        }

                        if (PolicyUtil.ALLOW.equalsIgnoreCase(statement.getEffect())) {
                            return Mono.just(FIND_ALLOW);
                        }
                    }

                    return Mono.just(FIND_NOTHING);
                })
                .collect(Collectors.toSet())
                .flatMap(set -> {
                    if (set.contains(FIND_FS_ERROR)) {
                        return Mono.just(FIND_FS_ERROR);
                    }

                    if (set.contains(FIND_ALLOW)) {
                        return Mono.just(FIND_ALLOW);
                    }

                    return Mono.just(FIND_DENY);
                });
    }

    public enum ActionType {
        ;
        String action;

        ActionType(String action) {
            this.action = action;
        }

        public String getActionType() {
            return this.action;
        }
    }

    private static Field modifierField = null;

    static {
        try {
            modifierField = Field.class.getDeclaredField("modifiers");
            modifierField.setAccessible(true);
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public static void setFinalFieldValue(Field field, Object object, Object value) throws IllegalAccessException {
        field.setAccessible(true);
        int modifiers = modifierField.getInt(field);
        modifiers &= ~Modifier.FINAL;
        modifierField.setInt(field, modifiers);
        field.set(object, value);
    }

    public static void buildActionType() {
        try {
            Constructor<?> constructor = ActionType.class.getDeclaredConstructors()[0];
            Method method = constructor.getClass().getDeclaredMethod("acquireConstructorAccessor");
            method.setAccessible(true);
            Object constructorAccessor = method.invoke(constructor);
            Method newInstance = constructorAccessor.getClass().getMethod("newInstance", Object[].class);
            newInstance.setAccessible(true);

            List<String> keys = pool.getCommand(REDIS_ACTION_INDEX).keys("*");
            // 创建新的枚举
            ActionType[] types = new ActionType[keys.size()];
            int i = 0;

            for (String key : keys) {
                String value = pool.getCommand(REDIS_ACTION_INDEX).get(key);
                Object[] actionType = new Object[]{key, i, value};
                types[i] = (ActionType) newInstance.invoke(constructorAccessor, new Object[]{actionType});
                i++;
            }

            // 重写枚举类型的内部ENUM$VALUES数组
            for (Field field : ActionType.class.getDeclaredFields()) {
                if (field.getName().contains("$VALUES")) {
                    setFinalFieldValue(field, null, types);
                    // 清除枚举的缓存
                    setFinalFieldValue(Class.class.getDeclaredField("enumConstants"), ActionType.class, null);
                    setFinalFieldValue(Class.class.getDeclaredField("enumConstantDirectory"), ActionType.class, null);
                }
            }
            IamAuth.init();
        } catch (Exception e) {
            logger.error("build action type fail", e);
            return;
        }

        logger.info("build action type successful");
    }

    public static Mono<String> checkTempUserAuth(MsHttpRequest request, String userId, ActionType actionType, String resource) {
        String[] accountId = {DEFAULT_ACCOUNTID};
        return pool_6380.getReactive(REDIS_IAM_INDEX)
                .get(userId)
                .flatMapMany(userValue -> {
                    if (StringUtils.isNotBlank(userValue)) {
                        UserVO userVo = JsonUtils.toObject(UserVO.class, userValue.getBytes());
                        accountId[0] = userVo.getAccountId();
                        List<String> groupIds = userVo.getGroupIds();
                        if (groupIds != null && !groupIds.isEmpty()) {
                            //临时用户的groupId中只有一个，存储roleId
                            return pool.getReactive(REDIS_IAM_INDEX).get(groupIds.get(0))
                                    .doOnNext(roleInfo -> {
                                        if ("AssumeRole".equals(request.getMember("Iam_Action"))) {
                                            putRoleNameToRequest(request, roleInfo);
                                        }
                                    })
                                    .map(roleInfo -> new Tuple2<>("role", getRolePolicyIds(roleInfo)))
                                    .concatWith(Mono.just(new Tuple2<>("user", userVo.getPolicyIds())));
                        }
                    }
                    return Mono.empty();
                })
                .flatMap(tuple2 -> {
                    logger.debug(tuple2.var1);
                    logger.debug(tuple2.var2);
                    String policyType = tuple2.var1;
                    List<String> policyList = tuple2.var2;
                    //临时用户可能不存在policy，此时角色策略校验通过即可
                    if ("user".equals(policyType) && policyList.isEmpty()) {
                        return Mono.just(true);
                    }
                    return Flux.fromStream(policyList.stream())
                            .flatMap(policyId -> {
                                return pool.getReactive(REDIS_IAM_INDEX).exists(policyId)
                                        .flatMap(l -> l == 1 ? pool.getReactive(REDIS_IAM_INDEX).get(policyId)
                                                : pool_6380.getReactive(REDIS_IAM_INDEX).get(policyId));
                            })
                            .flatMap(IamUtils::getStatement)
                            .flatMap(statement -> {
                                if (PolicyUtil.isActionTypeInActionList(actionType, statement.getAction()) &&
                                        PolicyUtil.isResourceInResourceList(resource, statement.getResource())) {
                                    if (PolicyUtil.DENY.equalsIgnoreCase(statement.getEffect())) {
                                        logger.debug("check fail ,exist DENY");
                                        return Flux.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                                    }

                                    if (PolicyUtil.ALLOW.equalsIgnoreCase(statement.getEffect())) {
                                        return Mono.just(FIND_ALLOW);
                                    }
                                }

                                return Mono.just(FIND_NOTHING);
                            })
                            .collect(Collectors.toSet())
                            .flatMap(set -> {
                                if (set.contains(FIND_ALLOW)) {
                                    return Mono.just(true);
                                }
                                logger.debug("check fail, do not contain allow");
                                return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                            });
                })
                .collect(Collectors.toSet())
                .flatMap(set -> {
                    if (set.isEmpty()) {
                        logger.debug("check fail, element is empty");
                        return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                    }
                    return Mono.just(accountId[0]);
                });
    }

    public static Mono<String> checkTempUserAuthFromRocksDb(MsHttpRequest request, Credential credential, ActionType actionType, String resource) {
        String[] accountId = {DEFAULT_ACCOUNTID};
        //临时用户只从6380表0中获取
        return Mono.just(credential)
                .flatMapMany(userValue -> {
                    accountId[0] = credential.getAccountId();
                    List<String> groupIds = credential.getGroupIds();
                    if (groupIds != null && !groupIds.isEmpty()) {
                        //临时用户的groupId中只有一个，存储roleId
                        return pool.getReactive(REDIS_IAM_INDEX).get(groupIds.get(0))//角色ID还是在6381
                                .doOnNext(roleInfo -> {
                                    if ("AssumeRole".equals(request.getMember("Iam_Action"))) {
                                        putRoleNameToRequest(request, roleInfo);
                                    }
                                })
                                .map(roleInfo -> new Tuple2<>("role", getRolePolicyIds(roleInfo)))
                                .concatWith(Mono.just(new Tuple2<>("user", credential.getPolicyIds())));
                    }

                    return Mono.empty();
                })
                .flatMap(tuple2 -> {
                    logger.debug(tuple2.var1);
                    logger.debug(tuple2.var2);
                    String policyType = tuple2.var1;
                    List<String> policyList = tuple2.var2;
                    //临时用户可能不存在policy，此时角色策略校验通过即可
                    if ("user".equals(policyType) && policyList.isEmpty()) {//附加给凭证的策略为空
                        return Mono.just(true);
                    }
                    return Flux.fromStream(policyList.stream())
                            .flatMap(policyId -> {
                                if (credential.getInlinePolicy() != null && policyId.equals(credential.getInlinePolicy().getPolicyId())) {
                                    return Mono.just(credential.getInlinePolicy().getPolicy());
                                } else {
                                    return pool.getReactive(REDIS_IAM_INDEX).get(policyId);
                                }
                            })
                            .flatMap(IamUtils::getStatement)
                            .flatMap(statement -> {
                                if (PolicyUtil.isActionTypeInActionList(actionType, statement.getAction()) &&
                                        PolicyUtil.isResourceInResourceList(resource, statement.getResource())) {
                                    if (PolicyUtil.DENY.equalsIgnoreCase(statement.getEffect())) {
                                        logger.debug("check fail ,exist DENY. credential: {}", credential);
                                        return Flux.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                                    }

                                    if (PolicyUtil.ALLOW.equalsIgnoreCase(statement.getEffect())) {
                                        return Mono.just(FIND_ALLOW);
                                    }
                                }

                                return Mono.just(FIND_NOTHING);
                            })
                            .collect(Collectors.toSet())
                            .flatMap(set -> {
                                if (set.contains(FIND_ALLOW)) {
                                    return Mono.just(true);
                                }
                                logger.debug("check fail, do not contain allow. credential: {}", credential);
                                return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                            });
                })
                .collect(Collectors.toSet())
                .flatMap(set -> {
                    if (set.isEmpty()) {
                        logger.debug("check fail, element is empty. credential: {}", credential);
                        return Mono.error(new MsException(ErrorNo.ACCESS_FORBIDDEN, "IAM check fail"));
                    }
                    long cur = System.currentTimeMillis() / 1000;
                    long dur = credential.deadline - cur;
                    if (dur > 0) {
                        StsCredentialSyncTask.credentialCache.put(credential.accessKey, credential);
                    }
                    return Mono.just(accountId[0]);
                });
    }
    private static List<String> getRolePolicyIds(String roleValue) {
        if (StringUtils.isNotBlank(roleValue)) {
            JsonObject roleObj = new JsonObject(roleValue);
            JsonArray policyIds = roleObj.getJsonArray("policyIds");
            if (policyIds != null && !policyIds.isEmpty()) {
                return policyIds.stream().map(String::valueOf).collect(Collectors.toList());
            }
        }
        return new ArrayList<>();
    }

    private static void putRoleNameToRequest(MsHttpRequest request, String roleValue) {
        if (StringUtils.isNotBlank(roleValue)) {
            JsonObject roleObj = new JsonObject(roleValue);
            String roleName = roleObj.getString("roleName");
            request.addMember("assumeRoleName", roleName);
        }
    }
}