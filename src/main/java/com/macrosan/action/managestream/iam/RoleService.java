package com.macrosan.action.managestream.iam;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.message.consturct.SocketReqMsgBuilder;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.message.jsonmsg.InlinePolicy;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import com.macrosan.message.xmlmsg.role.Role;
import com.macrosan.message.xmlmsg.role.assumeRole.AssumeRoleResponse;
import com.macrosan.message.xmlmsg.role.assumeRole.AssumeRoleResult;
import com.macrosan.message.xmlmsg.role.assumeRole.AssumedRoleUser;
import com.macrosan.message.xmlmsg.role.assumeRole.Credentials;
import com.macrosan.message.xmlmsg.role.attachRolePolicy.AttachRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.createRole.CreateRoleResponse;
import com.macrosan.message.xmlmsg.role.createRole.CreateRoleResult;
import com.macrosan.message.xmlmsg.role.deleteRole.DeleteRoleResponse;
import com.macrosan.message.xmlmsg.role.deleteRolePolicy.DeleteRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.detachRolePolicy.DetachRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.getRole.GetRoleResponse;
import com.macrosan.message.xmlmsg.role.getRole.GetRoleResult;
import com.macrosan.message.xmlmsg.role.getRolePoliy.GetRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.getRolePoliy.GetRolePolicyResult;
import com.macrosan.message.xmlmsg.role.listAttachedRolePolicies.AttachedPolicies;
import com.macrosan.message.xmlmsg.role.listAttachedRolePolicies.AttachedPolicy;
import com.macrosan.message.xmlmsg.role.listAttachedRolePolicies.ListAttachedRolePoliciesResponse;
import com.macrosan.message.xmlmsg.role.listAttachedRolePolicies.ListAttachedRolePoliciesResult;
import com.macrosan.message.xmlmsg.role.listRolePolicies.ListRolePoliciesResponse;
import com.macrosan.message.xmlmsg.role.listRolePolicies.ListRolePoliciesResult;
import com.macrosan.message.xmlmsg.role.listRoles.ListRolesResponse;
import com.macrosan.message.xmlmsg.role.listRoles.ListRolesResult;
import com.macrosan.message.xmlmsg.role.putRolePolicy.PutRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.updateAssumeRolePolicy.UpdateAssumeRolePolicyResponse;
import com.macrosan.message.xmlmsg.role.updateRole.UpdateRoleResponse;
import com.macrosan.message.xmlmsg.role.updateRole.UpdateRoleResult;
import com.macrosan.message.xmlmsg.role.updateRoleDescription.UpdateRoleDescriptionResponse;
import com.macrosan.message.xmlmsg.role.updateRoleDescription.UpdateRoleDescriptionResult;
import com.macrosan.message.xmlmsg.section.PolicyInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.IAMConstants.POLICY_NOT_EXIST_ERR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSiteWithoutBucket;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.sts.RolePolicyUtils.*;
import static com.macrosan.utils.sts.RoleUtils.MARKER;
import static com.macrosan.utils.sts.RoleUtils.*;
import static com.macrosan.utils.sts.StsParamPatternUtils.*;

/**
 * @Description: sts
 * @Author wanhao
 * @Date 2023/7/28 0028 上午 11:17
 */
@Log4j2
public class RoleService {
    private static final IamRedisConnPool POOL = IamRedisConnPool.getInstance();
    private static final RedisConnPool POOL_6379 = RedisConnPool.getInstance();
    private static final Redis6380ConnPool POOL_6380 = Redis6380ConnPool.getInstance();
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();
    private static RoleService instance = null;


    private RoleService() {
        super();
    }

    public static RoleService getInstance() {
        if (instance == null) {
            instance = new RoleService();
        }
        return instance;
    }

    public Mono<ResponseMsg> assumeRoleDouble(UnifiedMap<String, String> paramMap) {
        log.debug("assumeRoleDouble: {}", paramMap);
        String accountId = paramMap.get(USER_ID);//获取账户名
        MsAclUtils.checkIfAnonymous(accountId);

        String ak = paramMap.get(ACCESS_KEY);//获取ak
        if (!paramMap.containsKey(SITE_FLAG) && !paramMap.containsKey(SITE_FLAG_UPPER)) {//多站点转发的请求不进行校验
            //只有iam用户可以访问
            checkIamUser(ak);
        }

        String roleMrn = paramMap.get(ROLE_ARN);//获取需要扮演的角色ARN
        if (!checkRoleMrn(roleMrn)) {
            throw new MsException(MALFORMED_ROLE_ARN, "The roleMrn is not valid.");
        }
        Tuple2<List<String>, List<String>> arnsMembers = getPolicyArnsMembers(paramMap, roleMrn);//根据请求参数获取托管策略id和mrn
        paramMap.put("policyIds", String.valueOf(new JsonArray(arnsMembers.var1)));
        paramMap.put("policyMrns", String.valueOf(new JsonArray(arnsMembers.var2)));
        String userName = paramMap.get(USERNAME);//获取用户名
        String assumeRoleName = paramMap.get("assumeRoleName");//若使用临时凭证请求该接口，则获取凭证扮演的角色名

        //检查是否允许创建临时凭证，校验方法和调用该接口的用户或角色
        if (!paramMap.containsKey(SITE_FLAG_UPPER)) {
            checkAssumeAction(roleMrn, accountId, userName, assumeRoleName);
        }

        String roleAccountId = roleMrn.split(":")[4];//要扮演角色所属的账户
        String roleName = roleMrn.split(":")[5].split("/")[1];//要扮演角色名称

        String sessionName = paramMap.get(ROLE_SESSION_NAME);//临时会话（凭证）名
        if (!paramMap.containsKey(SITE_FLAG_UPPER)) {
            checkRoleSessionName(sessionName);
        }

        //获取临时访问凭证的有效时间
        long durationSeconds = checkAndGetDurationSeconds(roleMrn, paramMap.get(DURATION_SECONDS));
        paramMap.put(DURATION_SECONDS, String.valueOf(durationSeconds));


        String policy = paramMap.get(POLICY);//获取内联策略内容
        if (policy != null) {
            policy = checkPolicyFormat(policy);//统一内容格式
        }
        paramMap.put(POLICY, policy);

        Tuple2<String, String> roleInfo = getRoleId(roleMrn);//获取要扮演的角色id和name
        paramMap.put("roleId", roleInfo.var1);
        paramMap.put("roleName", roleInfo.var2);
        long endTime = System.currentTimeMillis() + durationSeconds * 1000;//过期时间用于返回结果显示，可记录在缓存与rocksdb
        String endTimeStr = MsDateUtils.stampToDateFormat(String.valueOf(endTime));//调整为yyy-MM-dd HH:mm:ss格式

        String mrn = "mrn::sts::" + roleAccountId + ":role/" + roleName + "/" + sessionName;
        String sessionToken = generateToken();

        String policyId = POLICY_ID_PREFIX + getNewId();
        String requestId = paramMap.get(REQUESTID);

        long deadline = System.currentTimeMillis() / 1000 + durationSeconds;
        String assumeId = paramMap.get("AssumeId".toLowerCase());
        Tuple3<String, String, String> tempAkInfo = getAkAndSkInfo(paramMap.get("AccessKey".toLowerCase()), paramMap.get("SecretKey".toLowerCase()), roleAccountId, assumeId, sessionName);
        if (policy != null) {
            policyId = paramMap.get("PolicyId".toLowerCase());
        }
        deadline = Long.parseLong(paramMap.get("Deadline".toLowerCase()));

        try (StatefulRedisConnection<String, String> tmpConnection = POOL_6380.getSharedConnection(REDIS_IAM_INDEX).newMaster()) {
            RedisCommands<String, String> command = tmpConnection.sync();
            command.multi();
            if (policy != null) {//设置了内联策略
                arnsMembers.var1.add(policyId);
                command.setex(policyId, durationSeconds, policy);
            }
            JsonArray groupIds = new JsonArray();
            groupIds.add(roleInfo.var1);
            String assumeInfo = getAssumeInfo(roleAccountId, groupIds, new JsonArray(arnsMembers.var1));

            command.setex(assumeId, durationSeconds, assumeInfo);
            command.setex(tempAkInfo.var1, durationSeconds, tempAkInfo.var3);
            command.exec();

        }

        //todo 然后是写入到索引池的rocksdb_STS中，以凭证AK获取vnode
        //首先需要将生成的凭证重构其信息结构，使用=开头，账户+角色+临时ak作为key的形式
        List<String> groupId = new ArrayList<>();
        groupId.add(roleInfo.var1);
        Credential credential = new Credential()
                .setAssumeId(assumeId)
                .setAccountId(roleAccountId)
                .setGroupIds(groupId)
                .setPolicyIds(arnsMembers.var1)
                .setAccessKey(tempAkInfo.var1)
                .setSecretKey(tempAkInfo.var2)
                .setUseName(sessionName);
        if (policy != null) {
            InlinePolicy inlinePolicy = new InlinePolicy()
                    .setPolicyId(policyId)
                    .setPolicy(policy);
            credential.setInlinePolicy(inlinePolicy);
        }
        credential.setDurationSeconds(durationSeconds)
                .setDeadline(deadline);

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(credential.accessKey);

        String finalAssumeId = assumeId;
        Tuple3<String, String, String> finalTempAkInfo = tempAkInfo;
        String finalPolicy = policy;
        String finalPolicyId = policyId;
        String finalDeadline = String.valueOf(deadline);
        return Mono.just(metaStoragePool.getBucketVnodeId(credential.accessKey)) //使用账户id和凭证id计算存储的vnode
                .flatMap(vnode -> metaStoragePool.mapToNodeInfo(vnode).zipWith(Mono.just(vnode)))
                .flatMap(tuple2 -> {
                    String credentialKey = credential.getCredentialKey(tuple2.getT2());
                    return ErasureClient.putCredential(credential, credentialKey, tuple2.getT1());
                })
                .timeout(Duration.ofSeconds(60))
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
                })
                .flatMap(b -> {
                    AssumeRoleResponse assumeRoleResponse = new AssumeRoleResponse()
                            .setAssumeRoleResult(new AssumeRoleResult()
                                    .setAssumedRoleUser(new AssumedRoleUser()
                                            .setArn(mrn)
                                            .setAssumedRoleId(finalAssumeId + ":" + sessionName))
                                    .setCredentials(new Credentials()
                                            .setAccessKeyId(finalTempAkInfo.var1)
                                            .setSecretAccessKey(finalTempAkInfo.var2)
                                            .setSessionToken(sessionToken)
                                            .setExpiration(endTimeStr)))
                            .setResponseMetadata(new ResponseMetadata()
                                    .setRequestId(requestId));

                    paramMap.put("AssumeId", finalAssumeId);
                    if (finalPolicy != null) {//设置了内联策略
                        paramMap.put("PolicyId", finalPolicyId);
                    }
                    paramMap.put("AccessKey", finalTempAkInfo.var1);
                    paramMap.put("SecretKey", finalTempAkInfo.var2);
                    paramMap.put("Deadline", finalDeadline);
                    return Mono.just(new ResponseMsg(SUCCESS, assumeRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE));
                });
    }

    public ResponseMsg assumeRole(UnifiedMap<String, String> paramMap) {
        log.debug("assumeRole: {}", paramMap);
        String accountId = paramMap.get(USER_ID);//获取账户名
        MsAclUtils.checkIfAnonymous(accountId);

        String ak = paramMap.get(ACCESS_KEY);//获取ak
        if (!paramMap.containsKey(SITE_FLAG) && !paramMap.containsKey(SITE_FLAG_UPPER)) {//多站点转发的请求不进行校验
            //只有iam用户可以访问
            checkIamUser(ak);
        }

        String roleMrn = paramMap.get(ROLE_ARN);//获取需要扮演的角色ARN
        if (!checkRoleMrn(roleMrn)) {
            throw new MsException(MALFORMED_ROLE_ARN, "The roleMrn is not valid.");
        }
        Tuple2<List<String>, List<String>> arnsMembers = getPolicyArnsMembers(paramMap, roleMrn);//根据请求参数获取托管策略id和mrn
        paramMap.put("policyIds", String.valueOf(new JsonArray(arnsMembers.var1)));
        paramMap.put("policyMrns", String.valueOf(new JsonArray(arnsMembers.var2)));
        String userName = paramMap.get(USERNAME);//获取用户名
        String assumeRoleName = paramMap.get("assumeRoleName");//若使用临时凭证请求该接口，则获取凭证扮演的角色名

        //检查是否允许创建临时凭证，校验方法和调用该接口的用户或角色
        if (!paramMap.containsKey(SITE_FLAG_UPPER)) {
            checkAssumeAction(roleMrn, accountId, userName, assumeRoleName);
        }

        String roleAccountId = roleMrn.split(":")[4];//要扮演角色所属的账户
        String roleName = roleMrn.split(":")[5].split("/")[1];//要扮演角色名称

        String sessionName = paramMap.get(ROLE_SESSION_NAME);//临时会话（凭证）名
        if (!paramMap.containsKey(SITE_FLAG_UPPER)) {
            checkRoleSessionName(sessionName);
        }

        //获取临时访问凭证的有效时间
        long durationSeconds = checkAndGetDurationSeconds(roleMrn, paramMap.get(DURATION_SECONDS));
        paramMap.put(DURATION_SECONDS, String.valueOf(durationSeconds));


        String policy = paramMap.get(POLICY);//获取内联策略内容
        if (policy != null) {
            policy = checkPolicyFormat(policy);//统一内容格式
        }
        paramMap.put(POLICY, policy);

        Tuple2<String, String> roleInfo = getRoleId(roleMrn);//获取要扮演的角色id和name
        paramMap.put("roleId", roleInfo.var1);
        paramMap.put("roleName", roleInfo.var2);
        long endTime = System.currentTimeMillis() + durationSeconds * 1000;//过期时间用于返回结果显示，可记录在缓存与rocksdb
        String endTimeStr = MsDateUtils.stampToDateFormat(String.valueOf(endTime));//调整为yyy-MM-dd HH:mm:ss格式

        String mrn = "mrn::sts::" + roleAccountId + ":role/" + roleName + "/" + sessionName;
        String sessionToken = generateToken();

        String policyId = POLICY_ID_PREFIX + getNewId();
        String requestId = paramMap.get(REQUESTID);

        String assumeId = ASSUME_PREFIX + getNewId();
        Tuple3<String, String, String> tempAkInfo = getAkAndSkInfo(roleAccountId, assumeId, sessionName);
        long deadline = System.currentTimeMillis() / 1000 + durationSeconds;
        final boolean doubleFlag = paramMap.containsKey(DOUBLE_FLAG) && "1".equals(paramMap.get(DOUBLE_FLAG));
        /*从站点请求主站点执行*/
        String localCluster = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!doubleFlag && !DoubleActiveUtil.dealSiteSyncRequestWithoutBucket(paramMap, MSG_TYPE_SITE_ASSUME_ROLE, localCluster, masterCluster, assumeId, policyId, tempAkInfo.var1, tempAkInfo.var2, policy, deadline)) {
            AssumeRoleResponse assumeRoleResponse = new AssumeRoleResponse()
                    .setAssumeRoleResult(new AssumeRoleResult()
                            .setAssumedRoleUser(new AssumedRoleUser()
                                    .setArn(mrn)
                                    .setAssumedRoleId(assumeId + ":" + sessionName))
                            .setCredentials(new Credentials()
                                    .setAccessKeyId(tempAkInfo.var1)
                                    .setSecretAccessKey(tempAkInfo.var2)
                                    .setSessionToken(sessionToken)
                                    .setExpiration(endTimeStr)))
                    .setResponseMetadata(new ResponseMetadata()
                            .setRequestId(requestId));
            return new ResponseMsg(SUCCESS, assumeRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
        }
        if (doubleFlag) {
            assumeId = paramMap.get("AssumeId".toLowerCase());
            tempAkInfo = getAkAndSkInfo(paramMap.get("AccessKey".toLowerCase()), paramMap.get("SecretKey".toLowerCase()), roleAccountId, assumeId, sessionName);
            if (policy != null) {
                policyId = paramMap.get("PolicyId".toLowerCase());
            }
            deadline = Long.parseLong(paramMap.get("Deadline"));
        } else if (paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            assumeId = paramMap.get("AssumeId");
            tempAkInfo = getAkAndSkInfo(paramMap.get("AccessKey"), paramMap.get("SecretKey"), roleAccountId, assumeId, sessionName);
            if (policy != null) {
                policyId = paramMap.get("PolicyId");
            }
            deadline = Long.parseLong(paramMap.get("Deadline"));
        }

        try (StatefulRedisConnection<String, String> tmpConnection = POOL_6380.getSharedConnection(REDIS_IAM_INDEX).newMaster()) {
            RedisCommands<String, String> command = tmpConnection.sync();
            command.multi();
            if (policy != null) {//设置了内联策略
                arnsMembers.var1.add(policyId);
                command.setex(policyId, durationSeconds, policy);
            }
            JsonArray groupIds = new JsonArray();
            groupIds.add(roleInfo.var1);
            String assumeInfo = getAssumeInfo(roleAccountId, groupIds, new JsonArray(arnsMembers.var1));

            command.setex(assumeId, durationSeconds, assumeInfo);
            command.setex(tempAkInfo.var1, durationSeconds, tempAkInfo.var3);
            command.exec();

        }

        //todo 然后是写入到索引池的rocksdb_STS中，以凭证AK获取vnode
        //首先需要将生成的凭证重构其信息结构，使用=开头，账户+角色+临时ak作为key的形式
        List<String> groupId = new ArrayList<>();
        groupId.add(roleInfo.var1);
        Credential credential = new Credential()
                .setAssumeId(assumeId)
                .setAccountId(roleAccountId)
                .setGroupIds(groupId)
                .setPolicyIds(arnsMembers.var1)
                .setAccessKey(tempAkInfo.var1)
                .setSecretKey(tempAkInfo.var2)
                .setUseName(sessionName);
        if (policy != null) {
            InlinePolicy inlinePolicy = new InlinePolicy()
                    .setPolicyId(policyId)
                    .setPolicy(policy);
            credential.setInlinePolicy(inlinePolicy);
        }
        credential.setDurationSeconds(durationSeconds)
                .setDeadline(deadline);

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(credential.accessKey);

        String vnode = metaStoragePool.getBucketVnodeId(credential.accessKey);//使用账户id和凭证id计算存储的vnode

        metaStoragePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    String credentialKey = credential.getCredentialKey(vnode);
                    return ErasureClient.putCredential(credential, credentialKey, nodeList);
                })
                .timeout(Duration.ofSeconds(60))
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
                })
                .block();

        AssumeRoleResponse assumeRoleResponse = new AssumeRoleResponse()
                .setAssumeRoleResult(new AssumeRoleResult()
                        .setAssumedRoleUser(new AssumedRoleUser()
                                .setArn(mrn)
                                .setAssumedRoleId(assumeId + ":" + sessionName))
                        .setCredentials(new Credentials()
                                .setAccessKeyId(tempAkInfo.var1)
                                .setSecretAccessKey(tempAkInfo.var2)
                                .setSessionToken(sessionToken)
                                .setExpiration(endTimeStr)))
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        paramMap.put("AssumeId", assumeId);
        if (policy != null) {//设置了内联策略
            paramMap.put("PolicyId", policyId);
        }
        paramMap.put("AccessKey", tempAkInfo.var1);
        paramMap.put("SecretKey", tempAkInfo.var2);
        paramMap.put("Deadline", String.valueOf(deadline));

        /*---------------处理双活请求：同步至其他节点-------------*/
        if (!doubleFlag) {
            int res = notifySlaveSiteWithoutBucket(paramMap, ACTION_ASSUME_ROLE, false);
            if (res != SUCCESS_STATUS){
                throw new MsException(res,"slave assume role fail");
            }
        }
        return new ResponseMsg(SUCCESS, assumeRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg createRole(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        paramMap.put("accountId", accountId);
        MsAclUtils.checkIfAnonymous(accountId);
        String roleName = paramMap.get(ROLE_NAME);
        String assumeRolePolicyDocument = paramMap.get(ASSUME_ROLE_POLICY_DOCUMENT);
        if (roleName == null) {
            throw new MsException(INVALID_ROLE_NAME, "Missing required parameter for this request: 'RoleName'");
        }
        checkRoleName(roleName);
        assumeRolePolicyDocument = checkAssumeRolePolicy(assumeRolePolicyDocument);

        String maxSessionDurationStr = paramMap.getOrDefault(MAX_SESSION_DURATION, DEFAULT_SESSION_DURATION_SECONDS + "");
        checkMaxSessionDuration(maxSessionDurationStr);

        String description = URLDecoder.decode(paramMap.getOrDefault(DESCRIPTION, ""), "utf-8");
        checkRoleDescription(description);

        long createTime = System.currentTimeMillis();
        String createDate = MsDateUtils.stampToDateFormat(String.valueOf(createTime));
        paramMap.put("createDate", createDate);

        String accountRoleState = accountId + ROLE_FLAG_PREFIX;
        String roleNameLower = roleName.toLowerCase();
        boolean roleNameExist = POOL.getCommand(REDIS_IAM_INDEX).hexists(accountRoleState, roleNameLower);
        if (roleNameExist) {
            throw new MsException(ROLE_ALREADY_EXISTS, "Role already exists.");
        }

        paramMap.put("assumeRolePolicyDocument", assumeRolePolicyDocument);
        paramMap.put("maxSessionDuration", maxSessionDurationStr);
        paramMap.put("description", description);

        String requestId = paramMap.get(REQUESTID);
        String arn = buildMrn(accountId, "role", roleName);
        paramMap.put("mrn", arn);


        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            Map<String, String> res;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildCreateRoleMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
                res = resMsg.getData();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                CreateRoleResponse createRoleResponse = new CreateRoleResponse()
                        .setCreateRoleResult(new CreateRoleResult()
                                .setRole(new Role()
                                        .setPath("/")
                                        .setMaxSessionDuration(maxSessionDurationStr)
                                        .setDescription(description)
                                        .setArn(arn)
                                        .setRoleName(roleName)
                                        .setAssumeRolePolicyDocument(assumeRolePolicyDocument)
                                        .setCreateDate(createDate)
                                        .setRoleId(res.get("roleId"))
                                ))
                        .setResponseMetadata(new ResponseMetadata()
                                .setRequestId(requestId));
                return new ResponseMsg(SUCCESS, createRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == TOO_MANY_ROLES) {
                throw new MsException(ErrorNo.TOO_MANY_ROLES, "The role number is over 50.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "createRole failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg deleteRole(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);
        String requestId = paramMap.get(REQUESTID);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
        String roleId = tuple2.var1;

        paramMap.put("roleId", roleId);

        DeleteRoleResponse deleteRoleResponse = new DeleteRoleResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildDeleteRoleMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, deleteRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ROLE_NOT_EXISTS) {
                throw new MsException(ErrorNo.ROLE_NOT_EXISTS, "The requested role does not exist.");
            } else if (code == ErrorNo.ROLE_ATTACH_POLICY) {
                throw new MsException(ErrorNo.ROLE_ATTACH_POLICY, "The role has attached the policy.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "deleteRole failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listRoles(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String requestId = paramMap.get(REQUESTID);
        String marker = paramMap.getOrDefault(MARKER, "");
        String newMarker = marker.toLowerCase();
        String maxItemsStr = paramMap.getOrDefault(MAXITEMS, "1000");
        checkMaxItems(maxItemsStr);
        int maxItems = Integer.parseInt(maxItemsStr);
        String rolePrefix = accountId + ROLE_FLAG_PREFIX;
        Map<String, String> roleNameMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(rolePrefix);
        Set<String> roleNameKey = roleNameMap.keySet();
        Iterator<String> iterator = roleNameKey.iterator();
        TreeSet<String> roleSet = new TreeSet<>();
        while (iterator.hasNext()) {
            roleSet.add(iterator.next());
        }
        if (!roleSet.isEmpty()) {
            if (!"".equals(newMarker) && !roleSet.contains(newMarker)) {
                throw new MsException(INVALID_ARGUMENT, "Invalid Marker");
            }
        }

        String roleKey;
        if ("".equals(newMarker)) {
            roleKey = roleSet.higher(newMarker);
        } else {
            roleKey = newMarker;
        }

        if (roleKey != null && !roleSet.contains(roleKey)) {
            roleKey = roleSet.higher(roleKey);
        }


        List<Role> roleList = new ArrayList<>();
        String key = null;
        if (roleKey != null && roleSet.contains(roleKey)) {
            key = roleKey;
            int num = 0;
            while (key != null && num < maxItems) {
                num++;
                String accountRole = key;
                String roleId = POOL.getCommand(REDIS_IAM_INDEX).hget(rolePrefix, accountRole);
                String roleJsonStr = POOL.getCommand(REDIS_IAM_INDEX).get(roleId);
                JsonObject roleJsonObj = new JsonObject(roleJsonStr);
                String description = roleJsonObj.getString("description");
                String roleName = roleJsonObj.getString("roleName");
                String arn = buildMrn(accountId, "role", roleName);
                String assumePolicyId = roleJsonObj.getString("assumePolicyId");
                String createDate = roleJsonObj.getString("createDate");
                String maxDurationSeconds = roleJsonObj.getString("maxSessionDuration");
                String policy = POOL.getCommand(REDIS_IAM_INDEX).get(assumePolicyId);
                Role role = new Role()
                        .setPath("/")
                        .setDescription(description)
                        .setMaxSessionDuration(maxDurationSeconds)
                        .setArn(arn)
                        .setRoleName(roleName)
                        .setAssumeRolePolicyDocument(policy)
                        .setCreateDate(createDate)
                        .setRoleId(roleId);
                roleList.add(role);
                key = roleSet.higher(key);
            }
        }
        ListRolesResult listRolesResult = new ListRolesResult().setRoles(roleList);
        if (key != null) {
            String roleName = key;
            listRolesResult.setTruncated(true).setMarker(roleName);
        }
        ListRolesResponse listRolesResponse = new ListRolesResponse()
                .setListRolesResult(listRolesResult)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        return new ResponseMsg(SUCCESS, listRolesResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg updateRole(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);

        String maxSessionDurationStr = null;
        if (paramMap.containsKey(MAX_SESSION_DURATION)) {
            maxSessionDurationStr = paramMap.get(MAX_SESSION_DURATION);
            boolean res = checkUpdateMaxSessionDuration(maxSessionDurationStr);
        }

        String description = URLDecoder.decode(paramMap.getOrDefault(DESCRIPTION, ""), "utf-8");
        checkRoleDescription(description);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
        String roleId = tuple2.var1;

        paramMap.put("roleId", roleId);
        if (maxSessionDurationStr != null) {
            paramMap.put("maxSessionDuration", maxSessionDurationStr);
        }
        paramMap.put("description", description);

        String requestId = paramMap.get(REQUESTID);
        UpdateRoleResponse updateRoleResponse = new UpdateRoleResponse()
                .setUpdateRoleResult(new UpdateRoleResult())
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildUpdateRoleMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, updateRoleResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "updateRole failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg getRole(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);
        String requestId = paramMap.get(REQUESTID);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
        String roleId = tuple2.var1;
        JsonObject roleJsonObj = tuple2.var2;
        String arn = buildMrn(accountId, "role", roleName);
        String assumePolicyId = roleJsonObj.getString("assumePolicyId");
        String createDate = roleJsonObj.getString("createDate");
        String description = roleJsonObj.getString("description");
        String maxSessionDuration = roleJsonObj.getString("maxSessionDuration");
        String policy = POOL.getCommand(REDIS_IAM_INDEX).get(assumePolicyId);

        GetRoleResponse getRoleResponse = new GetRoleResponse()
                .setGetRoleResult(new GetRoleResult()
                        .setRole(new Role()
                                .setPath("/")
                                .setDescription(description)
                                .setMaxSessionDuration(maxSessionDuration)
                                .setArn(arn)
                                .setRoleName(roleName)
                                .setAssumeRolePolicyDocument(policy)
                                .setCreateDate(createDate)
                                .setRoleId(roleId)))
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        return new ResponseMsg(SUCCESS, getRoleResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg attachRolePolicy(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);

        MsAclUtils.checkIfAnonymous(accountId);
        String requestId = paramMap.get(REQUESTID);
        String roleNameStr = paramMap.get(ROLE_NAME);
        String policyMrnStr = paramMap.get(POLICY_ARN);
        ArrayList<String> roleIdList = new ArrayList<>();
        List<String> roleNameList = Arrays.asList(roleNameStr.split(","));
        roleNameList.forEach(roleName -> {
            checkRoleNameIsNull(roleName);
            String policiesKey = getRoleManagedPoliciesKey(accountId, roleName);
            boolean exists = POOL.getCommand(REDIS_IAM_INDEX).exists(policiesKey) != 0;
            if (exists) {
                Long policyNum = POOL.getCommand(REDIS_IAM_INDEX).hlen(policiesKey);
                if (policyNum >= 5) {
                    throw new MsException(TOO_MANY_ENTITIES, "Too many policy in role");
                }
            }

            Tuple2<String, JsonObject> roleTuple2 = getRoleInfoByRoleName(accountId, roleName);
            JsonObject roleJson = roleTuple2.var2;
            roleIdList.add(roleTuple2.var1);
        });


        List<String> policyMrnList = Arrays.asList(policyMrnStr.split(","));
        ArrayList<String> policyIdList = new ArrayList<>();
        policyMrnList.forEach(policyMrn -> {
            checkPolicyMrn(policyMrn);
            String[] split = policyMrn.split(":");
            if (StringUtils.isNotBlank(split[4]) && !accountId.equals(split[4])) {
                throw new MsException(NO_AUTHORIZED, "You are not authorized to perform this operation.");
            }
            String policyName = split[5].split("/")[1];
            checkPolicyName(policyName);
            PolicyInfo policyInfo = getPolicyInfoFromIam(accountId, policyName);
            if (TEMPLATE_POLICY_TYPE.equals(policyInfo.getPolicyType())) {
                throw new MsException(POLICY_NOT_ATTACHABLE, "The policy is not attachable to the role.");
            }
            policyIdList.add(policyInfo.getPolicyId());
        });

        AttachRolePolicyResponse attachRolePolicyResponse = new AttachRolePolicyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));


        String policyIdListStr = policyIdList.stream().collect(Collectors.joining(","));
        String roleIdListStr = roleIdList.stream().collect(Collectors.joining(","));
        paramMap.put("roleId", roleIdListStr);
        paramMap.put("policyId", policyIdListStr);

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildAttachRolePolicyMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, attachRolePolicyResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.TOO_MANY_ENTITIES) {
                throw new MsException(TOO_MANY_ENTITIES, "Too many policy in role");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "attachRolePolicy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg detachRolePolicy(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String requestId = paramMap.get(REQUESTID);
        String roleName = paramMap.get(ROLE_NAME);
        getRoleInfoByRoleName(accountId, roleName);
        checkRoleName(roleName);
        String policyMrn = paramMap.get(POLICY_ARN);
        checkPolicyMrn(policyMrn);
        String[] split = policyMrn.split(":");
        String policyName = split[5].split("/")[1];
        String rolePoliciesInfoKey = getRoleManagedPoliciesKey(accountId, roleName);
        Map<String, String> policyNameMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(rolePoliciesInfoKey);
        Set<String> policyNameKey = policyNameMap.keySet();
        for (String policyNameRedis : policyNameKey) {
            String policyNameRedisLower = policyNameRedis.toLowerCase();
            String policyNameLower = policyName.toLowerCase();
            if (policyNameRedisLower.equals(policyNameLower)) {
                policyName = policyNameRedis;
            }
        }

        if (StringUtils.isNotBlank(split[4]) && !accountId.equals(split[4])) {
            throw new MsException(NO_AUTHORIZED, "You are not authorized to perform this operation.");
        }
        String rolePoliciesKey = getRoleManagedPoliciesKey(accountId, roleName);
        String policyInfo = POOL.getCommand(REDIS_IAM_INDEX).hget(rolePoliciesKey, policyName);
        if (policyInfo == null) {
            throw new MsException(POLICY_NOT_EXISTS_ROLE, "The policy does not exist in the role. ");
        }
        JsonObject object = new JsonObject(policyInfo);
        String policyId = object.getString("policyId");

        Tuple2<String, JsonObject> roleInfo = getRoleInfoByRoleName(accountId, roleName);

        paramMap.put("policyId", policyId);
        paramMap.put("roleId", roleInfo.var1);

        DetachRolePolicyResponse detachRolePolicyResponse = new DetachRolePolicyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildDetachRolePolicyMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, detachRolePolicyResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "detachRolePolicy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg getRolePolicy(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String roleName = paramMap.get(ROLE_NAME);
        getRoleInfoByRoleName(accountId, roleName);
        checkRoleName(roleName);

        String policyName = paramMap.get(POLICY_NAME);
        checkPolicyNameIsNull(policyName);

        String requestId = paramMap.get(REQUESTID);
        String rolePoliciesInfoKey = getRoleInlinePoliciesKey(accountId, roleName);
        Map<String, String> policyNameMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(rolePoliciesInfoKey);
        Set<String> policyNameKey = policyNameMap.keySet();
        for (String policyNameRedis : policyNameKey) {
            String policyNameRedisLower = policyNameRedis.toLowerCase();
            String policyNameLower = policyName.toLowerCase();
            if (policyNameRedisLower.equals(policyNameLower)) {
                policyName = policyNameRedis;
            }
        }
        String policyId = POOL.getCommand(REDIS_IAM_INDEX).hget(rolePoliciesInfoKey, policyName);
        if (policyId == null) {
            throw new MsException(POLICY_NOT_EXISTS_ROLE, "The policy does not exist.");
        }
        String policyStr = POOL.getCommand(REDIS_IAM_INDEX).get(policyId);
        GetRolePolicyResponse getRolePolicyResponse = new GetRolePolicyResponse()
                .setRolePolicyResult(new GetRolePolicyResult()
                        .setPolicyName(policyName)
                        .setRoleName(roleName)
                        .setPolicyDocument(policyStr))
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        return new ResponseMsg(SUCCESS, getRolePolicyResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }


    public ResponseMsg deleteRolePolicy(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String policyName = paramMap.get(POLICY_NAME);
        checkPolicyNameIsNull(policyName);
        String roleName = paramMap.get(ROLE_NAME);
        String requestId = paramMap.get(REQUESTID);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
        String roleId = tuple2.var1;
        JsonObject roleJsonObj = tuple2.var2;
        checkRoleNameIsNull(roleName);

        String inlinePolicyKey = getRoleInlinePoliciesKey(accountId, roleName);

        Map<String, String> policyNameMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(inlinePolicyKey);
        Set<String> policyNameKey = policyNameMap.keySet();
        for (String policyNameRedis : policyNameKey) {
            String policyNameRedisLower = policyNameRedis.toLowerCase();
            String policyNameLower = policyName.toLowerCase();
            if (policyNameRedisLower.equals(policyNameLower)) {
                policyName = policyNameRedis;
            }
        }
        String policyId = POOL.getCommand(REDIS_IAM_INDEX).hget(inlinePolicyKey, policyName);

        if (policyId == null) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        paramMap.put("policyId", policyId);
        paramMap.put("roleId", roleId);

        DeleteRolePolicyResponse deleteRolePolicyResponse = new DeleteRolePolicyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildDeleteRolePolicyMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, deleteRolePolicyResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "deleteRolePolicy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listRolePolicies(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String roleName = paramMap.get(ROLE_NAME);
        getRoleInfoByRoleName(accountId, roleName);
        checkRoleName(roleName);
        String requestId = paramMap.get(REQUESTID);

        String marker = paramMap.getOrDefault(MARKER, "");
        String maxItemsStr = paramMap.getOrDefault(MAXITEMS, "1000");
        checkMaxItems(maxItemsStr);
        int maxItems = Integer.parseInt(maxItemsStr);
        List<String> policyNameList = new ArrayList<>();

        String managedPoliciesKey = getRoleInlinePoliciesKey(accountId, roleName);
        List<String> policyList = POOL.getCommand(REDIS_IAM_INDEX).hkeys(managedPoliciesKey);
        TreeSet<String> policySet = new TreeSet<>(policyList);

        if (!policySet.isEmpty()) {
            if (!"".equals(marker) && !policySet.contains(marker)) {
                throw new MsException(INVALID_ARGUMENT, "Invalid Marker");
            }
        }


        String startKey;
        if ("".equals(marker)) {
            startKey = policySet.higher(marker);
        } else {
            startKey = marker;
        }

        if (startKey != null && !policySet.contains(startKey)) {
            startKey = policySet.higher(startKey);
        }

        String key = null;
        if (startKey != null && policySet.contains(startKey)) {
            key = startKey;
            int num = 0;
            while (key != null && num < maxItems) {
                num++;
                policyNameList.add(key);
                key = policySet.higher(key);
            }
        }
        ListRolePoliciesResult listRolePoliciesResult = new ListRolePoliciesResult().setPolicyNames(policyNameList);
        if (key != null) {
            listRolePoliciesResult.setTruncated(true).setMarker(key);
        }

        ListRolePoliciesResponse listRolePoliciesResponse = new ListRolePoliciesResponse()
                .setResult(listRolePoliciesResult)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        return new ResponseMsg(SUCCESS, listRolePoliciesResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg updateAssumeRolePolicy(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String requestId = paramMap.get(REQUESTID);
        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);

        String assumePolicy = paramMap.get(POLICY_DOCUMENT);
        assumePolicy = checkAssumeRolePolicy(assumePolicy);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);

        paramMap.put("roleId", tuple2.var1);
        paramMap.put("assumePolicy", assumePolicy);


        UpdateAssumeRolePolicyResponse updateAssumeRolePolicyResponse = new UpdateAssumeRolePolicyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildUpdateAssumeRolePolicyMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, updateAssumeRolePolicyResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "updateAssumeRolePolicy failed.");
            }
            sleep(10000);
        }

    }

    public ResponseMsg putRolePolicy(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String requestId = paramMap.get(REQUESTID);
        String policyName = paramMap.get(POLICY_NAME);
        checkPolicyName(policyName);
        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);
        String policyDocument = paramMap.get(POLICY_DOCUMENT);
        String remark = paramMap.getOrDefault(REMARK, "");
        if (!paramMap.containsKey("Encode")) {
            remark = URLEncoder.encode(remark, "utf-8");
        }
        policyDocument = checkPolicyFormat(policyDocument);
        String inlinePolicyKey = getRoleInlinePoliciesKey(accountId, roleName);
        Map<String, String> policyNameMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(inlinePolicyKey);
        Set<String> policyNameKey = policyNameMap.keySet();
        boolean exists = false;
        for (String policyNameRedis : policyNameKey) {
            String policyNameRedisLower = policyNameRedis.toLowerCase();
            String policyNameLower = policyName.toLowerCase();
            if (policyNameRedisLower.equals(policyNameLower)) {
                exists = true;
                policyName = policyNameRedis;
            }
        }

        Tuple2<String, JsonObject> roleTuple2 = getRoleInfoByRoleName(accountId, roleName);
        paramMap.put("policyDocument", policyDocument);
        paramMap.put("roleId", roleTuple2.var1);
        paramMap.put("policyName", policyName);
        paramMap.put(REMARK, remark);
        PutRolePolicyResponse putRolePolicyResponse = new PutRolePolicyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));

        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildPutRolePolicyMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, putRolePolicyResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "putRolePolicy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listAttachedRolePolicies(UnifiedMap<String, String> paramMap) {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);
        getRoleInfoByRoleName(accountId, roleName);
        String requestId = paramMap.get(REQUESTID);

        String marker = paramMap.getOrDefault(MARKER, "");
        String maxItemsStr = paramMap.getOrDefault(MAXITEMS, "1000");
        checkMaxItems(maxItemsStr);
        int maxItems = Integer.parseInt(maxItemsStr);
        List<AttachedPolicy> attachedPolicyList = new ArrayList<>();

        String inlinePoliciesKey = getRoleManagedPoliciesKey(accountId, roleName);
        Map<String, String> policiesMap = POOL.getCommand(REDIS_IAM_INDEX).hgetall(inlinePoliciesKey);
        TreeSet<String> policySet = new TreeSet<>(policiesMap.keySet());

        if (!policiesMap.isEmpty()) {
            if (!"".equals(marker) && !policySet.contains(marker)) {
                throw new MsException(INVALID_ARGUMENT, "Invalid Marker");
            }
        }

        String startKey;
        if ("".equals(marker)) {
            startKey = policySet.higher(marker);
        } else {
            startKey = marker;
        }

        if (startKey != null && !policySet.contains(startKey)) {
            startKey = policySet.higher(startKey);
        }

        String key = null;
        if (startKey != null && policySet.contains(startKey)) {
            key = startKey;
            int num = 0;
            while (key != null && num < maxItems) {
                num++;
                String s = policiesMap.get(key);
                JsonObject object = new JsonObject(s);
                String policyId = object.getString("policyId");
                boolean exists = POOL.getCommand(REDIS_IAM_INDEX).exists(policyId) != 0;
                if (exists) {
                    String policyMrn = object.getString("policyMrn");
                    AttachedPolicy attachedPolicy = new AttachedPolicy()
                            .setPolicyName(key)
                            .setPolicyArn(policyMrn);
                    attachedPolicyList.add(attachedPolicy);
                }
                key = policySet.higher(key);
            }
        }
        ListAttachedRolePoliciesResult listAttachedRolePoliciesResult = new ListAttachedRolePoliciesResult()
                .setAttachedPolicies(new AttachedPolicies()
                        .setAttachedPolicies(attachedPolicyList));
        if (key != null) {
            listAttachedRolePoliciesResult.setTruncated(true).setMarker(key);
        }

        ListAttachedRolePoliciesResponse listAttachedRolePoliciesResponse = new ListAttachedRolePoliciesResponse()
                .setResult(listAttachedRolePoliciesResult)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));
        return new ResponseMsg(SUCCESS, listAttachedRolePoliciesResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg updateRoleDescription(UnifiedMap<String, String> paramMap) throws
            UnsupportedEncodingException {
        log.debug(paramMap);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String roleName = paramMap.get(ROLE_NAME);
        checkRoleNameIsNull(roleName);

        String description = URLDecoder.decode(paramMap.get(DESCRIPTION), "utf-8");
        if (description == null) {
            throw new MsException(MISSING_ROLE_DESCRIPTION, "Missing required parameter for this request: 'Description'.");
        }
        checkRoleDescription(description);

        Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
        String roleId = tuple2.var1;
        JsonObject roleJsonObj = tuple2.var2;

        paramMap.put("roleId", roleId);

        String arn = buildMrn(accountId, "role", roleName);
        String assumePolicyId = roleJsonObj.getString("assumePolicyId");
        String createDate = roleJsonObj.getString("createDate");
        String maxSessionDuration = roleJsonObj.getString("maxSessionDuration");
        String policy = POOL.getCommand(REDIS_IAM_INDEX).get(assumePolicyId);

        String requestId = paramMap.get(REQUESTID);
        UpdateRoleDescriptionResponse updateRoleDescriptionResponse = new UpdateRoleDescriptionResponse()
                .setUpdateRoleDescriptionResult(new UpdateRoleDescriptionResult()
                        .setRole(new Role()
                                .setPath("/")
                                .setArn(arn)
                                .setCreateDate(createDate)
                                .setRoleId(roleId)
                                .setRoleName(roleName)
                                .setAssumeRolePolicyDocument(policy)
                                .setDescription(description)
                                .setMaxSessionDuration(maxSessionDuration)))
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(requestId));


        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = SocketReqMsgBuilder.buildUpdateRoleDescriptionMsg(paramMap);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                code = resMsg.getCode();
            } catch (Exception e) {
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg(SUCCESS, updateRoleDescriptionResponse.setResponseMetadata(new ResponseMetadata().setRequestId(requestId))).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "updateRoleDescription failed.");
            }
            sleep(10000);
        }
    }


}
