package com.macrosan.utils.sts;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.*;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.IAMConstants.POLICY_NOT_EXIST_ERR;
import static com.macrosan.constants.ServerConstants.IAM;
import static com.macrosan.constants.ServerConstants.MRN;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.POLICY_NAME_PATTERN;
import static com.macrosan.utils.sts.RolePolicyUtils.*;
import static com.macrosan.utils.sts.StsParamPatternUtils.DEFAULT_DURATION_SECONDS;
import static com.macrosan.utils.sts.StsParamPatternUtils.checkAssumeDurationSeconds;


/**
 * @Description: TODO
 * @Author wanhao
 * @Date 2023/7/28 0028 下午 3:17
 */
@Log4j2
public class RoleUtils {
    public static final String POLICY_NAME = "PolicyName";
    public static final String POLICY_DOCUMENT = "PolicyDocument";
    public static final String ROLE_NAME = "RoleName";
    public static final String REMARK = "Remark";
    public static final String ASSUME_ROLE_POLICY_DOCUMENT = "AssumeRolePolicyDocument";
    public static final String DESCRIPTION = "Description";
    public static final String MAX_SESSION_DURATION = "MaxSessionDuration";
    public static final String TAGS = "Tags";
    public static final String PATH = "Path";
    public static final String MARKER = "Marker";
    public static final String MAXITEMS = "MaxItems";
    public static final String ROLE_ARN = "RoleArn";
    public static final String ROLE_SESSION_NAME = "RoleSessionName";
    public static final String DURATION_SECONDS = "DurationSeconds";
    public static final String POLICY = "Policy";
    public static final String POLICY_ARN = "PolicyArn";
    public static final String POLICY_ARNS_MEMBER_PREFIX = "PolicyArns.member.";


    public static final int ID_LENGTH = 17;
    public static final char[] ID_POOL = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    public static final int ID_POOL_SIZE = ID_POOL.length;
    public static final String POLICY_ID_PREFIX = "MPOL";
    public static final String ROLE_PREFIX = "MROL";
    public static final String ASSUME_PREFIX = "MTMP";

    public static final String ARN_ROLE_PREFIX = "role/";
    public static final String ARN_POLICY_PREFIX = "policy/";

    public static final String ARN = "arn";


    public static final char[] AK_POOL = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    public static final int AK_POOL_SIZE = AK_POOL.length;
    public static final int AK_LENGTH = 16;
    public static final String AK_PREFIX = "MAKI";

    public static final char[] SK_POOL = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    public static final int SK_POOL_SIZE = SK_POOL.length;
    public static final int SK_LENGTH = 40;

    public static final String MRN_PREFIX = "mrn::iam::"; // mrn的前缀
    public static final String ROLE_FLAG_PREFIX = "^ROLE^";
    public static final String POLICY_FLAG_PREFIX = "^POLICY^";
    public static final String INLINE_POLICY_FLAG_PREFIX = "^INLINE_POLICY^";
    public static final String ASSUME_ROLE_PREFIX = "^ASSUME^";

    public static final String ASSUME_ROLE_ACTION_TYPE = "sts:AssumeRole";
    public static final String ATTACH_ROLE_POLICY = "iam:AttachRolePolicy";

    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();
    private static final RedisConnPool POOL = RedisConnPool.getInstance();

    public static long checkAndGetDurationSeconds(String roleArn, String durationSecondsStr) {
        String[] split = roleArn.split(":");
        String accountId = split[4];
        String resource = split[5];
        String roleName = resource.split("/")[1];
        String accountRole = accountId + ROLE_FLAG_PREFIX;
        String roleNameLower = roleName.toLowerCase();
        String roleId = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRole, roleNameLower);
        if (roleId == null) {
            throw new MsException(ROLE_NOT_EXISTS, "The role: " + roleName + " doesn't exist.");
        }
        String roleStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(roleId);
        JsonObject roleJson = new JsonObject(roleStr);
        String maxSessionDurationStr = roleJson.getString("maxSessionDuration");
        long maxSessionDuration = Long.parseLong(maxSessionDurationStr);

        if (durationSecondsStr == null) {
            return DEFAULT_DURATION_SECONDS;
        } else {
            checkAssumeDurationSeconds(durationSecondsStr);
            long durationSeconds = Long.parseLong(durationSecondsStr);
            if (maxSessionDuration < durationSeconds) {
                throw new MsException(INVALID_DURATION_SECONDS, "The requested DurationSeconds exceeds the MaxSessionDuration set for this role.");
            }
            return durationSeconds;
        }
    }

    /**
     * 随机生成一个id
     */
    public static String getNewId() {
        StringBuilder res = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < ID_LENGTH; i++) {
            res.append(ID_POOL[random.nextInt(ID_POOL_SIZE)]);
        }

        return res.toString();
    }



    public static String getNewAccessKey() {
        StringBuilder res = new StringBuilder(AK_PREFIX);

        Random random = new Random();
        for (int i = 0; i < AK_LENGTH; i++) {
            res.append(AK_POOL[random.nextInt(AK_POOL_SIZE)]);
        }

        return res.toString();
    }

    public static String getNewSecretKey() {
        StringBuilder res = new StringBuilder();

        Random random = new Random();
        for (int i = 0; i < SK_LENGTH; i++) {
            res.append(SK_POOL[random.nextInt(SK_POOL_SIZE)]);
        }

        return res.toString();
    }

    /**
     * 封装凭证相关信息
     * @param accountId
     * @param groupIds
     * @param policyIds
     * @return
     */
    public static String getAssumeInfo(String accountId, JsonArray groupIds, JsonArray policyIds) {
        JsonObject assumeObject = new JsonObject();
        assumeObject.put("accountId", accountId);
        assumeObject.put("groupIds", groupIds);
        assumeObject.put("policyIds", policyIds);
        return assumeObject.toString();
    }

    /**
     * 生成凭证ak、sk信息
     * @param accountId
     * @param userId
     * @param userName
     * @return 返回临时ak，sk，封装的jsonString
     */
    public static Tuple3<String, String, String> getAkAndSkInfo(String accountId, String userId, String userName) {
        String tempAk = getNewAccessKey();
        String tempSk = getNewSecretKey();
        JsonObject object = new JsonObject();
        object.put("accountId", accountId);
        object.put("secretKey", tempSk);
        object.put("userId", userId);
        object.put("userName", userName);
        return new Tuple3<>(tempAk, tempSk, object.toString());
    }

    public static Tuple3<String, String, String> getAkAndSkInfo(String accessKey, String secretKey, String accountId, String userId, String userName) {
        String tempAk = accessKey;
        String tempSk = secretKey;
        JsonObject object = new JsonObject();
        object.put("accountId", accountId);
        object.put("secretKey", tempSk);
        object.put("userId", userId);
        object.put("userName", userName);
        return new Tuple3<>(tempAk, tempSk, object.toString());
    }

    public static String buildMrn(String accountId, String type, String resource) {
        if (null == resource) {
            return MRN_PREFIX + accountId + ":" + type;
        }
        return MRN_PREFIX + accountId + ":" + type + "/" + resource;
    }


    public static boolean checkRoleMrn(String arn) {
        if (arn == null) {
            return false;
        }
        if (arn.startsWith(SYSTEM_POLICY_PREFIX)) {
            return false;
        }
        String[] split = arn.split(":");
        if (split.length != 6) {
            return false;
        }
        String mrn = split[0];
        String partition = split[1];
        String service = split[2];
        String accountId = split[4];
        String resource = split[5];

        if ((!MRN.equals(mrn) && !ARN.equals(mrn)) || !IAM.equals(service) || StringUtils.isBlank(accountId)) {
            return false;
        }

        if (resource == null || !resource.startsWith(ARN_ROLE_PREFIX) || resource.contains("*") || resource.contains("?")) {
            return false;
        }

        if (POOL.getCommand(REDIS_USERINFO_INDEX).exists(accountId) == 0) {
            throw new MsException(ErrorNo.ACCOUNT_NOT_EXISTS, "The account doesn't exist.");
        }

        String[] split1 = resource.split("/");
        if (split1.length != 2) {
            return false;
        }
        String roleName = split1[1];
        if (StringUtils.isBlank(roleName)) {
            return false;
        }
        // 判断账户下是否有角色
        String accountRole = accountId + ROLE_FLAG_PREFIX;
        if (IAM_POOL.getCommand(REDIS_IAM_INDEX).exists(accountRole) == 0) {
            throw new MsException(ROLE_NOT_EXISTS, "The role: " + roleName + " doesn't exist.");
        }
        return true;
    }


    public static void checkAssumeAction(String roleArn, String accountId, String userName, String assumeRoleName) {
        String[] split = roleArn.split(":");
        String roleAccountId = split[4];
        String resource = split[5];
        String roleName = resource.split("/")[1];

        String accountRole = roleAccountId + ROLE_FLAG_PREFIX;
        String roleNameLower = roleName.toLowerCase();
        String roleId= IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRole, roleNameLower);
        if (roleId == null) {
            throw new MsException(ROLE_NOT_EXISTS, "The role: " + roleName + " doesn't exist.");
        }
        String roleStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(roleId);
        JsonObject roleObj = new JsonObject(roleStr);
        String assumePolicyId = roleObj.getString("assumePolicyId");
        boolean match = false;
        String policyStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(assumePolicyId);
        JSONObject policyJson = JSONObject.parseObject(policyStr);
        PolicyJsonObj policy = policyJson.toJavaObject(PolicyJsonObj.class);
        for (Statement statement : policy.getStatement()) {
            if (RolePolicyUtils.isActionTypeInActionList(ASSUME_ROLE_ACTION_TYPE, statement.getAction()) && RolePolicyUtils.checkStatementPrincipalMatch(roleAccountId, roleName, accountId, userName, statement.getPrincipal(), assumeRoleName)) {
                if (DENY.equalsIgnoreCase(statement.getEffect())) {
                    throw new MsException(NO_ASSUME_AUTHORIZED, "it's not authorized to perform: sts:AssumeRole.");
                }
                match = true;
            }
        }
        if (!match) {
            throw new MsException(NO_ASSUME_AUTHORIZED, "it's not authorized to perform: sts:AssumeRole.");
        }
    }

    /**
     * 获取role的policy集合，并合并成一个policy返回
     *
     * @param roleArn
     * @return
     */
    public static String getRolePolicies(String roleArn) {
        String[] split = roleArn.split(":");
        String accountId = split[4];
        String roleName = split[5].split("/")[1];
        String accountRole = accountId + ROLE_FLAG_PREFIX;
        String roleNameLower = roleName.toLowerCase();
        String roleId = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRole, roleNameLower);
        String roleStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(roleId);
        JsonObject roleJson = new JsonObject(roleStr);
        JsonArray policyIds = roleJson.getJsonArray("policyIds");
        List<Statement> statementList = new ArrayList<>();
        for (int i = 0; i < policyIds.size(); i++) {
            String policyId = policyIds.getString(i);
            String policyText = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(policyId);
            if (policyText != null) {
                JSONObject policy = JSONObject.parseObject(policyText);
                PolicyJsonObj policyObj = policy.toJavaObject(PolicyJsonObj.class);
                statementList.addAll(policyObj.getStatement());
            }
        }
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("version", "1.0");
        jsonObject.put("statement", new JsonArray(statementList));
        return jsonObject.toString();
    }

    public static Tuple2<String, JsonObject> getRoleInfoByRoleName(String accountId, String roleName) {
        String accountRole = accountId + ROLE_FLAG_PREFIX;
        if(roleName == null){
            throw new MsException(ROLE_NOT_EXISTS, "The requested role does not exist.");
        }
        String roleNameLower = roleName.toLowerCase();
        String roleId = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRole, roleNameLower);
        if (roleId == null) {
            throw new MsException(ROLE_NOT_EXISTS, "The requested role does not exist.");
        }
        String roleStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(roleId);
        JsonObject jsonObject = new JsonObject(roleStr);
        return new Tuple2<>(roleId, jsonObject);
    }

    public static SocketSender sender = SocketSender.getInstance();

    public static String putPolicyToIam(String accountId, String policyName, String policyDocument) {
        if (policyName == null) {
            throw new MsException(MISSING_POLICY_NAME, "Missing required parameter for this request: 'PolicyName'");
        }
        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NAME_INPUT_ERR,
                    "createPolicy failed, policy name input error, policy_name: " + policyName + ".");
        }

        //策略文档长度校验，不得长于2048
        if (policyDocument.length() > 2048) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,
                    "The length of policy document is over 2048.");
        }
        String policy;
        try {
            policy = URLEncoder.encode(policyDocument, "UTF-8");
        } catch (Exception e) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The format of policy text is error.");
        }
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_POLICY, 0);
        reqMsg.put("accountId", accountId);
        reqMsg.put("policyName", policyName);
        reqMsg.put("policyDocument", policy);
        reqMsg.put("remark", "");
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), reqMsg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return res.get("policyId");
            } else if (code == ErrorNo.TOO_MANY_POLICY) {
                throw new MsException(ErrorNo.TOO_MANY_POLICY, "Too many policies.");
            } else if (code == ErrorNo.POLICY_TEXT_FORMAT_ERROR) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The format of policy text is error.");
            } else if (code == ErrorNo.POLICY_EXISTED) {
                throw new MsException(ErrorNo.POLICY_EXISTED, "The policy exists.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create policy failed.");
            }
            sleep(1000);
        }
    }

    public static void deletePolicyFromIam(String accountId, String policyName) {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_DELETE_POLICY, 0)
                .put("msgType", "deleteUser")
                .put("accountId", accountId)
                .put("policyName", policyName);
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), reqMsg, MapResMsg.class, true);
            int code = resMsg.getCode();
            if (code == ErrorNo.SUCCESS_STATUS) {
                return;
            } else if (code == ErrorNo.TOO_MANY_POLICY) {
                throw new MsException(ErrorNo.TOO_MANY_POLICY, "Too many policies.");
            } else if (code == ErrorNo.POLICY_TEXT_FORMAT_ERROR) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The format of policy text is error.");
            } else if (code == ErrorNo.POLICY_EXISTED) {
                throw new MsException(ErrorNo.POLICY_EXISTED, "The policy exists.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create policy failed.");
            }
            sleep(1000);
        }
    }

    public static void checkIamUser(String ak) {
        if (StringUtils.isBlank(ak) || IAM_POOL.getCommand(REDIS_IAM_INDEX).exists(ak) == 0) {
            throw new MsException(NO_AUTHORIZED, "You are not authorized to perform this operation. You should be authorized by IAM.");
        }
    }

    public static String updatePolicyToIam(String accountId, String policyName, String policyDocument) {
        if (policyName == null) {
            throw new MsException(MISSING_POLICY_NAME, "Missing required parameter for this request: 'PolicyName'");
        }

        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        //策略文档长度校验，不得长于2048
        if (policyDocument.length() > 2048) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,
                    "The length of policy document is over 2048.");
        }

        String policy;
        try {
            policy = URLEncoder.encode(policyDocument, "UTF-8");
        } catch (Exception e) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The format of policy text is error.");
        }
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_POLICY, 0)
                .put("accountId", accountId)
                .put("policyName", policyName)
                .put("newPolicyDocument", policy)
                .put("newRemark", "");
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return res.get("policyId");
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_TEXT_FORMAT_ERROR) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The policy format is error.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create policy failed.");
            }
            sleep(1000);
        }
    }


    public static Tuple2<String, String> getRoleId(String roleMrn) {
        String[] split = roleMrn.split(":");
        String accountId = split[4];
        String roleName = split[5].split("/")[1];
        String accountRole = accountId + ROLE_FLAG_PREFIX;
        String roleNameLower = roleName.toLowerCase();
        String roleId = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRole, roleNameLower);
        if (roleId == null) {
            throw new MsException(ROLE_NOT_EXISTS, "The role: " + roleName + " doesn't exist.");
        }
        return new Tuple2<String, String>(roleId, roleName);
    }

    public static List<EntitiesForPolicy.PolicyRoles.Member> getRolesForPolicy(String accountId, String policyName) {
        List<EntitiesForPolicy.PolicyRoles.Member> list = new LinkedList<>();
        for (String attachedKey : IAM_POOL.getCommand(REDIS_IAM_INDEX).keys(accountId + POLICY_FLAG_PREFIX + "*")) {
            String exists = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(attachedKey, policyName);
            if (StringUtils.isNotBlank(exists)) {
                String roleName = attachedKey.substring((accountId + POLICY_FLAG_PREFIX).length());

                list.add(new EntitiesForPolicy.PolicyRoles.Member()
                        .setRoleName(roleName)
                        .setRoleId(getRoleInfoByRoleName(accountId, roleName).var1));
            }
        }

        return list;
    }

    public static String generateToken() {
        double desiredLength = 344;
        int byteCount = (int) Math.ceil(desiredLength * 6 / 8);

        byte[] randomBytes = new byte[byteCount];
        new SecureRandom().nextBytes(randomBytes);

        String base64Token = Base64.getEncoder().encodeToString(randomBytes);

        return base64Token.substring(0, 344);
    }
}
