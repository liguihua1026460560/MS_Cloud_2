package com.macrosan.utils.sts;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.jsonmsg.Principal;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.section.PolicyInfo;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.IAMConstants.POLICY_NOT_EXIST_ERR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildGetPolicyMsg;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildGetUserMsg;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.ACCOUNTID_PATTERN;
import static com.macrosan.utils.sts.RoleUtils.*;
import static com.macrosan.utils.sts.StsParamPatternUtils.checkPolicyName;

/**
 * @Description: 校验策略格式
 * @Author wanhao
 * @Date 2023/7/31 0031 下午 5:18
 */
@Log4j2
public class RolePolicyUtils {

    private static final RedisConnPool POOL = RedisConnPool.getInstance();
    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();

    public static final String ALLOW = "Allow";
    public static final String DENY = "Deny";
    public static final String SYSTEM_POLICY_PREFIX = "mrn::iam:::";
    public static final String TEMPLATE_POLICY_TYPE = "3";

    public static String getWebAddr() {
        return POOL.getCommand(REDIS_SYSINFO_INDEX).get("webaddr");
    }

    public static String checkPolicyFormat(String policyText) {
        JSONObject policy;
        try {
            policyText = convertPolicyToJsonStr(policyText);
            if (policyText.length() > 2048) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The length of policy is more than the 2048");
            }

            if (checkJsonStr(policyText)) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The policy statement is invalid");
            }
            policy = JSONObject.parseObject(policyText);
            PolicyJsonObj policyObj = policy.toJavaObject(PolicyJsonObj.class);
            if (isPolicyJsonObjError(policyObj)) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The policy is invalid ");
            }
            return policyText;
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            }
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The policy is invalid, policy: " + policyText);
        }
    }

    public static String convertPolicyToJsonStr(String policyContent) {
        if (policyContent == null) {
            return null;
        }
        StringBuilder jsonForMatStr = new StringBuilder();
        // 当前字符是否在引号内
        boolean foundQuot = false;
        for (int i = 0; i < policyContent.length(); i++) {
            char c = policyContent.charAt(i);
            // 保证""中的内容不被去掉空格与换行
            if ('"' == c) {
                foundQuot = !foundQuot;
            }
            if (foundQuot) {
                jsonForMatStr.append(c);
                continue;
            }
            switch (c) {
                case ' ':
                    break;
                case '\t':
                    break;
                case '\n':
                    break;
                case '\r':
                    break;
                default:
                    jsonForMatStr.append(c);
                    break;
            }
        }
        return jsonForMatStr.toString().replaceFirst("Version", "version").replaceAll("Statement", "statement")
                .replaceAll("Resource", "resource").replaceAll("Effect", "effect").replaceAll("Action", "action");
    }

    /**
     * 判断是否字符串中出现相同字段
     *
     * @param text
     * @return
     */
    public static boolean checkJsonStr(String text) {
        int flag1 = 0;
        String search1 = "{";
        int effect = 0;
        int action = 0;
        int resource = 0;
        String search2 = "effect";
        String search3 = "action";
        String search4 = "resource";
        for (int i = 0; i < text.length() - search1.length(); i++) {
            if (text.substring(i, i + search1.length()).contains(search1)) {
                flag1 += 1;
            }
        }
        for (int i = 0; i < text.length() - search2.length(); i++) {
            if (text.substring(i, i + search2.length()).contains(search2)) {
                effect += 1;
            }
        }
        for (int i = 0; i < text.length() - search3.length(); i++) {
            if (text.substring(i, i + search3.length()).contains(search3)) {
                action += 1;
            }
        }
        for (int i = 0; i < text.length() - search4.length(); i++) {
            if (text.substring(i, i + search4.length()).contains(search4)) {
                resource += 1;
            }
        }
        log.debug("flag1: " + flag1 + "effect：" + effect + "action :" + action + "resource: " + resource);
        //获取statements中“{”的数目
        int temp = flag1 - 1;
        return temp != effect || temp != action || temp != resource;
    }

    /**
     * 判断策略json转化成的PolicyJsonObj是否非法
     */
    public static boolean isPolicyJsonObjError(PolicyJsonObj policyJsonObj) {
        String version = policyJsonObj.getVersion();
        List<Statement> statements = policyJsonObj.getStatement();
        if (!"1.0".equals(version)) {
            return true;
        }
        if (statements.size() == 0) {
            return true;
        }
        for (Statement statement : statements) {
            List<String> actions = statement.getAction();
            if (actions.size() == 0) {
                return true;
            }
            for (String action : actions) {
                if (!checkActionAuth(action)) {
                    log.debug("check action " + action + " fail");
                    return true;
                }
            }

            String effect = statement.getEffect();
            if (!(DENY.equalsIgnoreCase(effect) || ALLOW.equalsIgnoreCase(effect))) {
                return true;
            }

            List<String> resources = statement.getResource();
            if (resources.size() == 0) {
                return true;
            }
            for (String resource : resources) {
                if (StringUtils.isBlank(resource) || resource.matches("^\\?+$")) {
                    return true;
                }
            }
        }
        return false;
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
//        if (action.startsWith("s3")) {
//            action = "moss" + action.substring("s3".length());
//        }
        for (IamUtils.ActionType type : IamUtils.ActionType.values()) {
            if (isMatch(type.getActionType().toLowerCase(Locale.ROOT), action.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断包含?和*的字符串和常规字符串是否匹配
     *
     * @param s 被比较的字符串
     * @param p 表示包含?和*的字符串
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


    public static boolean sameMemberCheck(String str, String member, int size) {
        int count = 0;
        int start = 0;
        while (str.indexOf(member, start) >= 0 && start < str.length()) {
            count++;
            start = str.indexOf(member, start) + member.length();
        }
        return count > size;
    }

    static boolean isActionTypeInActionList(String actionType, List<String> actionList) {
        for (String action : actionList) {
            if (isMatch(actionType.trim().toLowerCase(), action.trim().toLowerCase())) {
                return true;
            }
        }
        return false;
    }


    public static String checkAssumeRolePolicy(String policyDocument) {
        if (policyDocument == null) {
            throw new MsException(MISSING_ASSUME_ROLE_POLICY_DOCUMENT, "Missing required parameter for this request: 'AssumeRolePolicyDocument'.");
        }
        //策略文档长度校验，不得长于2048
        if (policyDocument.length() > 2048) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,
                    "The length of policy document is over 2048.");
        }
        JSONObject policy;
        try {
            policyDocument = convertPolicyToJsonStr(policyDocument);
            policyDocument = policyDocument.replaceAll("Principal", "principal");
            policy = JSONObject.parseObject(policyDocument);
            int statement = policy.getJSONArray("statement").size();
            checkSameElement(policyDocument, statement);
            PolicyJsonObj policyObj = policy.toJavaObject(PolicyJsonObj.class);
            if (isAssumePolicyError(policyObj)) {
                throw new MsException(1, "isPolicyJsonObjError");
            }
            return policyDocument;
        } catch (Exception e) {
            log.debug("", e);
            throw new MsException(POLICY_TEXT_FORMAT_ERROR, "The policy is invalid.");
        }
    }

    public static void checkSameElement(String text, int size) {
        String[] s = {"effect", "action", "principal"};
        for (String s1 : s) {
            boolean b = sameMemberCheck(text, s1, size);
            if (b) {
                throw new MsException(1, "same element");
            }
        }
    }

    public static boolean principalCheck(String mrnStr) {
        if (StringUtils.isBlank(mrnStr)) {
            return false;
        }
        if ("*".equals(mrnStr)) {
            return true;
        }
        String[] split = mrnStr.split(":");
        if (split.length != 1) {
            if (split.length != 6) {
                return false;
            }
            String mrn = split[0];
            String partition = split[1];
            String service = split[2];
            String accountId = split[4];
            String resource = split[5];

            if (!MRN.equals(mrn) || StringUtils.isNotEmpty(partition) || StringUtils.isBlank(accountId) || !IAM.equals(service) || resource == null || resource.contains("?")) {
                return false;
            }

            if (!ACCOUNTID_PATTERN.matcher(accountId).matches()) {
                return false;
            }

            if (POOL.getCommand(REDIS_USERINFO_INDEX).exists(accountId) == 0) {
                return false;
            }

            if (!ROOT.equals(resource)) {
                if (!resource.startsWith(USERPREFIX) && !resource.startsWith(ARN_ROLE_PREFIX)) {
                    return false;
                }

                String[] split1 = resource.split("/");
                if (split1.length != 2) {
                    return false;
                }
                String name = split1[1];
                if ("*".equals(name) || (name.endsWith("*") && countOccurrences(name, '*') == 1)) {
                    return true;
                }
                if (name.contains("*")) {
                    return false;
                }
                if (resource.startsWith(USERPREFIX)) {
                    return checkUserExists(accountId, name);
                }
                return checkRoleExists(accountId, name);
            }
            return true;
        }
        String accountId = split[0];
        if (!ACCOUNTID_PATTERN.matcher(accountId).matches()) {
            return false;
        }

        return POOL.getCommand(REDIS_USERINFO_INDEX).exists(accountId) != 0;
    }

    public static boolean checkStatementPrincipalMatch(String roleAccountId, String roleName, String accountId, String userName, Principal principal, String assumeRoleName) {
        if (principal == null || principal.getService() == null) {
            return true;
        }

        String root = MRN_PREFIX + accountId + ":root";
        if (assumeRoleName != null) {
            String role = MRN_PREFIX + accountId + ":role/" + assumeRoleName;
            List<String> service = principal.getService();
            for (String mrn : service) {
                if (isMatch(root, mrn) || isMatch(role, mrn)) {
                    return true;
                }

            }
            return false;
        }
        String user = MRN_PREFIX + accountId + ":user/" + userName;
        List<String> service = principal.getService();
        for (String mrn : service) {
            if (mrn.equals(accountId)) {
                return true;
            }
            if (isMatch(root, mrn) || isMatch(user, mrn)) {
                return true;
            }

        }
        //若信任的目标为role，需要递归获取role信任链中的所有信任用户
//        for (String mrn : service) {
//            String[] split = mrn.split(":");
//            if (split.length == 6 && split[5].startsWith(ARN_ROLE_PREFIX)) {
//                Set<String> endSet = new HashSet<>();
//                endSet.add(roleAccountId + "-" + roleName);
//                Set<String> set = getUserMrnsByRoleName(endSet, mrn);
//                for (String s : set) {
//                    //有一个角色的信任列表中包含该用户，直接返回
//                    if (isMatch(root, s) || isMatch(user, s)) {
//                        return true;
//                    }
//                }
//            }
//        }
        return false;
    }

    public static int countOccurrences(String input, char target) {
        int count = 0;
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == target) {
                count++;
            }
        }
        return count;
    }


    /**
     * 判断createRole时信任策略是否非法
     */
    public static boolean isAssumePolicyError(PolicyJsonObj policyJsonObj) {
        if (policyJsonObj == null) {
            log.debug("policyJsonObj is null");
            return true;
        }
        String version = policyJsonObj.getVersion();
        List<Statement> statements = policyJsonObj.getStatement();
        if (!"1.0".equals(version)) {
            return true;
        }
        if (statements.size() == 0) {
            return true;
        }
        for (Statement statement : statements) {
            List<String> actions = statement.getAction();
            if (actions.size() == 0) {
                return true;
            }
//            for (String action : actions) {
//                if (!checkActionAuth(action)) {
//                    log.debug("check action " + action + " fail");
//                    return true;
//                }
//            }

            Set<String> actionSet = new HashSet<>(actions);
            if (actionSet.size() != 1 || !actionSet.contains("sts:AssumeRole")) {
                return true;
            }

            String effect = statement.getEffect();
            if (!("deny".equalsIgnoreCase(effect) || "allow".equalsIgnoreCase(effect))) {
                return true;
            }

            Principal principal = statement.getPrincipal();
            if (principal == null) {
                return true;
            }
            List<String> service = principal.service;
            if (service != null) {
                for (String serviceValue : service) {
                    if (!principalCheck(serviceValue)) {
                        throw new MsException(ErrorNo.INVALID_POLICY_PRINCIPAL, "The  policy is invalid in principal");
                    }
                }
            }
        }
        return false;
    }


    /**
     * 角色的内联策略
     *
     * @param accountId
     * @param roleName
     * @return
     */
    public static String getRoleInlinePoliciesKey(String accountId, String roleName) {
        String roleNameLower = roleName.toLowerCase();
        return accountId + INLINE_POLICY_FLAG_PREFIX + roleNameLower;
    }

    /**
     * 角色的托管策略
     */
    public static String getRoleManagedPoliciesKey(String accountId, String roleName) {
        String roleNameLower = roleName.toLowerCase();
        return accountId + POLICY_FLAG_PREFIX + roleNameLower;
    }


    public static PolicyInfo getPolicyInfoFromIam(String accountId, String policyName) {
        PolicyInfo policyInfo = new PolicyInfo();
        for (int tryTime = 10; ; tryTime -= 1) {
            int code;
            try {
                SocketReqMsg msg = buildGetPolicyMsg(accountId, policyName);
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                Map<String, String> res = resMsg.getData();
                code = resMsg.getCode();
                if (code == ErrorNo.SUCCESS_STATUS) {
                    policyInfo.setPolicyId(res.get("policyId"));
                    policyInfo.setPolicyName(res.get("policyName"));
                    policyInfo.setPolicyType(res.get("type"));
                    policyInfo.setRemark(res.get("remark"));
                    String createTime = res.get("createTime");
                    createTime = createTime.contains(":") ? createTime : MsDateUtils.stampToSimpleDate(createTime);
                    policyInfo.setCreateTime(createTime);
                    policyInfo.setPolicyMRN(res.get("mrn"));
                    log.debug("Iam get policy successfully.");
                    return policyInfo;
                }
            }catch (Exception e){
                sleep(30000);
                throw e;
            }

            if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get policy failed.");
            }
            sleep(10000);
        }
    }

    public static void checkPolicyMrn(String policyMrn) {
        if (policyMrn == null) {
            throw new MsException(MISSING_POLICY_ARN, "Missing required parameter for this request: 'PolicyArn'.");
        }
        if (!checkPolicyMrnFormat(policyMrn)) {
            throw new MsException(MALFORMED_POLICY_MRN, "The PolicyMrn is not valid.");
        }
    }

    private static boolean checkPolicyMrnFormat(String policyMrn) {
        if (policyMrn == null) {
            return false;
        }
        String[] split = policyMrn.split(":");
        if (split.length != 6) {
            return false;
        }
        String mrn = split[0];
        String service = split[2];
        String accountId = split[4];
        String resource = split[5];

        if ((!MRN.equals(mrn) && !ARN.equals(mrn)) || !IAM.equals(service)) {
            return false;
        }

        if (StringUtils.isNotBlank(accountId) && POOL.getCommand(REDIS_USERINFO_INDEX).exists(accountId) == 0) {
            throw new MsException(ErrorNo.ACCOUNT_NOT_EXISTS, "The account doesn't exist.");
        }

        if (resource == null || !resource.startsWith(ARN_POLICY_PREFIX)) {
            return false;
        }
        String[] split1 = resource.split("/");
        return split1.length == 2 && !StringUtils.isBlank(split1[1]);
    }

    public static Tuple2<List<String>, List<String>> getPolicyArnsMembers(UnifiedMap<String, String> paramMap, String roleMrn) {
        List<String> policyIds = new ArrayList<>();
        List<String> policyMrns = new ArrayList<>();
        List<String> list = paramMap.entrySet().stream()
                .filter(entry -> {
                    String key = entry.getKey();
                    return key.startsWith(POLICY_ARNS_MEMBER_PREFIX) && key.endsWith(".arn");
                })
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            return new Tuple2<>(policyIds, policyMrns);
        }
        if (list.size() > 10) {
            throw new MsException(TOO_MANY_ENTITIES, "The number of policyMrns is over 10.");
        }
        Set<String> set = new HashSet<>(list);
        String roleAccountId = roleMrn.split(":")[4];
        for (String policyMrn : set) {
            checkPolicyMrn(policyMrn);
            String accountId = "";
            String policyName;
            if (policyMrn.startsWith(SYSTEM_POLICY_PREFIX)) {
                String[] resource = policyMrn.split(SYSTEM_POLICY_PREFIX);
                policyName = resource[1].split("/")[1];
            } else {
                String[] split = policyMrn.split(":");
                accountId = split[4];
                String resource = split[5];
                policyName = resource.split("/")[1];
                if (!accountId.equals(roleAccountId)) {
                    throw new MsException(POLICY_ROLE_NOT_MATCH, "Policy not attached to requested role");
                }
            }
            checkPolicyName(policyName);
            PolicyInfo policyInfo = getPolicyInfoFromIam(accountId, policyName);
            policyIds.add(policyInfo.getPolicyId());
            policyMrns.add(policyInfo.getPolicyMRN());
        }
        return new Tuple2<>(policyIds, policyMrns);
    }

    public static Set<String> getUserMrnsByRoleName(Set<String> endList, String roleMrn) {
        //保存整个信任链的mrns
        Set<String> set = new HashSet<>();
        String[] split = roleMrn.split(":");
        String accountId;
        String roleName;
        if (split.length == 6 && split[5].startsWith(ARN_ROLE_PREFIX)) {
            accountId = split[4];
            String resource = split[5];
            roleName = resource.split("/")[1];
        } else {
            set.add(roleMrn);
            return set;
        }

        //在信任链的下层信任列表中存在上层的信任角色，信任链结束递归
        if (!endList.add(accountId + "-" + roleName)) {
            return set;
        }
        try {
            Tuple2<String, JsonObject> tuple2 = getRoleInfoByRoleName(accountId, roleName);
            JsonObject roleObj = tuple2.var2;
            String assumePolicyId = roleObj.getString("assumePolicyId");
            String assumeInfoStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(assumePolicyId);
            JSONObject policy = JSONObject.parseObject(assumeInfoStr);
            PolicyJsonObj policyObj = policy.toJavaObject(PolicyJsonObj.class);
            List<Statement> statementList = policyObj.getStatement();
            boolean match = false;
            List<Statement> availableList = new ArrayList<>();
            for (Statement statement : statementList) {
                //没有sts:AssumeRole权限，跳过本条
                if (!isActionTypeInActionList(ASSUME_ROLE_ACTION_TYPE, statement.getAction())) {
                    continue;
                }
                //存在sts:AssumeRole，effect为Deny，信任链中断
                if (DENY.equalsIgnoreCase(statement.getEffect())) {
                    return new HashSet<>();
                }
                match = true;
                Principal principal = statement.getPrincipal();
                if (principal != null && principal.getService() != null) {
                    availableList.add(statement);
                }
            }
            if (match) {
                for (Statement statement : availableList) {
                    Principal principal = statement.getPrincipal();
                    List<String> service = principal.getService();
                    for (String s : service) {
                        set.addAll(getUserMrnsByRoleName(endList, s));
                    }
                }
            }
        } catch (MsException e) {
            //角色信任链中存在已经被删除的角色，则信任链中断
            if (e.getErrCode() == ROLE_NOT_EXISTS) {
                return set;
            }
        }
        return set;
    }

    public static boolean checkUserExists(String accountId, String userName) {
        if (StringUtils.isBlank(userName)) {
            return false;
        }
        SocketReqMsg msg = buildGetUserMsg(accountId, userName);
        for (int tryTime = 10; ; tryTime -= 1) {
            try {
                MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
                int code = resMsg.getCode();
                if (code == ErrorNo.SUCCESS_STATUS) {
                    return true;
                } else if (code == ErrorNo.USER_NOT_EXISTS) {
                    return false;
                }
            }catch (Exception e){
                sleep(30000);
                throw e;
            }
            if (tryTime == 0) {
                return false;
            }
            sleep(10000);
        }
    }

    public static boolean checkRoleExists(String accountId, String roleName) {
        if (StringUtils.isBlank(roleName)) {
            return false;
        }
        String roleNameLower = roleName.toLowerCase();
        String accountRoleState = accountId + ROLE_FLAG_PREFIX;
        String roleNameExist = IAM_POOL.getCommand(REDIS_IAM_INDEX).hget(accountRoleState, roleNameLower);
        if (StringUtils.isEmpty(roleNameExist)) {
            return false;
        }
        return true;
    }
}
