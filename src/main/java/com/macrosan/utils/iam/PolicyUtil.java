package com.macrosan.utils.iam;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.iam.IamUtils.ActionType;
import com.macrosan.utils.sts.PolicyJsonObj;
import com.macrosan.utils.sts.Statement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.macrosan.constants.SysConstants.RESOURCE_PREFIX;

/**
 * <p>
 * 策略对比工具
 * </p>
 *
 * @author LiuChuang
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2018年12月6日 下午1:41:43
 */
public class PolicyUtil {
    static final String ALLOW = "allow";
    static final String DENY = "deny";
    private static final Logger logger = LoggerFactory.getLogger(PolicyUtil.class);

    private PolicyUtil() {
    }

    /**
     * 判断actionType是否存在于actionList中
     *
     * @param actionType
     * @param actionList
     * @return 若存在，则为true，反之则为false
     */
    static boolean isActionTypeInActionList(ActionType actionType, List<String> actionList) {
        // 获取字符串形式
        String actionTypeStr = actionType.getActionType();
        for (String action : actionList) {
//            if (action.startsWith("s3")) {
//                action = "moss" + action.substring("s3".length());
//            }
            if (isMatch(actionTypeStr.trim().toLowerCase(), action.trim().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断资源字符串是否存在于resourceList中
     *
     * @param resource
     * @param resourceList
     * @return 若存在，则为true，反之则为false
     */
    static boolean isResourceInResourceList(String resource, List<String> resourceList) {
        for (String r : resourceList) {
            if (ServerConfig.isBucketUpper() && r.startsWith(RESOURCE_PREFIX)) {
                r = r.substring(RESOURCE_PREFIX.length());
                String[] split = r.split("/", 2);
                String bucket = split[0].toLowerCase();
                if (split.length > 1) {
                    r = RESOURCE_PREFIX + bucket + "/" + split[1];
                } else {
                    r = RESOURCE_PREFIX + bucket;
                }
            }
//            if (ServerConfig.isBucketUpper() && r.startsWith(S3_RESOURCE_PREFIX)) {
//                r = r.substring(S3_RESOURCE_PREFIX.length());
//                String[] split = r.split("/", 2);
//                String bucket = split[0].toLowerCase();
//                if (split.length > 1) {
//                    r = RESOURCE_PREFIX + bucket + "/" + split[1];
//                } else {
//                    r = RESOURCE_PREFIX + bucket;
//                }
//            }
//            if (r.startsWith(S3_RESOURCE_PREFIX)) {
//                r = RESOURCE_PREFIX + r.substring(S3_RESOURCE_PREFIX.length());
//            }
            if (isMatch(resource.trim(), r.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断method是否存在于actionList中
     *
     * @param method
     * @param actionList
     * @return 若存在，则为true，反之则为false
     */
    static boolean isMethodInFsActionList(String method, List<String> actionList) {
        for (String action : actionList) {
            if (isMatch(method.trim().toLowerCase(), action.trim().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isEffectDeny(String effect) {
        return DENY.equalsIgnoreCase(effect);
    }

    public static boolean isEffectAllow(String effect) {
        return ALLOW.equalsIgnoreCase(effect);
    }

    /**
     * 判断包含?和*的字符串和常规字符串是否匹配
     *
     * @param s 被比较的字符串
     * @param p 表示包含?和*的字符串
     * @return
     */
    private static boolean isMatch(String s, String p) {
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

    public static String convertPolicyToJsonStr(String policyContent){
        if (policyContent == null) {
            return "";
        }
        StringBuffer jsonForMatStr = new StringBuffer();
        // 当前字符是否在引号内
        boolean foundQuot = false;
        for (int i = 0; i < policyContent.length(); i++) {
            char c = policyContent.charAt(i);
            // 保证""中的内容不被去掉空格与换行
            if ('"' == c) {
                if (foundQuot) {
                    foundQuot = false;
                } else {
                    foundQuot = true;
                }
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
        return jsonForMatStr.toString().replaceFirst("\"Version\"", "\"version\"").replaceAll("Statement", "statement")
                .replaceAll("Resource", "resource").replaceAll("Effect", "effect").replaceAll("Action", "action").replaceAll("Principal", "principal");
    }

    /*
     * 判断是否字符串中出现相同字段
     * */
    public static boolean checkJsonStr(String text){
        int flag1 = 0;
        String search1 = "{";
        int effect = 0;int action = 0;int resource = 0;
        String search2 = "effect"; String search3 = "action"; String search4 = "resource";
        for (int i = 0; i < text.length()-search1.length(); i++) {
            if(text.substring(i, i+search1.length()).indexOf(search1) != -1) {
                flag1 += 1;
            }
        }
        for (int i = 0; i < text.length()-search2.length(); i++) {
            if(text.substring(i, i+search2.length()).indexOf(search2) != -1) {
                effect += 1;
            }
        }
        for (int i = 0; i < text.length()-search3.length(); i++) {
            if(text.substring(i, i+search3.length()).indexOf(search3) != -1) {
                action += 1;
            }
        }
        for (int i = 0; i < text.length()-search4.length(); i++) {
            if(text.substring(i, i+search4.length()).indexOf(search4) != -1) {
                resource += 1;
            }
        }
        //获取statements中“{”的数目
        int temp = flag1 -1;
        if(temp == effect && temp == action && temp == resource){
            return false;
        }
        return true;
    }

    /**
     * 判断策略json转化成的PolicyJsonObj是否非法
     *
     * @param policyJsonObj
     * @return 若非法，返true
     */
    public static boolean isPolicyJsonObjError(PolicyJsonObj policyJsonObj) {
        if (policyJsonObj == null) {
            logger.debug("policyJsonObj is null");
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
            for (String action : actions) {
                if (!checkActionAuth(action)) {
                    logger.debug("check action " + action + " fail");
                    return true;
                }
            }

            String effect = statement.getEffect();
            if (!("deny".equalsIgnoreCase(effect) || "allow".equalsIgnoreCase(effect))) {
                return true;
            }

            List<String> resources = statement.getResource();
            if (resources.size() == 0) {
                return true;
            }
            for (String resource : resources) {
                if (resource == null || "".equals(resource) || resource.matches("^\\?+$")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean checkActionAuth(String action) {
        if (StringUtils.isBlank(action)) {
            return false;
        }

        for (ActionType type : ActionType.values()) {
            logger.debug(type.getActionType());
            logger.debug(action);
            if (isMatch(type.getActionType(), action)) {
                return true;
            }
        }

        return false;
    }
}
