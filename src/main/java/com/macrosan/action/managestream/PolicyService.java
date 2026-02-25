package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.AuthInfosResponse;
import com.macrosan.message.xmlmsg.EntityInfosResponse;
import com.macrosan.message.xmlmsg.PolicyInfoResponse;
import com.macrosan.message.xmlmsg.PolicyInfosResponse;
import com.macrosan.message.xmlmsg.section.*;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.*;
import static com.macrosan.utils.sts.StsParamPatternUtils.ROLE_NAME_PATTERN;


/**
 * PolicyService
 * 非线程安全单例模式
 *
 * @author shilinyong
 * @date 2019/07/17
 */
public class PolicyService extends BaseService {

    private static final Logger logger = LogManager.getLogger(PolicyService.class.getName());

    private static PolicyService instance = null;

    private PolicyService() {
        super();
    }


    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static PolicyService getInstance() {
        if (instance == null) {
            instance = new PolicyService();
        }
        return instance;
    }

    public PolicyInfo setPolicyInfoFromIAM(Map<String, String> res) throws UnsupportedEncodingException {
        PolicyInfo policyInfo = new PolicyInfo();
        policyInfo.setPolicyId(res.get("policyId"));
        policyInfo.setPolicyName(res.get("policyName"));
        policyInfo.setPolicyType(res.get("type"));
        policyInfo.setRemark(res.get("remark"));
        String createTime = res.get("createTime");
        createTime = createTime.contains(":") ? createTime : MsDateUtils.stampToSimpleDate(createTime);
        policyInfo.setCreateTime(createTime);
        policyInfo.setPolicyMRN(res.get("mrn"));
        policyInfo.setPolicyDocument(URLDecoder.decode(res.get("policyDocument"), "UTF-8"));

        return policyInfo;
    }

    /**
     * 创建策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createPolicy(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME,"");
        String policyDocument = paramMap.getOrDefault("PolicyDocument","");
        String remark = paramMap.getOrDefault("Remark","");
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(userId);

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

        //备注长度不能大于512
        if (remark.length() > 512) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 512.");
        }

        PolicyInfoResponse policyInfoResponse = new PolicyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreatePolicyMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                policyInfoResponse.setPolicyInfo(setPolicyInfoFromIAM(res));

                return new ResponseMsg().setData(policyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.TOO_MANY_POLICY) {
                throw new MsException(ErrorNo.TOO_MANY_POLICY, "Too many policies.");
            } else if (code == ErrorNo.POLICY_TEXT_FORMAT_ERROR) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The format of policy text is error.");
            } else if (code == ErrorNo.POLICY_EXISTED) {
                throw new MsException(ErrorNo.POLICY_EXISTED, "The policy exists.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deletePolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME,"");

        MsAclUtils.checkIfAnonymous(accountId);
        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeletePolicyMsg(accountId, policyName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete policy successfully,policyName: {}", policyName);
                return new ResponseMsg();
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_ATTACHED_ERROR) {
                throw new MsException(ErrorNo.DELETE_POLICY_FAIL, "The policy has been attached to entities.");
            } else if (code == ErrorNo.POLICY_CANNOT_BE_DELETED){
                throw new MsException(ErrorNo.POLICY_CANNOT_BE_DELETED, "The policy cannot be deleted");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 更新策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg updatePolicy(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME,"");
        String newPolicyDocument = paramMap.getOrDefault("NewPolicyDocument","");
        String newRemark = paramMap.getOrDefault("NewRemark","");
        MsAclUtils.checkIfAnonymous(accountId);

        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        //策略文档长度校验，不得长于2048
        if (newPolicyDocument.length() > 2048) {
            throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,
                    "The length of policy document is over 2048.");
        }

        //备注长度不能大于512
        if (newRemark.length() > 512) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 512.");
        }

        PolicyInfoResponse policyInfoResponse = new PolicyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdatePolicyMsg(accountId, policyName, newPolicyDocument, newRemark);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();
            if (code == ErrorNo.SUCCESS_STATUS) {
                policyInfoResponse.setPolicyInfo(setPolicyInfoFromIAM(res));
                logger.debug("Update policy successfully,policyName: {}", policyName);
                return new ResponseMsg().setData(policyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_TEXT_FORMAT_ERROR) {
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR, "The policy format is error.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询策略列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listPolicies(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        PolicyInfosResponse policyInfosResponse = new PolicyInfosResponse();
        PolicyInfos policyInfos = new PolicyInfos();
        List<PolicyInfo> policyList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListPoliciesMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    policyList.add(setPolicyInfoFromIAM(re));
                }
                policyInfos.setPolicyInfo(policyList);
                policyInfosResponse.setPolicyInfos(policyInfos);
                logger.debug("List policies successfully.");
                return new ResponseMsg().setData(policyInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List policies failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询特定策略信息
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg getPolicy(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME,"");
        MsAclUtils.checkIfAnonymous(accountId);
        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        PolicyInfoResponse policyInfoResponse = new PolicyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetPolicyMsg(accountId, policyName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                policyInfoResponse.setPolicyInfo(setPolicyInfoFromIAM(res));
                logger.debug("Get policy successfully.");
                return new ResponseMsg().setData(policyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 列举策略所附加的实体信息列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listEntitiesForPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME,"");
        MsAclUtils.checkIfAnonymous(accountId);
        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        EntityInfosResponse entityInfosResponse = new EntityInfosResponse();
        EntityInfos entityInfos = new EntityInfos();
        List<EntityInfo> entityList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntitiesForPolicyMsg(accountId, policyName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (int i = 0; i < res.size(); i++) {
                    EntityInfo entityInfo = new EntityInfo();
                    entityInfo.setEntityId(res.get(i).get("objectId"));
                    entityInfo.setEntityName(res.get(i).get("objectName"));
                    entityInfo.setEntityNickName(res.get(i).get("nickname"));
                    entityInfo.setEntityType(res.get(i).get("objectType"));
                    entityList.add(entityInfo);
                }
                entityInfos.setEntityInfo(entityList);
                entityInfosResponse.setEntityInfos(entityInfos);
                logger.debug("List entities for policy successfully.");
                return new ResponseMsg().setData(entityInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List entities for policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 为用户附加策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg attachUserPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userNames = paramMap.getOrDefault("UserNames","");
        String policyNames = paramMap.getOrDefault("PolicyNames","");
        MsAclUtils.checkIfAnonymous(accountId);
        List<String> userList = Arrays.asList(userNames.split(","));
        List<String> policyList = Arrays.asList(policyNames.split(","));

        userList.forEach(user -> {
            if (!USER_NAME_PATTERN.matcher(user).matches()) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
        });
        policyList.forEach(policy -> {
            if (!POLICY_NAME_PATTERN.matcher(policy).matches()) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
        });

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAttachEntityPolicyMsg(accountId, userNames, policyNames, AUTH_OBJECT_TYPE_USER);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Attach user policy successfully.");
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_OBJ || code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.TOO_MANY_AUTH) {
                throw new MsException(ErrorNo.TOO_MANY_AUTH, "The number of policies to the entity has reached upper limit.");
            } else if (code == ErrorNo.AUTH_EXISTS) {
                throw new MsException(ErrorNo.AUTH_EXISTS, "The policy authorization already exists.");
            } else if (code == ErrorNo.POLICY_CANNOT_BE_ATTACHED) {
                throw new MsException(ErrorNo.POLICY_CANNOT_BE_ATTACHED, "Policies are not allowed to be attached");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Attach user policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 为用户分离策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg detachUserPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        String policyName = paramMap.get(IAM_POLICY_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(policyName) || !POLICY_NAME_PATTERN.matcher(policyName).matches()){
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDetachEntityPolicyMsg(accountId, userName, policyName, AUTH_OBJECT_TYPE_USER);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Detach user policy successfully.");
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.AUTH_NOT_EXISTED) {
                throw new MsException(ErrorNo.AUTH_NOT_EXISTED, "The policy authorization does not exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Detach user policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询指定用户的策略列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listUserPolicies(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        PolicyInfosResponse policyInfosResponse = new PolicyInfosResponse();
        PolicyInfos policyInfos = new PolicyInfos();
        List<PolicyInfo> policyList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntityPoliciesMsg(accountId, userName, AUTH_OBJECT_TYPE_USER);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    policyList.add(setPolicyInfoFromIAM(re));
                }
                policyInfos.setPolicyInfo(policyList);
                policyInfosResponse.setPolicyInfos(policyInfos);
                logger.debug("List user policies successfully.");
                return new ResponseMsg().setData(policyInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List user policies failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 为用户组附加策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg attachGroupPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupNames = paramMap.getOrDefault("GroupNames","");
        String policyNames = paramMap.getOrDefault("PolicyNames","");
        MsAclUtils.checkIfAnonymous(accountId);

        List<String> groupList = Arrays.asList(groupNames.split(","));
        List<String> policyList = Arrays.asList(policyNames.split(","));

        groupList.forEach(group -> {
            if (!GROUP_NAME_PATTERN.matcher(group).matches()) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
        });
        policyList.forEach(policy -> {
            if (!POLICY_NAME_PATTERN.matcher(policy).matches()) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
        });

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAttachEntityPolicyMsg(accountId, groupNames, policyNames, AUTH_OBJECT_TYPE_GROUP);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Attach group policy successfully.");
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_OBJ || code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.TOO_MANY_AUTH) {
                throw new MsException(ErrorNo.TOO_MANY_AUTH, "The number of policies to the entity has reached upper limit.");
            } else if (code == ErrorNo.AUTH_EXISTS) {
                throw new MsException(ErrorNo.AUTH_EXISTS, "The policy authorization already exists.");
            } else if (code == ErrorNo.POLICY_CANNOT_BE_ATTACHED) {
                throw new MsException(ErrorNo.POLICY_CANNOT_BE_ATTACHED, "Policies are not allowed to be attached");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Attach group policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 为用户和用户组附加策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg attachEntitiesPolicy(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String policyNames = paramMap.get("PolicyNames");
        String groupNames = null;
        String userNames = null;
        String roleNames = null;
        String entityNames;
        List<String> userList = new ArrayList<>();
        List<String> groupList = new ArrayList<>();
        List<String> roleList = new ArrayList<>();

        if (paramMap.containsKey("UserNames")) {
            userNames = paramMap.get("UserNames");
            userList = Arrays.asList(userNames.split(","));
        }
        if (paramMap.containsKey("GroupNames")) {
            groupNames = paramMap.get("GroupNames");
            groupList = Arrays.asList(groupNames.split(","));
        }
        if (paramMap.containsKey("RoleNames")) {
            roleNames = paramMap.get("RoleNames");
            roleList = Arrays.asList(roleNames.split(","));
        }
        MsAclUtils.checkIfAnonymous(accountId);
        List<String> policyList = Arrays.asList(policyNames.split(","));
        List<String> typeList = new LinkedList<>();

        if (groupList.isEmpty()) {
            entityNames = userNames + "," + roleNames;
        } else if (userList.isEmpty()) {
            entityNames = groupNames + "," + roleNames;
        } else if (roleList.isEmpty()) {
            entityNames = userNames + "," + groupNames;
        } else {
            entityNames = userNames + "," + groupNames + "," + roleNames;
        }

        logger.info("entityNames: {}", entityNames);

        userList.forEach(user -> {
            if (!USER_NAME_PATTERN.matcher(user).matches()) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            typeList.add(AUTH_OBJECT_TYPE_USER);
        });
        groupList.forEach(group -> {
            if (!GROUP_NAME_PATTERN.matcher(group).matches()) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            typeList.add(AUTH_OBJECT_TYPE_GROUP);
        });
        roleList.forEach(role -> {
            if (!ROLE_NAME_PATTERN.matcher(role).matches()) {
                throw new MsException(ErrorNo.ROLE_NOT_EXISTS, ROLE_NOT_EXIST_ERR);
            }
            typeList.add(AUTH_OBJECT_TYPE_ROLE);
        });
        policyList.forEach(policy -> {
            if (!POLICY_NAME_PATTERN.matcher(policy).matches()) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
        });

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAttachEntityPolicyMsg(accountId, entityNames, policyNames, StringUtils.join(typeList, ","));
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Attach group policy successfully.");
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, "attach entity doesn't exist.");
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.ROLE_NOT_EXISTS) {
                throw new MsException(ErrorNo.ROLE_NOT_EXISTS, ROLE_NOT_EXIST_ERR);
            } if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.TOO_MANY_AUTH) {
                throw new MsException(ErrorNo.TOO_MANY_AUTH, "The number of policies to the entity has reached upper limit.");
            } else if (code == ErrorNo.AUTH_EXISTS) {
                throw new MsException(ErrorNo.AUTH_EXISTS, "The policy authorization already exists.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Attach group policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 为用户组分离策略
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg detachGroupPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String policyName = paramMap.get(IAM_POLICY_NAME);
        String entityType = AUTH_OBJECT_TYPE_GROUP;
        MsAclUtils.checkIfAnonymous(accountId);

        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()){
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(policyName) || !POLICY_NAME_PATTERN.matcher(policyName).matches()){
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDetachEntityPolicyMsg(accountId, groupName, policyName, entityType);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Detach group policy successfully.");
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_OBJ || code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.AUTH_NOT_EXISTED) {
                throw new MsException(ErrorNo.AUTH_NOT_EXISTED, "The policy authorization does not exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Detach group policy failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询指定用户组的策略列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listGroupPolicies(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String entityType = AUTH_OBJECT_TYPE_GROUP;
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()){
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        PolicyInfosResponse policyInfosResponse = new PolicyInfosResponse();
        PolicyInfos policyInfos = new PolicyInfos();
        List<PolicyInfo> policyList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntityPoliciesMsg(accountId, groupName, entityType);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    policyList.add(setPolicyInfoFromIAM(re));
                }
                policyInfos.setPolicyInfo(policyList);
                policyInfosResponse.setPolicyInfos(policyInfos);
                return new ResponseMsg().setData(policyInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List group policies failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询账户下所有授权的策略列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listAuths(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        AuthInfosResponse authInfosResponse = new AuthInfosResponse();
        AuthInfos authInfos = new AuthInfos();
        List<AuthInfo> authList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetAccountAuthorizationDetailsMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (int i = 0; i < res.size(); i++) {
                    AuthInfo authInfo = new AuthInfo();
                    authInfo.setEntityName(res.get(i).get("objectName"));
                    authInfo.setEntityType(res.get(i).get("objectType"));
                    authInfo.setPolicyName(res.get(i).get("policyName"));
                    authInfo.setPolicyType(res.get(i).get("policyType"));
                    authInfo.setPolicyRemark(res.get(i).get("policyRemark"));
                    authList.add(authInfo);
                }
                authInfos.setAuthInfo(authList);
                authInfosResponse.setAuthInfos(authInfos);
                return new ResponseMsg().setData(authInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get account authorization details failed.");
            }
            sleep(10000);
        }
    }
}