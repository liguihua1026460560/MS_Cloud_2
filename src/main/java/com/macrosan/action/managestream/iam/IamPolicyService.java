package com.macrosan.action.managestream.iam;

import com.alibaba.fastjson.JSON;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.*;
import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy.PolicyGroups;
import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy.PolicyUsers;
import com.macrosan.utils.iam.PolicyUtil;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.sts.PolicyJsonObj;
import com.macrosan.utils.sts.RoleUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.MSG_TYPE_CREATE_POLICY;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/28 19:29
 * @Email :   joshua_zhang_email@163.com
 */

@Log4j2
public class IamPolicyService extends BaseService {
    private static IamPolicyService instance = null;

    private IamPolicyService() {
        super();
    }

    public static IamPolicyService getInstance() {
        if (instance == null) {
            instance = new IamPolicyService();
        }

        return instance;
    }

    public ResponseMsg createPolicy(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String policyName = paramMap.getOrDefault(IAM_POLICY_NAME, "");
        String policyDocument = paramMap.getOrDefault("PolicyDocument", "");
        String userId = paramMap.get(USER_ID);
        String description = paramMap.getOrDefault("Description", "");
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
        if (description.length() > 512) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 512.");
        }
        String policyText = PolicyUtil.convertPolicyToJsonStr(policyDocument);
        try {
            if (PolicyUtil.checkJsonStr(policyText)){
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,"The strategy format is incorrect");
            }
            JSON json = JSON.parseObject(policyText);
            PolicyJsonObj policyJsonObj = JSON.toJavaObject(json,PolicyJsonObj.class);
            if (PolicyUtil.isPolicyJsonObjError(policyJsonObj)){
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,"The strategy format is incorrect");
            }
        }catch (Exception e){
            throw e;
        }
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_CREATE_POLICY, 0);
        msg.put("accountId", userId);
        msg.put("policyName", policyName);
        msg.put("remark", URLEncoder.encode(description, "UTF-8"));
        msg.put("policyDocument", URLEncoder.encode(policyDocument, "UTF-8"));

        CreatePolicyResponse response = new CreatePolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                CreatePolicyResponse.CreatePolicyResult result = new CreatePolicyResponse.CreatePolicyResult();
                result.setPolicy(setPolicyFromIAM(res));
                response.setCreatePolicyResult(result);

                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
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
            sleep(10000);
        }
    }

    public ResponseMsg deletePolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME, "");
        String policyName = getPolicyName(policyArn,accountId);

        MsAclUtils.checkIfAnonymous(accountId);
        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        DeletePolicyResponse response = new DeletePolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeletePolicyMsg(accountId, policyName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam delete policy successfully,policyName: {}", policyName);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            } else if (code == ErrorNo.POLICY_ATTACHED_ERROR) {
                throw new MsException(ErrorNo.POLICY_ATTACHED_ERROR, "The policy has been attached to entities.");
            } else if (code == ErrorNo.POLICY_CANNOT_BE_DELETED) {
                throw new MsException(ErrorNo.POLICY_CANNOT_BE_DELETED, "The policy cannot be deleted");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete policy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listPolicies(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        ListPoliciesResponse response = new ListPoliciesResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));

        ListPoliciesResponse.ListPoliciesResult result = new ListPoliciesResponse.ListPoliciesResult();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListPoliciesMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();
            ListPoliciesResponse.Policies policies = new ListPoliciesResponse.Policies();

            List<PolicyMember> members = new ArrayList<>();
            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    PolicyMember policyMember = setPolicyMemberIAM(re);
                    if (policyMember != null){
                        members.add(policyMember);
                    }
                }
                policies.setMember(members);
                result.setPolicies(policies);
                response.setListPoliciesResult(result);
                log.debug("Iam list policies successfully.");
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List policies failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listEntitiesForPolicy(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME, "");
        MsAclUtils.checkIfAnonymous(accountId);
        String policyName = getPolicyName(policyArn,accountId);

        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        ListEntitiesForPolicyResponse.ListEntitiesForPolicyResult result = new ListEntitiesForPolicyResponse.ListEntitiesForPolicyResult();
        ListEntitiesForPolicyResponse response = new ListEntitiesForPolicyResponse()
                .setListEntitiesForPolicyResult(result)
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntitiesForPolicyMsg(accountId, policyName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (int i = 0; i < res.size(); i++) {
                    String type = res.get(i).get("objectType");
                    if ("1".equals(type)) {
                        //user
                        PolicyUsers.Member member = new PolicyUsers.Member()
                                .setUserId(res.get(i).get("objectId"))
                                .setUserName(res.get(i).get("objectName"));

                        result.getPolicyUsers().getMembers().add(member);
                    } else if ("2".equals(type)) {
                        //group
                        PolicyGroups.Member member = new PolicyGroups.Member()
                                .setGroupId(res.get(i).get("objectId"))
                                .setGroupName(res.get(i).get("objectName"));
                        result.getPolicyGroups().getMembers().add(member);
                    }
                }

                result.getPolicyRoles().setMembers(RoleUtils.getRolesForPolicy(accountId, policyName));

                log.debug("List entities for policy successfully.");
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List entities for policy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg getPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME, "");
        MsAclUtils.checkIfAnonymous(accountId);
        String policyName = getPolicyName(policyArn,accountId);

        //策略名称校验，策略名称需要符合格式并且不准为空
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches() || policyName.isEmpty()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        GetPolicyResponse response = new GetPolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        GetPolicyResponse.GetPolicyResult result = new GetPolicyResponse.GetPolicyResult();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetPolicyMsg(accountId, policyName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                result.setPolicy(setPolicyFromIAM(res));
                response.setGetPolicyResult(result);
                log.debug("Iam get policy successfully.");
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.POLICY_NOT_EXISTS) {
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get policy failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg updatePolicy(UnifiedMap<String,String> paramMap) throws UnsupportedEncodingException {
        String accountId = paramMap.get(USER_ID);
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME,"");
        String newPolicyDocument = paramMap.getOrDefault("PolicyDocument","");
        String policyName = getPolicyName(policyArn,accountId);
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

        String policyText = PolicyUtil.convertPolicyToJsonStr(newPolicyDocument);
        try {
            if (PolicyUtil.checkJsonStr(policyText)){
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,"The strategy format is incorrect");
            }
            JSON json = JSON.parseObject(policyText);
            PolicyJsonObj policyJsonObj = JSON.toJavaObject(json,PolicyJsonObj.class);
            if (PolicyUtil.isPolicyJsonObjError(policyJsonObj)){
                throw new MsException(ErrorNo.POLICY_TEXT_FORMAT_ERROR,"The strategy format is incorrect");
            }
        }catch (Exception e){
            throw e;
        }

        CreatePolicyVersionResponse response = new CreatePolicyVersionResponse().setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdatePolicyMsg(accountId, policyName, newPolicyDocument, "");
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();
            if (code == ErrorNo.SUCCESS_STATUS) {
                CreatePolicyVersionResponse.CreatePolicyVersionResult result = new CreatePolicyVersionResponse.CreatePolicyVersionResult();
                CreatePolicyVersionResponse.PolicyVersion policyVersion = new CreatePolicyVersionResponse.PolicyVersion();
                policyVersion.setVersionId("v1");
                policyVersion.setDefaultVersion(false);
                policyVersion.setCreateDate(MsDateUtils.stampToDateFormat(res.get("modifyTime")));
                result.setPolicyVersion(policyVersion);
                response.setCreatePolicyVersionResult(result);
                log.debug("Update policy successfully,policyName: {}", policyName);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
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


    public ResponseMsg attachUserPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userNames = paramMap.getOrDefault(IAM_USER_NAME, "");
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME, "");
        MsAclUtils.checkIfAnonymous(accountId);
        String policyName = getPolicyName(policyArn,accountId);

        if (!POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        List<String> userList = Arrays.asList(userNames.split(","));
        userList.forEach(user -> {
            if (!USER_NAME_PATTERN.matcher(user).matches()) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
        });

        AttachUserPolicyResponse response = new AttachUserPolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAttachEntityPolicyMsg(accountId, userNames, policyName, AUTH_OBJECT_TYPE_USER);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam attach user policy successfully.");
                return new ResponseMsg().setData(response);
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

    public ResponseMsg detachUserPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        String policyArn = paramMap.get(AWS_IAM_POLICY_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        String policyName = getPolicyName(policyArn,accountId);

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(policyName) || !POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        DetachUserPolicyResponse response = new DetachUserPolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDetachEntityPolicyMsg(accountId, userName, policyName, AUTH_OBJECT_TYPE_USER);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Detach user policy successfully.");
                return new ResponseMsg().setData(response);
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

    public ResponseMsg listAttachedUserPolicies(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        ListAttachedUserPoliciesResponse response = new ListAttachedUserPoliciesResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        ListAttachedUserPoliciesResponse.ListAttachedUserPoliciesResult result = new ListAttachedUserPoliciesResponse.ListAttachedUserPoliciesResult();
        List<ListAttachedUserPoliciesResponse.StringMember> members = new ArrayList<>();

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntityPoliciesMsg(accountId, userName, AUTH_OBJECT_TYPE_USER);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    members.add(setPolicyInfoFromIAM(re));
                }
                ListAttachedUserPoliciesResponse.AttachedPolicies policies = new ListAttachedUserPoliciesResponse.AttachedPolicies();
                policies.setMembers(members);
                result.setAttachedPolicies(policies);
                result.setTruncated(false);
                response.setListUserPoliciesResult(result);
                log.debug("Iam list user policies successfully.");
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List user policies failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg attachGroupPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupNames = paramMap.getOrDefault(IAM_GROUP_NAME, "");
        String policyArn = paramMap.getOrDefault(AWS_IAM_POLICY_NAME, "");
        MsAclUtils.checkIfAnonymous(accountId);

        String policyName = getPolicyName(policyArn,accountId);

        if (!POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        List<String> groupList = Arrays.asList(groupNames.split(","));
        List<String> policyList = Arrays.asList(policyName.split(","));

        groupList.forEach(group -> {
            if (!GROUP_NAME_PATTERN.matcher(group).matches()) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
        });

        AttachGroupPolicyResponse response = new AttachGroupPolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAttachEntityPolicyMsg(accountId, groupNames, policyName, AUTH_OBJECT_TYPE_GROUP);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Attach group policy successfully.");
                return new ResponseMsg().setData(response);
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

    public ResponseMsg detachGroupPolicy(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String policyArn = paramMap.get(AWS_IAM_POLICY_NAME);
        String entityType = AUTH_OBJECT_TYPE_GROUP;
        MsAclUtils.checkIfAnonymous(accountId);
        String policyName = getPolicyName(policyArn,accountId);

        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(policyName) || !POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }

        DetachGroupPolicyResponse response = new DetachGroupPolicyResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDetachEntityPolicyMsg(accountId, groupName, policyName, entityType);
            addSyncFlagParam(msg, paramMap);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam detach group policy successfully.");
                return new ResponseMsg().setData(response);
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

    public ResponseMsg listAttachedGroupPolicies(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String entityType = AUTH_OBJECT_TYPE_GROUP;
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        ListAttachedGroupPoliciesResponse response = new ListAttachedGroupPoliciesResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        ListAttachedGroupPoliciesResponse.ListAttachedGroupPoliciesResult result = new ListAttachedGroupPoliciesResponse.ListAttachedGroupPoliciesResult();
        List<ListAttachedUserPoliciesResponse.StringMember> members = new ArrayList<>();
        ListAttachedUserPoliciesResponse.AttachedPolicies policies = new ListAttachedUserPoliciesResponse.AttachedPolicies();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListEntityPoliciesMsg(accountId, groupName, entityType);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    members.add(setPolicyInfoFromIAM(re));
                }

                policies.setMembers(members);
                result.setAttachedPolicies(policies);
                result.setTruncated(false);
                response.setListAttachedGroupPoliciesResult(result);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_OBJ) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List group policies failed.");
            }
            sleep(10000);
        }
    }

    public Policy setPolicyFromIAM(Map<String, String> res) {
        Policy policy = new Policy();
        policy.setDescription(res.get("remark"));
        policy.setPolicyId(res.get("policyId"));
        policy.setPolicyName(res.get("policyName"));
        String createTime = res.get("createTime");
        policy.setCreateDate(MsDateUtils.stampToDateFormat(createTime));
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        policy.setArn(mrn);
        policy.setPath("/");
        policy.setAttachmentCount(-1);

        String modifyTime = res.get("modifyTime");
        policy.setUpdateDate(MsDateUtils.stampToDateFormat(modifyTime));
        policy.setDefaultVersionId("v1");

        return policy;
    }

    public PolicyMember setPolicyMemberIAM(Map<String, String> res) {
        PolicyMember member = new PolicyMember();
        member.setPolicyName(res.get("policyName"));
        String createTime = res.get("createTime");
        createTime = createTime.contains(":") ? createTime : MsDateUtils.stampToISO8601(createTime);
        member.setCreateDate(createTime);
        member.setUpdateDate(createTime);
        member.setDescription(res.get("remark"));
        member.setPolicyId(res.get("policyId"));
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        if (StringUtils.isEmpty(mrn)){
            return null;
        }
        member.setArn(mrn);
        member.setPath("/");
        member.setDefaultVersionId("v1");
        member.setAttachable(true);

        return member;
    }

    public ListAttachedUserPoliciesResponse.StringMember setPolicyInfoFromIAM(Map<String, String> res) {
        ListAttachedUserPoliciesResponse.StringMember member = new ListAttachedUserPoliciesResponse.StringMember();
        member.setPolicyName(res.get("policyName"));
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        member.setPolicyArn(mrn);
        return member;
    }


    private String getPolicyName(String arn,String accountId) {
        String[] params = arn.split(":");
        if (params.length < 6) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }
        if("".equals(params[4])){
            String[] sysPolicyName = params[5].split("/");
            if(sysPolicyName.length == 2){
                return sysPolicyName[1];
            }else{
                throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
            }
        }
        if (!params[4].equals(accountId)){
            throw new MsException(ErrorNo.ACCESS_DENY,"The validation fails, refused to visit.");
        }
        String[] policyName = params[5].split("/");
        if (policyName.length < 2) {
            throw new MsException(ErrorNo.POLICY_NOT_EXISTS, POLICY_NOT_EXIST_ERR);
        }
        return policyName[1];
    }
}
