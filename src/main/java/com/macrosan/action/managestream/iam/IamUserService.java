package com.macrosan.action.managestream.iam;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.*;
import com.macrosan.message.xmlmsg.iam.CreateUserResponse.CreateUserResult;
import com.macrosan.message.xmlmsg.iam.GetUserResponse.GetUserResult;
import com.macrosan.message.xmlmsg.iam.ListUsersResponse.ListUsersResult;
import com.macrosan.message.xmlmsg.iam.ListUsersResponse.Users;
import com.macrosan.message.xmlmsg.iam.UpdateUserResponse.UpdateUserResult;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.PatternConst;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.MSG_TYPE_CREATE_USER;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.USER_NAME_PATTERN;

@Log4j2
public class IamUserService extends BaseService {

    private static IamUserService instance = null;

    private IamUserService() {
        super();
    }

    public static IamUserService getInstance() {
        if (instance == null) {
            instance = new IamUserService();
        }
        return instance;
    }

    public ResponseMsg createUser(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(AWS_IAM_USER_NAME, "");
        MsAclUtils.checkIfAnonymous(accountId);

        //用户名称校验
        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "createUser failed, user name input error, user_name: " + userName + ".");
        }


        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_USER, 0);

        reqMsg.put("accountId", accountId);
        reqMsg.put("userName", userName);
        //aws sdk 创建用户时没有密码. 但是 MOSS iam 密码是必要的
        reqMsg.put("passWord", AWS_IAM_USER_DEFAULT_PASSWD);
        reqMsg.put("userNickName", "");
        reqMsg.put("remark", "");
        reqMsg.put("validityTime", "0");
        reqMsg.put("endTimeSecond", "0");
        reqMsg.put("validityGrade", "0");
        CreateUserResult result = new CreateUserResult();

        CreateUserResponse createUserResponse = new CreateUserResponse()
                .setCreateUserResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), reqMsg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                result.setUser(getUserFromIam(res));

                if (!deleteUserAccess(accountId,userName)){
                    throw new MsException(ErrorNo.UNKNOWN_ERROR,"Create user failed");
                }

                log.debug("Iam create user successfully,userName: {}", userName);
                return new ResponseMsg().setData(createUserResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.TOO_MANY_USER) {
                throw new MsException(ErrorNo.TOO_MANY_USER, "The account has too many users.");
            } else if (code == ErrorNo.USER_EXISTED) {
                throw new MsException(ErrorNo.USER_EXISTED, "The user already exits.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create user failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listUsers(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String marker = paramMap.getOrDefault("Marker", "");
        String maxStr = paramMap.getOrDefault("MaxItems", "1000");

        if (!PatternConst.LIST_PARTS_PATTERN.matcher(maxStr).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error.");
        }
        int max = Integer.parseInt(maxStr);
        if (max > 1000 || max <= 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error, must be int in [1, 1000]");
        }

        ListUsersResult result = new ListUsersResult();

        ListUsersResponse response = new ListUsersResponse()
                .setListUsersResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));


        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListUsersMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                TreeMap<String, ListUsersResponse.Member> members = new TreeMap<>();

                for (Map<String, String> map : res) {
                    ListUsersResponse.Member member = getMemberFromIam(map);
                    members.put(member.getUserName(), member);
                }

                String key = members.higherKey(marker);
                int num = 0;
                List<ListUsersResponse.Member> memberList = new LinkedList<>();

                while (key != null && num < max) {
                    ListUsersResponse.Member member = members.get(key);
                    memberList.add(member);
                    num++;
                    key = members.higherKey(key);
                }

                result.setUsers(new Users().setMember(memberList));

                result.setTruncated(key != null);

                log.debug("Iam list users successfully.");
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List users failed.");
            }
            sleep(10000);
        }
    }

    private static User getUser(String accountId, String userName) {
        SocketReqMsg msg = buildGetUserMsg(accountId, userName);
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return getUserFromIam(res);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get user failed.");
            }
            sleep(10000);
        }
    }


    public ResponseMsg getUser(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        //没有UserName参数，使用当前用户
        if (userName == null) {
            userName = paramMap.get(USERNAME);
        }

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        GetUserResult result = new GetUserResult();
        GetUserResponse response = new GetUserResponse()
                .setGetUserResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        User user = getUser(accountId, userName);
        result.setUser(user);

        log.debug("Iam get user successfully.");
        return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    public ResponseMsg deleteUser(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        //没有UserName参数，使用当前用户
        if (userName == null) {
            userName = paramMap.get(USERNAME);
        }

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        SocketReqMsg msg = buildDeleteUserMsg(accountId, userName);
        DeleteUserResponse response = new DeleteUserResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));


        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam delete user successfully,userName: {}", userName);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.USER_ADDED_GROUP) {
                throw new MsException(ErrorNo.USER_ADDED_GROUP, "The user has added the group.");
            } else if (code == ErrorNo.USER_ATTACH_POLICY) {
                throw new MsException(ErrorNo.USER_ATTACH_POLICY, "The user has attached the policy.");
            } else if (code == ErrorNo.DELETE_AK_FAIL) {
                throw new MsException(ErrorNo.DELETE_AK_FAIL, "Delete Ak failed.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete user failed.");
            }
            sleep(10000);
        }

    }

    public ResponseMsg updateUser(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }


        UpdateUserResult result = new UpdateUserResult();
        UpdateUserResponse response = new UpdateUserResponse()
                .setUpdateUserResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        String newName = paramMap.get("NewUserName");
        if (newName == null) {
            newName = userName;
            //不更新userName，这个接口不用做任何操作
            User user = getUser(accountId, userName);
            result.setUser(user);
            log.debug("Iam update user do nothing, userName: {}", userName);
            return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
        }

        if (StringUtils.isEmpty(newName) || !USER_NAME_PATTERN.matcher(newName).matches()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,"The input of user name is error.");
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildEditUserName(accountId, userName, newName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam update user do successfully, userName: {}", userName);
                User user = getUser(accountId, newName);
                result.setUser(user);
                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.USER_EXISTED) {
                throw new MsException(ErrorNo.USER_EXISTED, "user exist");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update userName failed.");
            }
            sleep(10000);
        }

    }

    public static User getUserFromIam(Map<String, String> res) {
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");

        String createTime = res.get("createTime");
        String stamp = createTime.contains(":") ? String.valueOf(MsDateUtils.dateToStamp(createTime)) : createTime;
        createTime = MsDateUtils.stampToDateFormat(stamp);

        return new User()
                .setUserId(res.get("userId"))
                .setUserName(res.get("userName"))
                .setArn(mrn)
                .setPath("")
                .setCreateDate(createTime);
    }

    public static ListUsersResponse.Member getMemberFromIam(Map<String, String> res) {
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");

        String createTime = res.get("createTime");
        String stamp = createTime.contains(":") ? String.valueOf(MsDateUtils.dateToStamp(createTime)) : createTime;
        createTime = MsDateUtils.stampToDateFormat(stamp);

        return new ListUsersResponse.Member()
                .setUserId(res.get("userId"))
                .setPath("")
                .setUserName(res.get("userName"))
                .setArn(mrn)
                .setCreateDate(createTime);
    }

    private static boolean deleteUserAccess(String accountId,String userName){
        for (int tryTime = 3;; tryTime -= 1){
            SocketReqMsg listMsg = buildListAccessKeysMsg(accountId, userName);
            ListMapResMsg listResMsg = sender.sendAndGetResponse(getWebAddr(), listMsg, ListMapResMsg.class, true);

            if (listResMsg.getCode() != ErrorNo.SUCCESS_STATUS){
                return false;
            }

            String ak = null;
            List<Map<String, String>> res = listResMsg.getData();
            for (Map<String,String> re : res){
                ak = re.get("ak");
            }
            if (StringUtils.isEmpty(ak)){
                return false;
            }

            SocketReqMsg msg = buildDeleteAccessKeyMsg(accountId,userName,ak);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return true;
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT || code == ErrorNo.AK_NOT_EXISTS || code == ErrorNo.MOSS_FAIL || code == ErrorNo.USER_NOT_EXISTS) {
                return false;
            }

            if (tryTime == 0) {
                return false;
            }
            sleep(10000);
        }

    }

}
