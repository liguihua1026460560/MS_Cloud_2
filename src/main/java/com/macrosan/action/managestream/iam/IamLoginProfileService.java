package com.macrosan.action.managestream.iam;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import static com.macrosan.constants.IAMConstants.AWS_IAM_USER_NAME;
import static com.macrosan.constants.IAMConstants.AWS_IAM_USER_DEFAULT_PASSWD;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.*;
import com.macrosan.message.xmlmsg.iam.CreateLoginProfileResponse.CreateLoginProfileResult;
import com.macrosan.message.xmlmsg.iam.GetLoginProfileResponse.GetLoginProfileResult;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.MSG_TYPE_UPDATE_USER;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildUpdateAWSUserPasswdMsg;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildUpdateUserPasswdMsg;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.USER_NAME_PATTERN;
import static com.macrosan.utils.regex.PatternConst.USER_PASSWORD_PATTERN;

@Log4j2
public class IamLoginProfileService extends BaseService {
    private static IamLoginProfileService instance = null;

    private IamLoginProfileService() {
        super();
    }

    public static IamLoginProfileService getInstance() {
        if (instance == null) {
            instance = new IamLoginProfileService();
        }
        return instance;
    }

    public ResponseMsg createLoginProfile(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(AWS_IAM_USER_NAME, "");
        String passWord = paramMap.getOrDefault("Password", "");
        String passwordResetRequired = paramMap.getOrDefault("PasswordResetRequired","false");

        MsAclUtils.checkIfAnonymous(accountId);

        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "createLoginProfile failed, user name input error, user_name: " + userName + ".");
        }

        if (!USER_PASSWORD_PATTERN.matcher(passWord).matches() || passWord.isEmpty()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "createLoginProfile failed, invalid password, user_password: " + passWord + ".");
        }


        CreateLoginProfileResult result = new CreateLoginProfileResult();
        CreateLoginProfileResponse response = new CreateLoginProfileResponse()
                .setCreateLoginProfileResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateAWSUserPasswdMsg(accountId, passWord, AWS_IAM_USER_DEFAULT_PASSWD, userName);
            msg.put("validityTime", "0");
            msg.put("endTimeSecond", "0");
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam createLoginProfile successfully,userName: {}", userName);

                LoginProfile profile = new LoginProfile()
                        .setPasswordResetRequired(Boolean.parseBoolean(passwordResetRequired))
                        .setUserName(userName)
                        .setCreateDate(MsDateUtils.stampToDateFormat("0"));

                result.setLoginProfile(profile);

                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD, "The password input is invaild.");
            } else if (code == ErrorNo.USER_PASSWORD_ERROR) {
                //409 EntityAlreadyExists. he request was rejected because it attempted to create a resource that already exists.
                throw new MsException(ErrorNo.USER_PASSWORD_ERROR, "The oldPassword input is error.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Reset account password failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg deleteLoginProfile(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(AWS_IAM_USER_NAME, "");

        MsAclUtils.checkIfAnonymous(accountId);

        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "deleteLoginProfile failed, user name input error, user_name: " + userName + ".");
        }

        DeleteLoginProfileResponse response = new DeleteLoginProfileResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));


        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_USER, 0);
        msg.put("accountId", accountId);
        msg.put("userName", userName);
        msg.put("newPassWord", AWS_IAM_USER_DEFAULT_PASSWD);
        msg.put("newRemark", "");
        msg.put("newUserNickName", "");
        msg.put("validityTime", "0");
        msg.put("endTimeSecond", "0");
        msg.put("validityGrade", "0");
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam deleteLoginProfile successfully,userName: {}", userName);
                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD, "The password input is invaild.");
            } else if (code == ErrorNo.USER_PASSWORD_ERROR) {
                throw new MsException(ErrorNo.USER_PASSWORD_ERROR, "The oldPassword input is error.");
            } else if (code == ErrorNo.USER_PASSWORD_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_PASSWORD_NOT_EXISTS, "The password is not exists");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Reset account password failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg getLoginProfile(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(AWS_IAM_USER_NAME, "");

        MsAclUtils.checkIfAnonymous(accountId);

        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "getLoginProfile failed, user name input error, user_name: " + userName + ".");
        }

        GetLoginProfileResult result = new GetLoginProfileResult();
        GetLoginProfileResponse response = new GetLoginProfileResponse()
                .setGetLoginProfileResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        SocketReqMsg msg = buildUpdateAWSUserPasswdMsg(accountId, AWS_IAM_USER_DEFAULT_PASSWD, AWS_IAM_USER_DEFAULT_PASSWD, userName);
        msg.put("validityTime", "0");
        msg.put("endTimeSecond", "0");
        msg.put("validityGrade", "0");
        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.USER_PASSWORD_ERROR) {
                log.debug("Iam getLoginProfile successfully,userName: {}", userName);

                LoginProfile profile = new LoginProfile()
                        .setPasswordResetRequired(false)
                        .setUserName(userName)
                        .setCreateDate(MsDateUtils.stampToDateFormat("0"));

                result.setLoginProfile(profile);

                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD, "The password input is invaild.");
            } else if (code == ErrorNo.SUCCESS_STATUS) {
                //404 NoSuchEntity The request was rejected because it referenced a resource entity that does not exist. The error message describes the resource.
                throw new MsException(ErrorNo.USER_PASSWORD_NOT_EXISTS, "No LoginProfile in User [" + userName + "]");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Reset account password failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg updateLoginProfile(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(AWS_IAM_USER_NAME, "");
        String passWord = paramMap.getOrDefault("Password", "");

        MsAclUtils.checkIfAnonymous(accountId);

        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "updateLoginProfile failed, user name input error, user_name: " + userName + ".");
        }

        if (!USER_PASSWORD_PATTERN.matcher(passWord).matches() || passWord.isEmpty()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "updateLoginProfile failed, invalid password, user_password: " + passWord + ".");
        }

        //判断 logionProfile是否存在
        getLoginProfile(paramMap);

        UpdateLoginProfileResponse response = new UpdateLoginProfileResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_USER, 0);
        msg.put("accountId", accountId);
        msg.put("userName", userName);
        msg.put("newPassWord", passWord);
        msg.put("newRemark", "");
        msg.put("newUserNickName", "");
        msg.put("validityTime", "0");
        msg.put("validityGrade", "0");
        msg.put("endTimeSecond", "0");

        for (int tryTime = 10; ; tryTime -= 1) {

            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam updateLoginProfile successfully,userName: {}", userName);
                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD,
                        "updateUser failed, invalid password, user_password: " + passWord + ".");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Reset account password failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg changePassword(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        //直接使用当前用户
        String userName = paramMap.get(USERNAME);
        String oldPassword = paramMap.getOrDefault("OldPassword", "");
        String newPassword = paramMap.getOrDefault("NewPassword", "");
        if (StringUtils.isNotEmpty(paramMap.get("isRole"))){
            throw new MsException(ErrorNo.USER_NOT_EXISTS,USER_NOT_EXIST_ERR);
        }

        MsAclUtils.checkIfAnonymous(accountId);

        if (!USER_PASSWORD_PATTERN.matcher(newPassword).matches() || newPassword.isEmpty()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "reset password failed, invalid password, user_password: " + newPassword + ".");
        }

        if(oldPassword.equals(newPassword)){
            throw new MsException(ErrorNo.USER_PASSWORD_ERROR,
                    "reset password failed, oldPasswd is same with newPasswd " + ".");
        }

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        ChangePasswordResponse response = new ChangePasswordResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));


        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateAWSUserPasswdMsg(accountId, newPassword, oldPassword, userName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam changePassword successfully,userName: {}", userName);
                return new ResponseMsg(code).setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD, "The password input is invaild.");
            } else if (code == ErrorNo.USER_PASSWORD_ERROR) {
                throw new MsException(ErrorNo.USER_PASSWORD_ERROR, "The oldPassword input is error.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Reset account password failed.");
            }
            sleep(10000);
        }

    }
} 
