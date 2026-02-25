package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.UserInfoResponse;
import com.macrosan.message.xmlmsg.UserInfosResponse;
import com.macrosan.message.xmlmsg.section.UserInfo;
import com.macrosan.message.xmlmsg.section.UserInfos;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.*;


/**
 * UserService
 * 非线程安全单例模式
 *
 * @author shilinyong
 * @date 2019/07/17
 */
public class UserService extends BaseService {

    private static final Logger logger = LogManager.getLogger(UserService.class.getName());

    private static UserService instance = null;

    private UserService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static UserService getInstance() {
        if (instance == null) {
            instance = new UserService();
        }
        return instance;
    }

    public static UserInfo setUserInfoFromIAM(Map<String, String> res) {
        String ak = StringUtils.isNotEmpty(res.get("ak")) ? res.get("ak") : res.get("accessKey");
        String sk = StringUtils.isNotEmpty(res.get("sk")) ? res.get("sk") : res.get("secretKey");
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        String nickname = StringUtils.isNotEmpty(res.get("nickname")) ? res.get("nickname") : res.get("userNickName");
        UserInfo userInfo = new UserInfo();
        userInfo.setUserId(res.get("userId"));
        userInfo.setUserName(res.get("userName"));
        userInfo.setUserNickName(nickname);
        userInfo.setRemark(res.get("remark"));
        userInfo.setAccessKey(ak);
        userInfo.setSecretKey(sk);
        String createTime = res.get("createTime");
        createTime = createTime.contains(":")?createTime: MsDateUtils.stampToSimpleDate(createTime);
        userInfo.setCreateTime(createTime);
        userInfo.setUserMRN(mrn);
        return userInfo;
    }

    /**
     * 创建用户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createUser(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(IAM_USER_NAME, "");
        String passWord = paramMap.getOrDefault("PassWord","");
        String userNickName = paramMap.getOrDefault("UserNickName","");
        String remark = paramMap.getOrDefault("Remark","");
        String validityTime = paramMap.getOrDefault("ValidityTime","0");
        String validityGrade = paramMap.getOrDefault("ValidityGrade","0");
        if ("".equals(validityGrade)) {
            validityGrade = "0";
        }
        if(!validityGrade.equals("0") && !validityGrade.equals("1")){
            throw new MsException(ErrorNo.ACCOUNT_VALIDITY_GRADE_ERROR,
                    "The validityGrade is illegal");
        }
        if(!validityTime.equals("0") && !validityTime.equals("")) {
            //密码有效期检验
            if(!VALIDITY_TIME_PATTERN.matcher(validityTime).matches()){
                throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                        "The validityTime is illegal");
            }
            int validityTimeInt = Integer.parseInt(validityTime);
            long endTimeSecond = System.currentTimeMillis() / 1000 + validityTimeInt * 24 * 3600;
            paramMap.put("EndTimeSecond", String.valueOf(endTimeSecond));
            if (!("0".equals(validityGrade) && validityTimeInt <= 365) && !("1".equals(validityGrade) && validityTimeInt <= 90)) {
                throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                        "The validityTime is illegal");
            }
        }else {
            paramMap.put("EndTimeSecond","0");
            paramMap.put("ValidityTime","0");
        }

        paramMap.put("ValidityGrade", validityGrade);
        MsAclUtils.checkIfAnonymous(accountId);

        //用户名称校验
        if (!USER_NAME_PATTERN.matcher(userName).matches() || userName.isEmpty()) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "createUser failed, user name input error, user_name: " + userName + ".");
        }
        //用户密码校验
        if (!USER_PASSWORD_PATTERN.matcher(passWord).matches() || passWord.isEmpty()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "createUser failed, invalid password, user_password: " + passWord + ".");
        }
        //用户昵称长度不能大于32
        if (userNickName.length() > 32) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 32.");
        }

        //用户备注长度不能大于128
        if (remark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }

        UserInfoResponse userInfoResponse = new UserInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateUserMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                userInfoResponse.setUserInfo(setUserInfoFromIAM(res));
                logger.debug("Create user successfully,userName: {}", userName);
                return new ResponseMsg().setData(userInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.TOO_MANY_USER) {
                throw new MsException(ErrorNo.TOO_MANY_USER, "The account has too many users.");
            } else if (code == ErrorNo.USER_EXISTED) {
                throw new MsException(ErrorNo.USER_EXISTED, "The user already exits.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create user failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除用户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteUser(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        //用户名称校验
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteUserMsg(accountId, userName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete user successfully,userName: {}", userName);
                return new ResponseMsg();
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

    /**
     * 更新用户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg updateUser(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        logger.info("updateUser {}", paramMap);
        String accountId = paramMap.get(USER_ID);
        paramMap.put("accountId",accountId);
        String userName = paramMap.getOrDefault(IAM_USER_NAME,"");
        String newPassWord = paramMap.getOrDefault("NewPassWord","");
        String newUserNickName = paramMap.getOrDefault("NewUserNickName","");
        String newRemark = paramMap.getOrDefault("NewRemark","");
        String validityTime = paramMap.getOrDefault("ValidityTime", "");
        String validityGrade = paramMap.getOrDefault("ValidityGrade","0");
        if ("".equals(validityGrade)) {
            validityGrade = "0";
        }
        if(!validityGrade.equals("0") && !validityGrade.equals("1")){
            throw new MsException(ErrorNo.ACCOUNT_VALIDITY_GRADE_ERROR,
                    "The validityGrade is illegal");
        }
        paramMap.put("ValidityGrade", validityGrade);

        if (!paramMap.containsKey(SITE_FLAG_UPPER) && !paramMap.containsKey(REGION_FLAG_UPPER)) {
            if(!validityTime.equals("") && !validityTime.equals("0")) {
                //密码有效期检验
                if(!VALIDITY_TIME_PATTERN.matcher(validityTime).matches()){
                    throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                            "The validityTime is illegal");
                }
                int validityTimeInt = Integer.parseInt(validityTime);
                long endTimeSecond = System.currentTimeMillis() / 1000 + validityTimeInt * 24 * 3600;
                paramMap.put("EndTimeSecond", String.valueOf(endTimeSecond));
                if (!("0".equals(validityGrade) && validityTimeInt <= 365) && !("1".equals(validityGrade) && validityTimeInt <= 90)) {
                    throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                            "The validityTime is illegal");
                }
            }else if(validityTime.equals("0")){
                paramMap.put("EndTimeSecond","0");
            }else {
                paramMap.put("EndTimeSecond","");
            }
        }
        MsAclUtils.checkIfAnonymous(accountId);

        //用户名称校验
        if (StringUtils.isNotEmpty(userName) && !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        //用户新密码校验
        if (StringUtils.isNotEmpty(newPassWord) && !USER_PASSWORD_PATTERN.matcher(newPassWord).matches()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "updateUser failed, invalid password, user_password: " + newPassWord + ".");
        }
        //用户昵称长度不能大于32
        if (newUserNickName.length() > 32) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 32.");
        }
        //用户备注长度不能大于128
        if (newRemark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }
        UserInfoResponse userInfoResponse = new UserInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateUserMsg(paramMap);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                userInfoResponse.setUserInfo(setUserInfoFromIAM(res));
                logger.debug("Update user successfully,userName: {}", userName);
                return new ResponseMsg().setData(userInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.INVAILD_PASSWD){
                throw new MsException(ErrorNo.INVAILD_PASSWD,
                        "updateUser failed, invalid password, user_password: " + newPassWord + ".");
            } else if (code == ErrorNo.PASSWORD_INPUT_ERR) {
                throw new MsException(ErrorNo.PASSWORD_INPUT_ERR,
                        "updateUser failed, password is exist, user_password: " + newPassWord + ".");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update user failed.");
            }
            sleep(10000);
        }
    }

  /**
   * 更新用户名
   *
   * @param paramMap 请求参数
   * @return 相应返回码
   */
//  public ResponseMsg updateUserName(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
//    //获取url参数值
//    String accountId = paramMap.get(USER_ID);
//    String userName = paramMap.getOrDefault(IAM_USER_NAME,"");
//    String newName = paramMap.getOrDefault("newName","");
//    String id = paramMap.getOrDefault("AccountId", "");
//    MsAclUtils.checkIfAnonymous(accountId);
//
//    //用户名称校验
//    if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
//      throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
//              "user name input error, username: " + userName + ".");
//    }
//
//    if (StringUtils.isEmpty(newName) || !USER_NAME_PATTERN.matcher(newName).matches()) {
//      throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
//              "newUserName input error, newUserName: " + newName + ".");
//    }
//
//    //判断使用者权限
//    if(!accountId.equals(id)){
//      throw new MsException(ErrorNo.ACCESS_DENY, "account has no permission.");
//    }
//
//    for (int tryTime = 10; ; tryTime -= 1) {
//      SocketReqMsg msg = buildEditUserName(id, userName, newName);
//      addSyncFlagParam(msg, paramMap);
//      MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
//      int code = resMsg.getCode();
//
//      if (code == ErrorNo.SUCCESS_STATUS) {
//        logger.debug("Update user name successfully,userName: {}", userName);
//        return new ResponseMsg(code);
//      } else if (code == ErrorNo.USER_NOT_EXISTS) {
//        throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
//      } else if(code == ErrorNo.USER_EXISTED){
//          throw new MsException(ErrorNo.USER_EXISTED,"user exist");
//      }
//
//      if (tryTime == 0) {
//        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update userName failed.");
//      }
//      sleep(10000);
//    }
//  }

  /**
     * 重置用户密码
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg updateUserPasswd(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        paramMap.put("accountId",accountId);
        String newPassWord = paramMap.getOrDefault("NewPassWord", "");
        String oldPassWord = paramMap.getOrDefault("OldPassWord", "");
        String userName = paramMap.get("UserName");
        String validityTime = paramMap.getOrDefault("ValidityTime","");
        String validityGrade = paramMap.getOrDefault("ValidityGrade", "0");
        if ("".equals(validityGrade)) {
            validityGrade = "0";
        }
        if(!validityGrade.equals("0") && !validityGrade.equals("1")){
            throw new MsException(ErrorNo.ACCOUNT_VALIDITY_GRADE_ERROR,
                    "The validityGrade is illegal");
        }
        if(!validityTime.equals("") && !validityTime.equals("0")) {
            //密码有效期检验
            if(!VALIDITY_TIME_PATTERN.matcher(validityTime).matches()){
                throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                        "The validityTime is illegal");
            }
            int validityTimeInt = Integer.parseInt(validityTime);
            long endTimeSecond = System.currentTimeMillis() / 1000 + validityTimeInt * 24 * 3600;
            paramMap.put("EndTimeSecond", String.valueOf(endTimeSecond));
            if (!("0".equals(validityGrade) && validityTimeInt <= 365) && !("1".equals(validityGrade) && validityTimeInt <= 90)) {
                throw new MsException(ErrorNo.ACCOUNT_VALIDITY_TIME_ERROR,
                        "The validityTime is illegal");
            }
        }else if(validityTime.equals("0")){
            paramMap.put("EndTimeSecond","0");
        }else {
            paramMap.put("EndTimeSecond","");
        }
        MsAclUtils.checkIfAnonymous(accountId);

        paramMap.put("ValidityGrade", validityGrade);
        //用户新密码校验
        if (!USER_PASSWORD_PATTERN.matcher(newPassWord).matches() || newPassWord.isEmpty()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "reset password failed, invalid password, user_password: " + newPassWord + ".");
        }

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        if(newPassWord.equals(oldPassWord)){
            throw new MsException(ErrorNo.USER_PASSWORD_ERROR, "The oldpasswd is same with newpasswd.");
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateUserPasswdMsg(paramMap);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("reset user password successfully,userName: {}", userName);
                return new ResponseMsg(code);
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

    /**
     * 查询用户列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listUsers(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        UserInfosResponse userInfosResponse = new UserInfosResponse();
        UserInfos userInfos = new UserInfos();
        List<UserInfo> userList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListUsersMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    userList.add(setUserInfoFromIAM(re));
                }
                userInfos.setUserInfo(userList);
                userInfosResponse.setUserInfos(userInfos);
                logger.debug("List users successfully.");
                return new ResponseMsg().setData(userInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List users failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询特定用户信息
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg getUser(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        UserInfoResponse userInfoResponse = new UserInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetUserMsg(accountId, userName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                userInfoResponse.setUserInfo(setUserInfoFromIAM(res));
                logger.debug("Get user successfully.");
                return new ResponseMsg().setData(userInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get user failed.");
            }
            sleep(10000);
        }
    }
}