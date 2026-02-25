package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.AccessKeyInfoResponse;
import com.macrosan.message.xmlmsg.AccessKeyInfosResponse;
import com.macrosan.message.xmlmsg.AccountAccessKeyInfosResponse;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.message.xmlmsg.section.AccessKeyInfo;
import com.macrosan.message.xmlmsg.section.AccessKeyInfos;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.AccountConstants.DEFAULT_MGT_USER_ID;
import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_USERINFO_INDEX;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.*;


/**
 * AccessKeyService
 * 非线程安全单例模式
 *
 * @author shilinyong
 * @date 2019/08/01
 */
public class AccessKeyService extends BaseService {

    private static final Logger logger = LogManager.getLogger(AccessKeyService.class.getName());

    private static AccessKeyService instance = null;

    private AccessKeyService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static AccessKeyService getInstance() {
        if (instance == null) {
            instance = new AccessKeyService();
        }
        return instance;
    }

    public static AccessKeyInfo setAccessKeyInfoFromIAM(Map<String, String> res) {
        AccessKeyInfo accessKeyInfo = new AccessKeyInfo();
        accessKeyInfo.setAccessKeyId(res.get("ak"));
        String createTime = res.get("createTime");
        createTime = createTime.contains(":")?createTime:MsDateUtils.stampToSimpleDate(createTime);
        accessKeyInfo.setCreateTime(createTime);

        return accessKeyInfo;
    }

    public static AccessKeyInfo setAccountAccessKeyInfoFromIAM(Map<String, String> res) {
        
        AccessKeyInfo accessKeyInfo = new AccessKeyInfo();
        accessKeyInfo.setAccessKeyId(res.get("ak"));
        accessKeyInfo.setAccessKeySecret(res.get("sk"));
        String createTime = res.get("createTime");
        createTime = createTime.contains(":")?createTime: MsDateUtils.stampToSimpleDate(createTime);
        accessKeyInfo.setCreateTime(createTime);

        return accessKeyInfo;
    }

    /**
     * 创建账户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createAccountAccessKey(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.getOrDefault("AccountId","");
        String accountName = paramMap.getOrDefault("AccountName","");
        String _id = paramMap.get(USER_ID);
        String accessKey = paramMap.getOrDefault("AccessKey","");
        String secretKey = paramMap.getOrDefault("SecretKey","");

        if(!"".equals(accessKey) || !"".equals(secretKey)) {
            if (!ACCESSKEY_PATTERN.matcher(accessKey).matches()) {
                throw new MsException(ErrorNo.ACCESSKEY_NOT_MATCH, "The input ak is invalid.");
            }

            if (!SECRETKEY_PATTERN.matcher(secretKey).matches()) {
                throw new MsException(ErrorNo.SECRETKEY_NOT_MATCH, "The input sk is invalid.");
            }
        }

        if (StringUtils.isEmpty(accountId)){
            accountId = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "id");
        }

        if(StringUtils.isEmpty(accountId)) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName is not exist");
        }

        paramMap.put("AccountId", accountId);

        if (!_id.equalsIgnoreCase(accountId) && !_id.equalsIgnoreCase(DEFAULT_MGT_USER_ID)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user and not ak owner account, no permission.");
        }

        AccessKeyInfoResponse accessKeyInfoResponse = new AccessKeyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateAccessKeyAccountMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                AccessKeyInfo accessKeyInfo = setAccessKeyInfoFromIAM(res);
                accessKeyInfo.setAccessKeySecret(res.get("sk"));
                accessKeyInfoResponse.setAccessKeyInfo(accessKeyInfo);
                logger.debug("Create accessKey successfully,ak: {}", res.get("ak"));
                return new ResponseMsg().setData(accessKeyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            } else if (code == ErrorNo.TOO_MANY_AK) {
                throw new MsException(ErrorNo.TOO_MANY_AK, "The account has over two aks.");
            } else if (code == ErrorNo.AK_EXISTS){
                throw new MsException(ErrorNo.AK_EXISTS, "The ak exist.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 创建账户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createAdminAccessKey(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);

        paramMap.put("AccountId", accountId);

        if (!"admin".equalsIgnoreCase(accountId)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }

        AccessKeyInfoResponse accessKeyInfoResponse = new AccessKeyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateAdminAccessKeyMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                AccessKeyInfo accessKeyInfo = setAccessKeyInfoFromIAM(res);
                accessKeyInfo.setAccessKeySecret(res.get("sk"));
                accessKeyInfoResponse.setAccessKeyInfo(accessKeyInfo);
                logger.debug("Create accessKey successfully,ak: {}", res.get("ak"));
                return new ResponseMsg().setData(accessKeyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            } else if (code == ErrorNo.TOO_MANY_AK) {
                throw new MsException(ErrorNo.TOO_MANY_AK, "The account has over two aks.");
            } else if (code == ErrorNo.AK_EXISTS){
                throw new MsException(ErrorNo.AK_EXISTS, "The ak exist.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            } else if (code == ErrorNo.INVALID_ARGUMENT) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "only one read ak.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除账户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteAccountAccessKey(UnifiedMap<String, String> paramMap) {
        logger.info("paramMap {}", paramMap);
        //获取url参数值
        String accountId = paramMap.getOrDefault("AccountId", "");
        String accessKeyId = paramMap.getOrDefault("AccessKeyId", "");
        String accountName = paramMap.getOrDefault("AccountName", "");
        String _id = paramMap.get(USER_ID);

        if(!ACCESSKEY_PATTERN.matcher(accessKeyId).matches()) {
           throw new MsException(ErrorNo.AK_NOT_MATCH, "The input ak is invalid.");
        }

        if (!ACCOUNT_NAME_PATTERN.matcher(accountName).matches()) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName is not exist");
        }

        if (StringUtils.isEmpty(accountId)){
            accountId = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "id");
        } else {
            if (!accountId.equals(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "id"))) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName does not match accountId");
            }
        }

        if(accountId == null || accountId.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName is not exist");
        }

        if (!_id.equalsIgnoreCase(accountId) && !_id.equalsIgnoreCase(DEFAULT_MGT_USER_ID)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user and not ak owner account, no permission.");
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteAccessKeyAccountMsg(accountId,accessKeyId);
            msg.put("accountName", accountName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete accessKey successfully,accessKeyId: {}", accessKeyId);
                return new ResponseMsg();
            }
            else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            }
            else if (code == ErrorNo.AK_NOT_EXISTS) {
                throw new MsException(ErrorNo.AK_NOT_EXISTS, "The ak does not exist.");
            }
            else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除账户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteAdminAccessKey(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        logger.info(paramMap);
        String accessKeyId = paramMap.getOrDefault("AccessKeyId", "");
        String _id = paramMap.get(USER_ID);

        if (!ACCESSKEYID_PATTERN.matcher(accessKeyId).matches()) {
            throw new MsException(ErrorNo.AK_NOT_MATCH, "The input ak is invalid.");
        }

        if (!DEFAULT_MGT_USER_ID.equals(_id)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteAdminAccessKeyMsg(accessKeyId);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete accessKey successfully,accessKeyId: {}", accessKeyId);
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            } else if (code == ErrorNo.AK_NOT_EXISTS) {
                throw new MsException(ErrorNo.AK_NOT_EXISTS, "The ak does not exist.");
            } else if (code == ErrorNo.DELETE_AK_FAIL) {
                throw new MsException(ErrorNo.DELETE_AK_FAIL, "The account has at least one ak.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 创建用户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createAccessKey(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String userName = paramMap.get(IAM_USER_NAME);
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);
        String accessKey = paramMap.getOrDefault("AccessKey","");
        String secretKey = paramMap.getOrDefault("SecretKey","");

        if(!"".equals(accessKey) || !"".equals(secretKey)) {
            if (!ACCESSKEY_PATTERN.matcher(accessKey).matches()) {
                throw new MsException(ErrorNo.ACCESSKEY_NOT_MATCH, "The input ak is invalid.");
            }

            if (!SECRETKEY_PATTERN.matcher(secretKey).matches()) {
                throw new MsException(ErrorNo.SECRETKEY_NOT_MATCH, "The input sk is invalid.");
            }
        }

        //用户名称校验,用户名不能为空
        if (StringUtils.isEmpty(userName)) {
            throw new MsException(ErrorNo.USER_NAME_INPUT_ERR,
                    "createAK failed, user name input error, user_name: " + userName + ".");
        }

        AccessKeyInfoResponse accessKeyInfoResponse = new AccessKeyInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateAccessKeyMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                AccessKeyInfo accessKeyInfo = setAccessKeyInfoFromIAM(res);
                accessKeyInfo.setAccessKeySecret(res.get("sk"));
                accessKeyInfoResponse.setAccessKeyInfo(accessKeyInfo);
                logger.debug("Create accessKey successfully,ak: {}", res.get("ak"));
                return new ResponseMsg().setData(accessKeyInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.TOO_MANY_AK) {
                throw new MsException(ErrorNo.TOO_MANY_AK, "The user has over two aks.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.AK_EXISTS){
                throw new MsException(ErrorNo.AK_EXISTS, "The ak exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除用户密钥
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteAccessKey(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(IAM_USER_NAME,"");
        String accessKeyId = paramMap.getOrDefault("AccessKeyId","");
        MsAclUtils.checkIfAnonymous(accountId);

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteAccessKeyMsg(accountId, userName, accessKeyId);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete accessKey successfully,accessKeyId: {}", accessKeyId);
                return new ResponseMsg();
            }
            else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            else if (code == ErrorNo.AK_NOT_EXISTS) {
                throw new MsException(ErrorNo.AK_NOT_EXISTS, "The ak does not exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete ak failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询用户的ak列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listAccessKeys(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.getOrDefault(IAM_USER_NAME,"");
        MsAclUtils.checkIfAnonymous(accountId);

        AccessKeyInfosResponse userInfosResponse = new AccessKeyInfosResponse();
        AccessKeyInfos accessKeyInfos = new AccessKeyInfos();
        List<AccessKeyInfo> akList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListAccessKeysMsg(accountId, userName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    akList.add(setAccessKeyInfoFromIAM(re));
                }
                accessKeyInfos.setAccessKeyInfo(akList);
                userInfosResponse.setAccessKeyInfos(accessKeyInfos);
                return new ResponseMsg().setData(userInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, "The user does not exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List users failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询账户的ak列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listAccountAccessKeys(UnifiedMap<String, String> paramMap) {
        //获取url参数值

        String id = paramMap.get(USER_ID);

        String AccountName = paramMap.getOrDefault("AccountName","");

        String accountId = pool.getCommand(REDIS_USERINFO_INDEX).hget(AccountName, "id");

        if(accountId == null || accountId.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName  is not exist");
        }

        if (!id.equalsIgnoreCase(accountId) && !id.equalsIgnoreCase(DEFAULT_MGT_USER_ID)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user and not ak owner account, no permission.");
        }

        Long isExists = pool.getCommand(REDIS_USERINFO_INDEX).exists(AccountName);
        if(isExists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        AccountAccessKeyInfosResponse accountInfosResponse = new AccountAccessKeyInfosResponse();
        AccessKeyInfos accessKeyInfos = new AccessKeyInfos();
        List<AccessKeyInfo> akList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {

            SocketReqMsg msg = buildListAccountAccessKeysMsg(accountId,AccountName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for(Map<String, String> re : res){
                    akList.add(setAccountAccessKeyInfoFromIAM(re));
                }
                if(akList.size() == 0) {
                    AccessKeyInfo keyInfo = new AccessKeyInfo().setAccessKeyId("")
                            .setCreateTime("")
                            .setAccessKeySecret("");
                    akList.add(keyInfo);
                }
                accessKeyInfos.setAccessKeyInfo(akList);
                accountInfosResponse.setAccessKeyInfos(accessKeyInfos);
                return new ResponseMsg().setData(accountInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account does not exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List account ak failed.");
            }
            sleep(10000);
        }
    }

}