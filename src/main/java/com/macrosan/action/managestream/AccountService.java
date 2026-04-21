package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.message.xmlmsg.*;
import com.macrosan.message.xmlmsg.section.AccountInfo;
import com.macrosan.message.xmlmsg.section.AccountInfos;
import com.macrosan.message.xmlmsg.section.UserInfo;
import com.macrosan.message.xmlmsg.section.UserInfos;
import com.macrosan.utils.iam.PolicyUtil;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.PatternConst;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.macrosan.constants.AccountConstants.DEFAULT_MGT_USER_ID;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.*;


/**
 * AccountService
 * 非线程安全单例模式
 *
 * @author shilinyong
 * @date 2020/03/12
 */
public class AccountService extends BaseService {

    private static final Logger logger = LogManager.getLogger(AccountService.class.getName());

    private static AccountService instance = null;

    private AccountService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static AccountService getInstance() {
        if (instance == null) {
            instance = new AccountService();
        }
        return instance;
    }

    public static AccountInfo setAccountInfoFromIAM(Map<String, String> res) {
        AccountInfo accountInfo = new AccountInfo();
        accountInfo.setDisplayName(res.get("accountName"));
        accountInfo.setAccessKey(res.get("accessKey"));
        accountInfo.setSecretKey(res.get("secretKey"));
        if (res.get("ak") != null) {
            accountInfo.setAccessKey(res.get("ak"));
        }
        if (res.get("sk") != null) {
            accountInfo.setSecretKey(res.get("sk"));
        }
        return accountInfo;
    }

    public static AccountInfo setAccountInfoFromIAMForUp(Map<String, String> res) {
        AccountInfo accountInfo = new AccountInfo();

        accountInfo.setAccountId(res.get("accountId"));
        accountInfo.setAccountName(res.get("accountName"));
        accountInfo.setAccountNickName(res.get("nickname"));
        accountInfo.setRemark(res.get("remark"));
        String createTime =  res.get("createTime");
        createTime = createTime.contains(":")?createTime: MsDateUtils.stampToSimpleDate(createTime);
        accountInfo.setCreateTime(createTime);
        accountInfo.setAccountMRN(res.get("mrn"));
        accountInfo.setCapacityQuota(res.get("capacityQuota"));
        accountInfo.setSoftCapacityQuota(res.get("softCapacityQuota"));
        accountInfo.setObjectNumQuota(res.get("objNumQuota"));
        accountInfo.setSoftObjectNumQuota(res.get("softObjNumQuota"));
        accountInfo.setUsedCapacity(res.get("usedCapacity"));
        accountInfo.setLeftCapacity(res.get("leftCapacity"));
        accountInfo.setUsedRate(res.get("usedRate"));
        accountInfo.setThroughPut(res.get("throughPut"));
        accountInfo.setStorageStrategy(res.get("storageStrategy"));
        accountInfo.setUsedObjNum(res.get("usedObjNum"));
        accountInfo.setUsedObjNumRate(res.get("usedObjNumRate"));
        accountInfo.setLeftObjNum(res.get("leftObjNum"));
        if("0".equals(res.get("throughPut"))){
            accountInfo.setThroughPut("不限额");
        }
        accountInfo.setBandWidth(res.get("bandWidth"));
        if("0".equals(res.get("bandWidth"))){
            accountInfo.setBandWidth("不限额");
        }
        return accountInfo;
    }

  /**
   * 修改账户名
   * @param paramMap
   * @return
   */
//    public ResponseMsg updateAccountName(UnifiedMap<String, String> paramMap){
//        String _id = paramMap.get(USER_ID);
//        String accountName = paramMap.getOrDefault("AccountName", "");
//        String newAccountName = paramMap.getOrDefault("NewAccountName", "");
//
//        MsAclUtils.checkIfManageAccount(_id);
//
//        //账户名称校验
//        if (StringUtils.isEmpty(newAccountName) || !ACCOUNT_NAME_PATTERN.matcher(newAccountName).matches()) {
//            throw new MsException(ErrorNo.ACCOUNT_NAME_INPUT_ERROR,
//                    "editAccountName failed, new account name input error, new_account_name: " + newAccountName + ".");
//        }
//
//
//        for (int tryTime = 10; ; tryTime -= 1) {
//            SocketReqMsg msg = buildEditAccountName(accountName, newAccountName);
//            addSyncFlagParam(msg, paramMap);
//            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
//            Map<String, String> res = resMsg.getData();
//            int code = resMsg.getCode();
//            if (code == ErrorNo.SUCCESS_STATUS){
//              logger.debug("reset account name successfully,accountName: {}, newAccountName: {}", accountName, newAccountName);
//              return new ResponseMsg(code);
//            }else if(code == ErrorNo.ACCOUNT_EXISTED){
//              throw new MsException(ErrorNo.ACCOUNT_EXISTED, "new account already exits.");
//            }else if(code == ErrorNo.MOSS_FAIL){
//              throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
//            }else if(code == ErrorNo.NO_SUCH_ACCOUNT){
//                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account");
//            }
//
//          if (tryTime == 0) {
//            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create account failed.");
//          }
//          sleep(10000);
//        }
//    }

    /**
     * 创建账户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createAccount(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        logger.info(paramMap);
        String _id = paramMap.get(USER_ID);
        String accountName = paramMap.get("AccountName");
        String passWord = paramMap.get("Password");
        String accountNickName = paramMap.getOrDefault("AccountNickName", "");
        String remark = paramMap.getOrDefault("Remark", "");
        String validityTime = paramMap.getOrDefault("ValidityTime","0");
        String validityGrade = paramMap.getOrDefault("ValidityGrade","0");
        if ("".equals(validityGrade)) {
            validityGrade = "0";
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
        } else {
            paramMap.put("EndTimeSecond","0");
            paramMap.put("ValidityTime","0");
        }

        if(!validityGrade.equals("0") && !validityGrade.equals("1")){
            throw new MsException(ErrorNo.ACCOUNT_VALIDITY_GRADE_ERROR,
                    "The validityGrade is illegal");
        }
        paramMap.put("ValidityGrade", validityGrade);

        MsAclUtils.checkIfManageAccount(_id);

        //账户名称校验
        if (StringUtils.isEmpty(accountName) || !ACCOUNT_NAME_PATTERN.matcher(accountName).matches()) {
            throw new MsException(ErrorNo.ACCOUNT_NAME_INPUT_ERROR,
                    "createAccount failed, account name input error, account_name: " + accountName + ".");
        }
        //账户密码校验
        if (StringUtils.isEmpty(passWord) || !USER_PASSWORD_PATTERN.matcher(passWord).matches()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "createAccount failed, invalid password, account_password: " + passWord + ".");
        }
        //账户昵称长度不能大于32
        if (accountNickName.length() > 32) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 32.");
        }
        //备注长度不能大于128
        if (remark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }
        if (!checkStorageStrategy(paramMap)){
            throw new MsException(ErrorNo.NO_SUCH_STORAGE_STRATEGY,
                    "The specified storage strategy does not exist.");
        }

        CreateAccountResponse createAccountResponse = new CreateAccountResponse();
        for (int tryTime = 3; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateAccountMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();
            if (code == ErrorNo.SUCCESS_STATUS) {
                createAccountResponse.setAccountInfo(setAccountInfoFromIAM(res).setDisplayName(accountName));
                logger.debug("Create account successfully,accountName: {}", accountName);
                return new ResponseMsg().setData(createAccountResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.TOO_MANY_ACCOUNT) {
                throw new MsException(ErrorNo.TOO_MANY_ACCOUNT, "The account number is over 5000.");
            } else if (code == ErrorNo.ACCOUNT_EXISTED) {
                if (res.containsKey("idExist") && ("1".equals(paramMap.get(SITE_FLAG_UPPER)) || "1".equals(paramMap.get(REGION_FLAG_UPPER)))) {
                    throw new MsException(ErrorNo.ACCOUNT_ID_EXISTED, "The account id already exits.");
                }
                throw new MsException(ErrorNo.ACCOUNT_EXISTED, "The account already exits.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.INCONSISTENT_STORAGE_STRATEGY) {
                throw new MsException(ErrorNo.INCONSISTENT_STORAGE_STRATEGY, "The account storage strategy inconsistent.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create account failed.");
            }
            sleep(10000);
        }
    }

    public static boolean checkStorageStrategy(UnifiedMap<String, String> paramMap){
        boolean res = false;
        if(paramMap.containsKey("StorageStrategy")&& !paramMap.get("StorageStrategy").isEmpty()){
            String storageStrategy = paramMap.get("StorageStrategy");
            Long exists = pool.getCommand(REDIS_POOL_INDEX).exists(storageStrategy);
            if (exists != 0){
                res = true;
            }
        }else{
            List<String> strategys = pool.getCommand(REDIS_POOL_INDEX).keys("strategy_*");
            for (String strategy : strategys) {
                String is_default = pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "is_default");
                if("true".equals(is_default)){
                    paramMap.put("StorageStrategy",strategy);
                    res = true;
                }
            }
        }
        return res;
    }

    /**
     * 删除账户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteAccount(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String _id = paramMap.get(USER_ID);
        String accountId = paramMap.getOrDefault("AccountId","");
        String accountName = paramMap.getOrDefault("AccountName","");


        MsAclUtils.checkIfManageAccount(_id);

        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteAccountMsg(accountId,accountName);
            addSyncFlagParam(msg, paramMap);
            logger.info("msg:" + msg);
            logger.info("param: " + paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete account successfully,accountId: {}", accountId);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("del_user_set", accountName);
                return new ResponseMsg();
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
            } else if (code == ErrorNo.ACCOUNT_HAS_USERS_ERROR) {
                throw new MsException(ErrorNo.ACCOUNT_HAS_USERS_ERROR, "The account has users.");
            } else if (code == ErrorNo.ACCOUNT_HAS_GROUPS_ERROR) {
                throw new MsException(ErrorNo.ACCOUNT_HAS_GROUPS_ERROR, "The account has groups.");
            } else if (code == ErrorNo.ACCOUNT_HAS_POLICIES_ERROR) {
                throw new MsException(ErrorNo.ACCOUNT_HAS_POLICIES_ERROR, "The account has policies.");
            } else if (code == ErrorNo.ACCOUNT_HAD_BUCKET) {
                throw new MsException(ErrorNo.ACCOUNT_HAD_BUCKET, "MOSS had buckets.");
            } else if (code == ErrorNo.ACCOUNT_HAS_ROLES_ERROR) {
                throw new MsException(ErrorNo.ACCOUNT_HAS_ROLES_ERROR, "The account has roles.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            }

            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete account failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 更新账户
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg updateAccount(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        logger.debug("updateAccount {}", paramMap);
        String accountName = paramMap.get("AccountName");
        String newPassWord = paramMap.getOrDefault("NewPassWord","");
        String newAccountNickName = paramMap.getOrDefault("NewAccountNickName","");
        String newRemark = paramMap.getOrDefault("NewRemark","");
        String capacityQuota = paramMap.getOrDefault("CapacityQuota","");
        String softCapacityQuota = paramMap.getOrDefault("SoftCapacityQuota", "");
        String objNumQuota = paramMap.getOrDefault("ObjNumQuota", "");
        String softObjNumQuota = paramMap.getOrDefault("SoftObjNumQuota", "");
        String perfQuotaType = paramMap.getOrDefault("PerfQuotaType","");
        String perfQuotaValue = paramMap.getOrDefault("PerfQuotaValue","");
        String validityTime = paramMap.getOrDefault("ValidityTime","");
        String validityGrade = paramMap.getOrDefault("ValidityGrade","0");
        String EndTimeSecond = paramMap.getOrDefault("EndTimeSecond","0");
        String accountUid = paramMap.getOrDefault("AccountUid","");
        String accountGid = paramMap.getOrDefault("AccountGid","");
        String accountGids = paramMap.getOrDefault("AccountGids","");
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

        if (paramMap.containsKey(SITE_FLAG_UPPER) || paramMap.containsKey(REGION_FLAG_UPPER)) {
            if (StringUtils.isNotEmpty(EndTimeSecond)) {
                paramMap.put("EndTimeSecond", EndTimeSecond);
            }
        }

        paramMap.put("ValidityGrade", validityGrade);
        String nowStrategy = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "storage_strategy");
        String storageStrategy = paramMap.getOrDefault("StorageStrategy", nowStrategy);



        String _id = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(_id);

        //账户名称校验
        if (StringUtils.isEmpty(accountName) || !ACCOUNT_NAME_PATTERN.matcher(accountName).matches()) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        Long exists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if (exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        //账户新密码校验
        if(StringUtils.isNotEmpty(newPassWord)) {
            String oldPassWd = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName,"passwd");
            if (!newPassWord.equals(oldPassWd) &&!USER_PASSWORD_PATTERN.matcher(newPassWord).matches()){
                throw new MsException(ErrorNo.INVAILD_PASSWD,
                        "updateAccount failed, invalid password, account_password: " + newPassWord + ".");
            }
        }
        String passWordExists = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName,"passwd");
        Long other_region = pool.getCommand(REDIS_TOKEN_INDEX).exists("other_regions");
        Long master_cluster = pool.getCommand(REDIS_TOKEN_INDEX).exists("master_cluster");
        if (passWordExists.equals(newPassWord) && other_region != 1 && master_cluster != 1){
            throw new MsException(ErrorNo.PASSWORD_INPUT_ERR, "Password is exists");
        }
        //账户昵称和备注校验
        if (newAccountNickName.length() > 32 || newRemark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }
        //容量配额值校验
        if (!capacityQuota.isEmpty()) {
            if (!PatternConst.CAPACITY_QUOTA_PATTERN.matcher(capacityQuota).matches() || capacityQuota.startsWith("0") && capacityQuota.length() > 1) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "capacity quota value must be in [0,9223372036854775807]，current value is:" + capacityQuota);
            }
            try {
                Long.parseLong(capacityQuota);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "capacity quota value must be in [0,9223372036854775807]，current value is:" + capacityQuota);
            }
        }

        if (!softCapacityQuota.isEmpty()) {
            if (!PatternConst.CAPACITY_QUOTA_PATTERN.matcher(softCapacityQuota).matches() || softCapacityQuota.startsWith("0") && softCapacityQuota.length() > 1) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "capacity quota value must be in [0,9223372036854775807]，current value is:" + capacityQuota);
            }
            try {
                Long.parseLong(softCapacityQuota);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "soft capacity quota value must be in [0,9223372036854775807]，current value is:" + softCapacityQuota);
            }
        }

        if (!capacityQuota.isEmpty() && !softCapacityQuota.isEmpty() && !"0".equals(capacityQuota) && Long.parseLong(capacityQuota) <= Long.parseLong(softCapacityQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "soft capacity quota must less than hard quota.");
        }

        if (StringUtils.isNotEmpty(softCapacityQuota) && StringUtils.isEmpty(capacityQuota)) {
            String account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "account_quota")).orElse("0");
            if (!"0".equals(account_quota) && Long.parseLong(softCapacityQuota) >= Long.parseLong(account_quota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(capacityQuota) && StringUtils.isEmpty(softCapacityQuota)) {
            String soft_account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_quota")).orElse("0");
            if (!"0".equals(capacityQuota) && Long.parseLong(soft_account_quota) >= Long.parseLong(capacityQuota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        // 对象数配额检验
        if (!objNumQuota.isEmpty()) {
            if (!PatternConst.CAPACITY_QUOTA_PATTERN.matcher(objNumQuota).matches() || objNumQuota.startsWith("0") && objNumQuota.length() > 1) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "objnum quota value must be in [0,9223372036854775807]，current value is:" + objNumQuota);
            }
            try {
                Long.parseLong(objNumQuota);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "objnum quota value must be in [0,9223372036854775807]，current value is:" + objNumQuota);
            }
        }

        if (!softObjNumQuota.isEmpty()) {
            if (!PatternConst.CAPACITY_QUOTA_PATTERN.matcher(softObjNumQuota).matches() || softObjNumQuota.startsWith("0") && softObjNumQuota.length() > 1) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "soft objnum quota value must be in [0,9223372036854775807]，current value is:" + softObjNumQuota);
            }
            try {
                Long.parseLong(softObjNumQuota);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "soft objnum quota value must be in [0,9223372036854775807]，current value is:" + softObjNumQuota);
            }
        }

        if (!objNumQuota.isEmpty() && !softObjNumQuota.isEmpty() && !"0".equals(objNumQuota) && Long.parseLong(objNumQuota) <= Long.parseLong(softObjNumQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "soft objnum quota must less than hard quota.");
        }

        if (StringUtils.isNotEmpty(softObjNumQuota) && StringUtils.isEmpty(objNumQuota)) {
            String account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "account_objnum")).orElse("0");
            if (!"0".equals(account_quota) && Long.parseLong(softObjNumQuota) >= Long.parseLong(account_quota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft objnum quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(objNumQuota) && StringUtils.isEmpty(softObjNumQuota)) {
            String soft_account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_objnum")).orElse("0");
            if (!"0".equals(objNumQuota) && Long.parseLong(soft_account_quota) >= Long.parseLong(objNumQuota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft objnum quota must less than hard quota.");
            }
        }

        //性能配额类型校验
        if (!perfQuotaType.isEmpty()) {
            if (!perfQuotaType.equals("1") && !perfQuotaType.equals("2"))
                throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such perf type.");
        }
        //性能配额值校验
        if (perfQuotaType.equals("1") || perfQuotaType.equals("2")) {
            if (!perfQuotaValue.isEmpty() && !PatternConst.PERF_QUOTA_PATTERN.matcher(perfQuotaValue).matches()) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, perfQuotaType + " quota value must be in [0,9999999999999]，current value is:" + perfQuotaValue);
            }
        }
        //存储策略校验
        List<String> strategyList = pool.getCommand(REDIS_POOL_INDEX).keys("strategy_*");
        if(!strategyList.contains(storageStrategy)){
            throw new MsException(ErrorNo.NO_SUCH_STORAGE_STRATEGY,
                    "The specified storage strategy does not exist.");
        }

        //文件id校验
        if (StringUtils.isNotEmpty(accountUid)) {
            validateUidOrGid("accountUid", accountUid);
        }
        if (StringUtils.isNotEmpty(accountGid)) {
            validateUidOrGid("accountGid", accountGid);
        }
        if (StringUtils.isNotEmpty(accountGids)) {
            String[] accountGidsArray = accountGids.split(",");
            for (String accountGidFromArray : accountGidsArray) {
                validateUidOrGid("accountGids", accountGidFromArray);
            }
        }

        AccountInfoResponse accountInfoResponse = new AccountInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateAccountMsg(paramMap);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                accountInfoResponse.setAccountInfo(setAccountInfoFromIAMForUp(res));
                logger.debug("Update account successfully,accountName: {}", accountName);
                return new ResponseMsg().setData(accountInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.INCONSISTENT_STORAGE_STRATEGY) {
                throw new MsException(ErrorNo.INCONSISTENT_STORAGE_STRATEGY, "The account storage strategy inconsistent.");
            } else if (code == ErrorNo.ACCESS_FORBIDDEN) {
                throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "only read ak.");
            } else if (code == ErrorNo.ACCOUNT_UID_EXIST) {
                throw new MsException(ErrorNo.ACCOUNT_UID_EXIST, "Account uid already exist.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update account failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询账户列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listAccounts(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(accountId);

        AccountInfosResponse accountInfosResponse = new AccountInfosResponse();
        AccountInfos accountInfos = new AccountInfos();
        List<AccountInfo> accountList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListAccountsMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    accountList.add(setAccountInfoFromIAMForUp(re));
                }
                accountInfos.setAccountInfo(accountList);
                accountInfosResponse.setAccountInfos(accountInfos);
                logger.debug("List account successfully.");
                return new ResponseMsg().setData(accountInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List account failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 重置账户密码
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg resetAccountPasswd(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountName = paramMap.getOrDefault("AccountName","");
        String newPassWord = paramMap.getOrDefault("NewPassword", "");
        String oldPassWord = paramMap.getOrDefault("OldPassword", "");
        String validityTime = paramMap.getOrDefault("ValidityTime","");
        String validityGrade = paramMap.getOrDefault("ValidityGrade", "0");
        if ("".equals(validityGrade)) {
            validityGrade = "0";
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
        if(!validityGrade.equals("0") && !validityGrade.equals("1")){
            throw new MsException(ErrorNo.ACCOUNT_VALIDITY_GRADE_ERROR,
                    "The validityGrade is illegal");
        }
        paramMap.put("ValidityGrade", validityGrade);
        Long exists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if (exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        String _id = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(_id);

        //账户新密码校验
        if (StringUtils.isEmpty(newPassWord) || !USER_PASSWORD_PATTERN.matcher(newPassWord).matches()) {
            throw new MsException(ErrorNo.INVAILD_PASSWD,
                    "reset password failed, invalid password, account_password: " + newPassWord + ".");
        }
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateAccountPasswdMsg(paramMap);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("reset account password successfully,accountName: {}", accountName);
                return new ResponseMsg(code);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if (code == ErrorNo.INVAILD_PASSWD) {
                throw new MsException(ErrorNo.INVAILD_PASSWD, "The password input is invaild.");
            } else if (code == ErrorNo.ACCOUNT_PASSWORD_ERROR) {
                throw new MsException(ErrorNo.ACCOUNT_PASSWORD_ERROR, "The oldPassword input is error.");
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
//    public ResponseMsg listAccounts(UnifiedMap<String, String> paramMap) {
//        //获取url参数值
//        String accountId = paramMap.get(USER_ID);
//
//        AccountInfosResponse accountInfosResponse = new AccountInfosResponse();
//        AccountInfos accountInfos = new AccountInfos();
//        List<AccountInfo> accountList = new ArrayList<>();
//        for (int tryTime = 10; ; tryTime -= 1) {
//            SocketReqMsg msg = buildListAccountsMsg(accountId);
//            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, false);
//            List<Map<String, String>> res = resMsg.getData();
//            int code = resMsg.getCode();
//
//            if (code == ErrorNo.SUCCESS_STATUS) {
//                for (Map<String, String> re : res) {
//                    accountList.add(setAccountInfoFromIAM(re));
//                }
//                accountInfos.setAccountInfo(accountList);
//                accountInfosResponse.setAccountInfos(accountInfos);
//                logger.debug("List accounts successfully.");
//                return new ResponseMsg().setData(accountInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
//            }
//            if (tryTime == 0) {
//                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List accounts failed.");
//            }
//            sleep(10000);
//        }
//    }

    /**
     * 查询特定用户信息
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
//    public ResponseMsg getAccount(UnifiedMap<String, String> paramMap) {
//        //获取url参数值
//        String accountId = paramMap.get(USER_ID);
//        String accountName = paramMap.get(IAM_USER_NAME);
//
//        AccountInfoResponse accountInfoResponse = new AccountInfoResponse();
//        for (int tryTime = 10; ; tryTime -= 1) {
//            SocketReqMsg msg = buildGetAccountMsg(accountId,accountName);
//            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, false);
//            Map<String, String> res = resMsg.getData();
//            int code = resMsg.getCode();
//
//            if (code == ErrorNo.SUCCESS_STATUS) {
//                accountInfoResponse.setAccountInfo(setAccountInfoFromIAM(res));
//                logger.debug("Get account successfully.");
//                return new ResponseMsg().setData(accountInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
//            }
//            else if (code == ErrorNo.USER_NOT_EXISTS) {
//                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
//            }
//            if (tryTime == 0) {
//                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get account failed.");
//            }
//            sleep(10000);
//        }
//    }


    /**
     * 更新账户容量配额
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg putCapacityQuota(UnifiedMap<String, String> paramMap) {
        String accountName = paramMap.getOrDefault("AccountName","");
        String capacityQuota = paramMap.get("CapacityQuota");
        String softCapacityQuota = paramMap.get("SoftCapacityQuota");

        String id = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(id);

        if (checkObjectsAuth(id, accountName)) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "The account no such permission.");
        }

        if(StringUtils.isEmpty(accountName)) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        Long isExists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if(isExists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        if (StringUtils.isEmpty(capacityQuota) && StringUtils.isEmpty(softCapacityQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "quota and soft quota choose at least one of the two.");
        }

        if (StringUtils.isNotEmpty(capacityQuota)) {
            checkAccountCapQuota(capacityQuota);
        }

        if (StringUtils.isNotEmpty(softCapacityQuota)) {
            checkAccountCapQuota(softCapacityQuota);
        }

        if (StringUtils.isNotEmpty(capacityQuota) && StringUtils.isNotEmpty(softCapacityQuota)
                && !"0".equals(capacityQuota)
                && Long.parseLong(softCapacityQuota) >= Long.parseLong(capacityQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
        }

        if (StringUtils.isNotEmpty(softCapacityQuota) && StringUtils.isEmpty(capacityQuota)) {
            String account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "account_quota")).orElse("0");
            if (!"0".equals(account_quota) && Long.parseLong(softCapacityQuota) >= Long.parseLong(account_quota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(capacityQuota) && StringUtils.isEmpty(softCapacityQuota)) {
            String soft_account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_quota")).orElse("0");
            if (!"0".equals(capacityQuota) && Long.parseLong(soft_account_quota) >= Long.parseLong(capacityQuota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(capacityQuota)) {
            capacityQuota = String.valueOf(Long.parseLong(capacityQuota));
            paramMap.put("CapacityQuota", capacityQuota);
        }

        if (StringUtils.isNotEmpty(softCapacityQuota)) {
            softCapacityQuota = String.valueOf(Long.parseLong(softCapacityQuota));
            paramMap.put("SoftCapacityQuota", softCapacityQuota);
        }

        SocketReqMsg msg = buildUpdateAccountCapacityQuota(paramMap);
        MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
        Map<String, String> res = resMsg.getData();
        int code = resMsg.getCode();

        if (code == ErrorNo.NO_SUCH_ACCOUNT) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }
        else if (code == ErrorNo.MOSS_FAIL || code == -1){
            throw new MsException(ErrorNo.MOSS_FAIL, "MOSS update accountCapacityQuota  failed.");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 获取账户容量配额
     */
    public ResponseMsg getCapacityQuota(UnifiedMap<String, String> paramMap) {
        String id = paramMap.get(USER_ID);
        String accountName = paramMap.get("AccountName");

        if (checkObjectsAuth(id, accountName)) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "The account no such permission.");
        }

        if(StringUtils.isEmpty(accountName)) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        Long exists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if(exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        String accountQuota = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "account_quota");
        if(accountQuota == null) {
            accountQuota = "0";
        }
        String accountSoftQuota = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_quota");
        if (accountSoftQuota == null) {
            accountSoftQuota = "0";
        }
        accountQuota = String.valueOf(Long.parseLong(accountQuota));
        accountSoftQuota = String.valueOf(Long.parseLong(accountSoftQuota));
        AccountInfo accountInfo = new AccountInfo();
        accountInfo.setDisplayName(accountName);
        accountInfo.setCapacityQuota(accountQuota);
        accountInfo.setSoftCapacityQuota(accountSoftQuota);
        AccountCapacityQuotaResponse response = new AccountCapacityQuotaResponse();
        response.setAccountInfo(accountInfo);
        return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    /**
     * 更新账户对象数的配额
     * @param paramMap
     * @return
     */
    public ResponseMsg putAccountObjectsQuota(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        String accountName = paramMap.getOrDefault("AccountName", "");
        String objectNumQuota = paramMap.get("ObjectNumQuota");
        String softObjectNumQuota = paramMap.get("SoftObjectNumQuota");
        String id = paramMap.get(USER_ID);

        //判断当前用户是否拥有该接口的操作权限
        if (checkObjectsAuth(id, accountName)) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "The account no such permission.");
        }

        if(StringUtils.isEmpty(accountName)) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        Long isExists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if (isExists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        if (StringUtils.isEmpty(objectNumQuota) && StringUtils.isEmpty(softObjectNumQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "quota and soft quota choose at least one of the two.");
        }
        if (StringUtils.isNotEmpty(objectNumQuota)) {
            checkAccountCapQuota(objectNumQuota);
        }
        if (StringUtils.isNotEmpty(softObjectNumQuota)) {
            checkAccountCapQuota(softObjectNumQuota);
        }
        if (StringUtils.isNotEmpty(objectNumQuota) && StringUtils.isNotEmpty(softObjectNumQuota)
                && !"0".equals(objectNumQuota)
                && Long.parseLong(softObjectNumQuota) >= Long.parseLong(objectNumQuota)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
        }

        if (StringUtils.isNotEmpty(softObjectNumQuota) && StringUtils.isEmpty(objectNumQuota)) {
            String account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "account_objnum")).orElse("0");
            if (!"0".equals(account_quota) && Long.parseLong(softObjectNumQuota) >= Long.parseLong(account_quota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(objectNumQuota) && StringUtils.isEmpty(softObjectNumQuota)) {
            String soft_account_quota = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_objnum")).orElse("0");
            if (!"0".equals(objectNumQuota) && Long.parseLong(soft_account_quota) >= Long.parseLong(objectNumQuota)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "account soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(objectNumQuota)) {
            objectNumQuota = String.valueOf(Long.parseLong(objectNumQuota));
            paramMap.put("ObjnumQuota", objectNumQuota);
        }

        if (StringUtils.isNotEmpty(softObjectNumQuota)) {
            softObjectNumQuota = String.valueOf(Long.parseLong(softObjectNumQuota));
            paramMap.put("SoftObjNumQuota", softObjectNumQuota);
        }

        SocketReqMsg msg = buildUpdateAccountObjectsQuota(paramMap);
        MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
        Map<String, String> res = resMsg.getData();
        int code = resMsg.getCode();

        if (code == ErrorNo.NO_SUCH_ACCOUNT) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        } else if (code == ErrorNo.MOSS_FAIL || code == -1) {
            throw new MsException(ErrorNo.MOSS_FAIL, "MOSS update accountObjectsQuota failed.");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 判断当前用户是否为管理用户或者 当前用户是否在操作用户本身
     * @param userId
     * @param accountName
     */
    public static boolean checkObjectsAuth(String userId, String accountName) {
        String currentAccount = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, "name");
        return  !userId.equalsIgnoreCase(DEFAULT_MGT_USER_ID)
                && (StringUtils.isEmpty(currentAccount) || !currentAccount.equals(accountName));
    }

    /**
     * 获取账户对象数的配额
     * @param paramMap
     * @return
     */
    public ResponseMsg getAccountObjectsQuota(UnifiedMap<String, String> paramMap) {
        String id = paramMap.get(USER_ID);
        String accountName = paramMap.get("AccountName");

        if (checkObjectsAuth(id, accountName)) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "The account no such permission.");
        }

        if (StringUtils.isEmpty(accountName)) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }
        long exists = pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName);
        if (exists == 0) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
        }

        String accountObjectNum = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName,"account_objnum");
        String accountSoftObjectNum = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "soft_account_objnum");
        if (accountObjectNum == null) {
            accountObjectNum = "0";
        }
        if (accountSoftObjectNum == null) {
            accountSoftObjectNum = "0";
        }

        AccountInfo accountInfo = new AccountInfo();
        accountInfo.setDisplayName(accountName);
        accountInfo.setObjectNumQuota(accountObjectNum);
        accountInfo.setSoftObjectNumQuota(accountSoftObjectNum);
        AccountObjectsQuotaResponse response = new AccountObjectsQuotaResponse();
        response.setAccountInfo(accountInfo);
        return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    /**
     * 查询特定用户信息
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg getAccountInfo(UnifiedMap<String, String> paramMap) {
        //获取url参数值

            String _id = paramMap.get(USER_ID);
            MsAclUtils.checkIfManageAccount(_id);

            String accountName = paramMap.get("AccountName");
            if(StringUtils.isEmpty(accountName)) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "AccountName is not exist");
            }


            AccountInfoResponse accountInfoResponse = new AccountInfoResponse();

            SocketReqMsg msg = null;
            msg = buildGetAccountInfo(accountName);
            MapResMsg resMsg = null;

            resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);

            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account doesn't exist.");
            }
            accountInfoResponse.setAccountInfo(getAccountInfoFromIAM(res));
            logger.info("Get account successfully.");
            return new ResponseMsg().setData(accountInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);

    }

    private AccountInfo getAccountInfoFromIAM(Map<String, String> res) {
        logger.info(res);
        AccountInfo accountInfo = new AccountInfo();
        accountInfo.setAccountId(res.get("accountId"));
        accountInfo.setDisplayName(res.get("accountName"));
        accountInfo.setStorageStrategy(res.get("storageStrategy"));

        String nickname = res.getOrDefault("nickname", "");
        if("\"null\"".equals(nickname)) {
            nickname = nickname.replaceAll("\"","");
        }
        accountInfo.setAccountNickName(nickname);

        String remark = res.getOrDefault("remark", "");
        if("\"null\"".equals(remark)) {
            remark = remark.replaceAll("\"","");
        }
        accountInfo.setRemark(remark);
        return accountInfo;
    }

    private static void checkAccountCapQuota(String quota) {
        if (StringUtils.isBlank(quota) || !CAPACITY_QUOTA_PATTERN.matcher(quota).matches() || (quota.startsWith("0") && quota.length() > 1)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "quota is invalid");
        } else {
            try {
                Long.parseLong(quota);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "quota is invalid");
            }
        }
    }

    private void validateUidOrGid(String paramName, String paramValue) {
        try {
            int value = Integer.parseInt(paramValue);
            if (value < 0 || value > 65534) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT,
                        paramName + " must be an integer between 0 and 65534, actual value: " + value);
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    paramName + " must be a valid integer, actual value: " + paramValue);
        }
    }
}