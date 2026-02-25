package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.worm.DefaultRetention;
import com.macrosan.message.xmlmsg.worm.ObjectLockConfiguration;
import com.macrosan.message.xmlmsg.worm.Rule;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.trash.TrashUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;

/**
 * <p></p>
 *
 * @author admin
 */
public class ObjectLockService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(ObjectLockService.class);
    private static final ObjectLockService bucketWormService = new ObjectLockService();
    public static final String VALID_OBJECT_ENABLE = "Enabled";
    public static final String VALID_OBJECT_DISABLE = "Disabled";
    public static final String VALID_RETENTION_MODE = "COMPLIANCE";
    private ObjectLockService() {
        super();
    }

    public static ObjectLockService getInstance() {
        return bucketWormService;
    }

    public ResponseMsg putObjectLockConfiguration(UnifiedMap<String,String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        MsAclUtils.checkIfAnonymous(userId);
        //只有桶的所有者可以操作
        String method = "PutObjectLockConfiguration";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        String body = paramMap.get(BODY);

        TrashUtils.checkEnvTrash(bucketName,pool);

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("body",body);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_WORM, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        // 桶要开启对象锁定功能必须先启动多版本功能
        checkVersionStatusEnable(bucketName);
        Map<String, String> map = new HashMap<>(2);
        // 校验参数合法性
        ObjectLockConfiguration objectLockConfiguration = (ObjectLockConfiguration) JaxbUtils.toObject(body);
        validateObjectLockConfigurationArgument(objectLockConfiguration,bucketName);

        String oldLockStatus = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "lock_status");
        if (StringUtils.isNotEmpty(oldLockStatus)) {
            if (VALID_OBJECT_ENABLE.equals(oldLockStatus) && VALID_OBJECT_DISABLE.equals(objectLockConfiguration.getObjectLockStatus())) {
                throw new MsException(ErrorNo.InvalidBucketState, "The bucket worm can not disabled.");
            }
        }

        String objectLockStatus = objectLockConfiguration.getObjectLockStatus();
        if (StringUtils.isEmpty(objectLockStatus) && isNotFirstPutBucketWorm(bucketName)) {
            objectLockStatus = VALID_OBJECT_ENABLE;
        }
        Integer days = objectLockConfiguration.getRule().getDefaultRetention().getDays();
        Integer years = objectLockConfiguration.getRule().getDefaultRetention().getYears();
        map.put("lock_status", objectLockStatus);
        if (days != null && days != 0) {
            map.put("lock_periodic", "day:" + days);
        } else if (years != null && years != 0) {
            map.put("lock_periodic", "year:" + years);
        }

        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                .hmset(bucketName, map);

        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_WORM);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket version error!");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getObjectLockConfiguration(UnifiedMap<String,String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        //只有桶的所有者可以操作
        String method = "GetObjectLockConfiguration";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        String status = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "lock_periodic");
        if (StringUtils.isEmpty(status)) {
            throw new MsException(ErrorNo.NO_OBJECT_LOCK_CONFIGURATION, "NoSuch object lock configuration, bucket:" + bucketName);
        }

        ObjectLockConfiguration objectLockConfiguration = new ObjectLockConfiguration();
        objectLockConfiguration.setObjectLockStatus(VALID_OBJECT_ENABLE);
        Rule rule = new Rule();
        DefaultRetention defaultRetention = new DefaultRetention();
        defaultRetention.setMode("COMPLIANCE");
        String[] periodic = status.split(":");
        if ("day".equals(periodic[0])) {
            defaultRetention.setDays(Integer.parseInt(periodic[1]));
        } else if ("year".equals(periodic[0])) {
            defaultRetention.setYears(Integer.parseInt(periodic[1]));
        }
        rule.setDefaultRetention(defaultRetention);
        objectLockConfiguration.setRule(rule);
        byte[] bytes = JaxbUtils.toByteArray(objectLockConfiguration);

        return new ResponseMsg().setData(new String(bytes))
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));

    }

    private void checkVersionStatusEnable(String bucketName) {
        String versionStatus = pool.getCommand(REDIS_BUCKETINFO_INDEX)
                .hget(bucketName, BUCKET_VERSION_STATUS);
        if (!VALID_OBJECT_ENABLE.equals(versionStatus)) {
            throw new MsException(ErrorNo.WORM_STATE_CONFLICT,
                    "Warm configuration is mutually exclusive with the multi-version function!");
        }
    }

    private boolean isNotFirstPutBucketWorm(String bucketName) {
       return pool.getCommand(REDIS_BUCKETINFO_INDEX)
                .hexists(bucketName, "lock_periodic");
    }

    private void validateObjectLockConfigurationArgument(ObjectLockConfiguration objectLockConfiguration,String bucketName) {
        if (objectLockConfiguration == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "object lock configuration parameters cannot be null.");
        }
        String objectLockStatus = objectLockConfiguration.getObjectLockStatus();
        Rule rule = objectLockConfiguration.getRule();
        if (StringUtils.isEmpty(objectLockStatus) && !isNotFirstPutBucketWorm(bucketName)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "bucket" + bucketName + "is first execute put bucket worm, 'objectLockEnabled' value must is Enabled!");
        }

        // 有效值只能为Enabled
        if (StringUtils.isNotEmpty(objectLockStatus) && !VALID_OBJECT_ENABLE.equals(objectLockStatus)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid value objectLockStatus : " + objectLockStatus);
        }

        if (rule == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "You must configure object protection rules!");
        }

        DefaultRetention defaultRetention = rule.getDefaultRetention();
        if (defaultRetention == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,"You must configure defaultRetention for you worm rules!");
        }

        String mode = defaultRetention.getMode();
        Integer days = defaultRetention.getDays();
        if (days != null && days == 0) {
            days = null;
        }
        Integer years = defaultRetention.getYears();
        if (years != null && years == 0) {
            years = null;
        }
        logger.info("days:{}, years:{},mode:{}",days,years,mode);

        if (StringUtils.isNotEmpty(mode) && !VALID_RETENTION_MODE.equals(mode)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid value mode : " + mode);
        }

        boolean dayOrYear = (days == null && years == null) || (days != null && years != null);
        if (dayOrYear) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "bucket " + bucketName
                            +",The DefaultRetention period can be either Days or Years but you must select one. You cannot specify Days and Years at the same time.");
        }


        boolean validate = (days != null && (days < 1 || days > 36500)) || (years != null && (years < 1 || years > 100));
        if (validate) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT,
                    "The protection period of buckets ranges from 1 day to 100 years");
        }
    }
}
