package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.BucketLoggingStatus;
import com.macrosan.message.xmlmsg.LoggingEnabled;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.rmi.UnmarshalException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.isSwitchOn;
import static com.macrosan.utils.regex.PatternConst.OBJECT_NAME_PATTERN;

/**
 * @author zhangzhixin
 */
@Log4j2
public class BucketLogService extends BaseService {

    private static BucketLogService instance = null;
    private BucketLogService() {
        super();
    }

    public static BucketLogService getInstance() {
        if (instance == null) {
            instance = new BucketLogService();
        }
        return instance;
    }
    

    public static boolean checkBucketLogPrefix(String str) throws PatternSyntaxException {
        Matcher m = OBJECT_NAME_PATTERN.matcher(str);
        return m.matches();
    }

    /**
     * @param paramMap
     * @return
     */
    public ResponseMsg putBucketLog(UnifiedMap<String, String> paramMap) throws javax.xml.bind.UnmarshalException {
        //获取请求体中的信息
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String body = paramMap.get(BODY);
        BucketLoggingStatus result = (BucketLoggingStatus) JaxbUtils.toObject(body);
        if (result == null) {
            log.error("put bucket {} log {}", bucketName, body);
            throw new MsException(ErrorNo.MALFORMED_XML, "The XML you provided was not well-formed or did not validate against our published schema.");
        }

        /**-----------------处理双活请求------------**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER,CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER,CLUSTER_NAME);
        paramMap.put("body",paramMap.get(BODY));
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap,MSG_TYPE_SITE_PUT_BUCKET_LOGGING,localCluster,masterCluster)){
            log.info("The slave cluster send message to master cluster");
            return new ResponseMsg().setHttpCode(SUCCESS);
        }


        MsAclUtils.checkIfAnonymous(userId);
        //只有桶的所有者可以操作
        String method = "PutBucketLogging";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        if (result.getLogEnabled() == null) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "address");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "log_acl");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "targetPrefix");

            /**---------------处理双活请求：同步至其他节点-------------**/
            int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_LOGGING);
            if (res != SUCCESS_STATUS){
                throw new MsException(res,"slave put bucket log fail");
            }

            return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
        }

        String targetBucket = null;
        String targetPrefix = null;

        if (result.getLogEnabled().getTargetBucket() != null) {
            targetBucket = ServerConfig.isBucketUpper() ? result.getLogEnabled().getTargetBucket().toLowerCase() : result.getLogEnabled().getTargetBucket();
        }
        if (result.getLogEnabled().getTargetPrefix() != null) {
            targetPrefix = result.getLogEnabled().getTargetPrefix();
        }

        if (targetBucket == null || targetBucket.isEmpty()) {

            //关闭桶日志
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "address");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "log_acl");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "targetPrefix");

            /**---------------处理双活请求：同步至其他节点-------------**/
            int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_LOGGING);
            if (res != SUCCESS_STATUS){
                throw new MsException(res,"slave put bucket log fail");
            }


            return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
        } else {
            //进行初始检验
            checkBeforeChange(bucketName, targetBucket, userId);
            if (CheckUtils.bucketFsCheck(bucketName)){
                throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketLogging");
            }
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "address", targetBucket);
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "log_acl", PERMISSION_PRIVATE);
            if (null != targetPrefix) {
                //当前缀为空时，直接将原有的前缀删除，并返回成功
                if (targetPrefix.isEmpty()) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, "targetPrefix");
                    /**---------------处理双活请求：同步至其他节点-------------**/
                    int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_LOGGING);
                    if (res != SUCCESS_STATUS){
                        throw new MsException(res,"slave put bucket log fail");
                    }
                    return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
                }
                //检测前缀是否超过512字节
                if (targetPrefix.length() > 512) {
                    throw new MsException(ErrorNo.PREFIX_LENGTH_ILLEGAL,"The prefix length is illegal");
                }
                //检测前缀中是否存在特殊字符
                if (!checkBucketLogPrefix(targetPrefix)) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT,"The prefix contains illegal characters");
                }
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "targetPrefix", targetPrefix);
            }
        }

        /**---------------处理双活请求：同步至其他节点-------------**/
        int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_LOGGING);
        if (res != SUCCESS_STATUS){
            throw new MsException(res,"slave put bucket log fail");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]).addHeader(CONTENT_LENGTH, "0");
    }


    public ResponseMsg getBucketLog(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String isFront = paramMap.get("front");
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        MsAclUtils.checkIfAnonymous(userId);
        String bucketUserId = bucketInfo.get("user_id");
        String method = "GetBucketLogging";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, false);
        BucketLoggingStatus bucketLoggingStatus = new BucketLoggingStatus();

        //检测用户是否有访问的权限
        String targetPrefix = null;
        LoggingEnabled loggingEnabled = new LoggingEnabled();
        if (bucketInfo.get("address") != null && bucketInfo.get("log_acl") == null){
            if (StringUtils.isNotEmpty(isFront)){
                throw new MsException(ErrorNo.TARGET_BUCKET_DELETED,"Target bucket deleted");
            }
        }

        String targetBucketName = bucketInfo.get("address");
        changeBucketLogCheck(userId, bucketUserId);
        targetPrefix = bucketInfo.get("targetPrefix");
        loggingEnabled.setTargetBucket(targetBucketName).setTargetPrefix(targetPrefix);
        bucketLoggingStatus.setLogEnabled(loggingEnabled);
        return new ResponseMsg().setData(bucketLoggingStatus).setHttpCode(200);
    }

    private void changeBucketLogCheck(String userId, String bucketUserId) {
        if (!userId.equals(bucketUserId)) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "no permission.user " + userId + " can not change bucket log setting.");
        }
    }


    /**
     * 是否有权限对目标桶进行写
     *
     * @param targetBucketName
     * @param userId
     */
    private void checkTargetBucket(String targetBucketName, String userId) {
        if ((targetBucketName != null) && (userId != null)) {
            Map<String,String> targetBucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(targetBucketName);
            if (targetBucketInfo.isEmpty()){
                throw new MsException(ErrorNo.TARGET_BUCKET_EXIST,"The target bucket for logging does not exist");
            }
            int aclNum = Integer.parseInt(targetBucketInfo.get(BUCKET_ACL));
            String targetBucketUserId = targetBucketInfo.get(BUCKET_USER_ID);

            if (!targetBucketUserId.equals(userId) && (aclNum & PERMISSION_SHARE_READ_WRITE_NUM) != 0 && (aclNum & PERMISSION_FULL_CON_NUM) != 0 && (aclNum & PERMISSION_WRITE_NUM) != 0){
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,"No such bucket permission!");
            }
        }
    }

    /**
     * 检测桶是否存在
     *
     * @param bucketName 桶名
     */
    private void checkBucketExist(String bucketName, String userId) {
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name:" + bucketName);
        } else if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN,
                    "no permission.user " + userId + " can not configure " + bucketName + " logging.");
        }
    }

    /**
     * 进行初始检测
     *
     * @param sourceBucket 源桶名
     * @param targetBucket 目标桶名
     * @param userId       发送请求的userID
     */
    private void checkBeforeChange(String sourceBucket, String targetBucket, String userId) {
        //检测是否为匿名账户
        MsAclUtils.checkIfAnonymous(userId);

        //检测测源桶和目标桶是否存在
        checkBucketExist(sourceBucket, userId);

        //检测目标桶是否存在和有权限写
        checkTargetBucket(targetBucket,userId);

//        //检测请求的ID是否为源桶的所有者
//        userCheck(userId, sourceBucket);

        //检测源桶和目标桶是否处于同一区域，同一个账户下
        String region = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(targetBucket, "region_name");
        if (!region.equals(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(sourceBucket, "region_name")) || !REGION.equals(region)) {
            throw new MsException(ErrorNo.NOT_IN_SAME_REGION, "source bucket and targetBucket don't in same region");
        }
        String id = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(sourceBucket, "user_id");
        if (!id.equals(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(targetBucket, "user_id"))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No permission to open bucket log");
        }

        Map<String, String> destinationBucketInfo = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hgetall(targetBucket);
        Map<String, String> sourceBucketInfo = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hgetall(sourceBucket);
        String destinationSite = destinationBucketInfo.get(CLUSTER_NAME);
        String sourceSite = sourceBucketInfo.get(CLUSTER_NAME);
        if (!isSwitchOn(destinationBucketInfo) && !isSwitchOn(sourceBucketInfo) && !destinationSite.equals(sourceSite)) {
            throw new MsException(ErrorNo.INVALID_SITE_CONSTRAINT, "The site specified by the source bucket and destination bucket is inconsistent.");
        }

        // 源源桶启用数据同步，目标桶也必须启用数据同步功能
        if (isSwitchOn(sourceBucketInfo) && !isSwitchOn(destinationBucketInfo)) {
            throw new MsException(ErrorNo.InvalidS3DestinationBucket, "InvalidS3DestinationBucket");
        }
    }

    private String getPrefix(String str){
        Pattern pattern = Pattern.compile("<TargetPrefix>(.*?)</TargetPrefix>");
        Matcher matcher = pattern.matcher(str);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

}
