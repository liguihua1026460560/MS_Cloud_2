package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSONArray;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.archive.ArchieveUtils;
import com.macrosan.doubleActive.archive.ArchiveAnalyzer;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.lifecycle.LifecycleConfiguration;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.lifecycle.LifecycleValidation;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;

public class BucketLifecycleService extends BaseService {
    private static final Logger logger = LogManager.getLogger(BucketLifecycleService.class.getName());
    public static final String BUCKET_LIFECYCLE_RULES = "bucket_lifecycle_rules";
    public static final String BUCKET_BACKUP_RULES = "bucket_backup_rules";
    private static BucketLifecycleService bucketLifecycleService = new BucketLifecycleService();

    public static BucketLifecycleService getInstance() {
        return bucketLifecycleService;
    }

    private BucketLifecycleService() {
        super();
    }

    /**
     * 设置桶生命周期
     *
     * @param paramMap 请求参数
     * @return 相应的返回值
     */
    public ResponseMsg putBucketLifecycle(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId, bucketName);
        DoubleActiveUtil.noSyncSiteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        //只有桶的所有者可以操作
        String method = "PutBucketLifecycle";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        //xml解析验证合法性
        String body = paramMap.get(BODY);
        logger.info(body);
        LifecycleConfiguration lifecycleConfig = (LifecycleConfiguration) JaxbUtils.toObject(body);
        LifecycleValidation.validateLifecycleConfig(body, lifecycleConfig, false);
        paramMap.put("body", body);
        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
        if ("0".equals(syncPolicy)) {
            if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_LIFECYCLE, localCluster, masterCluster)) {
                return new ResponseMsg().setHttpCode(SUCCESS);
            }
        }
        BucketLifecycleService.deleteLifecycleRecord(bucketName);
        String lifecycleConfigString = new String(JaxbUtils.toByteArray(lifecycleConfig));
        pool.getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).hset(BUCKET_LIFECYCLE_RULES, bucketName, lifecycleConfigString);
        HashSet transitionStrategySet = LifecycleValidation.getTransitionStrategys(lifecycleConfig);
        if (!transitionStrategySet.isEmpty()) {
            String oldTransitionStrategy = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "transition_strategy");
            if (oldTransitionStrategy != null) {
                transitionStrategySet.addAll(JSONArray.parseArray(oldTransitionStrategy));
            }
            JSONArray transitionStrategyArray = new JSONArray();
            transitionStrategySet.forEach(transitionStrategy -> transitionStrategyArray.add(transitionStrategy));
            pool.getShortMasterCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hset(bucketName, "transition_strategy", transitionStrategyArray.toString());
        }
//        logger.info("主站点开始发往从站点 : " + JSONObject.toJSONString(paramMap));
        if ("0".equals(syncPolicy)) {
            paramMap.put("noAsync", "1");
            int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_LIFECYCLE);
            if (res != SUCCESS_STATUS) {
                throw new MsException(res, "slave put bucket version error!");
            }
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 获取bucket权限
     *
     * @param paramMap 请求参数
     * @return 相应的xml
     */
    public ResponseMsg getBucketLifecycle(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.noSyncSiteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId, bucketName);
        //只有桶的所有者可以操作
        String method = "GetBucketLifecycle";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String lifecycleConfig = getLifecycleConfig(bucketName);
        if (lifecycleConfig == null) {
            throw new MsException(ErrorNo.NO_LIFECYCLE_CONFIGURATION, "NoSuchLifecycleConfiguration bucket:" + bucketName);
        } else {
            return new ResponseMsg().setData(lifecycleConfig).addHeader(CONTENT_TYPE, "application/xml")
                    .addHeader(CONTENT_LENGTH, String.valueOf(lifecycleConfig.getBytes().length));
        }
    }

    /**
     * 获取bucket权限
     *
     * @param paramMap 请求参数
     * @return 相应的xml
     */
    public ResponseMsg deleteBucketLifecycle(UnifiedMap<String, String> paramMap) {
        logger.info(paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.noSyncSiteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId, bucketName);
        //只有桶的所有者可以操作
        String method = "DeleteBucketLifecycle";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
        if ("0".equals(syncPolicy)) {
            if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_LIFECYCLE, localCluster, masterCluster)) {
                return new ResponseMsg().setHttpCode(DEL_SUCCESS);
            }
        }

        deleteLifecycleConfig(bucketName);
        deleteLifecycleRecord(bucketName);

        if ("0".equals(syncPolicy)) {
            /**主站点执行多区域转发**/
            paramMap.put("noAsync", "1");
            int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_LIFECYCLE);
            if (res != SUCCESS_STATUS) {
                throw new MsException(res, "slave put bucket version error!");
            }
        }

        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    public ResponseMsg putBucketBackup(UnifiedMap<String, String> paramMap) {
//        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

//        MsAclUtils.checkIfAnonymous(userId);
//        userCheck(userId, bucketName);
        final boolean siteFlag = paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase());
        DoubleActiveUtil.siteConstraintCheck(bucketName, siteFlag);

        //只有桶的所有者可以操作
//        if (bucketFsCheck(bucketName)) {
//            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not create backup");
//        }
        //xml解析验证合法性
        String body = paramMap.get(BODY);
        logger.info(body);
        LifecycleConfiguration lifecycleConfig = (LifecycleConfiguration) JaxbUtils.toObject(body);
        if (!siteFlag) {
            LifecycleValidation.validateLifecycleConfig(body, lifecycleConfig, true);
            /*从站点请求主站点执行*/
            String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
            String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
            if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BACK_UP, localCluster, masterCluster)) {
                return new ResponseMsg().setHttpCode(SUCCESS);
            }
        }
        paramMap.put("body", body);
        BucketLifecycleService.deleteBackupConfig(bucketName);
        BucketLifecycleService.deleteBucketArchiveCount(bucketName, lifecycleConfig);
        String lifecycleConfigString = new String(JaxbUtils.toByteArray(lifecycleConfig));
        pool.getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).hset(BUCKET_BACKUP_RULES, bucketName, lifecycleConfigString);
//        logger.info("主站点开始发往从站点 : " + JSONObject.toJSONString(paramMap));
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_BACKUP);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket backup error!");
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getBucketBackup(UnifiedMap<String, String> paramMap) {
//        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

//        MsAclUtils.checkIfAnonymous(userId);
//        userCheck(userId, bucketName);
        //只有桶的所有者可以操作
        String backupConfig = getBackupConfig(bucketName);

        if (backupConfig == null) {
            throw new MsException(ErrorNo.NO_LIFECYCLE_CONFIGURATION, "NoSuchLifecycleConfiguration bucket:" + bucketName);
        } else {
            return new ResponseMsg().setData(backupConfig).addHeader(CONTENT_TYPE, "application/xml")
                    .addHeader(CONTENT_LENGTH, String.valueOf(backupConfig.getBytes().length));
        }
    }

    public ResponseMsg deleteBucketBackup(UnifiedMap<String, String> paramMap) {
        logger.debug(paramMap);
//        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

//        MsAclUtils.checkIfAnonymous(userId);
//        userCheck(userId, bucketName);

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        boolean isMasterCluster = StringUtils.isEmpty(masterCluster) || localCluster.equals(masterCluster);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_BACKUP, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }

        deleteBackupConfig(bucketName);
        deleteBucketArchiveCount(bucketName, null);
        /**主站点执行多区域转发**/
        int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_BACKUP);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket version error!");
        }

        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    public static void deleteBackupConfig(String bucketName) {
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_BACKUP_RULES, bucketName);
    }

    public static void deleteLifecycleConfig(String bucketName) {
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_LIFECYCLE_RULES, bucketName);
    }

    public static void deleteLifecycleRecord(String bucketName) {
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + LIFECYCLE_RECORD);
    }

    public static void deleteBackupRecord(String bucketName) {
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + BACKUP_RECORD);
    }

    public static void deleteBucketArchiveCount(String bucketName, LifecycleConfiguration lifecycleConfiguration) {
        HashSet<String> analyzerKeySet = new HashSet<>();
        if (lifecycleConfiguration != null) {
            Tuple2<HashSet<String>, HashSet<String>> tuple2 = ArchieveUtils.generateAnalyzerKeySet(bucketName, lifecycleConfiguration);
            analyzerKeySet = tuple2.var1;
            HashSet<String> backupKeySet = tuple2.var2;

            Map<String, String> backupMap = pool.getCommand(REDIS_TASKINFO_INDEX).hgetall(bucketName + BACKUP_RECORD);
            for (String key : backupMap.keySet()) {
                String keyName = bucketName + File.separator + key.split(File.separator, 2)[1];
                if (backupKeySet.contains(keyName)) {
                    continue;
                }
                logger.info("删除策略扫描标记: " + key);
                pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(bucketName + BACKUP_RECORD, key);
            }
        } else {
            pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + BACKUP_RECORD);
        }
        ScanArgs scanArgs = new ScanArgs().match(bucketName + File.separator + "*").limit(10);
        KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
        keyScanCursor.setCursor("0");
        KeyScanCursor<String> scan;
        do {
            try {
                // 执行 SCAN 命令，并传入当前游标
                scan = pool.getCommand(REDIS_TASKINFO_INDEX).scan(keyScanCursor, scanArgs);
                keyScanCursor.setCursor(scan.getCursor());  // 更新游标
                List<String> keys = scan.getKeys();  // 获取匹配的键
                for (String key : keys) {
                    if (!key.endsWith(ArchiveAnalyzer.mark)) {
                        continue;
                    }
                    String tmp = key.split(ArchiveAnalyzer.mark)[0];
                    if (analyzerKeySet.contains(tmp)) {
                        continue;
                    }
                    pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(key);
                    logger.info("删除归档计数: " + key);
                }
            } catch (Exception e) {
                logger.error("删除" + bucketName + "归档计数时发生错误: ", e);
                break;
            }
        } while (!scan.isFinished());
    }

    private String getLifecycleConfig(String bucketName) {
        return pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_LIFECYCLE_RULES, bucketName);
    }

    private String getBackupConfig(String bucketName) {
        return pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_BACKUP_RULES, bucketName);
    }
}
