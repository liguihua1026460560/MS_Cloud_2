package com.macrosan.action.managestream;/**
 * @author niechengxing
 * @create 2023-02-09 17:42
 */

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.Encryption.ApplyServerSideEncryptionByDefault;
import com.macrosan.message.xmlmsg.Encryption.Rule;
import com.macrosan.message.xmlmsg.Encryption.ServerSideEncryptionConfiguration;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2023-02-09 17:42
 */
@Log4j2
public class BucketEncryptionService extends BaseService {
    private static BucketEncryptionService instance = null;
    private static final String SSE_ALGORITHM = "crypto";
    private static final List<String> algorithmList = Arrays.asList("AES256","SM4");
    private BucketEncryptionService(){
        super();
    }
    public static BucketEncryptionService getInstance(){
        if (instance == null) {
            instance = new BucketEncryptionService();
        }
        return instance;
    }

    public ResponseMsg putBucketEncryption(UnifiedMap<String,String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String accountId = paramMap.get(USER_ID);//这里传的是账户id，一个账户下的用户调用该请求，传入的userId是同一个
        String method = "PutEncryptionConfiguration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap,bucketName,accountId,method);
        checkBucket(bucketName,accountId);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        if(StoragePoolFactory.getDeduplicate(bucketName)){
            throw new MsException(ErrorNo.INVALID_ENCRYPTION_STATEMENT_ERROR,"the strategy is deduplicated.");
        }
        if (bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketEncryption");
        }
        /**从站点请求主站点执行**/
        paramMap.put("body",paramMap.get(BODY));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_ENCRYPTION, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        ServerSideEncryptionConfiguration encryptionConfiguration = (ServerSideEncryptionConfiguration) JaxbUtils.toObject(paramMap.get(BODY));

        if (encryptionConfiguration == null){
            throw new MsException(ErrorNo.MALFORMED_XML,"The XML was not well-formed.");
        } else {
            if (encryptionConfiguration.getRule() != null){
                if (encryptionConfiguration.getRule().getApplyServerSideEncryptionByDefault() == null) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName,SSE_ALGORITHM);
                    /**---------------处理双活请求：同步至其他节点-------------**/
                    int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_ENCRYPTION);
                    if (res != SUCCESS_STATUS){
                        throw new MsException(res,"slave put bucket encryption fail");
                    }
                    log.info("set encryption successfully!!!!!");
                    return new ResponseMsg(SUCCESS_STATUS);
                } else {
                    if (encryptionConfiguration.getRule().getApplyServerSideEncryptionByDefault().getSSEAlgorithm() == null){
                        throw new MsException(ErrorNo.MALFORMED_XML,
                                "The XML was not well-formed.");
                    } else if (!algorithmList.contains(encryptionConfiguration.getRule().getApplyServerSideEncryptionByDefault().getSSEAlgorithm())){
                        throw new MsException(ErrorNo.SSE_ALGORITHM_INPUT_ERROR,
                                "The encryption algorithm provided is incorrect.");
                    } else {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName,SSE_ALGORITHM,
                                encryptionConfiguration.getRule().getApplyServerSideEncryptionByDefault().getSSEAlgorithm());
                    }
                }
            } else {
                throw new MsException(ErrorNo.MALFORMED_XML,
                        "The XML was not well-formed.");
            }
        }
        /**---------------处理双活请求：同步至其他节点-------------**/
        int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_ENCRYPTION);
        if (res != SUCCESS_STATUS){
            throw new MsException(res,"slave put bucket encryption fail");
        }

        return new ResponseMsg(SUCCESS_STATUS);
    }

    public ResponseMsg getBucketEncryption(UnifiedMap<String,String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String accountId = paramMap.get(USER_ID);
        String method = "GetEncryptionConfiguration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, accountId, method);
        checkBucket(bucketName,accountId);//检查账户id是否为桶的所有者，IAM用户创建的桶的所有者是管理该用户的账户。
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        String algorithm = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName,SSE_ALGORITHM);
        if (algorithm == null){
            throw new MsException(ErrorNo.BUCKET_ENCRYPTION_NOT_FOUND,
                    "the server side encryption configuration of the bucket " + bucketName + " is not found.");
        }

        ServerSideEncryptionConfiguration encryptionConfiguration = new ServerSideEncryptionConfiguration();
        encryptionConfiguration.setRule(returnRule(returnDefaultEncryption(algorithm)));
        return new ResponseMsg(SUCCESS_STATUS).setData(encryptionConfiguration);
    }

    public ResponseMsg deleteBucketEncryption(UnifiedMap<String,String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String accountId = paramMap.get(USER_ID);
        String method = "PutEncryptionConfiguration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap,bucketName,accountId,method);
        checkBucket(bucketName,accountId);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        /**从站点请求主站点执行**/
        paramMap.put("body",paramMap.get(BODY));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_BUCKET_ENCRYPTION, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName,SSE_ALGORITHM);
        log.info("delete the encryption rule of bucket {} successfully",bucketName);

        /**---------------处理双活请求：同步至其他节点-------------**/
        int res = notifySlaveSite(paramMap,ACTION_DEL_BUCKET_ENCRYPTION);
        if (res != SUCCESS_STATUS){
            throw new MsException(res,"slave delete bucket encryption fail");
        }
        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    /**
     * 校验桶是否存在以及用户类型
     */
    private void checkBucket(String bucketName,String userId){
        MsAclUtils.checkIfAnonymous(userId);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_DENY,
                    "No permission.User " + userId + " can not operate encryption configuration of " + bucketName + ".");
        }
    }

    private ApplyServerSideEncryptionByDefault returnDefaultEncryption(String algorithm){
        ApplyServerSideEncryptionByDefault applyServerSideEncryptionByDefault = new ApplyServerSideEncryptionByDefault();
        applyServerSideEncryptionByDefault.setSSEAlgorithm(algorithm);
        return applyServerSideEncryptionByDefault;
    }

    private Rule returnRule(ApplyServerSideEncryptionByDefault defaultEncryption){
        Rule rule = new Rule();
        rule.setApplyServerSideEncryptionByDefault(defaultEncryption);
        return rule;
    }
}

