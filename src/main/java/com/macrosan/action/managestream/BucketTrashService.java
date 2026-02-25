package com.macrosan.action.managestream;


import com.macrosan.action.core.BaseService;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.trash.TrashUtils;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import com.macrosan.constants.ErrorNo;

import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.utils.trash.TrashUtils.bucketTrash;

/**
 * @author zhangzhixin
 */

@Log4j2
public class BucketTrashService extends BaseService {

    private static BucketTrashService instance = null;

    public static BucketTrashService getInstance(){
        if (instance == null){
            instance = new BucketTrashService();
        }

        return instance;
    }

    /**
     * 配置桶回收站相关的配置
     * @param paramMap
     * @return
     */
    public ResponseMsg putBucketTrash(UnifiedMap<String, String> paramMap){
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        String trashSettingText = paramMap.get("body");

        String method = "PutBucketTrash";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap,bucketName,userId,method);

        checkPermissions(bucketName,userId);
        TrashUtils.checkEnvVersion(bucketName,pool);
        if (bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketTrash");
        }
        if(!checkBucketStatus(bucketName)){
            throw new MsException(ErrorNo.BUCKET_OPEN_VERSION,"The bucket does not meet the conditions");
        }

        JsonObject body = new JsonObject();
        try {
            body = new JsonObject(trashSettingText);
            if (body == null){
                throw new MsException(ErrorNo.MALFORMED_JSON,"Json str is error");
            }
        }catch (Exception e){
            throw new MsException(ErrorNo.MALFORMED_JSON,"Json str is error");
        }


        if(!body.fieldNames().contains("trashDir")){
            throw new MsException(ErrorNo.MALFORMED_JSON,"Json str don't have trashDir");
        }

        String dir = body.getString("trashDir");
        if (dir.length() > 64){
            throw  new MsException(ErrorNo.TRASH_NAME_TOO_LONG,"trash directory name too long");
        }

        if (!checkStr(dir)){
            throw  new MsException(ErrorNo.INVALID_TRASH_DIRECTORY_NAME,"trash directory has special chars");
        }

        /**-----------------处理双活请求------------**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER,CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER,CLUSTER_NAME);
        paramMap.put(BODY,trashSettingText);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap,MSG_TYPE_SITE_PUT_BUCKET_TRASH,localCluster,masterCluster)){
            log.info("The slave cluster send message to master cluster, the bucket is " + bucketName);
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));



        if (StringUtils.isBlank(dir)){
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName,"trashDir",".trash/");
        }else{
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName,"trashDir",dir + "/");
        }

        /**---------------处理双活请求：同步至其他节点-------------**/
        int res = notifySlaveSite(paramMap,ACTION_PUT_BUCKET_TRASH);
        if (res != SUCCESS_STATUS){
            throw new MsException(res,"slave put bucket log fail");
        }


        return new ResponseMsg();
    }


    /**
     * 获取桶回收站相关配置
     * @param paramMap
     * @return
     */
    public ResponseMsg getBucketTrash(UnifiedMap<String, String> paramMap){
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);

        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String method = "GetBucketTrash";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap,bucketName,userId,method);

        checkPermissions(bucketName,userId);
        JsonObject result = new JsonObject();
        String dir = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName,"trashDir");
        if (StringUtils.isBlank(dir)){
            throw new MsException(ErrorNo.NO_SUCH_TRASH_DIRECTORY,"The bucket does not have the function of opening the bucket trash");
        }
        result.put("trashDir",dir);


        return new ResponseMsg(SUCCESS).setData(result.toString());
    }

    /**
     * 删除桶回收站相关配置
     * @param paramMap
     * @return
     */
    public ResponseMsg deleteBucketTrash(UnifiedMap<String, String> paramMap){
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);


        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));


        String method = "DeleteBucketTrash";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap,bucketName,userId,method);
        
        checkPermissions(bucketName,userId);

        String result = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName,"trashDir");
        if (StringUtils.isBlank(result)){
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }

        /**-----------------处理双活请求------------**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER,CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER,CLUSTER_NAME);
        paramMap.put("body",paramMap.get(BODY));
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap,MSG_TYPE_SITE_DEL_BUCKET_TRASH,localCluster,masterCluster)){
            log.info("The slave cluster send message to master cluster");
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName,"trashDir");
        bucketTrash.remove(bucketName);

        /**---------------处理双活请求：同步至其他节点-------------**/
        int res = notifySlaveSite(paramMap,ACTION_DEL_BUCKET_TRASH);
        if (res != SUCCESS_STATUS){
            throw new MsException(res,"slave put bucket log fail");
        }

        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    private ResponseMsg checkPermissions(String bucketName,String userId){
        Map<String,String> info = getBucketMapByName(bucketName);
        String id = info.get(BUCKET_USER_ID);
        if (!id.equals(userId)){
            throw  new MsException(ErrorNo.ACCESS_FORBIDDEN,"no such bucket");
        }

        return null;
    }

    /**
     * 检测字符是否符合标准
     * @param str
     * @return
     */
    private boolean checkStr(String str){
        String reg = "[0-9a-zA-Z\u4e00-\u9fa5._-]*";
        return str.matches(reg);
    }

    /**
     * 检测桶是否开启多版本
     * @param bucketName 桶名
     * @return true（未开启）
     */
    private boolean checkBucketStatus(String bucketName){
        String result = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName,"versionstatus");
        if (StringUtils.isBlank(result)){
            return true;
        }
        return false;
    }
}
