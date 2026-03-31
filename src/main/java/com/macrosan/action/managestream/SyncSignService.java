package com.macrosan.action.managestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DataSyncSignHandler;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSyncSignHandler.BUCKET_SIGN_TYPE;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.httpserver.MossHttpClient.CLUSTERS_AMOUNT;

public class SyncSignService extends BaseService {
    private static SyncSignService instance = null;

    private SyncSignService() {
        super();
    }

    public static SyncSignService getInstance() {
        if (instance == null) {
            instance = new SyncSignService();
        }
        return instance;
    }


    private Map<String, Integer> getBucketSignsMap(String bucket) {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_SIGN_TYPE)
                .defaultIfEmpty("")
                .flatMap(s -> {
                    Map<String, Integer> map = new LinkedHashMap<>();
                    if (StringUtils.isNotBlank(s)) {
                        HashMap<String, Integer> hashMap = Json.decodeValue(s, new TypeReference<HashMap<String, Integer>>() {
                        });
                        for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                            map.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return Mono.just(map);
                }).block();
    }

    public ResponseMsg getAWSSigns(UnifiedMap<String, String> paramMap) {
        String bucket = paramMap.get(BUCKET_NAME);

        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucket);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        Map<String, Integer> map = getBucketSignsMap(bucket);

        return new ResponseMsg()
                .addHeader("bucket_sign_type", Json.encode(map));
    }

    public ResponseMsg setAWSSign(UnifiedMap<String, String> paramMap) {
        String bucket = paramMap.get(BUCKET_NAME);
        String srcIndex = paramMap.get("srcIndex");
        String dstIndex = paramMap.get("dstIndex");
        String signType = paramMap.get("signType");
        checkIndex(srcIndex);
        checkIndex(dstIndex);
        checkSignType(signType);

        String userId = paramMap.get(USER_ID);

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_SET_AWS_SIGN, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        //判断用户是否是桶的所有者
        DoubleActiveUtil.siteConstraintCheck(bucket, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        userCheck(userId, bucket);

        Map<String, Integer> map = getBucketSignsMap(bucket);
        map.put(srcIndex + "-" + dstIndex, Integer.parseInt(signType));
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, BUCKET_SIGN_TYPE, Json.encode(map));

        int res = notifySlaveSite(paramMap, ACTION_SET_AWS_SIGN);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave setAWSSign error!");
        }
        return new ResponseMsg();
    }

    public ResponseMsg deleteAWSSign(UnifiedMap<String, String> paramMap) {
        String bucket = paramMap.get(BUCKET_NAME);
        String srcIndex = paramMap.get("srcIndex");
        String dstIndex = paramMap.get("dstIndex");
        checkIndex(srcIndex);
        checkIndex(dstIndex);

        String userId = paramMap.get(USER_ID);

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DELETE_AWS_SIGN, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        //判断用户是否是桶的所有者
        DoubleActiveUtil.siteConstraintCheck(bucket, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        userCheck(userId, bucket);

        Map<String, Integer> map = getBucketSignsMap(bucket);
        map.remove(srcIndex + "-" + dstIndex);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, BUCKET_SIGN_TYPE, Json.encode(map));

        int res = notifySlaveSite(paramMap, ACTION_DELETE_AWS_SIGN);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave deleteAWSSign error!");
        }

        return new ResponseMsg();
    }

    void checkIndex(String indexStr) {
        int i = Integer.parseInt(indexStr);
        if (i < 0 || i > CLUSTERS_AMOUNT) {
            throw new MsException(UNKNOWN_ERROR, "Index for authorization is wrong, " + indexStr);
        }
    }

    void checkSignType(String signType) {
        int i = Integer.parseInt(signType);
        try {
            DataSyncSignHandler.SignType.parse(i);
        } catch (Exception e) {
            throw new MsException(UNKNOWN_ERROR, "SignType for authorization is wrong, " + signType);
        }
    }
}
