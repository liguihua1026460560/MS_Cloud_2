package com.macrosan.doubleActive.deployment;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;

/**
 * 桶后续开启数据同步开关后执行单独的历史数据同步
 */
public class BucketHisSyncHandler {
    private static BucketHisSyncHandler instance;

    public static BucketHisSyncHandler getInstance() {
        if (instance == null) {
            instance = new BucketHisSyncHandler();
        }
        return instance;
    }

    private RedisConnPool pool = RedisConnPool.getInstance();

    public static final String BUCKET_HIS_SYNC_SET = "bucket_his_sync_set";

    /**
     * 保存桶名和DeployRecord
     */
    Map<String, DeployRecord> bucketDeployMaP = new ConcurrentHashMap<>();

    /**
     * 检查是否有新加入的同步桶
     */
    public void check() {
        pool.getReactive(SysConstants.REDIS_SYSINFO_INDEX)
                .smembers(BUCKET_HIS_SYNC_SET)
                .filter(bucket -> !bucketDeployMaP.containsKey(bucket))
                .subscribe(bucket -> {
                    // 初始化deployRecord
                    long timeStamp = System.currentTimeMillis() + 15 * 60_000L;
                    String str = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, DEPLOEY_RECORD);
                });
    }
}
