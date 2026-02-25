package com.macrosan.doubleActive.deployment;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.deployment.BucketSyncChecker.*;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_AK_SK_MAP;

@Log4j2
public class BucketSyncUtils {
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    public static final Logger logger = LogManager.getLogger(BucketSyncUtils.class.getName());

    public static void updateExtraMap(String bucket) {
        synchronized (EXTRA_AK_SK_MAP) {
            pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                    .doOnNext(map -> {
                        if ("on".equals(map.get("data-synchronization-switch")) || "suspend".equals(map.get("data-synchronization-switch"))) {
                            String bucketName = map.get("bucket_name");
                            String serverAk = StringUtils.isNotBlank(map.get("server_ak")) ? map.get("server_ak") : "";
                            String serverSk = StringUtils.isNotBlank(map.get("server_sk")) ? map.get("server_sk") : "";
                            if (EXTRA_AK_SK_MAP.containsKey(bucketName)) {
                                if (!serverAk.equals(EXTRA_AK_SK_MAP.get(bucketName).var1) || !serverSk.equals(EXTRA_AK_SK_MAP.get(bucketName).var2)) {
                                    Tuple2<String, String> tuple2 = new Tuple2<>(serverAk, serverSk);
                                    EXTRA_AK_SK_MAP.put(bucket, tuple2);
                                    logger.info("update ak {} sk {} to {}", serverAk, serverSk, bucket);
                                }
                            } else {
                                Tuple2<String, String> tuple2 = new Tuple2<>(serverAk, serverSk);
                                EXTRA_AK_SK_MAP.put(bucket, tuple2);
                                logger.info("add ak {} sk {} to {}", serverAk, serverSk, bucket);
                            }
                        }
                    })
                    .subscribe();
        }
    }

    static void preDealObj(String bucketVnode, MetaData metaData) {
        String rocksKey = Utils.getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId);
        bucketDeploy.dealingObjMap.computeIfAbsent(metaData.bucket + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).put(rocksKey, metaData);
    }

    static void preDealPartList(String bucketName, String bucketVnode, Upload upload) {
        String initPartKey = InitPartInfo.getPartKey("", bucketName, upload.getKey(), upload.getUploadId());
        bucketDeploy.dealingPartMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).put(initPartKey, upload.getKey());
    }

    static void finishDealObj(String bucketVnode, MetaData metaData) {
        String rocksKey = Utils.getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId);
        Map<String, MetaData> dealingObjMap = bucketDeploy.dealingObjMap.get(metaData.bucket + "_" + bucketVnode);
        dealingObjMap.remove(rocksKey);
        //该桶下没有待处理obj且objList也结束了，认为该桶的历史数据record已全部落盘。
        if (dealingObjMap.isEmpty() && !BS_COM_SYNCING_BUCKET.contains(metaData.bucket)) {
            synchronized (Objects.requireNonNull(BUCKETS_OBJ_STATUS_MAP.put(metaData.bucket, 1))) {
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                .hset(metaData.bucket, obj_bucket_sync_finished, "1"));
            }
        }
    }

    /**
     * part_multi_list中的一条Upload已经都处理完成（part_init、part_upload相关的record均已落盘）时调用。
     */
    static void finishDealPartList(String bucketName, String bucketVnode, Upload upload) {
        Map<String, String> dealingPartMap = bucketDeploy.dealingPartMap.get(bucketName + "_" + bucketVnode);
        String initPartKey = InitPartInfo.getPartKey("", bucketName, upload.getKey(), upload.getUploadId());
        dealingPartMap.remove(initPartKey);
        //该桶下没有待处理obj且objList也结束了，认为该桶的历史数据record已全部落盘。
        if (dealingPartMap.isEmpty() && !BS_PART_SYNCING_BUCKET.contains(bucketName)) {
            synchronized (Objects.requireNonNull(BUCKETS_PART_STATUS_MAP.put(bucketName, 1))) {
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                .hset(bucketName, part_bucket_sync_finished, "1"));
            }
        }
    }

    /**
     * 扫描完成后执行，更改桶同步状态为已同步。
     * 注意执行到此并非表示历史数据同步的record已写入完成，只是表示list或part_multi_list这一步结束了。
     *
     * @param bucket   桶名
     * @param redisStr 表7桶下的redis字段，obj_his_sync_finished或part_his_sync_finished
     */
    static synchronized void endList(String bucket, String redisStr) {
        Set<String> listingMap;
        switch (redisStr) {
            case obj_bucket_sync_finished:
                listingMap = BS_COM_SYNCING_BUCKET;
                break;
            case part_bucket_sync_finished:
                listingMap = BS_PART_SYNCING_BUCKET;
                break;
            default:
                listingMap = new HashSet<>();
        }
        listingMap.remove(bucket);
        if (BS_COM_SYNCING_BUCKET.contains(bucket) || BS_PART_SYNCING_BUCKET.contains(bucket)) {
            return;
        }
        syncingBucketNum.decrementAndGet();
        logger.info("bucketSync endList: {}, {}, {}", redisStr, bucket, syncingBucketNum);
    }

    public static boolean bucketSyncIsRunning() {
        Set<String> smembers = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(NEED_SYNC_BUCKETS);
        if (smembers != null) {
            if (smembers.isEmpty()) {
                return false;
            } else {
                log.info("some bucket sync process is running. ");
                return true;
            }
        } else {
            return false;
        }
    }

}
