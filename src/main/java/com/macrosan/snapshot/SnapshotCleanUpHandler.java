package com.macrosan.snapshot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.BucketSnapshot;
import com.macrosan.snapshot.enums.MergeStep;
import com.macrosan.snapshot.pojo.SnapshotMergeProgress;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SNAPSHOT_INDEX;
import static com.macrosan.snapshot.BucketSnapshotStarter.SNAP_SCHEDULER;
import static com.macrosan.snapshot.SnapshotListTemplate.*;
import static com.macrosan.snapshot.ViewSnapshotMergeHandler.updateTaskStatus;
import static com.macrosan.snapshot.utils.SnapshotMergeUtil.*;

/**
 * @author zhaoyang
 * @date 2024/08/02
 **/
@Log4j2
public class SnapshotCleanUpHandler {
    public static final RedisConnPool POOL = RedisConnPool.getInstance();
    public static final Set<String> RUNNING_CLEAN_UP = new ConcurrentHashSet<>();
    public static final int MAX_SCAN_SNAPSHOT_COUNT = 10;

    /**
     * 快照回滚后，删除跳过的快照标记下的所有对象和分段
     *
     * @param bucketName          桶名
     * @param discardSnapshotMark 快照回滚时的快照标记
     */
    protected static void cleanUpDiscardObject(String bucketName, String discardSnapshotMark) {
        String lock = bucketName + "_" + discardSnapshotMark;
        synchronized (RUNNING_CLEAN_UP) {
            if (RUNNING_CLEAN_UP.contains(lock)) {
                log.debug("scan discard object is already scanning,bucket is:{},snapshot mark is:{}", bucketName, discardSnapshotMark);
                return;
            }
            if (RUNNING_CLEAN_UP.size() > MAX_SCAN_SNAPSHOT_COUNT) {
                log.debug("scan discard object is limit reached,snapshot marks is:{}", RUNNING_CLEAN_UP);
                return;
            }
            RUNNING_CLEAN_UP.add(lock);
        }
        log.info("start scan discard object,bucket is:{},snapshot mark is:{}", bucketName, discardSnapshotMark);
        try {
            // 保存进度的key
            String progressRedisKey = getCleanUpProgressRedisKey(bucketName, discardSnapshotMark);
            SnapshotMergeProgress cleanUpProgress = new SnapshotMergeProgress();
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            Disposable[] disposable = new Disposable[1];
            disposable[0] = POOL.getReactive(REDIS_SNAPSHOT_INDEX).sismember(SNAPSHOT_DISCARD_LIST_PREFIX + bucketName, discardSnapshotMark)
                    .publishOn(SNAP_SCHEDULER)
                    .filter(b -> b)
                    .flatMap(b -> POOL.getReactive(REDIS_SNAPSHOT_INDEX).hgetall(progressRedisKey).defaultIfEmpty(new HashMap<>()))
                    .doOnNext(cleanUpProgress::from)
                    .flatMap(b -> {
                        if (cleanUpProgress.getStep() != null && !cleanUpProgress.getStep().equals(MergeStep.CLEAN_UP_OBJ)) {
                            return Mono.just(true);
                        }
                        Function<MetaData, Mono<Boolean>> versionConsumer;
                        versionConsumer = (meta) -> deleteMetaAndFile(bucketName, meta.key, meta.versionId, discardSnapshotMark);
                        return listObjectTemplate(bucketName,cleanUpProgress.getBeginPrefix(),null,storagePool,discardSnapshotMark,versionConsumer, progressRedisKey)
//                        return listVersionsTemplate(bucketName, storagePool, versionConsumer, discardSnapshotMark)
                                .flatMap(r -> {
                                    if (!r) {
                                        return Mono.error(new RuntimeException("listVersionsTemplate error"));
                                    }
                                    cleanUpProgress.setStep(MergeStep.CLEAN_UP_INIT_PART);
                                    cleanUpProgress.setBeginPrefix(null);
                                    return updateTaskStatus(progressRedisKey, MergeStep.CLEAN_UP_INIT_PART);
                                });
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        if (!cleanUpProgress.getStep().equals(MergeStep.CLEAN_UP_INIT_PART)) {
                            return Mono.just(true);
                        }
                        // list 未合并的分段对象并进行删除
                        Function<Tuple2<String, String>, Mono<Boolean>> uploadConsumer;
                        uploadConsumer = (upload) -> deleteMultiUploadMetaAndFile(bucketName, upload.var1(), upload.var2(), discardSnapshotMark);
                        return listMultiPartTemplate(bucketName, cleanUpProgress.getKeyMarker(), cleanUpProgress.getUploadIdMarker(), storagePool, uploadConsumer, discardSnapshotMark, progressRedisKey)
                                .flatMap(r -> {
                                    if (!r) {
                                        return Mono.error(new RuntimeException("listMultiPartTemplate error"));
                                    }
                                    cleanUpProgress.setStep(MergeStep.CLEAN_UP_PART);
                                    cleanUpProgress.setBeginPrefix(null);
                                    return updateTaskStatus(progressRedisKey, MergeStep.CLEAN_UP_PART);
                                });
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        // 快照创建后可能只上传了part，而initpart 是快照创建前上传的，因此还需扫描快照创建前的initpart，并且删除其中partinfo是快照创建后上传的
                        return POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, SNAPSHOT_LINK)
                                .flatMap(snapshotLink -> {
                                    if (snapshotLink == null) {
                                        return Mono.just(true);
                                    }
                                    TreeSet<String> snapshotMarks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
                                    });
                                    String prevSnapshotMark = snapshotMarks.first();
                                    Function<Tuple2<String, String>, Mono<Boolean>> partConsumer = (upload) -> deletePartOnly(bucketName, upload.var1(), upload.var2(), prevSnapshotMark, discardSnapshotMark, true);
                                    return listMultiPartTemplate(bucketName, cleanUpProgress.getKeyMarker(), cleanUpProgress.getUploadIdMarker(), storagePool, partConsumer, prevSnapshotMark, progressRedisKey);
                                })
                                .onErrorReturn(false)
                                .switchIfEmpty(Mono.just(true));
                    })
                    .filter(b -> b)
                    .flatMap(b -> completeScanSkippedSnapshot(bucketName, discardSnapshotMark))
                    .doOnError(e -> {
                        log.error("cleanUpDiscardObject error2, ", e);
                    })
                    .doFinally(s -> {
                        RUNNING_CLEAN_UP.remove(lock);
                        disposable[0].dispose();
                    })
                    .subscribe();
        } catch (Exception e) {
            RUNNING_CLEAN_UP.remove(lock);
            log.error("scan discard object error,bucket:{},snapshotMark:{}", bucketName, discardSnapshotMark, e);
        }
    }

    private static Mono<Boolean> completeScanSkippedSnapshot(String bucketName, String discardSnapshotMark) {
        return POOL.getReactive(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, discardSnapshotMark)
                .defaultIfEmpty("")
                .publishOn(SNAP_SCHEDULER)
                .doOnNext(discardSnapshot -> {
                    if (StringUtils.isBlank(discardSnapshot)) {
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).srem(SNAPSHOT_DISCARD_LIST_PREFIX + bucketName, discardSnapshotMark);
                        // 删除合并任务进度
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).del(getCleanUpProgressRedisKey(bucketName, discardSnapshotMark));
                    } else {
                        BucketSnapshot bucketSnapshot = Json.decodeValue(discardSnapshot, BucketSnapshot.class);
                        // 删除合并完成的视图
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(BUCKET_SNAPSHOT_PREFIX + bucketName, discardSnapshotMark, bucketSnapshot.getParent());
                        // 删除合并任务进度
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).del(getMergeProgressRedisKey(bucketName, discardSnapshotMark));
                        // 删除合并任务
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, discardSnapshotMark);
                    }
                    log.info("scan discard object end,bucket:{},skippedSnapshotMark:{}", bucketName, discardSnapshotMark);
                })
                .map(b -> true);
    }


    /**
     * 快照创建前初始化分段，快照创建后删除快照创建后上传的分段(删除部分分段)
     *
     * @param bucket
     * @param object
     * @param uploadId
     * @param initSnapshotMark
     * @param needDeleteSnapshotMark
     * @param isDeleteFile
     * @return
     */
    protected static Mono<Boolean> deletePartOnly(String bucket, String object, String uploadId, String initSnapshotMark, String needDeleteSnapshotMark, boolean isDeleteFile) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucket, object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        return bucketPool.mapToNodeInfo(vnodeId)
                .flatMap(nodeList -> PartClient.getInitPartInfo(bucket, object, uploadId, nodeList, null, initSnapshotMark, null)
                        .flatMap(initPartInfo -> {
                            if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                                return Mono.error(new RuntimeException("get meta error"));
                            }
                            if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                return Mono.just(true);
                            }
                            StoragePool dataPool = StoragePoolFactory.getStoragePool(initPartInfo);
                            return Mono.just(vnodeId.equals(migrateVnode))
                                    .flatMap(b -> {
                                        if (!b) {
                                            return Mono.just(true);
                                        }
                                        return bucketPool.mapToNodeInfo(migrateVnode)
                                                .flatMap(migrateVnodeList -> {
                                                    if (isDeleteFile) {
                                                        return PartUtils.deleteMultiPartUploadData(dataPool, initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, migrateVnodeList, null, initSnapshotMark, needDeleteSnapshotMark).zipWith(Mono.just(migrateVnodeList));
                                                    }
                                                    return Mono.just(true).zipWith(Mono.just(migrateVnodeList));
                                                })
                                                .flatMap(r -> {
                                                    if (!r.getT1()) {
                                                        log.debug("delete migrate vnode multiPart Data error,bucket:{},object:{},uploadId:{},initMark:{},needDeleteMark:{}", bucket, object, uploadId, initSnapshotMark, needDeleteSnapshotMark);
                                                        return Mono.just(false);
                                                    }
                                                    return PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, r.getT2(), true, initSnapshotMark, needDeleteSnapshotMark)
                                                            .flatMap(res -> {
                                                                if (!res) {
                                                                    log.debug("delete migrate vnode multiPart meta error,bucket:{},object:{},uploadId:{},initMark:{},needDeleteMark:{}", bucket, object, uploadId, initSnapshotMark, needDeleteSnapshotMark);
                                                                }
                                                                return Mono.just(true);
                                                            });
                                                });
                                    })
                                    .flatMap(b -> {
                                        if (!b) {
                                            log.error("delete migrate vnode  multi part error,bucket:{},object:{},uploadId:{},initMark:{},needDeleteMark:{}", bucket, object, uploadId, initSnapshotMark, needDeleteSnapshotMark);
                                            return Mono.just(false);
                                        }
                                        return bucketPool.mapToNodeInfo(vnodeId)
                                                .flatMap(vnodeList -> {
                                                    if (isDeleteFile) {
                                                        return PartUtils.deleteMultiPartUploadData(dataPool, initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, vnodeList, null, initSnapshotMark, needDeleteSnapshotMark).zipWith(Mono.just(vnodeList));
                                                    }
                                                    return Mono.just(true).zipWith(Mono.just(vnodeList));
                                                })
                                                .flatMap(r -> {
                                                    if (!r.getT1()) {
                                                        log.debug("delete multiPart Data error,bucket:{},object:{},uploadId:{},initMark:{},needDeleteMark:{}", bucket, object, uploadId, initSnapshotMark, needDeleteSnapshotMark);
                                                        return Mono.just(false);
                                                    }
                                                    return PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, r.getT2(), true, initSnapshotMark, needDeleteSnapshotMark)
                                                            .flatMap(res -> {
                                                                if (!res) {
                                                                    log.debug("delete multiPart meta error,bucket:{},object:{},uploadId:{},initMark:{},needDeleteMark:{}", bucket, object, uploadId, initSnapshotMark, needDeleteSnapshotMark);
                                                                }
                                                                return Mono.just(true);
                                                            });
                                                });
                                    });
                        }));
    }
}
