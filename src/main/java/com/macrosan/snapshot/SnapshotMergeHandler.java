package com.macrosan.snapshot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.RedisLock;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.xmlmsg.BucketSnapshot;
import com.macrosan.snapshot.enums.MergeStep;
import com.macrosan.snapshot.enums.MergeTaskType;
import com.macrosan.snapshot.pojo.SnapshotMergeProgress;
import com.macrosan.snapshot.pojo.SnapshotMergeRecord;
import com.macrosan.snapshot.pojo.SnapshotMergeTask;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.macrosan.action.managestream.BucketSnapshotService.SNAPSHOT_LOCK_KEY;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.snapshot.BucketSnapshotStarter.*;
import static com.macrosan.snapshot.SnapshotListTemplate.*;
import static com.macrosan.snapshot.ViewSnapshotMergeHandler.updateTaskStatus;
import static com.macrosan.snapshot.utils.SnapshotMergeUtil.*;
import static com.macrosan.snapshot.enums.SnapshotMergeStrategy.*;

/**
 * @author zhaoyang
 * @date 2024/06/26
 **/
@Log4j2
public class SnapshotMergeHandler {

    public static final RedisConnPool POOL = RedisConnPool.getInstance();


    /**
     * 快照删除后，合并快照前的数据
     *
     * @param mergeTask 合并任务
     */
    public static void mergeSnapshotObject(SnapshotMergeTask mergeTask) {
        synchronized (RUNNING_MERGE_TASKS) {
            if (RUNNING_MERGE_TASKS.size() >= MAX_MERGE_SNAPSHOT_COUNT) {
                log.debug("scan merge snapshot object is limit reached,running tasks:{}", RUNNING_MERGE_TASKS);
                return;
            }
            if (RUNNING_MERGE_TASKS.get(mergeTask.getBucketName()) != null) {
                log.debug("scan merge snapshot object is already scanning,bucket is:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                return;
            }
            RUNNING_MERGE_TASKS.put(mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark());
        }
        log.info("merge snapshot task [start],bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
        try {
            String bucketName = mergeTask.getBucketName();
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            Disposable[] disposable = new Disposable[1];
            SnapshotMergeProgress mergeProgress = new SnapshotMergeProgress();
            // 保存进度的key
            String progressRedisKey = getMergeProgressRedisKey(mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark());
            disposable[0] = POOL.getReactive(REDIS_SNAPSHOT_INDEX).hexists(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, mergeTask.getSrcSnapshotMark())
                    .publishOn(SNAP_SCHEDULER)
                    .filter(b -> b)
                    .flatMap(b -> {
                        // 任务开始执行前，更新数据合并的映射，方便某些情况下，通过映射可以得知对象在哪个快照标记下
                        return Mono.just(b)
                                .publishOn(SNAP_SCHEDULER)
                                .handle((o, sink) -> {
                                    POOL.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(mergeTask.getBucketName(), DATA_MERGE_MAPPING, mergeTask.getTargetSnapshotMark());
                                    sink.next(true);
                                });
                    })
                    .flatMap(b -> POOL.getReactive(REDIS_SNAPSHOT_INDEX).hgetall(progressRedisKey).defaultIfEmpty(new HashMap<>()))
                    .doOnNext(mergeProgress::from)
                    .flatMap(v -> {
                        if (mergeProgress.getStep() != null && !mergeProgress.getStep().equals(MergeStep.COPY_OBJ)) {
                            return Mono.just(true);
                        }
                        Function<MetaData, Mono<Boolean>> objectConsumer = (metaData) -> createSnapshotMergeRecord(bucketName, metaData.key, metaData.versionId, mergeTask, storagePool)
                                .flatMap(SnapshotMergeHandler::dealMergeRecord);
                        return listObjectTemplate(bucketName, mergeProgress.getBeginPrefix(), null, storagePool, mergeTask.getSrcSnapshotMark(), objectConsumer, progressRedisKey)
//                        return listVersionsTemplate(bucketName, storagePool, objectConsumer, mergeTask.getSrcSnapshotMark())
                                .flatMap(b -> {
                                    if (!b) {
                                        return Mono.error(new RuntimeException("listVersionsTemplate error"));
                                    }
                                    log.info("merge  object [complete],bucket is:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                    mergeProgress.setStep(MergeStep.COPY_INIT_PART);
                                    mergeProgress.setBeginPrefix(null);
                                    return updateTaskStatus(progressRedisKey, MergeStep.COPY_INIT_PART);
                                });
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        if (!mergeProgress.getStep().equals(MergeStep.COPY_INIT_PART)) {
                            return Mono.just(true);
                        }
                        Function<Tuple2<String, String>, Mono<Boolean>> partConsumer = (tuple2) -> createInitPartSnapshotMergeRecord(bucketName, tuple2.var1, tuple2.var2, mergeTask, storagePool)
                                .flatMap(SnapshotMergeHandler::dealInitPartMergeRecord);
                        return listMultiPartTemplate(bucketName, mergeProgress.getKeyMarker(), mergeProgress.getUploadIdMarker(), storagePool, partConsumer, mergeTask.getSrcSnapshotMark(), progressRedisKey)
                                .flatMap(r -> {
                                    if (!r) {
                                        return Mono.error(new RuntimeException("listMultiPartTemplate error"));
                                    }
                                    log.info("merge initPart [complete],bucket is:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                    mergeProgress.setStep(MergeStep.COPY_PART);
                                    mergeProgress.setBeginPrefix(null);
                                    return updateTaskStatus(progressRedisKey, MergeStep.COPY_PART);
                                });
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        if (mergeTask.getType().equals(MergeTaskType.SNAPSHOT_MERGE) || mergeTask.getSnapshotLink() == null) {
                            return Mono.just(true);
                        }
                        TreeSet<String> snapshotMarks = Json.decodeValue(mergeTask.getSnapshotLink(), new TypeReference<TreeSet<String>>() {
                        });
                        String prevSnapshotMark = snapshotMarks.first();
                        Function<Tuple2<String, String>, Mono<Boolean>> partConsumer = (tuple2) -> writeBackPartOnly(mergeTask.getBucketName(), tuple2.var1, tuple2.var2, storagePool, prevSnapshotMark, mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark(), mergeTask.getSnapshotLink());
                        return SnapshotListTemplate.listMultiPartTemplate(mergeTask.getBucketName(), mergeProgress.getKeyMarker(), mergeProgress.getUploadIdMarker(), storagePool, partConsumer, prevSnapshotMark, progressRedisKey)
                                .doOnNext(s -> {
                                    if (!s) {
                                        throw new RuntimeException("listMultiPartTemplate error2");
                                    }
                                    log.info("merge part [complete],bucket is:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                });
                    })
                    .filter(b -> b)
                    .flatMap(b -> completeMergeTask(mergeTask))
                    .doFinally(s -> {
                        RUNNING_MERGE_TASKS.remove(mergeTask.getBucketName());
                        disposable[0].dispose();
                    })
                    .doOnError(e -> log.error("mergeSnapshotObject error, ", e))
                    .subscribe();
        } catch (Exception e) {
            RUNNING_MERGE_TASKS.remove(mergeTask.getBucketName());
            log.error("merge snapshot object error,bucket:{},srcMark:{},targetMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark(), e);
        }
    }

    /**
     * 判断该对象合并的操作类型 生成合并记录
     * 快照前上传过，快照创建后未上传过，则进行回写
     * 快照前上传过，快照创建后也上传过，则直接删除快照创建前对象的数据块和元数据
     *
     * @param bucketName        桶名
     * @param key               对象名
     * @param versionId         版本号
     * @param snapshotMergeTask 快照合并任务
     * @param bucketPool        存储池
     * @return 快照合并记录
     */
    private static Mono<SnapshotMergeRecord<MetaData>> createSnapshotMergeRecord(String bucketName, String key, String versionId, SnapshotMergeTask snapshotMergeTask, StoragePool bucketPool) {
        if (snapshotMergeTask.getType().equals(MergeTaskType.SNAPSHOT_MERGE)) {
            return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName))
                    .flatMap(vnodeList -> ErasureClient.getObjectMetaVersion(bucketName, key, versionId, vnodeList, null, snapshotMergeTask.getSrcSnapshotMark(), null).zipWith(Mono.just(vnodeList)))
                    .flatMap(tuple2 -> {
                        MetaData srcMetaData = tuple2.getT1();
                        if (srcMetaData.equals(ERROR_META)) {
                            return Mono.just(SnapshotMergeRecord.error());
                        }
                        if (srcMetaData.equals(NOT_FOUND_META) || srcMetaData.deleteMark) {
                            return Mono.just(SnapshotMergeRecord.none());
                        }
                        // 原对象对目标迁移快照已不可见，则直接删除
                        if (srcMetaData.isUnView(snapshotMergeTask.getTargetSnapshotMark())) {
                            return ErasureClient.getObjectMetaVersion(bucketName, key, versionId, tuple2.getT2(), null, snapshotMergeTask.getTargetSnapshotMark(), null)
                                    .map(targetMetaData -> {
                                        if (targetMetaData.equals(ERROR_META)) {
                                            return SnapshotMergeRecord.error();
                                        }
                                        if (targetMetaData.equals(NOT_FOUND_META) || targetMetaData.deleteMark || targetMetaData.deleteMarker) {
                                            return new SnapshotMergeRecord<>(DELETE_ALL, snapshotMergeTask, srcMetaData);
                                        }
                                        if ((targetMetaData.fileName != null && targetMetaData.fileName.equals(srcMetaData.fileName))
                                                || (targetMetaData.partUploadId != null && targetMetaData.partUploadId.equals(srcMetaData.partUploadId))) {
                                            // 对象元数据已迁移到目标快照下，但未删除旧的元数据，此时只需删除源元数据即可
                                            return new SnapshotMergeRecord<>(DELETE_META, snapshotMergeTask, srcMetaData);
                                        }
                                        return new SnapshotMergeRecord<>(DELETE_ALL, snapshotMergeTask, srcMetaData);
                                    });

                        }
                        return Mono.just(new SnapshotMergeRecord<>(WRITE_BACK, snapshotMergeTask, srcMetaData));
                    });
        }
        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName))
                .flatMap(vnodeList -> ErasureClient.getObjectMetaVersion(bucketName, key, versionId, vnodeList, null, snapshotMergeTask.getSrcSnapshotMark(), null).zipWith(Mono.just(vnodeList)))
                .flatMap(tuple2 -> {
                    MetaData srcMetaData = tuple2.getT1();
                    if (srcMetaData.equals(ERROR_META)) {
                        return Mono.just(SnapshotMergeRecord.error());
                    }
                    if (srcMetaData.equals(NOT_FOUND_META) || srcMetaData.deleteMark) {
                        return Mono.just(SnapshotMergeRecord.none());
                    }
                    return ErasureClient.getObjectMetaVersion(bucketName, key, versionId, tuple2.getT2(), null, snapshotMergeTask.getTargetSnapshotMark(), null)
                            .map(targetMetaData -> {
                                if (targetMetaData.equals(ERROR_META)) {
                                    return SnapshotMergeRecord.error();
                                }
                                // 目前快照下对象不存在，则进行迁移
                                if (targetMetaData.equals(NOT_FOUND_META) || targetMetaData.deleteMark) {
                                    return new SnapshotMergeRecord<>(WRITE_BACK, snapshotMergeTask, srcMetaData);
                                }
                                if ((targetMetaData.fileName != null && targetMetaData.fileName.equals(srcMetaData.fileName))
                                        || (targetMetaData.partUploadId != null && targetMetaData.partUploadId.equals(srcMetaData.partUploadId))) {
                                    // 对象元数据已迁移到目标快照下，但未删除旧的元数据，此时只需删除源元数据即可
                                    return new SnapshotMergeRecord<>(DELETE_META, snapshotMergeTask, srcMetaData);
                                }
                                // 目前快照对象存在，则进行删除
                                return new SnapshotMergeRecord<>(DELETE_ALL, snapshotMergeTask, srcMetaData);
                            });
                });


    }

    public static Mono<SnapshotMergeRecord<InitPartInfo>> createInitPartSnapshotMergeRecord(String bucketName, String key, String uploadId, SnapshotMergeTask snapshotMergeTask, StoragePool bucketPool) {
        if (snapshotMergeTask.getType().equals(MergeTaskType.SNAPSHOT_MERGE)) {
            return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName))
                    .flatMap(vnodeList -> PartClient.getInitPartInfo(bucketName, key, uploadId, vnodeList, null, snapshotMergeTask.getSrcSnapshotMark(), null).zipWith(Mono.just(vnodeList)))
                    .flatMap(tuple2 -> {
                        InitPartInfo initPartInfo = tuple2.getT1();
                        if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                            return Mono.just(SnapshotMergeRecord.error());
                        }
                        if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                            return Mono.just(SnapshotMergeRecord.none());
                        }
                        // 快照前创建的init是可见的，则说明还没有进行合并或者 abort ，则进行回写
                        if (initPartInfo.isViewable(snapshotMergeTask.getTargetSnapshotMark())) {
                            return Mono.just(new SnapshotMergeRecord<>(WRITE_BACK, snapshotMergeTask, initPartInfo));
                        }
                        // 快照创建前初始化的分段，快照创建后进行了分段合并或分段暂停
                        // 查看合并后的对象是否存在
                        return ErasureClient.getObjectMetaVersion(bucketName, key, initPartInfo.metaData.versionId, tuple2.getT2(), null, snapshotMergeTask.getTargetSnapshotMark(), null)
                                .flatMap(meta -> {
                                    if (meta.equals(ERROR_META)) {
                                        return Mono.just(SnapshotMergeRecord.error());
                                    }
                                    // 合并后对象不存在 或合并后的对象和该initPart upload不一样，则删除initPart 对应的元数据和分段的数据块
                                    if (meta.equals(NOT_FOUND_META) || !initPartInfo.uploadId.equals(meta.partUploadId)) {
                                        return Mono.just(new SnapshotMergeRecord<>(DELETE_ALL, snapshotMergeTask, initPartInfo));
                                    }

                                    // uploadid 相同，则表示合并的对象元数据中可能存在 快照创建前的分段，则应该将对象元数据进行修改
                                    return Mono.just(new SnapshotMergeRecord<>(UPDATE_META, snapshotMergeTask, initPartInfo));
                                });
                    });
        }
        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName))
                .flatMap(vnodeList -> PartClient.getInitPartInfo(bucketName, key, uploadId, vnodeList, null, snapshotMergeTask.getSrcSnapshotMark(), null).zipWith(Mono.just(vnodeList)))
                .flatMap(tuple2 -> {
                    InitPartInfo initPartInfo = tuple2.getT1();
                    if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                        return Mono.just(SnapshotMergeRecord.error());
                    }
                    if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                        return Mono.just(SnapshotMergeRecord.none());
                    }
                    return Mono.just(new SnapshotMergeRecord<>(WRITE_BACK, snapshotMergeTask, initPartInfo));
                });
    }

    /**
     * 处理合并记录--根据记录类型进行操作
     *
     * @param snapshotMergeRecord 合并记录
     * @return 处理结果
     */
    private static Mono<Boolean> dealMergeRecord(SnapshotMergeRecord<MetaData> snapshotMergeRecord) {
        switch (snapshotMergeRecord.getMergeType()) {
            case ERROR:
                return Mono.just(false);
            case DELETE_META:
                return deleteObjectMeta(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().key, snapshotMergeRecord.getData().versionId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
            case DELETE_ALL:
                // 数据块和元数据都删除
                return deleteMetaAndFile(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().key, snapshotMergeRecord.getData().versionId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
            case WRITE_BACK:
                // 将元数据写入目标快照
                return writeBack(snapshotMergeRecord)
                        .flatMap(b -> {
                            if (!b) {
                                log.error("writeBack error bucket:{},object:{},scrSnap:{},targetSnap:{}", snapshotMergeRecord.getData().bucket, snapshotMergeRecord.getData().key, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark(), snapshotMergeRecord.getMergeTask().getTargetSnapshotMark());
                                return Mono.just(false);
                            }
                            // 删除源快照下的对象元数据
                            return deleteObjectMeta(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().key, snapshotMergeRecord.getData().versionId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
                        });
            case NONE:
            default:
                return Mono.just(true);
        }
    }

    public static Mono<Boolean> dealInitPartMergeRecord(SnapshotMergeRecord<InitPartInfo> snapshotMergeRecord) {
        switch (snapshotMergeRecord.getMergeType()) {
            case ERROR:
                return Mono.just(false);
            case DELETE_META:
                return deleteMultiUploadMeta(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().object, snapshotMergeRecord.getData().uploadId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
            case DELETE_ALL:
                // 数据块和元数据都删除
                return deleteMultiUploadMetaAndFile(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().object, snapshotMergeRecord.getData().uploadId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
            case WRITE_BACK:
                return writeBackInitPart(snapshotMergeRecord)
                        .flatMap(b -> {
                            if (!b) {
                                return Mono.just(false);
                            }
                            // 不需要删除数据块，只删除元数据
                            return deleteMultiUploadMeta(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().object, snapshotMergeRecord.getData().uploadId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
                        });
            case UPDATE_META:
                return updatePostSnapshotMetadata(snapshotMergeRecord);
            case NONE:
            default:
                return Mono.just(true);
        }
    }


    private static Mono<Boolean> completeMergeTask(SnapshotMergeTask snapshotMergeTask) {
        String bucketName = snapshotMergeTask.getBucketName();
        if (snapshotMergeTask.getType().equals(MergeTaskType.SNAPSHOT_MERGE)) {
            return POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, SNAPSHOT_LINK)
                    .defaultIfEmpty("")
                    .publishOn(SNAP_SCHEDULER)
                    .doOnNext(snapshotLinkRelationship -> {
                        if (StringUtils.isNotEmpty(snapshotLinkRelationship)) {
                            Set<String> set = Json.decodeValue(snapshotLinkRelationship, new TypeReference<Set<String>>() {
                            });
                            if (set.contains(snapshotMergeTask.getSrcSnapshotMark())) {
                                // 1.修改桶的快照链接关系
                                POOL.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, SNAPSHOT_LINK);
                            }
                        }
                        // 2.将快照从桶快照列表中移除
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(BUCKET_SNAPSHOT_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark());
                        // 删除合并任务进度
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).del(getMergeProgressRedisKey(bucketName, snapshotMergeTask.getSrcSnapshotMark()));
                        // 3.将快照从正在删除列表中移除
                        POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark());
                        log.info(" scan merge snapshot object [end] bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", bucketName, snapshotMergeTask.getSrcSnapshotMark(), snapshotMergeTask.getTargetSnapshotMark());
                    }).map(b -> true);
        } else {
            return POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, SNAPSHOT_LINK)
                    .defaultIfEmpty("")
                    .publishOn(SNAP_SCHEDULER)
                    .doOnNext(snapshotLink -> {
                        Runnable runnable = () -> {
                            if (StringUtils.isNotEmpty(snapshotLink)) {
                                // 获取锁成功后，重新获取数据
                                TreeSet<String> createdSnapshots = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
                                });
                                String createdSnapshotMark = createdSnapshots.first();
                                String createdSnapshotJson = POOL.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, createdSnapshotMark);
                                if (StringUtils.isNotEmpty(createdSnapshotJson)) {
                                    BucketSnapshot bucketSnapshot = Json.decodeValue(createdSnapshotJson, BucketSnapshot.class);
                                    bucketSnapshot.getChildren().removeIf(child -> child.equals(snapshotMergeTask.getSrcSnapshotMark()));
                                    POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(BUCKET_SNAPSHOT_PREFIX + bucketName, createdSnapshotMark, Json.encode(bucketSnapshot));
                                }
                            }
                            // 将快照从桶快照列表中移除
                            POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(BUCKET_SNAPSHOT_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark());
                            // 删除合并任务
                            POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hdel(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark());
                            // 删除合并任务进度
                            POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).del(getMergeProgressRedisKey(bucketName, snapshotMergeTask.getSrcSnapshotMark()));
                            log.info(" scan merge snapshot object [end] bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", bucketName, snapshotMergeTask.getSrcSnapshotMark(), snapshotMergeTask.getTargetSnapshotMark());
                        };
                        boolean tryLock = RedisLock.tryLock(REDIS_SNAPSHOT_INDEX, SNAPSHOT_LOCK_KEY, 10, TimeUnit.SECONDS, runnable);
                        if (!tryLock) {
                            log.error("completeMergeTask error try lock fail");
                        }
                    })
                    .map(b -> true);
        }
    }

    protected static Mono<Boolean> writeBackPartOnly(String bucket, String object, String uploadId, StoragePool storagePool, String initSnapshotMark, String needWriteBackSnapshotMark, String writeBackTargetSnapshotMark, String snapshotLink) {
        Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucket, object);
        String vnodeId = bucketVnodeIdTuple.var1;
        return storagePool.mapToNodeInfo(vnodeId)
                .flatMap(nodeList -> PartClient.getInitPartInfo(bucket, object, uploadId, nodeList, null, initSnapshotMark, null).zipWith(Mono.just(nodeList)))
                .flatMap(tuple2 -> {
                    InitPartInfo initPartInfo = tuple2.getT1();
                    List<Tuple3<String, String, String>> nodeList = tuple2.getT2();
                    if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                        return Mono.error(new RuntimeException("get meta error"));
                    }
                    if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                        return Mono.just(true);
                    }
                    Function<Integer, Mono<Boolean>> partConsumer = (partNumber) -> PartClient.getPartInfo(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId,
                                    String.valueOf(partNumber), PartInfo.getPartKey(vnodeId, initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, String.valueOf(partNumber), needWriteBackSnapshotMark), nodeList, null, needWriteBackSnapshotMark, null,null)
                            .flatMap(partInfo -> {
                                if (partInfo.equals(PartInfo.ERROR_PART_INFO)) {
                                    return Mono.just(false);
                                }
                                if (partInfo.equals(PartInfo.NOT_FOUND_PART_INFO) || partInfo.equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO) || partInfo.delete) {
                                    return Mono.just(true);
                                }
                                partInfo.setInitSnapshotMark(initSnapshotMark);
                                return writeBackPart(partInfo, writeBackTargetSnapshotMark, snapshotLink);
                            });
                    return listPartTemplate(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, storagePool, partConsumer, needWriteBackSnapshotMark);
                })
                .flatMap(r -> {
                    if (!r) {
                        return Mono.just(false);
                    }
                    // 迁移到目标快照标记后，删除源快照标记中的partInfo
                    return SnapshotCleanUpHandler.deletePartOnly(bucket, object, uploadId, initSnapshotMark, needWriteBackSnapshotMark, false);
                });
    }

}
