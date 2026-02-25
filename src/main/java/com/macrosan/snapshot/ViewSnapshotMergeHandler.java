package com.macrosan.snapshot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.snapshot.enums.MergeStep;
import com.macrosan.snapshot.enums.SnapshotMergeStrategy;
import com.macrosan.snapshot.pojo.SnapshotMergeProgress;
import com.macrosan.snapshot.pojo.SnapshotMergeRecord;
import com.macrosan.snapshot.pojo.SnapshotMergeTask;
import com.macrosan.snapshot.utils.SnapshotMergeUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.function.Function;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SNAPSHOT_INDEX;
import static com.macrosan.snapshot.BucketSnapshotStarter.*;
import static com.macrosan.snapshot.SnapshotListTemplate.listPartTemplate;
import static com.macrosan.snapshot.SnapshotMergeHandler.createInitPartSnapshotMergeRecord;
import static com.macrosan.snapshot.utils.SnapshotMergeUtil.getMergeProgressRedisKey;
import static com.macrosan.snapshot.utils.SnapshotMergeUtil.writeBackPart;

/**
 * @author zhaoyang
 * @date 2024/08/23
 **/
@Log4j2
public class ViewSnapshotMergeHandler {
    public static final RedisConnPool POOL = RedisConnPool.getInstance();

    public static void mergeViewSnapshot(SnapshotMergeTask mergeTask) {
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
        log.info("merge view task [start],bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
        try {
            String bucket = mergeTask.getBucketName();
            String endstamp = String.valueOf(mergeTask.getCtime());
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(mergeTask.getBucketName());
            Disposable[] disposable = new Disposable[1];
            SnapshotMergeProgress mergeProgress = new SnapshotMergeProgress();
            String progressRedisKey = getMergeProgressRedisKey(mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark());
            disposable[0] = POOL.getReactive(REDIS_SNAPSHOT_INDEX).hgetall(progressRedisKey)
                    .defaultIfEmpty(new HashMap<>())
                    .publishOn(SNAP_SCHEDULER)
                    .flatMap(mergeProgressMap -> {
                        mergeProgress.from(mergeProgressMap);
                        if (mergeProgress.getStep() == null || mergeProgress.getStep().equals(MergeStep.COPY_OBJ)) {
                            Function<MetaData, Mono<Boolean>> consumer = (metaData -> SnapshotMergeUtil.writeBack(new SnapshotMergeRecord<>(SnapshotMergeStrategy.WRITE_BACK, mergeTask, metaData)));
                            return SnapshotListTemplate.listObjectTemplate(bucket, mergeProgress.getBeginPrefix(), endstamp, storagePool, mergeTask.getSrcSnapshotMark(), consumer, progressRedisKey)
                                    .flatMap(b -> {
                                        if (!b) {
                                            return Mono.error(new RuntimeException("listObjectTemplate error"));
                                        }
                                        log.info("merge view COPY_OBJ complete,bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                        // 修改状态
                                        mergeProgress.setStep(MergeStep.COPY_INIT_PART);
                                        mergeProgress.setBeginPrefix(null);
                                        return updateTaskStatus(getMergeProgressRedisKey(bucket, mergeTask.getSrcSnapshotMark()), MergeStep.COPY_INIT_PART);
                                    });
                        }
                        return Mono.just(true);
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        if (mergeProgress.getStep().equals(MergeStep.COPY_INIT_PART)) {
                            Function<Tuple2<String, String>, Mono<Boolean>> partConsumer = (tuple2) -> createInitPartSnapshotMergeRecord(bucket, tuple2.var1, tuple2.var2, mergeTask, storagePool)
                                    .flatMap(SnapshotMergeHandler::dealInitPartMergeRecord);
                            return SnapshotListTemplate.listMultiPartTemplate(bucket, mergeProgress.getKeyMarker(), mergeProgress.getUploadIdMarker(), storagePool, partConsumer, mergeTask.getSrcSnapshotMark(), progressRedisKey)
                                    .flatMap(s -> {
                                        if (!s) {
                                            return Mono.error(new RuntimeException("listMultiPartTemplate error"));
                                        }
                                        log.info("merge view COPY_INIT_PART complete,bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                        // 修改状态
                                        mergeProgress.setStep(MergeStep.COPY_PART);
                                        mergeProgress.setBeginPrefix(null);
                                        return updateTaskStatus(getMergeProgressRedisKey(bucket, mergeTask.getSrcSnapshotMark()), MergeStep.COPY_PART);
                                    });
                        }
                        return Mono.just(true);
                    })
                    .flatMap(b -> {
                        if (!b) {
                            return Mono.error(new RuntimeException("updateTaskStatus error"));
                        }
                        if (mergeProgress.getStep().equals(MergeStep.COPY_PART)) {
                            return POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, SNAPSHOT_LINK)
                                    .flatMap(snapshotLink -> {
                                        if (snapshotLink == null) {
                                            return Mono.just(true);
                                        }
                                        TreeSet<String> snapshotMarks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
                                        });
                                        String prevSnapshotMark = snapshotMarks.first();
                                        Function<Tuple2<String, String>, Mono<Boolean>> partConsumer = (tuple2) -> writeBackPartOnly(bucket, tuple2.var1, tuple2.var2, storagePool, prevSnapshotMark, mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                        return SnapshotListTemplate.listMultiPartTemplate(bucket, mergeProgress.getBeginPrefix(),mergeProgress.getUploadIdMarker() , storagePool, partConsumer, prevSnapshotMark, progressRedisKey)
                                                .flatMap(s -> {
                                                    if (!s) {
                                                        return Mono.error(new RuntimeException("listMultiPartTemplate error"));
                                                    }
                                                    log.info("merge view COPY_PART complete,bucket:{},srcSnapshotMark:{},targetSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark());
                                                    // 修改状态
                                                    mergeProgress.setStep(MergeStep.REMOVE);
                                                    mergeProgress.setBeginPrefix(null);
                                                    return updateTaskStatus(getMergeProgressRedisKey(bucket, mergeTask.getSrcSnapshotMark()), MergeStep.REMOVE);
                                                });
                                    })
                                    .onErrorReturn(false)
                                    .switchIfEmpty(Mono.just(true));
                        }
                        return Mono.just(true);
                    })
                    .flatMap(b -> {
                        if (b) {
                            return completeMergeView(mergeTask);
                        }
                        return Mono.just(false);
                    })
                    .doFinally(s -> {
                        RUNNING_MERGE_TASKS.remove(mergeTask.getBucketName());
                        disposable[0].dispose();
                    })
                    .doOnError(e -> log.error("mergeViewSnapshot error, ", e))
                    .subscribe();
        } catch (Exception e) {
            RUNNING_MERGE_TASKS.remove(mergeTask.getBucketName());
            log.error("mergeViewSnapshot error,bucket:{},srcMark:{},targetMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark(), e);
        }
    }

    private static Mono<Boolean> completeMergeView(SnapshotMergeTask mergeTask) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Mono.just(1)
                .publishOn(SNAP_SCHEDULER)
                .subscribe(b -> {
                    // 合并完成，取消双写
                    Map<String, String> map = new HashMap<>(2);
                    map.put(CURRENT_SNAPSHOT_MARK, mergeTask.getTargetSnapshotMark());
                    POOL.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(mergeTask.getBucketName(), map);
                    res.onNext(true);
                    log.info("merge view task [end],bucket:{},srcSnapshotMark:{},targetSnapshotMark:{},close double write,currentSnapshotMark:{}", mergeTask.getBucketName(), mergeTask.getSrcSnapshotMark(), mergeTask.getTargetSnapshotMark(), mergeTask.getTargetSnapshotMark());
                });
        return res;
    }

    public static Mono<Boolean> updateTaskStatus(String redisKey, MergeStep step) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Mono.just(1)
                .subscribeOn(SNAP_SCHEDULER)
                .subscribe(b -> {
                    POOL.getShortMasterCommand(REDIS_SNAPSHOT_INDEX)
                            .hset(redisKey, "step", step.toString());
                    res.onNext(true);
                });
        return res;
    }




    protected static Mono<Boolean> writeBackPartOnly(String bucket, String object, String uploadId, StoragePool storagePool, String initSnapshotMark, String needWriteBackSnapshotMark, String writeBackTargetSnapshotMark) {
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
                                return writeBackPart(partInfo, writeBackTargetSnapshotMark, null);
                            });
                    return listPartTemplate(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, storagePool, partConsumer, needWriteBackSnapshotMark);
                });
    }


}
