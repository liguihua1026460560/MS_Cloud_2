package com.macrosan.snapshot.utils;

import com.macrosan.ec.DelDeleteMark;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.snapshot.pojo.SnapshotMergeRecord;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.params.DelMarkParams;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.CLEAN_UP_PROGRESS;
import static com.macrosan.constants.ServerConstants.MERGE_PROGRESS;
import static com.macrosan.constants.SysConstants.MAX_UPLOAD_PART_NUM;
import static com.macrosan.ec.ErasureClient.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.snapshot.SnapshotListTemplate.listPartTemplate;

/**
 * @author zhaoyang
 * @date 2024/08/08
 **/
@Log4j2
public class SnapshotMergeUtil {


    public static String getMergeProgressRedisKey(String bucket, String srcSnapshotMark) {
        return MERGE_PROGRESS + bucket + "_" + srcSnapshotMark;
    }

    public static String getCleanUpProgressRedisKey(String bucket, String discardSnapshotMark) {
        return CLEAN_UP_PROGRESS + bucket + "_" + discardSnapshotMark;
    }

    /**
     * 删除对象元数据和数据块
     *
     * @param bucket       桶名
     * @param object       对象名
     * @param versionId    版本号
     * @param snapshotMark 待删除对象的快照标记
     * @return 删除结果
     */
    public static Mono<Boolean> deleteMetaAndFile(String bucket, String object, String versionId, String snapshotMark) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucket, object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        MetaData[] metaData = new MetaData[1];
        List<String> needDeleteFile = new ArrayList<>();
        String[] curVersion = new String[]{null};
        String[] versionStatus = new String[]{null};
        return VersionUtil.getVersionNum(bucket, object)
                .flatMap(version -> MsObjVersionUtils.versionStatusReactive(bucket)
                        .doOnNext(status -> {
                            curVersion[0] = version;
                            versionStatus[0] = status;
                        }).flatMap(status -> bucketPool.mapToNodeInfo(vnodeId))
                )
                .flatMap(vnodeList -> getObjectMetaVersion(bucket, object, versionId, vnodeList, null, snapshotMark, null))
                .flatMap(meta -> {
                    if (meta.equals(ERROR_META)) {
                        return Mono.error(new RuntimeException("get meta error"));
                    }
                    if (meta.equals(NOT_FOUND_META)) {
                        return Mono.just(1);
                    }
                    metaData[0] = meta;
                    if (meta.fileName != null) {
                        needDeleteFile.add(meta.fileName);
                    } else if (meta.partUploadId != null) {
                        for (PartInfo partInfo : meta.partInfos) {
                            if (snapshotMark.equals(partInfo.snapshotMark)){
                                needDeleteFile.add(partInfo.fileName);
                            }
                        }
                    }
                    if (migrateVnode != null) {
                        return bucketPool.mapToNodeInfo(migrateVnode)
                                .flatMap(vnodeList -> setDelMark(new DelMarkParams(meta.bucket, meta.key, meta.versionId, meta, vnodeList, versionStatus[0], curVersion[0]).migrate(true), meta.snapshotMark, null))
                                .flatMap(b -> {
                                    if (b == 0) {
                                        return Mono.error(new RuntimeException("delete object internal error"));
                                    }
                                    return bucketPool.mapToNodeInfo(vnodeId)
                                            .flatMap(vnodeList -> setDelMark(new DelMarkParams(meta.bucket, meta.key, meta.versionId, meta, vnodeList, versionStatus[0], curVersion[0]), meta.snapshotMark, null));
                                })
                                .doOnNext(b -> {
                                    if (b == 0) {
                                        log.error("setDelMark error");
                                    }
                                });
                    }
                    return bucketPool.mapToNodeInfo(vnodeId)
                            .flatMap(vnodeList -> setDelMark(new DelMarkParams(meta.bucket, meta.key, meta.versionId, meta, vnodeList, versionStatus[0], curVersion[0]), meta.snapshotMark, null))
                            .doOnNext(b -> {
                                if (b == 0) {
                                    log.error("setDelMark error");
                                }
                            });
                })
                .timeout(Duration.ofSeconds(30))
                .flatMap(b -> b == 1 ? DelDeleteMark.deleteVersionObj(needDeleteFile, metaData) : Mono.just(b != 0))
                .doOnError(e -> log.error(e.getMessage()))
                .onErrorReturn(false);
    }

    /**
     * 删除对象元数据
     *
     * @param bucket       桶名
     * @param object       对象名
     * @param versionId    版本号
     * @param snapshotMark 待删除对象的快照标记
     * @return 删除结果
     */
    public static Mono<Boolean> deleteObjectMeta(String bucket, String object, String versionId, String snapshotMark) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucket, object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        MetaData[] metaData = new MetaData[1];
        return bucketPool.mapToNodeInfo(vnodeId)
                .flatMap(vnodeList -> getObjectMetaVersion(bucket, object, versionId, vnodeList, null, snapshotMark, null))
                .flatMap(meta -> {
                    metaData[0] = meta;
                    if (migrateVnode == null) {
                        return Mono.just(true);
                    }
                    String metaKey = Utils.getMetaDataKey(migrateVnode, bucket, object, versionId, metaData[0].stamp, snapshotMark);
                    return bucketPool.mapToNodeInfo(migrateVnode)
                            .flatMap(vnodeList -> ErasureClient.discardObjectAllMeta(bucket, metaKey, Json.encode(metaData[0]), vnodeList))
                            .flatMap(r -> {
                                if (!r) {
                                    return Mono.error(new RuntimeException("deleteObjectMeta error"));
                                }
                                return Mono.just(true);
                            });
                })
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(false);
                    }
                    String metaKey = Utils.getMetaDataKey(vnodeId, bucket, object, versionId, metaData[0].stamp, snapshotMark);
                    return bucketPool.mapToNodeInfo(vnodeId)
                            .flatMap(vnodeList -> ErasureClient.discardObjectAllMeta(bucket, metaKey, Json.encode(metaData[0]), vnodeList))
                            .flatMap(r -> {
                                if (!r) {
                                    return Mono.error(new RuntimeException("deleteObjectMeta error"));
                                }
                                return Mono.just(true);
                            });
                });
    }


    /**
     * 删除initPart和part
     *
     * @param bucket             桶名
     * @param object             对象名
     * @param uploadId           uploadId
     * @param snapshotMark       待删除对象的快照标记
     * @param deletePartMetaOnly 是否只删除分段元数据
     * @param ignoreDeleteFile   忽略删除的文件
     * @return result
     */
    private static Mono<Boolean> deleteMultiUpload(String bucket, String object, String uploadId, String snapshotMark, boolean deletePartMetaOnly, Integer... ignoreDeleteFile) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucket, object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        return bucketPool.mapToNodeInfo(vnodeId)
                .flatMap(nodeList -> PartClient.getInitPartInfo(bucket, object, uploadId, nodeList, null, snapshotMark, null)
                        .flatMap(initPartInfo -> {
                            if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                                return Mono.error(new RuntimeException("get meta error"));
                            }
                            if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                return Mono.just(true);
                            }
                            return Mono.just(migrateVnode != null)
                                    .flatMap(b -> {
                                        StoragePool dataPool = StoragePoolFactory.getStoragePool(initPartInfo);
                                        if (b) {
                                            return bucketPool.mapToNodeInfo(migrateVnode)
                                                    .flatMap(vnodeList -> PartClient.abortMultiPartUpload(dataPool, vnodeList, initPartInfo, null, snapshotMark, deletePartMetaOnly, ignoreDeleteFile))
                                                    .flatMap(r -> {
                                                        if (r) {
                                                            return PartClient.abortMultiPartUpload(dataPool, nodeList, initPartInfo, null, snapshotMark, deletePartMetaOnly, ignoreDeleteFile);
                                                        }
                                                        return Mono.just(false);
                                                    });

                                        }
                                        return PartClient.abortMultiPartUpload(dataPool, nodeList, initPartInfo, null, snapshotMark, deletePartMetaOnly, ignoreDeleteFile);
                                    });
                        }));

    }


    /**
     * 删除initPart元数据和initPart对应partInfo的元数据
     */
    public static Mono<Boolean> deleteMultiUploadMeta(String bucket, String object, String uploadId, String snapshotMark) {
        return deleteMultiUpload(bucket, object, uploadId, snapshotMark, true);
    }

    /**
     * 删除指定分段的元数据和数据块
     */
    public static Mono<Boolean> deleteMultiUploadMetaAndFile(String bucket, String object, String uploadId, String snapshotMark, Integer... ignoreDeleteFile) {
        return deleteMultiUpload(bucket, object, uploadId, snapshotMark, false, ignoreDeleteFile);
    }

    /**
     * 回写数据---将快照创建前的对象元数据写入到快照创建后的元数据中，数据块不进行删除
     *
     * @param snapshotMergeRecord 快照合并记录
     * @return 回写结果
     */
    public static Mono<Boolean> writeBack(SnapshotMergeRecord<MetaData> snapshotMergeRecord) {
        MetaData metaData = snapshotMergeRecord.getData();
        metaData.setUnView(null);
        metaData.setWeakUnView(null);
        // 修改快照标记
        metaData.snapshotMark = snapshotMergeRecord.getMergeTask().getTargetSnapshotMark();
        if (metaData.partInfos != null) {
            for (PartInfo partInfo : metaData.partInfos) {
                if (partInfo.getSnapshotMark().equals(snapshotMergeRecord.getMergeTask().getSrcSnapshotMark())) {
                    partInfo.setSnapshotMark(metaData.getSnapshotMark());
                }
            }
        }
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        String snapshotLink = snapshotMergeRecord.getMergeTask().getSnapshotLink();

        return Mono.just(migrateVnode == null)
                .flatMap(b -> {
                    if (b) {
                        return Mono.just(true);
                    }
                    String metaDataKey = Utils.getMetaDataKey(migrateVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                    return bucketPool.mapToNodeInfo(migrateVnode)
                            .flatMap(nodeList -> putMetaData(metaDataKey, metaData, nodeList, true, snapshotLink));
                })
                .flatMap(b -> {
                    if (!b) {
                        log.error("put metadata error");
                        return Mono.just(false);
                    }
                    String metaDataKey = Utils.getMetaDataKey(vnodeId, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                    return bucketPool.mapToNodeInfo(vnodeId)
                            .flatMap(nodeList -> putMetaData(metaDataKey, metaData, nodeList, false, snapshotLink));
                });
    }

    public static Mono<Boolean> writeBackInitPart(SnapshotMergeRecord<InitPartInfo> snapshotMergeRecord) {
        InitPartInfo initPartInfo = snapshotMergeRecord.getData();
        initPartInfo.setUnView(null);
        // 修改快照标记
        String scrSnapshotMark = initPartInfo.snapshotMark;
        initPartInfo.snapshotMark = snapshotMergeRecord.getMergeTask().getTargetSnapshotMark();
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(initPartInfo.bucket, initPartInfo.object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        Function<Integer, Mono<Boolean>> partConsumer = (partNumber) -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(vnodeId))
                .flatMap(vnodeList -> PartClient.getPartInfo(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, String.valueOf(partNumber), PartInfo.getPartKey(vnodeId, initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, String.valueOf(partNumber), scrSnapshotMark), vnodeList, null, scrSnapshotMark, null,null).zipWith(Mono.just(vnodeList)))
                .flatMap(tuple2 -> {
                    PartInfo partInfo = tuple2.getT1();
                    if (partInfo.equals(PartInfo.ERROR_PART_INFO)) {
                        return Mono.just(false);
                    }
                    if (partInfo.equals(PartInfo.NOT_FOUND_PART_INFO) || partInfo.equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO)
                            || partInfo.delete || partInfo.isUnView(snapshotMergeRecord.getMergeTask().getTargetSnapshotMark())) {
                        return Mono.just(true);
                    }
                    return writeBackPart(partInfo, snapshotMergeRecord.getMergeTask().getTargetSnapshotMark(), snapshotMergeRecord.getMergeTask().getSnapshotLink());
                });
        return listPartTemplate(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, bucketPool, partConsumer, scrSnapshotMark)
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(false);
                    }
                    if (migrateVnode != null) {
                        return bucketPool.mapToNodeInfo(migrateVnode)
                                .flatMap(nodeList -> PartClient.initPartUpload(initPartInfo, nodeList, null, true, null))
                                .flatMap(r -> bucketPool.mapToNodeInfo(vnodeId)
                                        .flatMap(nodeList -> PartClient.initPartUpload(initPartInfo, nodeList, null, false, null)));
                    }
                    return bucketPool.mapToNodeInfo(vnodeId)
                            .flatMap(nodeList -> PartClient.initPartUpload(initPartInfo, nodeList, null, false, null));
                });
    }

    public static Mono<Boolean> writeBackPart(PartInfo partInfo, String targetMergeSnapshotMark, String snapshotLink) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(partInfo.bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(partInfo.bucket, partInfo.object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        partInfo.setUnView(null);
        partInfo.snapshotMark = targetMergeSnapshotMark;
        return Mono.just(migrateVnode != null)
                .flatMap(b -> {
                    if (b) {
                        return bucketPool.mapToNodeInfo(migrateVnode)
                                .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, null, null, true, snapshotLink))
                                .flatMap(r -> {
                                    if (!r) {
                                        return Mono.just(false);
                                    }
                                    return bucketPool.mapToNodeInfo(vnodeId)
                                            .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, null, null, false, snapshotLink));
                                });
                    }
                    return bucketPool.mapToNodeInfo(vnodeId)
                            .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, null, null, false, snapshotLink));
                });
    }

    public static Mono<Boolean> updatePostSnapshotMetadata(SnapshotMergeRecord<InitPartInfo> snapshotMergeRecord) {
        InitPartInfo info = snapshotMergeRecord.getData();
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(info.bucket);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(info.bucket, info.object);
        String vnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        List<Integer> oldSnapshotPartNum = new ArrayList<>();
        return bucketPool.mapToNodeInfo(vnodeId)
                .flatMap(vnodeList -> getObjectMetaVersionRes(info.bucket, info.object, info.metaData.versionId, vnodeList, null, snapshotMergeRecord.getMergeTask().getTargetSnapshotMark(), null).zipWith(Mono.just(vnodeList)))
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1().var1;
                    if (meta.equals(ERROR_META)) {
                        return Mono.just(false);
                    }
                    if (meta.equals(NOT_FOUND_META) || !info.uploadId.equals(meta.partUploadId)) {
                        return deleteMultiUploadMetaAndFile(snapshotMergeRecord.getMergeTask().getBucketName(), snapshotMergeRecord.getData().object, snapshotMergeRecord.getData().uploadId, snapshotMergeRecord.getMergeTask().getSrcSnapshotMark());
                    }
                    // 将元数据中分段的快照标记修改为当前快照标记
                    return checkPartInfo(info, meta, tuple2.getT2())
                            .flatMap(set -> {
                                if (set.isEmpty()) {
                                    return Mono.just(true);
                                }
                                for (PartInfo partInfo : meta.partInfos) {
                                    if (set.contains(Integer.valueOf(partInfo.getPartNum()))) {
                                        partInfo.setSnapshotMark(meta.snapshotMark);
                                        oldSnapshotPartNum.add(Integer.valueOf(partInfo.getPartNum()));
                                    }
                                }
                                // 更新对象元数据
                                return Mono.just(migrateVnode != null)
                                        .flatMap(b -> {
                                            if (!b) {
                                                return Mono.just(true);
                                            }
                                            return bucketPool.mapToNodeInfo(migrateVnode)
                                                    .flatMap(vnodeList -> updateMetaDataAcl(Utils.getVersionMetaDataKey(migrateVnode, meta.bucket, meta.key, meta.versionId, meta.snapshotMark), meta, vnodeList, null, tuple2.getT1().var2()))
                                                    .doOnNext(r -> {
                                                        if (r != 1) {
                                                            log.error("updatePostSnapshotMetadata error1,bucket{},object:{},versionId:{},res:{}", meta.bucket, meta.key, meta.versionId, r);
                                                        }
                                                    }).map(r -> r == 1);
                                        })
                                        .flatMap(b -> {
                                            if (!b) {
                                                return Mono.just(false);
                                            }
                                            return updateMetaDataAcl(Utils.getVersionMetaDataKey(vnodeId, meta.bucket, meta.key, meta.versionId, meta.snapshotMark), meta, tuple2.getT2(), null, tuple2.getT1().var2())
                                                    .doOnNext(r -> {
                                                        if (r != 1) {
                                                            log.error("updatePostSnapshotMetadata error2,bucket{},object:{},versionId:{},res:{}", meta.bucket, meta.key, meta.versionId, r);
                                                        }
                                                    }).map(r -> r == 1);
                                        });
                            })
                            .doOnError(log::error)
                            .onErrorReturn(false);
                })
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(false);
                    }
                    // 更新完合并后的对象元数据后，将快照前的分段删除，删除分段相关的所有元数据，但不删除oldSnapshotPartNum相关的分段数据块
                    return deleteMultiUploadMetaAndFile(info.bucket, info.object, info.uploadId, info.snapshotMark, oldSnapshotPartNum.toArray(new Integer[0]))
                            .doOnNext(r -> {
                                if (!r) {
                                    log.error("deleteMultiUpload error,bucket{},object:{},versionId:{}", info.bucket, info.object, info.uploadId);
                                }
                            });
                }).onErrorReturn(false);
    }


    public static Mono<Set<Integer>> checkPartInfo(InitPartInfo initPartInfo, MetaData metaData, List<Tuple3<String, String, String>> nodeList) {
        ListPartsResult listPartsResult = new ListPartsResult()
                .setMaxParts(MAX_UPLOAD_PART_NUM)
                .setBucket(initPartInfo.bucket)
                .setKey(initPartInfo.object)
                .setUploadId(initPartInfo.uploadId);
        return PartClient.listParts(listPartsResult, nodeList, null, initPartInfo.snapshotMark, null)
                .handle((b, sink) -> {
                    if (!b) {
                        sink.error(new RuntimeException("checkPartInfo error"));
                        return;
                    }
                    Set<String> partInfoSet = Arrays.stream(metaData.partInfos).map(PartInfo::getFileName).collect(Collectors.toSet());
                    sink.next(listPartsResult.getParts().stream().filter(part -> partInfoSet.contains(part.getFileName())).map(Part::getPartNumber).collect(Collectors.toSet()));
                });
    }
}
