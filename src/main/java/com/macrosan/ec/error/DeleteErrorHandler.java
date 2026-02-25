package com.macrosan.ec.error;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.params.DelMarkParams;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

import static com.macrosan.constants.SysConstants.ASYNC_CLUSTER_SIGNAL;
import static com.macrosan.ec.ErasureClient.updateMetaData;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

@Log4j2
public class DeleteErrorHandler {

    @HandleErrorFunction(value = ERROR_SET_DEL_MARK)
    public static Mono<Boolean> setDeleteMark(String bucket, String object, String versionNum, String versionId, String currentSnapshotMark, String snapshotLink) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        final String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        String[] versionStatus = new String[]{""};
        return MsObjVersionUtils.getObjVersionIdReactive(bucket)
                .doOnNext(status -> versionStatus[0] = status)
                .flatMap(status -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(bucket, object, StringUtils.isNotEmpty(versionId) ? versionId : "null", bucketVnodeList, null,
                        (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, null, res, snapshotLink, true), currentSnapshotMark, snapshotLink)
                        .flatMap(meta -> {
                            if (meta.isDeleteMark()) {
                                DelMarkParams markParams = new DelMarkParams(bucket, object, versionId, meta, bucketVnodeList, versionStatus[0], meta.versionNum);
                                return ErasureClient.setDelMark(markParams, currentSnapshotMark, snapshotLink).map(r -> r == 1);
                            }
                            return Mono.just(false);
                        }))
                .map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_MARK_DELETE_META)
    public static Mono<Boolean> setDeleteMark(String bucket, String object, String versionNum, String versionId, String vnode, MetaData pendingMark, String currentSnapshotMark, String snapshotLink,
                                              String updateQuotaKeyStr) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        final String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        String[] versionStatus = new String[]{""};
        return MsObjVersionUtils.getObjVersionIdReactive(bucket)
                .doOnNext(status -> versionStatus[0] = status)
                .flatMap(status -> storagePool.mapToNodeInfo(StringUtils.isEmpty(vnode) ? bucketVnode : vnode))
                .flatMap(bucketVnodeList -> {
                            return Mono.defer(() -> {
                                if (StringUtils.isNotBlank(updateQuotaKeyStr)) {
                                    return ErasureClient.getObjectMetaVersionFsQuotaRecover(bucket, object, versionId, bucketVnodeList, null, pendingMark == null ? null : pendingMark.snapshotMark,
                                            null, updateQuotaKeyStr, false);
                                }
                                return ErasureClient.getObjectMetaVersion(bucket, object, StringUtils.isNotEmpty(versionId) ? versionId : "null", bucketVnodeList, null, pendingMark == null ? null :
                                        pendingMark.snapshotMark, null);
                            })
                                    .flatMap(meta -> {
                                        if (pendingMark != null) {
                                            if (meta.isDeleteMark()) {
                                                pendingMark.setVersionNum(meta.versionNum);
                                            }
                                            DelMarkParams delMarkParams = new DelMarkParams(bucket, object, versionId, pendingMark, bucketVnodeList, versionStatus[0], pendingMark.versionNum);
                                            if (StringUtils.isNotBlank(updateQuotaKeyStr)) {
                                                delMarkParams.updateQuotaKeyStr = updateQuotaKeyStr;
                                            }
                                            return ErasureClient.setDelMark(delMarkParams, currentSnapshotMark, snapshotLink).map(r -> r == 1);
                                        }
                                        // 以下代码为兼容旧版本消息
                                        if (meta.isDeleteMark()) {
                                            DelMarkParams markParams = new DelMarkParams(bucket, object, versionId, meta, bucketVnodeList, versionStatus[0], meta.versionNum);
                                            if (StringUtils.isNotBlank(updateQuotaKeyStr)) {
                                                markParams.updateQuotaKeyStr = updateQuotaKeyStr;
                                            }
                                            return ErasureClient.setDelMark(markParams, currentSnapshotMark, snapshotLink).map(r -> r == 1);
                                        }
                                        return Mono.just(false);
                                    });
                        }
                )
                .map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_DELETE_OBJECT_META)
    public static Mono<Boolean> deleteObjectMeta(String bucket, String object, String versionId, String versionNum, String nodeId, String snapshotMark, String uploadIdOrFileName, String fileCookie) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        String key = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);
        long fileCookie0 = StringUtils.isNotBlank(fileCookie) ? Long.parseLong(fileCookie) : 0;
        return storagePool.mapToNodeInfo(bucketVnode)
                //deleteObjectMeta只在全部k+m成功时返回true。此时一定不存在磁盘问题，因此不用强制返回false。
                .flatMap(curList -> {
                    long finalNodeId = 0L;
                    if (null != nodeId) {
                        finalNodeId = Long.parseLong(nodeId);
                    }
                    return ErasureClient.deleteObjectMeta(key, bucket, object, versionId, versionNum, curList, null, finalNodeId, uploadIdOrFileName, snapshotMark, fileCookie0);
                });
    }

    @HandleErrorFunction(value = ERROR_DELETE_OBJECT_FILE)
    public static Mono<Boolean> deleteObjectFile(String storage, String[] fileName) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, null);
        return ErasureClient.deleteObjectFile(storagePool, fileName, null).map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_LIFECYCLE_DELETE_OBJECT_FILE)
    public static Mono<Boolean> lifecycleDeleteFile(String storage, String[] fileName) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, null);
        return ErasureClient.lifecycleDeleteFile(storagePool, fileName, null).map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_RESTORE_DELETE_OBJECT_FILE)
    public static Mono<Boolean> restoreDeleteFile(String storage, String[] fileName) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, null);
        return ErasureClient.restoreDeleteObjectFile(storagePool, fileName, null).map(b -> false);
    }

    // 重删删除失败修复修改
    @HandleErrorFunction(value = ERROR_DELETE_DEDUP_OBJECT)
    public static Mono<Boolean> deleteDedupObject(String storage, String[] dedupKey, String fileName, List<Tuple3<String, String, String>> nodeList, String isLifecycleClear) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, null);
        return ErasureClient.deleteDedupObjectFile(storagePool, dedupKey[0], nodeList, fileName, Boolean.parseBoolean(isLifecycleClear)).var1;
    }

    @HandleErrorFunction(value = ERROR_DELETE_PART_DATA)
    public static Mono<Boolean> deleteMultiPartUploadData(String storage, String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList, String initPartSnapshotMark, String currentSnapshotMark) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, bucket);
        return PartUtils.deleteMultiPartUploadData(storagePool, bucket, object, uploadId, bucketNodeList, null, initPartSnapshotMark, currentSnapshotMark)
                .flatMap(b -> {
                    if (b) {
                        //只在全部k+m成功时返回true。此时一定不存在磁盘问题，因此不用强制返回false。
                        return PartUtils.deleteMultiPartUploadMeta(bucket, object, uploadId, bucketNodeList, true, initPartSnapshotMark, currentSnapshotMark);
                    } else {
                        return Mono.just(false);
                    }
                });
    }

    @HandleErrorFunction(ERROR_DELETE_PART_META)
    public static Mono<Boolean> deleteMultiPartUploadMeta(String bucket, String object, String uploadId, String initPartSnapshotMark, String currentSnapshotMark, String vnode) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        if (StringUtils.isNotEmpty(vnode)) {
            bucketVnode = vnode;
        }
        return storagePool.mapToNodeInfo(bucketVnode)
                //只在全部k+m成功时返回true。此时一定不存在磁盘问题，因此不用强制返回false。
                .flatMap(list -> PartUtils.deleteMultiPartUploadMeta(bucket, object, uploadId, list, true, initPartSnapshotMark, currentSnapshotMark));
    }

    @HandleErrorFunction(ERROR_MARK_DELETE_PART_META)
    public static Mono<Boolean> markDeleteMultiPartUploadMeta(String bucket, String object, String uploadId, InitPartInfo initPartInfo, String currentSnapshotMark) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket, object);

        return storagePool.mapToNodeInfo(bucketVnode)
                //只在全部k+m成功时返回true。此时一定不存在磁盘问题，因此不用强制返回false。
                .flatMap(list -> PartUtils.deleteMultiPartUploadMeta(bucket, object, uploadId, list, true, null, initPartInfo, initPartInfo.snapshotMark, currentSnapshotMark));
    }

    @HandleErrorFunction(ERROR_DELETE_ROCKETS_VALUE)
    public static Mono<Boolean> deleteRocketsValue(String bucket, String key, String vnode) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
//        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return storagePool.mapToNodeInfo(vnode)
                .flatMap(list -> ErasureClient.deleteRocketsValue(bucket, key, list)).map(b -> false);
    }

    @HandleErrorFunction(ERROR_DELETE_UNSYNC_ROCKETS_VALUE)
    public static Mono<Boolean> deleteUnsyncRocketsValue(String bucket, String key, SocketReqMsg msg) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(list -> {
                    UnSynchronizedRecord record = null;
                    if (msg.get("value") != null) {
                        record = Json.decodeValue(msg.get("value"), UnSynchronizedRecord.class);
                    }
                    return ErasureClient.deleteUnsyncRocketsValue(bucket, key, record, list, msg.get(ASYNC_CLUSTER_SIGNAL));
                }).map(b -> false);
    }

    @HandleErrorFunction(ERROR_DELETE_OBJECT_ALL_META)
    public static Mono<Boolean> deleteObjectAllMeta(String bucket, String key, String value, String vnode) {
        MetaData metaData = Json.decodeValue(value, MetaData.class);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String versionKey = Utils.getVersionMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
        return storagePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersion(metaData.bucket, metaData.key, metaData.versionId, nodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(nodeList)))
                .map(b -> false);
    }

    @HandleErrorFunction(ERROR_DELETE_COMPONENT_RECORD)
    public static Mono<Boolean> deleteMultiMediaRecord(String bucket, String key, SocketReqMsg msg) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        String value = msg.get("value");
        ComponentRecord oldRecord = JSON.parseObject(value, new TypeReference<ComponentRecord>() {
        });
        return ComponentUtils.getComponentRecord(oldRecord)
                .flatMap(tuple2 -> {
                    ComponentRecord curRecord = tuple2.var1;
                    // 获取失败，重新放入队列，该消息不做处理
                    if (curRecord.equals(ComponentRecord.ERROR_COMPONENT_RECORD)) {
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        msg.put("poolQueueTag", poolQueueTag);
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_MULTI_MEDIA_RECORD);
                        return Mono.just(false);
                    }
                    // 数据被覆盖，则不进行删除
                    if (!Objects.equals(oldRecord.strategy.strategyMark, curRecord.strategy.strategyMark)) {
                        return Mono.just(true);
                    }
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> ComponentUtils.deleteComponentRecord(oldRecord));
                });
    }

    @HandleErrorFunction(ERROR_DEL_ES_META)
    public static Mono<Boolean> delESMeta(SocketReqMsg msg) {
        String value = msg.get("value");
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        List<String> esMetaList = JSON.parseObject(value, new TypeReference<List<String>>() {
        });
        return EsMetaTask.delEsMeta(esMetaList, bucket, lun, true);
    }

    @HandleErrorFunction(ERROR_FREE_AGGREGATION_SPACE)
    public static Mono<Boolean> freeAggregationSpace(String key) {
        String vnode = Utils.getVnode(key);
        return StoragePoolFactory.getMetaStoragePool("").mapToNodeInfo(vnode)
                .flatMap(nodeList -> AggregateFileClient.freeAggregationSpace(key, nodeList));
    }
}
