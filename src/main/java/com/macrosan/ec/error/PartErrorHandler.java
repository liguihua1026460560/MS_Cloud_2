/*
  Copyright (C), 2019-2020,杭州宏杉科技股份有限公司
  FileName: PartErrorHandler
  Author: xiangzicheng-PC
  Date: 2020/5/18 10:13
  Description: 分段部分接口异常处理
  History:
 */

package com.macrosan.ec.error;

import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.part.PartUtils.deleteMultiPartUploadMeta;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.PART_UPLOAD_META;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * 分段部分接口异常处理
 *
 * @author xiangzicheng-PC
 * @create 2020/5/18
 * @since 1.0.0
 */
@Log4j2
@SuppressWarnings("unused")
public class PartErrorHandler {
    /**
     * 处理初始化分段上传任务时部分节点未初始化成功的错误
     */
    @HandleErrorFunction(ERROR_INIT_PART_UPLOAD)
    public static Mono<Boolean> errorInitPartUpload(InitPartInfo value, String snapshotLink) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(value.bucket);
        String vnodeId = "";
        try {
            vnodeId = storagePool.getBucketVnodeId(value.bucket, value.object);
        } catch (Exception e) {
            log.error("init part upload error", e);
            if (e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                return Mono.just(false);
            }
        }

        return storagePool.mapToNodeInfo(vnodeId)
                .flatMap(node -> {
                    //因为此时自己节点信息与其他节点不一致，
                    //getInitPartInfo时会触发修复，如果修复成功 这条消息就消费成功。
                    // 修复失败，发送修复失败的消息（进入修复错误的异常处理函数），返回一个 err的initPartInfo对象
                    return PartClient.getInitPartInfo(value.bucket, value.object, value.uploadId, node, null, value.snapshotMark, snapshotLink);
                })
                .map(s -> false);
    }

    /**
     * 处理分段上传时，当前分段的元数据写入失败的异常处理
     */
    @HandleErrorFunction(ERROR_PART_UPLOAD_META)
    public static Mono<Boolean> errorPartUploadMetaData(PartInfo value, String snapshotLink) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(value.bucket);
        String vnodeId = storagePool.getBucketVnodeId(value.bucket, value.object);
        return storagePool.mapToNodeInfo(vnodeId)
                //getInitPartInfo会触发initpartInfo修复方法
                .flatMap(node -> PartClient.getInitPartInfo(value.bucket, value.object, value.uploadId, node, null, value.snapshotMark, snapshotLink))
                .flatMap(rocksInitInfo -> {
                    if (rocksInitInfo.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO)
                            || rocksInitInfo.delete || (rocksInitInfo.snapshotMark != null && rocksInitInfo.isUnView(value.snapshotMark))) {
                        return Mono.just(true);
                    } else {
                        //对象还未合并，重新执行写入
                        return storagePool.mapToNodeInfo(vnodeId)
                                .flatMap(nodeList -> {
                                    String partKey = value.getPartKey(vnodeId);
                                    return PartClient.getPartInfo(value.bucket, value.object, value.uploadId, value.partNum, partKey, nodeList, null, value.snapshotMark, snapshotLink,value.getTmpUpdateQuotaKeyStr())
                                            .flatMap(partInfo -> {
                                                if (partInfo.equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO) || partInfo.delete) {

                                                    return Mono.just(true);
                                                } else {

                                                    return ECUtils.putRocksKey(storagePool, partKey, Json.encode(value),
                                                            PART_UPLOAD_META, ERROR_PART_UPLOAD_META, nodeList, null, snapshotLink);
                                                }
                                            });
                                });
                    }
                })
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_ABORT_MULTI_PART)
    public static Mono<Boolean> abortMultiPartUpload(InitPartInfo initPartInfo, String currentSnapshotMark, Boolean deletePartMetaOnly, List<Integer> ignoreDeletePartNums) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        StoragePool objPool = StoragePoolFactory.getStoragePool(initPartInfo);
        Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(initPartInfo.bucket, initPartInfo.object);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        return bucketPool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketNodeList -> PartClient.abortMultiPartUpload(objPool, bucketNodeList, initPartInfo, null, currentSnapshotMark, deletePartMetaOnly, ignoreDeletePartNums.toArray(new Integer[0])))
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_COMPLETE_MULTI_PART)
    public static Mono<Boolean> completeMultiPartUpload(InitPartInfo initPartInfo, String snapshotLink) {
        String bucket = initPartInfo.bucket;
        String object = initPartInfo.object;
        String versionId = initPartInfo.metaData.versionId;
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        String bucketVnode = bucketPool.getBucketVnodeId(bucket, object);
//        String objVnode = DEFAULT_DATA_POOL.getObjectVnodeId(bucket, object).var1;

        return bucketPool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketNodeList -> ErasureClient.getObjectMetaVersion(bucket, object, versionId, bucketNodeList, null, initPartInfo.snapshotMark, null)
                        .flatMap(curMeta -> {
                            //获取元数据失败，重新发送该消息。
                            if (curMeta.equals(ERROR_META)) {
//                                String storageName = "storage_" + bucketPool.getVnodePrefix();
//                                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(bucketPool.getVnodePrefix());
//                                if (StringUtils.isEmpty(poolQueueTag)) {
//                                    String strategyName = "storage_" + bucketPool.getVnodePrefix();
//                                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                                }
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("initPartInfo", Json.encode(initPartInfo))
                                        .put("poolQueueTag", poolQueueTag);
                                Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));
                                ObjectPublisher.publish(CURRENT_IP, msg, ERROR_COMPLETE_MULTI_PART);
                                return Mono.just(false);
                            }
                            //不能用partUploadId相等判断要恢复，可能已经被覆盖，而是要使用fileName。
                            if (PartUtils.checkPartInfo(initPartInfo, curMeta)) {
                                Inode[] inode = new Inode[1];
                                if (curMeta.inode > 0) {
                                    initPartInfo.metaData.inode = curMeta.inode;
                                    initPartInfo.metaData.cookie = curMeta.cookie;
                                    if (StringUtils.isNotBlank(initPartInfo.metaData.tmpInodeStr)) {
                                        Inode tmpInode = Json.decodeValue(initPartInfo.metaData.tmpInodeStr, Inode.class);
                                        Inode curInode = Json.decodeValue(curMeta.tmpInodeStr, Inode.class);
                                        curInode.setXAttrMap(tmpInode.getXAttrMap());
                                        initPartInfo.metaData.tmpInodeStr = Json.encode(curInode);
                                    }else{
                                        initPartInfo.metaData.tmpInodeStr = curMeta.tmpInodeStr;
                                    }
                                    inode[0] = Json.decodeValue(curMeta.tmpInodeStr, Inode.class);

                                } else if (StringUtils.isNotBlank(initPartInfo.metaData.tmpInodeStr)) {
                                    Inode tmpInode = Json.decodeValue(initPartInfo.metaData.tmpInodeStr, Inode.class);
                                    initPartInfo.metaData.tmpInodeStr = Json.encode(tmpInode);
                                }
                                return PartUtils.completeMultiPart("", initPartInfo, bucketNodeList, null, snapshotLink)
                                        .flatMap(r -> {
                                            if (inode[0] != null && inode[0].getLinkN() > 1) {
                                                return Node.getInstance()
                                                        .emptyUpdate(inode[0].getNodeId(), inode[0].getBucket());
                                            }
                                            return Mono.just(r);
                                        })
                                        .map(s -> false);
                            } else {
                                String initPartSnapshotMark = initPartInfo.snapshotMark;
                                String completePartSnapshotMark = initPartInfo.metaData == null ? initPartInfo.snapshotMark : initPartInfo.metaData.snapshotMark;
                                //如果元数据已经不匹配，说明该key下的对象已覆盖或删除。此时手动删除InitPartInfo和partInfo。。
                                deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, bucketNodeList, false, initPartSnapshotMark, completePartSnapshotMark).subscribe();
                                return Mono.just(true);
                            }
                        }));
    }

    /**
     * 合并分段后将元数据存入es失败的异常处理
     */
    @HandleErrorFunction(ERROR_COMPLETE_MULTI_PART_META_PUT_ES)
    public static Mono<Boolean> completeMultiPartMetaPutEs(EsMeta esMeta) {
        return Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).flatMap(b -> Mono.just(false));
    }

}
