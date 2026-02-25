package com.macrosan.ec.error;

import com.macrosan.ec.DelDeleteMark;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.FsQuotaServerHandler;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.params.DelMarkParams;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.macrosan.ec.ErasureClient.getObjectMetaVersionFsQuotaRecover;
import static com.macrosan.ec.ErasureClient.updateMetaData;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.FS_GROUP_QUOTA;
import static com.macrosan.filesystem.quota.FSQuotaConstants.FS_USER_QUOTA;
import static com.macrosan.filesystem.utils.FSQuotaUtils.updateQuotaInfoError0;
import static com.macrosan.message.jsonmsg.Inode.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * @Author: WANG CHENXING
 * @Date: 2023/10/16
 * @Description: FUSE文件系统修复
 */

@Log4j2
public class FsErrorHandler {
    @HandleErrorFunction(value = ERROR_DEL_INODE_META)
    public static Mono<Boolean> setDeleteMark(String bucket, String object, String versionNum, String versionId, String needDeleteInode, String fileCookie, String updateQuotaKeyStr, String value) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        final String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        final long fileCookie0 = StringUtils.isNotEmpty(fileCookie) ? Long.parseLong(fileCookie) : 0;
        final MetaData deleteMark = StringUtils.isNotEmpty(value) ? Json.decodeValue(value, MetaData.class) : null;
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketVnodeList -> {
                    String versionId0 = StringUtils.isNotEmpty(versionId) ? versionId : "null";
                    return Mono.defer(() -> {
                                if (StringUtils.isNotBlank(updateQuotaKeyStr)) {
                                    return getObjectMetaVersionFsQuotaRecover(bucket, object, versionId0, bucketVnodeList, null, null, null, updateQuotaKeyStr, false);
                                }
                                return ErasureClient.getObjectMetaVersion(bucket, object, versionId0, bucketVnodeList, null,
                                        (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, null, res, true));
                            })
                            .flatMap(meta -> {
                                boolean b = deleteMark == null ? meta.isDeleteMark() : meta.isDeleteMark() && deleteMark.inode == meta.inode;
                                if (b) {
                                    return MsObjVersionUtils.versionStatusReactive(bucket)
                                            .flatMap(status -> ErasureClient.setDelMark(new DelMarkParams(bucket, object, versionId, null, bucketVnodeList, status, meta.versionNum)
                                                    .type(ERROR_DEL_INODE_META).needDeleteInode("1".equals(needDeleteInode)).fileCookie(fileCookie0).nodeId(meta.inode)))
                                            .map(r -> r == 1);
                                } else {
                                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                            .put("poolQueueTag", poolQueueTag);
                                    errorMsg
                                            .put("bucket", meta.bucket)
                                            .put("object", meta.key)
                                            .put("versionNum", versionNum)
                                            .put("versionId", versionId)
                                            .put("needDeleteInode", needDeleteInode)
                                            .put("fileCookie", String.valueOf(fileCookie))
                                            .put("value",deleteMark != null ? String.valueOf(deleteMark) : "");
                                    Optional.ofNullable(updateQuotaKeyStr).ifPresent(v -> errorMsg.put("updateQuotaKeyStr", v));
                                    if (ERROR_META.equals(meta)){
                                        ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DEL_INODE_META);
                                        return Mono.just(false);
                                    }
                                    if (deleteMark != null && deleteMark.inode != meta.inode){
                                        Node node = Node.getInstance();

                                        if (node == null){
                                            ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DEL_INODE_META);
                                        }else {
                                            if ("1".equals(needDeleteInode)){
                                                return Node.getInstance().markDeleteInode( bucket, deleteMark.inode)
                                                        .doOnNext(r ->{
                                                            if (ERROR_INODE.equals(r)){
                                                                ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DEL_INODE_META);
                                                            }else if (!NOT_FOUND_INODE.equals(r)){
                                                                deleteMark.versionNum = r.getVersionNum();
                                                                String key = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, null);
                                                                DelDeleteMark.putDeleteKey(key, Json.encode(deleteMark));
                                                            }
                                                        }).map(r -> false);
                                            }
                                        }
                                        return Mono.just(false);
                                    }
                                }
                                return Mono.just(false);
                            });
                })
                .map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_DEL_CHUNK)
    public static Mono<Boolean> repairDelChunk(String key, String value, String lun) {
        return FsUtils.deleteChunkMeta(key, value)
                .map(l -> false);
    }


    @HandleErrorFunction(value = ERROR_PUT_CHUNK)
    public static Mono<Boolean> repairChunk(String key, String value, String lun) {
        ChunkFile chunkFile = Json.decodeValue(value, ChunkFile.class);
        String bucketName = chunkFile.getBucket();
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        final String bucketVnode = metaPool.getBucketVnodeId(bucketName, key);
        Node node = Node.getInstance();
        if (node == null) {
            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaPool.getVnodePrefix());
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("key", key)
                    .put("value", value)
                    .put("lun", lun)
                    .put("poolQueueTag", poolQueueTag);
            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_CHUNK);
            return Mono.just(true);
        }
        return metaPool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> Node.getInstance().getChunk(chunkFile.getNodeId(), bucketName, key))
                .map(b -> false);
    }

    /**
     * 用于文件的异常修复，超时时长设置为30秒，一般类型的方法参数都可以解析到，无需额外添加
     **/
    @HandleErrorFunction(value = ERROR_PUT_INODE)
    public static Mono<Boolean> repairInode(String key, String value, String lun) {
        Inode inode = Json.decodeValue(value, Inode.class);
        String bucketName = inode.getBucket();
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        final String bucketVnode = metaPool.getBucketVnodeId(bucketName, key);
        Node node = Node.getInstance();
        if (node == null) {
            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaPool.getVnodePrefix());
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("key", key)
                    .put("value", value)
                    .put("lun", lun)
                    .put("poolQueueTag", poolQueueTag);
            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_INODE);
            return Mono.just(true);
        }
        return metaPool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> Node.getInstance().emptyUpdate(inode.getNodeId(), bucketName, inode))
                .flatMap(inode1 -> {
                    Map<String, ChunkFile> updateChunk = inode.getChunkFileMap();

                    // 若写入inode时chunk同时写入失败，则主动进行修复
                    if (updateChunk != null && updateChunk.size() > 0) {
                        return Flux.fromStream(updateChunk.keySet().stream())
                                .flatMap(chunkName -> Node.getInstance().getChunk(inode.getNodeId(), bucketName, chunkName), 1)
                                .collectList()
                                .map(l -> false);
                    } else {
                        return Mono.just(false);
                    }
                });
    }

    /**
     * 普通上传修复文件。
     */
    @HandleErrorFunction(value = ERROR_FS_PUT_FILE, timeout = 0L)
    public static Mono<Boolean> repairFsFile(String lun, String bucket, String object, String fileName, String storage,
                                             String versionId, String nodeId, List<Integer> errorChunksList, String fileSize,
                                             SocketReqMsg msg) {
        String fileOffset = msg.get("fileOffset");
        log.debug("【repair】lun:{}, bucket:{}, object:{}, fileName:{}, storage:{}, versionId:{}, errorChunksList:{}, msg:{}", lun, bucket, object, fileName, storage, versionId, errorChunksList, msg);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = InodeUtils.getVnodeFromObjectName(object, bucketPool, bucket);
        Node node = Node.getInstance();
        if (node == null) {
            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_FS_PUT_FILE);
            return Mono.just(true);
        }
        return FsUtils.checkFileInInode(bucket, Long.parseLong(nodeId), fileName, fileSize, fileOffset)
                .flatMap(checkRes -> {
                    if (checkRes.var1 == -1) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_FS_PUT_FILE);
                        return Mono.just(false);
                    } else if (checkRes.var1 == 0) {
                        return Mono.just(true);
                    } else {
                        return Node.getInstance().getInode(bucket, Long.parseLong(nodeId))
                                .flatMap(inode -> {
                                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_FS_PUT_FILE);
                                        return Mono.just(false);
                                    }
                                    StoragePool objPool = StoragePoolFactory.getStoragePool(storage, bucket);

                                    if (!checkRes.var2.equalsIgnoreCase(objPool.getVnodePrefix())) {
                                        //修复数据到加速池，但元数据实际记录已经不是加速池,不进行修复
                                        if (StorageStrategy.getStorageStrategy(bucket).isCache(objPool)) {
                                            return Mono.just(true);
                                        }
                                        //修复数据到数据池，元数据实际记录是加速池, 用数据池信息进行修复
                                    }

                                    String objVnode = objPool.getObjectVnodeId(fileName);
                                    String versionMetaKey = InodeUtils.getVersionMetaKey(object, bucketVnode, bucket, versionId, null);
                                    long size = Long.parseLong(fileSize);
                                    return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> PutErrorHandler.recoverSpecificChunks(objPool, versionMetaKey,
                                            lun, errorChunksList, ERROR_FS_PUT_FILE, fileName, size - 1, msg, nodeList));
                                });
                    }
                });
    }

    /**
     * 先修inode，再修 metaData
     **/
    @HandleErrorFunction(value = ERROR_FS_RENAME)
    public static Mono<Boolean> repairReName(String key, String bucket, String nodeId, String oldObjName, SocketReqMsg msg) {
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = "";
        try {
            bucketVnode = metaPool.getBucketVnodeId(bucket, key);
        } catch (Exception e) {
            log.error("repair nfs rename inode and meta error", e);
            if (e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                return Mono.just(false);
            }
        }

        Node node = Node.getInstance();
        if (node == null) {
            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_FS_RENAME);
            return Mono.just(true);
        }

        return metaPool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> Node.getInstance().emptyUpdate(Long.parseLong(nodeId), bucket)
                        .flatMap(inode -> {
                            if (InodeUtils.isError(inode)){
                                return ErasureClient.getObjectMetaVersion(bucket, oldObjName, inode.getVersionId(), nodeList, null);
                            }
                            return ErasureClient.getObjectMetaVersion(bucket, inode.getObjName(), inode.getVersionId(), nodeList, null)
                                    .flatMap(metaData -> ErasureClient.getObjectMetaVersion(bucket, oldObjName, inode.getVersionId(), nodeList, null));
                        })
                )
                .map(b -> false);
    }

    /**
     * cookie以metaData中存放的数值为准
     **/
    @HandleErrorFunction(value = ERROR_PUT_COOKIE)
    public static Mono<Boolean> repairCookie(String cookieKey, String value, String lun) {
        Inode cookieInode = Json.decodeValue(value, Inode.class);
        String bucketName = cookieInode.getBucket();
        return FsUtils.findCookie(bucketName, cookieInode.getCookie())
                .map(s -> false);
    }

    @HandleErrorFunction(value = ERROR_UPDATE_QUOTA_INFO)
    public static Mono<Boolean> repairPutQuotaInfo(String lun, String bucket, String dirNodeId, String num, String cap, String config) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        FSQuotaConfig fsQuotaConfig = Json.decodeValue(config, FSQuotaConfig.class);
        long dirNodeId0 = fsQuotaConfig.getNodeId();
        log.debug("repairQuotaInfo bucketVnode:{}, lun:{}, bucket:{}, dirName:{}, num:{}, cap:{},config:{}", bucketVnode, lun, bucket, dirNodeId0, num, cap, config);
        String finalLunName = lun.split("@")[1];
        return pool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    for (Tuple3<String, String, String> node : nodeList) {
                        if (node.var2.equals(finalLunName)) {
                            int id_ = 0;
                            if (fsQuotaConfig.getQuotaType() == FS_USER_QUOTA) {
                                id_ = fsQuotaConfig.getUid();
                            }
                            if (fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA) {
                                id_ = fsQuotaConfig.getGid();
                            }
                            String quotaBucketKey = FSQuotaUtils.getQuotaTypeKey(fsQuotaConfig);
                            return updateQuotaInfoError0(bucket, dirNodeId0, bucketVnode, finalLunName, Long.parseLong(cap), Long.parseLong(num), fsQuotaConfig.quotaType, id_, quotaBucketKey)
                                    .map(b -> false);
                        }
                    }
                    return Mono.just(false);
                });
    }

    @HandleErrorFunction(value = ERROR_DEL_QUOTA_INFO)
    public static Mono<Boolean> repairDelQuotaInfo(String lun, String bucket, String dirNodeId, String type, String id) {
        log.debug("repairDelQuotaInfo bucket:{},dirName:{},type:{},lun:{},id:{}", bucket, dirNodeId, type, lun, id);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        String finalLunName = lun.split("@")[1];
        int type0 = Integer.parseInt(type);
        int id0 = Integer.parseInt(id);
        long dirNodeId0 = Long.parseLong(dirNodeId);
        return pool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    for (Tuple3<String, String, String> node : nodeList) {
                        if (node.var2.equals(finalLunName)) {
                            try {
                                FsQuotaServerHandler.delQuota(bucket, dirNodeId0, finalLunName, bucketVnode, type0, id0);
                            } catch (RocksDBException e) {
                                log.error("repairDelQuotaInfo error bucket:{},dirName:{},type:{},lun:{},id:{}", bucket, dirNodeId0, type, finalLunName, id, e);
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    return Mono.just(false);
                });
    }
}
