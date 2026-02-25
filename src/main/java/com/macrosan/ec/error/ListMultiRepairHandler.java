package com.macrosan.ec.error;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.macrosan.constants.ServerConstants.SNAPSHOT_LINK;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.error.PartErrorHandler.errorPartUploadMetaData;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * 各种list接口的合并修复处理接口
 *
 * @author fanjunxi
 */
@Log4j2
public class ListMultiRepairHandler {
    @HandleErrorFunction(ERROR_REPAIR_LSFILE)
    public static Mono<Boolean> repairLsfile(List<Tuple3<String, String, String>> counterList) {
        for (Tuple3<String, String, String> tuple3 : counterList) {
            // 将错误信息发送到消息队列，防止并发过大
            String bucketName = tuple3.var1;
            String object = tuple3.var2;
            String versionId = "null".equals(tuple3.var3) ? tuple3.var3 : "";
            MetaData metaData = new MetaData();
            metaData.setBucket(bucketName);
            metaData.setKey(object);
            metaData.setVersionId(versionId);
            SocketReqMsg msg = new SocketReqMsg("", 0);
            msg.put("value", Json.encode(metaData));
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaStoragePool.getVnodePrefix());
            msg.put("poolQueueTag", poolQueueTag);
            ObjectPublisher.basicPublish(CURRENT_IP, msg, ERROR_PUT_OBJECT_META);
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_LSFILE_WITH_FS_QUOTA)
    public static Mono<Boolean> repairLsfileWithFsQuota(List<Tuple3<String, String, String>> counterList, String updateDirList) {
        for (Tuple3<String, String, String> tuple3 : counterList) {
            // 将错误信息发送到消息队列，防止并发过大

            String bucketName = tuple3.var1;
            String object = tuple3.var2;
            String versionId = "null".equals(tuple3.var3) ? tuple3.var3 : "";
            MetaData metaData = new MetaData();
            metaData.setBucket(bucketName);
            metaData.setKey(object);
            metaData.setVersionId(versionId);
            if (StringUtils.isNotBlank(updateDirList)) {
                Inode inode = new Inode();
                inode.getXAttrMap().put(QUOTA_KEY, updateDirList);
                inode.getInodeData().clear();
                metaData.tmpInodeStr = Json.encode(inode);
            }
            SocketReqMsg msg = new SocketReqMsg("", 0);
            msg.put("value", Json.encode(metaData));
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaStoragePool.getVnodePrefix());
            msg.put("poolQueueTag", poolQueueTag);
            ObjectPublisher.basicPublish(CURRENT_IP, msg, ERROR_PUT_OBJECT_META);
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_LSFILE_SNAP)
    public static Mono<Boolean> repairLsfile(List<Tuple2<String, Tuple3<String, String, String>>> counterList, String snapshotLink) {
        for (Tuple2<String, Tuple3<String, String, String>> tuple2 : counterList) {
            Tuple3<String, String, String> tuple3 = tuple2.var2;
            String bucketName = tuple3.var1;
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String object = tuple3.var2;
            String versionId = "null".equals(tuple3.var3) ? tuple3.var3 : "";
            String bucketVnode = storagePool.getBucketVnodeId(bucketName, object);
            storagePool.mapToNodeInfo(bucketVnode).publishOn(DISK_SCHEDULER)
                    .subscribe(nodeList -> ErasureClient.getObjectMetaVersion(bucketName, object, versionId, nodeList, null, tuple2.var1, snapshotLink));
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_LSVERSION)
    public static Mono<Boolean> repairLsVersion(List<Tuple3<String, String, String>> counterList) {
        for (Tuple3<String, String, String> tuple3 : counterList) {
            // 将错误信息发送到消息队列，防止并发过大
            String bucketName = tuple3.var1;
            String object = tuple3.var2;
            String versionId = tuple3.var3;
            MetaData metaData = new MetaData();
            metaData.setBucket(bucketName);
            metaData.setKey(object);
            metaData.setVersionId(versionId);
            SocketReqMsg msg = new SocketReqMsg("", 0);
            msg.put("value", Json.encode(metaData));
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaStoragePool.getVnodePrefix());
            msg.put("poolQueueTag", poolQueueTag);
            ObjectPublisher.basicPublish(CURRENT_IP, msg, ERROR_PUT_OBJECT_META);
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_LSVERSION_SNAP)
    public static Mono<Boolean> repairLsVersion(List<Tuple2<String, Tuple3<String, String, String>>> counterList, String snapshotLink) {
        for (Tuple2<String, Tuple3<String, String, String>> tuple2 : counterList) {
            Tuple3<String, String, String> tuple3 = tuple2.var2;
            String bucketName = tuple3.var1;
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String object = tuple3.var2;
            String versionId = tuple3.var3;
            String bucketVnode = storagePool.getBucketVnodeId(bucketName, object);
            storagePool.mapToNodeInfo(bucketVnode).publishOn(DISK_SCHEDULER)
                    .subscribe(nodeList -> ErasureClient.getObjectMetaVersion(bucketName, object, versionId, nodeList, null, tuple2.var1, snapshotLink));
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_PART_MULTI_LIST)
    public static Mono<Boolean> repairPartMultiList(List<Tuple3<String, String, String>> counterList) {
        for (Tuple3<String, String, String> tuple3 : counterList) {
            String bucketName = tuple3.var1;
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String object = tuple3.var2;
            String uploadId = tuple3.var3;
            String bucketVnode = storagePool.getBucketVnodeId(bucketName, object);
            storagePool.mapToNodeInfo(bucketVnode).publishOn(DISK_SCHEDULER)
                    .subscribe(nodeList -> PartClient.getInitPartInfo(bucketName, object, uploadId, nodeList, null));
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_PART_MULTI_LIST_SNAP)
    public static Mono<Boolean> repairPartMultiListSnap(List<Tuple2<String, Tuple3<String, String, String>>> counterList) {
        for (Tuple2<String, Tuple3<String, String, String>> tuple2 : counterList) {
            Tuple3<String, String, String> tuple3 = tuple2.var2;
            String bucketName = tuple3.var1;
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String object = tuple3.var2;
            String uploadId = tuple3.var3;
            String bucketVnode = storagePool.getBucketVnodeId(bucketName, object);
            storagePool.mapToNodeInfo(bucketVnode).publishOn(DISK_SCHEDULER)
                    .flatMap(nodeList -> {
                        if (tuple2.var1 == null) {
                            return Mono.just("").zipWith(Mono.just(nodeList));
                        }
                        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, SNAPSHOT_LINK)
                                .publishOn(DISK_SCHEDULER)
                                .switchIfEmpty(Mono.just("")).zipWith(Mono.just(nodeList));
                    })
                    .subscribe(t2 -> PartClient.getInitPartInfo(bucketName, object, uploadId, t2.getT2(), null, tuple2.var1, StringUtils.isBlank(t2.getT1()) ? null : t2.getT1()));
        }
        return Mono.just(false);
    }

    @HandleErrorFunction(ERROR_REPAIR_PART_LIST)
    public static Mono<Boolean> repairPartList(List<PartInfo> counterList, String snapshotLink) {
        for (PartInfo partInfo : counterList) {
            errorPartUploadMetaData(partInfo, snapshotLink).subscribeOn(DISK_SCHEDULER).subscribe();
        }
        return Mono.just(false);
    }
}
