package com.macrosan.ec.error;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.GET_OBJECT_CANCELED;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.UnsyncRecordDir;
import static com.macrosan.ec.ErasureClient.getCredential;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.CREATE_S3_INODE_TIME;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.FSQuotaUtils.addQuotaInfoToS3Inode;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.ERROR_AGGREGATION_META;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.NOT_FOUND_AGGREGATION_META;
import static com.macrosan.message.jsonmsg.Credential.ERROR_STS_TOKEN;
import static com.macrosan.message.jsonmsg.Credential.NOT_FOUND_STS_TOKEN;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.ERROR_UNSYNC_RECORD;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.NOT_FOUND_UNSYNC_RECORD;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.ERROR_COMPLETE_PART;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.snapshot.utils.SnapshotUtil.getDataMergeMapping;


@Log4j2
public class PutErrorHandler {
    @HandleErrorFunction(value = OVER_WRITE)
    public static Mono<Boolean> overWrite(String storage, String bucket, String object, SocketReqMsg msg) {

        HashSet<String> needDelete = new HashSet<>();
        HashSet<String> dupDelete = new HashSet<>();
        HashSet<String> aggregateDelete = new HashSet<>();
        Set<StoragePool> storagePools = new HashSet<>();
        if (StringUtils.isNotEmpty(storage)) {
            storagePools = Collections.singleton(StoragePoolFactory.getStoragePool(storage, bucket));
        } else {
            String storagesStr = msg.get("storages");
            if (storagesStr != null) {
                Set<String> storages = Json.decodeValue(storagesStr, new com.fasterxml.jackson.core.type.TypeReference<Set<String>>() {
                });
                storagePools = storages
                        .stream()
                        .map(storagePrefix -> StoragePoolFactory.getStoragePool(storagePrefix, bucket))
                        .collect(Collectors.toSet());
            }
        }
        for (StoragePool storagePool : storagePools) {
            for (int i = 0; i < storagePool.getK() + storagePool.getM(); i++) {
                String overWriteStr = msg.get("overWrite" + i);
                if (overWriteStr != null) {
                    String[] overWriteFiles = Json.decodeValue(overWriteStr, String[].class);
                    Collections.addAll(needDelete, overWriteFiles);
                }
            }
        }

        if (needDelete.isEmpty()) {
            return Mono.just(true);
        }

        String fileName = needDelete.iterator().next();
        if (StringUtils.isEmpty(fileName)) {
            return Mono.just(true);
        }
        boolean hasDup = false;
        for (String filename : needDelete) {
            if (filename.startsWith("?")) {
                aggregateDelete.add(filename);
            } else if (!filename.contains("^")) {
                dupDelete.add(filename);
            } else {
                hasDup = true;
            }
        }
        if (!aggregateDelete.isEmpty()) {
            return Flux.fromIterable(aggregateDelete)
                    .flatMap(file -> {
                        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool("");
                        String vnode = Utils.getVnode(file);
                        return metaStoragePool.mapToNodeInfo(vnode)
                                .flatMap(aNodeList -> AggregateFileClient.freeAggregationSpace(file, aNodeList));
                    })
                    .collectList()
                    .map(b -> false);
        }
        if (hasDup) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
            Set<StoragePool> finalStoragePools = storagePools;
            return ErasureClient.deleteDedupObjectFile(metaStoragePool, needDelete.toArray(new String[0]), null, false)
                    .flatMap(b -> {
                        if (b && needDelete.size() > dupDelete.size()) {
                            return Flux.fromIterable(finalStoragePools)
                                    .flatMap(storagePool -> ErasureClient.deleteObjectFile(storagePool, dupDelete.toArray(new String[0]), null))
                                    .collectList()
                                    .thenReturn(true);
                        }
                        return Mono.just(b);
                    })
                    .map(b -> false);
        }
        return Flux.fromIterable(storagePools)
                .flatMap(storagePool -> ErasureClient.deleteObjectFile(storagePool, needDelete.toArray(new String[0]), null))
                .collectList()
                .map(b -> false);
    }


    /**
     * 普通上传修复文件。
     */
    @HandleErrorFunction(value = ERROR_PUT_OBJECT_FILE, timeout = 0L)
    public static Mono<Boolean> recoverPutFile(String lun, String bucket, String object, String fileName, String storage,
                                               String versionId, List<Integer> errorChunksList, String updateEC, String fileSize,
                                               SocketReqMsg msg, String snapshotMark) {
        String fileOffset = msg.get("fileOffset");
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = InodeUtils.getVnodeFromObjectName(object, bucketPool, bucket);
        return bucketPool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketNodeList -> {
                    if (object.startsWith(ROCKS_INODE_PREFIX)) {
                        MetaData metaData = new MetaData();
                        metaData.bucket = bucket;
                        metaData.inode = Long.parseLong(Inode.getNodeIdFromInodeKey(object));
                        return Mono.just(metaData);
                    }
                    return ErasureClient.getObjectMetaVersion(bucket, object, versionId, bucketNodeList, null, snapshotMark, null);
                })
                .timeout(Duration.ofMillis(30_000))
                .flatMap(curMeta -> {
                    // 检查已有的metadata，其中的fileName如果与此次上传数据所属对象的fileName相同，表示文件还存在，可进行数据恢复；
                    // 为ERROR_META，也不进行恢复操作，但要重新发布。此时dstIp不变，但发布在本地的队列上。
                    // 若为NOT_FOUND_META，表示文件已不存在，不用进行数据恢复操作；
                    if (ERROR_META.equals(curMeta)) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_OBJECT_FILE);
                        return Mono.just(false);
                    }
                    if (curMeta.inode > 0) {
                        return FsUtils.checkFileInInode(bucket, curMeta.inode, fileName, fileSize, fileOffset)
                                .flatMap(checkRes -> {
                                    if (checkRes.var1 == -1) {
                                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_OBJECT_FILE);
                                        return Mono.just(false);
                                    } else if (checkRes.var1 == 0) {
                                        return Mono.just(true);
                                    } else {
                                        StoragePool objPool;
                                        if (storage == null) {
                                            objPool = StoragePoolFactory.getStoragePool(curMeta);
                                        } else {
                                            objPool = StoragePoolFactory.getStoragePool(storage, bucket);
                                        }
                                        if (!checkRes.var2.equalsIgnoreCase(objPool.getVnodePrefix())) {
                                            //修复数据到加速池，但元数据实际记录已经不是加速池,不进行修复
                                            if (StorageStrategy.getStorageStrategy(bucket).isCache(objPool)) {
                                                return Mono.just(true);
                                            }
                                            //修复数据到数据池，元数据实际记录是加速池, 用数据池信息进行修复
                                        }
                                        String objVnode = objPool.getObjectVnodeId(fileName);
                                        String versionMetaKey = InodeUtils.getVersionMetaKey(object, bucketVnode, bucket, versionId, snapshotMark);
                                        //inodeData可能会因为覆盖等操作导致记录的size和预期不符
                                        //inode格式的endIndex只能从原始的ERROR_MSG中获得
                                        long size = Long.parseLong(fileSize);
                                        return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                                lun, errorChunksList, ERROR_PUT_OBJECT_FILE, fileName, size - 1, msg, nodeList));
                                    }
                                });
                    } else if (fileName.equals(curMeta.fileName) && !curMeta.deleteMark) {
                        StoragePool objPool;
                        if (storage == null) {
                            objPool = StoragePoolFactory.getStoragePool(curMeta);
                        } else {
                            objPool = StoragePoolFactory.getStoragePool(storage, bucket);
                        }

                        if (!objPool.getVnodePrefix().equalsIgnoreCase(curMeta.getStorage())) {
                            //修复数据到加速池，但元数据实际记录已经不是加速池,不进行修复
                            if (StorageStrategy.getStorageStrategy(bucket).isCache(objPool)) {
                                return Mono.just(true);
                            }

                            //修复数据到数据池，元数据实际记录是加速池, 用数据池信息进行修复
                        }

                        String objVnode = objPool.getObjectVnodeId(curMeta);
                        String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);

                        return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                lun, errorChunksList, ERROR_PUT_OBJECT_FILE, fileName, curMeta.endIndex, msg, nodeList));
                    } else if (curMeta.equals(ERROR_META)) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_OBJECT_FILE);
                        return Mono.just(false);
                    } else if (!curMeta.equals(NOT_FOUND_META) && !fileName.equals(curMeta.fileName) && !curMeta.deleteMark && !curMeta.storage.equals(storage)
                            && StringUtils.isNotEmpty(updateEC) && updateEC.equals(curMeta.storage)) {
                        StoragePool objPool;
                        if (storage == null) {
                            objPool = StoragePoolFactory.getStoragePool(curMeta);
                        } else {
                            objPool = StoragePoolFactory.getStoragePool(storage, bucket);
                        }

                        if (!objPool.getVnodePrefix().equalsIgnoreCase(curMeta.getStorage())) {
                            //修复数据到加速池，但元数据实际记录已经不是加速池,不进行修复
                            if (StorageStrategy.getStorageStrategy(bucket).isCache(objPool)) {
                                return Mono.just(true);
                            }

                            //修复数据到数据池，元数据实际记录是加速池, 用数据池信息进行修复
                        }

                        String objVnode = objPool.getObjectVnodeId(fileName);
                        String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);

                        return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                lun, errorChunksList, ERROR_PUT_OBJECT_FILE, fileName, curMeta.endIndex, msg, nodeList));
                    } else if (snapshotMark != null && (curMeta.equals(NOT_FOUND_META) || curMeta.deleteMark)) {
                        // 可能由于快照删除，元数据被迁移到最新快照下了
                        return getDataMergeMapping(bucket)
                                .map(mapping -> {
                                    if (snapshotMark.compareTo(mapping) >= 0) {
                                        return true;
                                    }
                                    // 正在迁移，则去迁移目标下寻找
                                    msg.put("snapshotMark", mapping);
                                    ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_OBJECT_FILE);
                                    return true;
                                });

                    } else {
                        return Mono.just(true);
                    }
                });
    }

    @HandleErrorFunction(value = ERROR_PART_UPLOAD_FILE, timeout = 0L)
    public static Mono<Boolean> recoverPartUploadFile(String storage, String lun, String bucket, String object, String versionId, String uploadId, String partNum,
                                                      String fileName, String endIndex, List<Integer> errorChunksList, String updateEC, SocketReqMsg msg, String snapshotMark, String initSnapshotMark, String quotaDirList) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, bucket);
        String objVnode = storagePool.getObjectVnodeId(fileName);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = bucketPool.getBucketVnodeId(bucket, object);
        return bucketPool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketNodeList -> PartUtils.hasPartInfoOrMeta(bucket, object, fileName, uploadId, partNum, bucketNodeList, versionId, storage, updateEC, snapshotMark, initSnapshotMark))
                .timeout(Duration.ofMillis(30_000))
                .flatMap(b -> {
                            // 如果meta或partInfo含任意记录，则进行数据恢复。否则直接消费该条消息.
                            if (b) {
                                return storagePool.mapToNodeInfo(objVnode)
                                        .flatMap(nodeList -> recoverSpecificChunks(storagePool, null, lun, errorChunksList, ERROR_PART_UPLOAD_FILE,
                                                fileName, Long.parseLong(endIndex), msg, nodeList));
                            } else if (snapshotMark != null) {
                                return getDataMergeMapping(bucket)
                                        .map(mapping -> {
                                            if (snapshotMark.compareTo(mapping) >= 0) {
                                                return true;
                                            }
                                            // 正在迁移，则去迁移目标下寻找
                                            msg.put("snapshotMark", mapping);
                                            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PART_UPLOAD_FILE);
                                            return true;
                                        });
                            } else {
                                //额外判断inode>0时的chunk
                                return bucketPool.mapToNodeInfo(bucketVnode)
                                        .flatMap(bucketNodeList -> {
                                            if (StringUtils.isNotBlank(quotaDirList)) {
                                                return ErasureClient.getObjectMetaVersionFsQuotaRecover(bucket, object, versionId, bucketNodeList, null, snapshotMark, null, quotaDirList, false);
                                            }
                                            return ErasureClient.getObjectMetaVersion(bucket, object, versionId, bucketNodeList, null, snapshotMark, null);
                                        })
                                        .timeout(Duration.ofMillis(30_000))
                                        .flatMap(curMeta -> {
                                            if (ERROR_META.equals(curMeta)) {
                                                ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PART_UPLOAD_FILE);
                                                return Mono.just(false);
                                            }
                                            if (curMeta.inode > 0) {
                                                return FsUtils.checkFileInInode(bucket, curMeta.inode, fileName, "", "")
                                                        .flatMap(checkRes -> {
                                                            if (checkRes.var1 == -1) {
                                                                ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PART_UPLOAD_FILE);
                                                                return Mono.just(false);
                                                            } else if (checkRes.var1 == 0) {
                                                                return Mono.just(true);
                                                            } else {
                                                                return storagePool.mapToNodeInfo(objVnode)
                                                                        .flatMap(nodeList -> recoverSpecificChunks(storagePool, null, lun, errorChunksList, ERROR_PART_UPLOAD_FILE,
                                                                                fileName, Long.parseLong(endIndex), msg, nodeList));
                                                            }
                                                        });
                                            } else {
                                                return Mono.just(true);
                                            }
                                        });
                            }
                        }
                );
    }

    public static Mono<Boolean> recoverFileChunks(String suffix, List<Integer> toUploadChunkList,
                                                  String metaData, SocketReqMsg msg, List<Tuple3<String, String, String>> nodeList) {
        ArrayList<Tuple3<String, String, String>> newNodeList = new ArrayList<Tuple3<String, String, String>>();
        for (int i = 0; i < toUploadChunkList.size(); i++) {
            Integer index = toUploadChunkList.get(i);
            newNodeList.add(nodeList.get(index));
        }
        List<Integer> finishChunkList = new ArrayList<>();
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(suffix, metaData, newNodeList);
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_FILE_META, String.class, newNodeList);
        responseInfo.responses.subscribe(s -> {
            if (s.var2.equals(SUCCESS)) {
                if (!"0".equals(s.var3)) {
                    finishChunkList.add(toUploadChunkList.get(s.var1));
                }
            }
        });
        if (toUploadChunkList.size() == finishChunkList.size()) {
            return Mono.just(true);
        }
        return Mono.just(false);
    }

    public static Mono<Boolean> recoverSpecificChunks(StoragePool storagePool, String versionMetaKey, String lun, List<Integer> toUploadChunkList, ErrorConstant.ECErrorType errorType,
                                                      String fileName, long endIndex, SocketReqMsg msg,
                                                      List<Tuple3<String, String, String>> nodeList) throws RuntimeException {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Set<Integer> newErrorChunkList = new HashSet<>();

        ResponseInfo<String> responseInfo = recoverSpecificChunks(storagePool, versionMetaKey, toUploadChunkList, fileName, endIndex, msg, nodeList);
        responseInfo.responses
                .timeout(Duration.ofMinutes(10))
                .subscribe(s -> {
                            if (s.var2.equals(ERROR)) {
                                newErrorChunkList.add(s.var1);
                            }
                        },
                        e -> {
                            log.error("recover specific chunk error, ", e);
                            res.onError(e);
                        },
                        () -> {
                            //返回成功数量与待上传数据块数量相同，消费成功；否则继续发布未返回成功上传的数据块
                            if (responseInfo.successNum == toUploadChunkList.size()) {
                                res.onNext(true);
                            } else {
                                // 由于使用ECUtils.publishEcError上传的消息，consumer中的磁盘检查只检查了本节点（需要恢复数据的节点）的第一个lun
                                // 如果newErrorList中不包含此盘上的数据块，表示本节点的数据块已恢复，磁盘状态已正常，返回true，表示允许恢复磁盘状态。
                                boolean recovered = !newErrorChunkList.contains(ECUtils.getIndexByLunAndIp(nodeList, CURRENT_IP, lun));
                                res.onNext(recovered);
                                //重建修复磁盘被移除不再发布消息
                                if (errorType == RECOVER_DISK_FILE) {
                                    return;
                                }
                                // 若存在未上传成功的数据块则继续发布。
                                // todo f 若该条消息相关的disk已修复，按正常流程publish（判断新的出错盘的状态），否则直接publish，不确认盘状态，防止重复unavailable
//                                String storageName = "storage_" + storagePool.getVnodePrefix();
//                                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                                if (StringUtils.isEmpty(poolQueueTag)) {
//                                    String strategyName = "storage_" + storagePool.getVnodePrefix();
//                                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                                }
                                msg.put("poolQueueTag", poolQueueTag);
                                msg.put("errorChunksList", Json.encode(new ArrayList<>(newErrorChunkList)));
                                ECUtils.publishEcError(responseInfo.res, nodeList, msg, errorType);
                            }
                        }
                );
        return res;
    }


    /**
     * 按toUploadChunkList中的索引恢复特定数据块。
     *
     * @param toUploadChunkList 保存待上传的数据块索引
     * @param nodeList          根据objectVnode获得到的nodeList
     * @return 【响应结果，nodeList(可用于后续publish)】
     */
    private static ResponseInfo<String> recoverSpecificChunks(StoragePool storagePool, String versionMetaKey, List<Integer> toUploadChunkList,
                                                              String fileName, long endIndex, SocketReqMsg reqMsg,
                                                              List<Tuple3<String, String, String>> nodeList) {
        String crypto = reqMsg.get("crypto");
        String secretKey = reqMsg.get("secretKey");
        String flushStamp = reqMsg.get("flushStamp");
        String lastAccessStamp = reqMsg.get("lastAccessStamp");
        String fileOffset = reqMsg.get("fileOffset");
        //获取原始数据字节流，放入ecEncodeHanlder进行encode，生成dataFluxes，dataFluxes包含k+m个数据流
        Encoder ecEncodeHandler = storagePool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        AtomicBoolean getObjectCompleted = new AtomicBoolean(false);
        ECUtils.getObject(storagePool, fileName, false, 0, endIndex, endIndex + 1, nodeList, streamController, null, null)
                .doOnError(e -> {
                    getObjectCompleted.set(true);
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(() -> {
                    getObjectCompleted.set(true);
                    ecEncodeHandler.complete();
                })
                .subscribe(ecEncodeHandler::put);

        List<UnicastProcessor<Payload>> publisher = toUploadChunkList.stream()
                .map(i -> {
                            SocketReqMsg msg = new SocketReqMsg("", 0)
                                    .put("metaKey", versionMetaKey)
                                    .put("recover", "1")
                                    .put("fileName", fileName)
                                    .put("lun", nodeList.get(i).var2)
                                    .put("vnode", nodeList.get(i).var3)
                                    .put("compression", storagePool.getCompression());

                            CryptoUtils.putCryptoInfoToMsg(crypto, secretKey, msg);
                            if (flushStamp != null) {
                                msg.put("flushStamp", flushStamp);
                            }
                            if (StringUtils.isNotEmpty(lastAccessStamp)) {
                                msg.put("lastAccessStamp", lastAccessStamp);
                            }
                            if (StringUtils.isNotEmpty(fileOffset)) {
                                msg.put("fileOffset", fileOffset);
                            }
                            return msg;
                        }
                )
                .map(msg0 -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create();
                    processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));
                    return processor;
                })
                .collect(Collectors.toList());
        for (int i = 0; i < toUploadChunkList.size(); i++) {
            int index = i;
            int errorChunkIndex = toUploadChunkList.get(i);
            ecEncodeHandler.data()[errorChunkIndex].subscribe(bytes -> {
                        publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        publisher.get(index).onComplete();
                    });
        }

        Set<Integer> errorSet = new HashSet<>(toUploadChunkList);
        List<Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, String>>> noPutList = new LinkedList<>();
        for (int i = 0; i < ecEncodeHandler.data().length; i++) {
            if (!errorSet.contains(i)) {
                Tuple3<Integer, ErasureServer.PayloadMetaType, String> res = new Tuple3<>(i, CONTINUE, null);
                UnicastProcessor<Tuple3<Integer, ErasureServer.PayloadMetaType, String>> processor = UnicastProcessor.create();
                noPutList.add(processor);
                ecEncodeHandler.data()[i].doFinally(s -> {
                    processor.onNext(res);
                    processor.onComplete();
                }).doOnError(e -> {
                }).subscribe();
            }
        }
        Set<Integer> putObjectErrorSet = new HashSet<>();
        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList, toUploadChunkList, noPutList);
        responseInfo.responses = responseInfo.responses.doOnNext(tuple3 -> {
            if (tuple3.var2 == ErasureServer.PayloadMetaType.ERROR) {
                putObjectErrorSet.add(tuple3.var1);
                if (putObjectErrorSet.size() >= errorSet.size() && !getObjectCompleted.get()) {
                    // 需要修复得数据块全部put失败 则停止get流程
                    streamController.onError(new MsException(GET_OBJECT_CANCELED, "put_object failed,  cancel the get data process"));
                    return;
                }
            }
            if (tuple3.var1.equals(toUploadChunkList.get(toUploadChunkList.size() - 1))) {
                for (int j = 0; j < storagePool.getK(); j++) {
                    streamController.onNext(-1L);
                }
            }

        });

        return responseInfo;
    }

    /**
     * put元数据失败修复。
     * 此处不使用putObjectMeta而是用getObjectMeta（数据不一致时有updateMeta的处理），防止所有在线节点返回NOT_FOUND时还进行update。
     * 注意：由于使用getObjectMeta此时消费掉生产端ERROR_PUT_OBJECT_META_QUEUE中的消息后，还会向消费端ERROR_PUT_OBJECT_META_QUEUE中publish一次。
     */
    @HandleErrorFunction(ERROR_PUT_OBJECT_META)
    public static Mono<Boolean> recoverMeta(MetaData value, String snapshotLink, String vnode) {
        String bucketName = value.bucket;
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = "";
        try {
            bucketVnode = StringUtils.isNotBlank(vnode) ? vnode : storagePool.getBucketVnodeId(bucketName, value.key);
        } catch (Exception e) {
            log.error("recoverMeta error", e);
            if (e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                return Mono.just(false);
            }
        }
        String[] fsQuotaStr = new String[1];
        if (StringUtils.isNotBlank(value.tmpInodeStr)) {
            Inode inode = Json.decodeValue(value.tmpInodeStr, Inode.class);
            if (StringUtils.isNotBlank(inode.getXAttrMap().get(QUOTA_KEY))) {
                fsQuotaStr[0] = inode.getXAttrMap().get(QUOTA_KEY);
            }
        }
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    if (StringUtils.isNotBlank(fsQuotaStr[0])) {
                        return ErasureClient.getObjectMetaVersionFsQuotaRecover(value.bucket, value.key, value.versionId, nodeList, null, value.snapshotMark, snapshotLink, fsQuotaStr[0], false);
                    }
                    return ErasureClient.getObjectMetaVersion(value.bucket, value.key, value.versionId, nodeList, null, value.snapshotMark, snapshotLink);
                })
                .flatMap(res -> {
                    if (res.inode > 0) {
                        Inode oldInode = null;
                        Inode newInode = null;
                        if (StringUtils.isNotBlank(value.tmpInodeStr)) {
                            oldInode = Json.decodeValue(value.tmpInodeStr, Inode.class);
                        }
                        if (StringUtils.isNotBlank(res.tmpInodeStr)) {
                            newInode = Json.decodeValue(res.tmpInodeStr, Inode.class);
                        }

                        if (oldInode != null && newInode != null) {
                            newInode.setXAttrMap(oldInode.getXAttrMap());
                            return Node.getInstance().emptyUpdate(res.inode, res.bucket, newInode);
                        } else {
                            if (value.inode == 0
                                    && newInode != null
                                    && newInode.getUid() > 0
                                    && StringUtils.isNotBlank(newInode.getXAttrMap().get(CREATE_S3_INODE_TIME))
                            ) {
                                addQuotaInfoToS3Inode(newInode);
                                return Node.getInstance().emptyUpdate(res.inode, res.bucket, newInode);
                            }
                            return Node.getInstance().emptyUpdate(res.inode, res.bucket);
                        }
                    }
                    return Mono.just(res);
                })
                //一般队列消费返回false无影响；缓冲队列消费，后续磁盘恢复时将手动更改磁盘状态，返回false保证不出现快速无限循环。
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_PUT_DEDUPLICATE_META)
    public static Mono<Boolean> recoverDedupMeta(DedupMeta value, String realKey) {
        String etag = value.getEtag();
//        log.info("realKey = {}", realKey);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(etag);
        final String bucketVnode = storagePool.getBucketVnodeId(etag);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.getDeduplicateMeta(etag, value.getStorage(), realKey, nodeList, null))
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_DELETE_DEDUPLICATE_META)
    public static Mono<Boolean> rollBackDedupMeta(DedupMeta value, String realKey) {
        String etag = value.getEtag();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(etag);
        final String bucketVnode = storagePool.getBucketVnodeId(etag);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.deleteDedupMeta(storagePool, realKey, value, nodeList))
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_PUT_OBJECT_META_WITH_ES_META)
    public static Mono<Boolean> recoverMetaAndEsMeta(SocketReqMsg msg) {
        //处理上传对象时上传对象元数据成功数<k，导致后续没上传元数据至rocksDB
        MetaData value = Json.decodeValue(msg.get("metaData"), MetaData.class);
        EsMeta esMetaData = Json.decodeValue(msg.get("esMeta"), EsMeta.class);
        String bucketName = value.bucket;
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(value.bucket);

        final String bucketVnode = storagePool.getBucketVnodeId(bucketName, value.key);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersion(value.bucket, value.key, value.versionId, nodeList, null, value.snapshotMark, null))
                .flatMap(metaData -> {
                    return Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMetaData, false)).flatMap(b -> Mono.just(false));
                    //一般队列消费返回false无影响；缓冲队列消费，后续磁盘恢复时将手动更改磁盘状态，返回false保证不出现快速无限循环。
                });
    }

    /**
     * put失败回退
     */
    @HandleErrorFunction(value = ERROR_ROLL_BACK_FILE)
    public static Mono<Boolean> rollBackPutObject(String storage, String bucket, String object, String fileName) {
        if (fileName.contains("^")) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String vnode = fileName.split("\\/")[0].substring(1);
            return metaStoragePool.mapToNodeInfo(vnode)
                    .flatMap(curList -> ErasureClient.deleteDedupObjectFile(metaStoragePool, fileName, curList, null, false).var1)
                    .map(b -> false);
        }
        StoragePool pool = StoragePoolFactory.getStoragePool(storage, bucket);
        return pool.mapToNodeInfo(pool.getObjectVnodeId(fileName))
                .flatMap(curList -> ErasureClient.deleteObjectFile(pool, fileName, curList).var1)
                .map(b -> false);
    }


    @HandleErrorFunction(ERROR_PUT_BUCKET_INFO)
    public static Mono<Boolean> recoverBucketInfo(BucketInfo value) {
        String bucketName = value.bucketName;
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        final String bucketVnode = pool.getBucketVnodeId(bucketName);
        return pool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketVnodeList -> BucketSyncSwitchCache.isSyncSwitchOffMono(bucketName)
                        .flatMap(isSyncSwitchOff -> ErasureClient.updateBucketInfo(value.getBucketKey(bucketVnode), value, bucketVnodeList, isSyncSwitchOff)))
                //一般队列消费返回false无影响；缓冲队列消费，后续磁盘恢复时将手动更改磁盘状态，返回false保证不出现无限循环。
                .map(s -> false);
    }

    @HandleErrorFunction(ERROR_MERGE_TMP_BUCKET_INFO)
    public static Mono<Boolean> mergeTmpBucketInfo(String vnode, String bucket) {
        return ErasureClient.mergeTempBucketInfo(vnode, bucket).map(b -> false);
    }

    @HandleErrorFunction(value = ERROR_PUT_FILE_META, timeout = 0L)
    public static Mono<Boolean> recoverFileMeta(String lun, String bucket, String object, String fileName, String suffix, String endIndex, String storage,
                                                String versionId, List<Integer> errorChunksList, SocketReqMsg msg, String snapshotMark) {

        if (StringUtils.isEmpty(fileName)) {
            return ErasureClient.updateFileMeta(Json.decodeValue(msg.get("value"), MetaData.class), msg.get("key"), null, null, null)
                    .map(b -> false);
        }
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = bucketPool.getBucketVnodeId(bucket, object);
        MetaData metaNew = Json.decodeValue(msg.get("value"), MetaData.class);
        StoragePool storagePools = StoragePoolFactory.getStoragePool(storage, bucket);
        String obVnode = storagePools.getObjectVnodeId(fileName);
        return storagePools.mapToNodeInfo(obVnode)
                .flatMap(nodeList -> {
                    if (fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        String newChunkKey = fileName.split(ROCKS_FILE_META_PREFIX)[0] + suffix;
                        Node instance = Node.getInstance();
                        if (instance == null) {
                            ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_FILE_META);
                            return Mono.just(true);
                        }
                        return instance.getChunk(newChunkKey)
                                .map(l -> true);
                    }
                    return recoverFileChunks(suffix, errorChunksList, msg.get("value"), msg, nodeList);
                })
                .flatMap(b -> {
                    if (b) {
                        return Mono.just(true);
                    }
                    return bucketPool.mapToNodeInfo(bucketVnode)
                            .flatMap(bucketNodeList -> ErasureClient.getObjectMetaVersion(bucket, object, versionId, bucketNodeList, null, snapshotMark, null))
                            .timeout(Duration.ofMillis(30_000))
                            .flatMap(curMeta -> {
                                if (ERROR_META.equals(curMeta)) {
                                    ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_FILE_META);
                                    return Mono.just(false);
                                }
                                if (curMeta.getPartInfos() != null) {
                                    StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, bucket);
                                    String objVnode = storagePool.getObjectVnodeId(fileName);
                                    String copyFileName = fileName.split(ROCKS_FILE_META_PREFIX)[0] + suffix;
                                    boolean isExit = false;
                                    if (curMeta.inode > 0 && !curMeta.deleteMark) {
                                        return FsUtils.checkFileInInode(bucket, curMeta.inode, copyFileName, "", "")
                                                .flatMap(checkRes -> {
                                                    if (checkRes.var1 == -1) {
                                                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_FILE_META);
                                                        return Mono.just(false);
                                                    } else if (checkRes.var1 == 0) {
                                                        return Mono.just(true);
                                                    } else {
                                                        return storagePool.mapToNodeInfo(objVnode)
                                                                .flatMap(nodeList -> recoverSpecificChunks(storagePool, null, lun, errorChunksList, ERROR_PUT_FILE_META,
                                                                        copyFileName, metaNew.endIndex, msg, nodeList));
                                                    }
                                                });
                                    }
                                    for (PartInfo partInfo : curMeta.getPartInfos()) {
                                        if (copyFileName.equals(partInfo.fileName)) {
                                            isExit = true;
                                        }
                                    }
                                    //文件不存在,直接消费成功
                                    if (!isExit) {
                                        return Mono.just(true);
                                    }
                                    return storagePool.mapToNodeInfo(objVnode)
                                            .flatMap(nodeList -> recoverSpecificChunks(storagePool, null, lun, errorChunksList, ERROR_PUT_FILE_META,
                                                    copyFileName, metaNew.endIndex, msg, nodeList));
                                }
                                // 检查已有的metadata，其中的fileName如果与此次上传数据所属对象的fileName相同，表示文件还存在，可进行数据恢复；
                                // 为ERROR_META，也不进行恢复操作，但要重新发布。此时dstIp不变，但发布在本地的队列上。
                                // 若为NOT_FOUND_META，表示文件已不存在，不用进行数据恢复操作；
                                String copyFileName = fileName.split(ROCKS_FILE_META_PREFIX)[0] + suffix;
                                if (copyFileName.equals(curMeta.fileName) && !curMeta.deleteMark) {
                                    StoragePool objPool;
                                    if (storage == null) {
                                        objPool = StoragePoolFactory.getStoragePool(curMeta);
                                    } else {
                                        objPool = StoragePoolFactory.getStoragePool(storage, bucket);
                                    }

                                    if (!objPool.getVnodePrefix().equalsIgnoreCase(curMeta.getStorage())) {
                                        //修复数据到加速池，但元数据实际记录已经不是加速池,不进行修复
                                        if (StorageStrategy.getStorageStrategy(bucket).isCache(objPool)) {
                                            return Mono.just(true);
                                        }

                                        //修复数据到数据池，元数据实际记录是加速池, 用数据池信息进行修复
                                    }

                                    String objVnode = objPool.getObjectVnodeId(curMeta);
                                    String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);

                                    return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                            lun, errorChunksList, ERROR_PUT_FILE_META, copyFileName, curMeta.endIndex, msg, nodeList));
                                } else if (curMeta.equals(ERROR_META)) {
                                    ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_FILE_META);
                                    return Mono.just(false);
                                } else {
                                    return Mono.just(true);
                                }
                            });
                });

    }

    @HandleErrorFunction(value = ERROR_UPDATE_FILE_ACCESS_TIME, timeout = 0L)
    public static Mono<Boolean> recoverUpdateFileAccessTime(String fileName, String stamp, String lun, List<Integer> errorChunksList, String storage, String poolQueueTag) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, "");

        return storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(fileName))
                .flatMap(nodeList -> {
                    ArrayList<Tuple3<String, String, String>> newNodeList = new ArrayList<Tuple3<String, String, String>>();
                    for (int i = 0; i < errorChunksList.size(); i++) {
                        Integer index = errorChunksList.get(i);
                        newNodeList.add(nodeList.get(index));
                    }
                    List<Integer> finishChunkList = new ArrayList<>();
                    List<SocketReqMsg> msgs = newNodeList.stream()
                            .map(tuple -> new SocketReqMsg("", 0)
                                    .put("fileName", fileName)
                                    .put("stamp", stamp)
                                    .put("lun", tuple.var2))
                            .collect(Collectors.toList());
                    ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_FILE_ACCESS_TIME, String.class, newNodeList);
                    responseInfo.responses.subscribe(s -> {
                        if (s.var2.equals(SUCCESS)) {
                            finishChunkList.add(errorChunksList.get(s.var1));
                        }
                    });
                    if (errorChunksList.size() == finishChunkList.size()) {
                        return Mono.just(true);
                    }
                    return Mono.just(false);
                });
    }

    @HandleErrorFunction(ERROR_PUT_SYNC_RECORD)
    public static Mono<Boolean> recoverSyncRecord(UnSynchronizedRecord value, SocketReqMsg msg) {
        if (value == null) {
            return Mono.just(true);
        }
        String bucket = value.bucket;
        return needRecoverUnsyncRecord(bucket, value.rocksKey(), msg, ERROR_PUT_SYNC_RECORD)
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(true);
                    }
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                    String bucketVnode = storagePool.getBucketVnodeId(bucket);
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> {
                                boolean writeAsync = false;
                                if (StringUtils.isNotBlank(ASYNC_CLUSTERS)) {
                                    writeAsync = ASYNC_CLUSTERS.equals(msg.get(ASYNC_CLUSTER_SIGNAL));
                                }
                                return ECUtils.updateSyncRecord(value, nodeList, writeAsync);
                            })
                            .map(resInt -> resInt != 0);
                });
    }

    @HandleErrorFunction(ERROR_REWRITE_PART_COPY_RECORD)
    public static Mono<Boolean> recoverPartCopySyncRecord(UnSynchronizedRecord recordValue, MetaData metaValue, SocketReqMsg msg) {
        if (recordValue == null) {
            return Mono.just(true);
        }
        String bucket = recordValue.bucket;
        String lun = msg.get("lun");
        String key = msg.get("key");
        String daVersion = recordValue.syncStamp;
        if (StringUtils.isNotBlank(msg.get("DaVersion"))) {
            daVersion = msg.get("DaVersion");
        }
        String keyPrefix;

        // 检查有没有相关的part_init记录。没有则说明已被删除，不再执行本次修复。
        if (IS_THREE_SYNC) {
            if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                if (!lun.endsWith(UnsyncRecordDir)) {
                    keyPrefix =
                            recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                } else {
                    keyPrefix =
                            recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                }
            } else {
                if (THREE_SYNC_INDEX.equals(recordValue.index)) {
                    if (!lun.endsWith(UnsyncRecordDir)) {
                        keyPrefix =
                                recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                    } else {
                        keyPrefix =
                                recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                    }
                } else {
                    if (!lun.endsWith(UnsyncRecordDir)) {
                        keyPrefix = recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + daVersion;
                    } else {
                        keyPrefix = recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + UnsyncRecordDir + File.separator + daVersion;
                    }
                }
            }
        } else {
            if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(recordValue.index)) {
                if (!lun.endsWith(UnsyncRecordDir)) {
                    keyPrefix = recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + daVersion;
                } else {
                    keyPrefix = recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + UnsyncRecordDir + File.separator + daVersion;
                }
            } else {
                if (!lun.endsWith(UnsyncRecordDir)) {
                    keyPrefix =
                            recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                } else {
                    keyPrefix =
                            recordValue.recordKey.split(File.separator)[0] + File.separator + recordValue.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + recordValue.index + File.separator + daVersion;
                }
            }
        }
        String partMergeUri = key + "?uploadId=" + metaValue.partUploadId;
        String partMergeKey = keyPrefix + "1" + File.separator + ERROR_COMPLETE_PART + partMergeUri;

        String finalDaVersion = daVersion;
        return needRecoverUnsyncRecord(bucket, partMergeKey, msg, ERROR_REWRITE_PART_COPY_RECORD)
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(true);
                    }
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                    String bucketVnode = storagePool.getBucketVnodeId(bucket);
                    String recordUri = recordValue.uri.contains("?") ? recordValue.uri.split("\\?")[0] : recordValue.uri;
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> ECUtils.rewritePartCopyRecord(storagePool, recordUri, Json.encode(recordValue), finalDaVersion, Json.encode(metaValue), nodeList))
                            .map(res -> res);
                });
    }

    @HandleErrorFunction(ERROR_UPDATE_AFTER_INIT_RECORD)
    public static Mono<Boolean> recoverAfterInitRecord(UnSynchronizedRecord value, SocketReqMsg msg) {
        String key = msg.get("key");
        String bucket = value.bucket;

        return needRecoverUnsyncRecord(bucket, key, msg, ERROR_UPDATE_AFTER_INIT_RECORD)
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(true);
                    }
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                    String bucketVnode = storagePool.getBucketVnodeId(bucket);
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> ErasureClient.updateAfterInitRecord(storagePool, key, msg.get("value"), nodeList));
                });
    }

    public static Mono<Boolean> needRecoverUnsyncRecord(String bucket, String recordKey, SocketReqMsg msg, ErrorConstant.ECErrorType errorType) {
        if ("1".equals(msg.get("force"))) {
            return Mono.just(true);
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        return ErasureClient.getUnsyncRecord(bucket, recordKey)
                .flatMap(record -> {
                    // 获取失败，重新放入队列，该消息不做处理
                    if (record.equals(ERROR_UNSYNC_RECORD)) {
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        msg.put("poolQueueTag", poolQueueTag);
                        ObjectPublisher.publish(CURRENT_IP, msg, errorType);
                        return Mono.just(false);
                    }
                    // 已无该record，说明已被删除，不再修复，防止重复同步
                    if (record.equals(NOT_FOUND_UNSYNC_RECORD)) {
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                });
    }

    /**
     * 重新上传图像处理中上传失败的record
     *
     * @return
     */
    @HandleErrorFunction(ERROR_PUT_MULTI_MEDIA_RECORD)
    public static Mono<Boolean> putMultiMediaRecord(SocketReqMsg msg) {
        String value = msg.get("value");
        ComponentRecord oldRecord = JSON.parseObject(value, new TypeReference<ComponentRecord>() {
        });
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(oldRecord.bucket);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        msg.put("poolQueueTag", poolQueueTag);
        String bucketVnode = storagePool.getBucketVnodeId(oldRecord.bucket);
        return ComponentUtils.getComponentRecord(oldRecord)
                .flatMap(tuple2 -> {
                    // 获取失败，重新放入队列，该消息不做处理
                    if (tuple2.var1.equals(ComponentRecord.ERROR_COMPONENT_RECORD)) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_MULTI_MEDIA_RECORD);
                        return Mono.just(false);
                    }
                    // 已无该record，说明已被删除，不再修复
                    if (tuple2.var1.equals(ComponentRecord.NOT_FOUND_COMPONENT_RECORD)) {
                        return Mono.just(true);
                    }
                    // 使用get出来的数据 进行修复
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> ComponentUtils.updateComponentRecord(tuple2.var1, nodeList, null, tuple2.var2))
                            .flatMap(s -> {
                                if (s != 1) {
                                    // 修复失败 重新放入mq中
                                    ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_MULTI_MEDIA_RECORD);
                                }
                                return Mono.just(s == 1);
                            });
                });
    }

    @HandleErrorFunction(ERROR_PUT_STS_TOKEN)
    public static Mono<Boolean> putCredentialToken(String value, SocketReqMsg msg) {
        Credential credential = Json.decodeValue(value, Credential.class);
        //修复前先判断是否到期
        long cur = System.currentTimeMillis() / 1000;
        if (cur > credential.deadline) {
            return Mono.just(true);
        }
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(credential.accessKey);
        return getCredential(credential.accessKey)
                .flatMap(credential1 -> {
                    if (ERROR_STS_TOKEN.equals(credential1)) {
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaStoragePool.getVnodePrefix());
                        msg.put("poolQueueTag", poolQueueTag);
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_STS_TOKEN);
                        return Mono.just(false);
                    }
                    if (NOT_FOUND_STS_TOKEN.equals(credential1)) {
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                });
    }

    @HandleErrorFunction(value = ERROR_PUT_AGGREGATION_META)
    public static Mono<Boolean> recoverAggregationMeta(String key) {
        String vnode = key.split("/")[0].substring(1);
        String nameSpace = key.split("/")[1];
        String aggregationId = key.split("/")[2];
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(nameSpace);
        return storagePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> AggregateFileClient.getAggregationMeta(nameSpace, aggregationId, nodeList))
                .map(s -> false);
    }

    @HandleErrorFunction(value = ERROR_PUT_AGGREGATION_OBJECT_FILE, timeout = 0L)
    public static Mono<Boolean> recoverAggregateFile(String lun, String namespace, String aggregateId, String fileName, String storage, List<Integer> errorChunksList, String fileSize, SocketReqMsg msg, String updateEC) {
        log.debug("【recoverAggregateFile】lun:{}, nameSpace:{}, aggregateId:{}, fileName:{}, storage:{}, errorChunksList:{}, fileSize:{}, msg:{}", lun, namespace, aggregateId, fileName, storage, errorChunksList, fileSize, msg);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(aggregateId);
        String vnodeId = bucketPool.getBucketVnodeId(aggregateId);
        long endIndex = Long.parseLong(fileSize) - 1;
        return bucketPool.mapToNodeInfo(vnodeId)
                .flatMap(bucketNodeList -> AggregateFileClient.getAggregationMeta(namespace, aggregateId, bucketNodeList))
                .timeout(Duration.ofMillis(30_000))
                .flatMap(curMeta -> {
                    // 检查已有的metadata，其中的fileName如果与此次上传数据所属对象的fileName相同，表示文件还存在，可进行数据恢复；
                    // 为ERROR_META，也不进行恢复操作，但要重新发布。此时dstIp不变，但发布在本地的队列上。
                    // 若为NOT_FOUND_META，表示文件已不存在，不用进行数据恢复操作；
                    if (ERROR_AGGREGATION_META.equals(curMeta)) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_AGGREGATION_OBJECT_FILE);
                        return Mono.just(false);
                    }
                    if (fileName.equals(curMeta.getFileName()) && !curMeta.deleteMark) {
                        StoragePool objPool = StoragePoolFactory.getStoragePool(storage == null ? curMeta.getStorage() : storage, "");

                        assert objPool != null;
                        String objVnode = objPool.getObjectVnodeId(fileName);
                        String versionMetaKey = AggregationUtils.getAggregationKey(vnodeId, namespace, aggregateId);

                        return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                lun, errorChunksList, ERROR_PUT_AGGREGATION_OBJECT_FILE, fileName, endIndex, msg, nodeList));
                    } else if (curMeta.equals(ERROR_AGGREGATION_META)) {
                        ObjectPublisher.publish(CURRENT_IP, msg, ERROR_PUT_AGGREGATION_OBJECT_FILE);
                        return Mono.just(false);
                    } else if (!curMeta.equals(NOT_FOUND_AGGREGATION_META) && !fileName.equals(curMeta.getFileName()) && !curMeta.deleteMark && !curMeta.storage.equals(storage)
                            && StringUtils.isNotEmpty(updateEC) && updateEC.equals(curMeta.storage)) {
                        StoragePool objPool = StoragePoolFactory.getStoragePool(storage == null ? curMeta.getStorage() : storage, "");


                        assert objPool != null;
                        String objVnode = objPool.getObjectVnodeId(fileName);
                        String versionMetaKey = AggregationUtils.getAggregationKey(vnodeId, namespace, aggregateId);

                        return objPool.mapToNodeInfo(objVnode).flatMap(nodeList -> recoverSpecificChunks(objPool, versionMetaKey,
                                lun, errorChunksList, ERROR_PUT_OBJECT_FILE, fileName, endIndex, msg, nodeList));
                    } else {
                        return Mono.just(true);
                    }
                });
    }
}

