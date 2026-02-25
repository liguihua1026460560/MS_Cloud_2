package com.macrosan.storage.aggregation;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.*;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.transaction.UndoLog;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.aggregation.ConcurrentBitSet;
import com.macrosan.utils.functional.Function2;
import com.macrosan.utils.functional.Function4;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.ECUtils.updateRocksKey;
import static com.macrosan.ec.ErasureClient.deleteRocksKey;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.ERROR_AGGREGATION_META;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.NOT_FOUND_AGGREGATION_META;

@Log4j2
public class AggregateFileClient {

    /**
     * 计算聚合文件的hole率
     * @param fileMetadata 聚合文件元数据
     * @return bitSet1: bitSet1:记录聚合文件的空洞，bitSet2:记录检查成功的小文件
     */
    public static Mono<Tuple2<BitSet, BitSet>> calculateAggregateFileHoleRate(AggregateFileMetadata fileMetadata, BitSet checked) {
        String[] segmentKeys = fileMetadata.segmentKeys;
        ConcurrentBitSet res = new ConcurrentBitSet(fileMetadata.segmentKeys.length);
        res.set(0, fileMetadata.segmentKeys.length); // 默认空洞率为0%
        ConcurrentBitSet success = new ConcurrentBitSet(fileMetadata.segmentKeys.length);
        success.set(0, fileMetadata.segmentKeys.length); // 默认全部检查成功
        return Flux.range(0, segmentKeys.length)
                .flatMap(i -> {
                    // 已经确认成功的直接跳过
                    if (checked != null && checked.get(i)) {
                        return Mono.just(true);
                    }
                    String key = segmentKeys[i];
                    int startIndex = key.indexOf("/");
                    int endIndex = key.lastIndexOf("/");
                    String bucket = key.substring(0, startIndex);
                    String object = key.substring(startIndex + 1, endIndex);
                    String versionId = key.substring(endIndex + 1);
                    StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
                    String bucketVnodeId = pool.getBucketVnodeId(bucket, object);

                    return pool.mapToNodeInfo(bucketVnodeId)
                            .flatMap(nodeList -> ErasureClient.getObjectMetaVersion(bucket, object, versionId, nodeList, null, null, null, true))
                            .flatMap(metaData -> {
                                if (metaData.equals(MetaData.ERROR_META)) {
                                    success.clear(i);
                                } else if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || !fileMetadata.fileName.equals(metaData.fileName)) {
                                    res.clear(i);
                                }
                                return Mono.just(true);
                            });
                }, 1, 1)
                .collectList()
                .map(list -> new Tuple2<>(res, success));
    }

    public static Mono<Boolean> recordAggregationUndoLog(String key, UndoLog undoLog, List<Tuple3<String, String, String>> nodeList) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool("");
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", Json.encode(undoLog))
                        .put("lun", MSRocksDB.getAggregateLun(tuple.var2)))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, RECORD_AGGREGATION_UNDO_LOG, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {}, e -> log.error("", e), () -> {
            if (responseInfo.successNum == 0) {
                res.onNext(false);
            } else {
                if (responseInfo.successNum == pool.getK() + pool.getM()) {
                    if (undoLog.committed) {
                        ErasureClient.deleteRocksKey(key, MSRocksDB.IndexDBEnum.AGGREGATE_DB, "default", nodeList).subscribe();
                    }
                    res.onNext(true);
                } else if (responseInfo.successNum >= pool.getK()) {
                    res.onNext(true);
                } else {
                    res.onNext(false);
                }
            }
        });
        return res;
    }

    public static Mono<Boolean> putAggregationMeta(String key, AggregateFileMetadata metadata, List<Tuple3<String, String, String>> nodeList, MonoProcessor<Boolean> recoverDataProcessor) {
        boolean deleteMark = metadata.deleteMark;
        String value = Json.encode(metadata);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool("");
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", value)
                        .put("lun", MSRocksDB.getAggregateLun(tuple.var2)))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, PUT_AGGREGATION_META, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == 0) {
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(false);
                }
                res.onNext(false);
            } else {
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(true);
                }
                if (responseInfo.successNum == pool.getK() + pool.getM()) {
                    // delete aggregation meta
                    if (deleteMark) {
                        deleteRocksKey(key, MSRocksDB.IndexDBEnum.AGGREGATE_DB, "default", nodeList).subscribe();
                    }
                    res.onNext(true);
                } else if (responseInfo.successNum >= pool.getK()) {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key);
                    msg.put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, nodeList, msg, ERROR_PUT_AGGREGATION_META);
                    res.onNext(true);
                } else {
                    res.onNext(false);
                }
            }
        });
        return res;
    }

    public static Mono<Boolean> freeAggregationSpace(String key, List<Tuple3<String, String, String>> nodeList) {
        return freeAggregationSpace(key, nodeList, null);
    }

    public static Mono<Boolean> freeAggregationSpace(String key, List<Tuple3<String, String, String>> nodeList, BitSet bitSet) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool("");
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("lun", MSRocksDB.getAggregateLun(tuple.var2))
                        .put("value", bitSet == null ? "" : AggregationUtils.serialize(bitSet)))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, FREE_AGGREGATION_SPACE, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == 0) {
                res.onNext(false);
            } else {
                if (responseInfo.successNum >= pool.getK()) {
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key);
                    msg.put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, nodeList, msg, ERROR_FREE_AGGREGATION_SPACE);
                    res.onNext(true);
                } else {
                    res.onNext(false);
                }
            }
        });
        return res;
    }

    public static Mono<AggregateFileMetadata> getAggregationMeta(String namespace, String aggregationId, List<Tuple3<String, String, String>> nodeList) {
        return getAggregationMetaRes(namespace, aggregationId, nodeList, AggregateFileClient::updateAggregationMeta).map(tuple -> tuple.var1);
    }

    public static Mono<Tuple2<AggregateFileMetadata, Tuple2<ErasureServer.PayloadMetaType, AggregateFileMetadata>[]>> getAggregationMetaRes(
            String nameSpace, String aggregationId, List<Tuple3<String, String, String>> nodeList,
            Function4<String, AggregateFileMetadata, List<Tuple3<String, String, String>>, Tuple2<ErasureServer.PayloadMetaType, AggregateFileMetadata>[], Mono<Integer>> repairFunction) {

        String vnode = nodeList.get(0).var3;
        String realKey = AggregationUtils.getAggregationKey(vnode, nameSpace, aggregationId);

        nodeList = nodeList.stream()
                .map(tuple -> new Tuple3<>(tuple.var1, MSRocksDB.getAggregateLun(tuple.var2), tuple.var3))
                .collect(Collectors.toList());

        AggregateFileMetadata deleteMark = new AggregateFileMetadata()
                .setAggregationId(aggregationId)
                .setDeleteMark(true)
                .setStamp(String.valueOf(System.currentTimeMillis()))
                .setVersionNum(VersionUtil.getLastVersionNum(""));

        Function2<AggregateFileMetadata, AggregateFileMetadata, AggregateFileMetadata> merge = (real, current) -> {
            if (real == current || real.deleteMark || current.deleteMark) {
                return real;
            }
            BitSet finalBitSet = null;
            if (real.bitmap != null) {
                finalBitSet = AggregationUtils.deserialize(real.bitmap);
            }
            if (current.bitmap != null) {
                BitSet currentBitSet = AggregationUtils.deserialize(current.bitmap);
                if (finalBitSet == null) {
                    finalBitSet = currentBitSet;
                } else {
                    finalBitSet.and(currentBitSet);
                }
            }
            real.bitmap = finalBitSet == null ? null : AggregationUtils.serialize(finalBitSet);
            return real;
        };

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(aggregationId), realKey, AggregateFileMetadata.class,
                GET_AGGREGATION_META, NOT_FOUND_AGGREGATION_META, ERROR_AGGREGATION_META, deleteMark,
                AggregateFileMetadata::getVersionNum, Comparator.comparing(AggregateFileMetadata::getVersionNum), repairFunction, nodeList, null, null, merge);
    }

    private static Mono<Integer> updateAggregationMeta(String key, AggregateFileMetadata metaData, List<Tuple3<String, String, String>> nodeList,
                                                       Tuple2<ErasureServer.PayloadMetaType, AggregateFileMetadata>[] res) {

        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<ErasureServer.PayloadMetaType, AggregateFileMetadata> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        //实际写入db的metaData
        String versionNum = VersionUtil.getVersionNum(false);
        metaData.setVersionNum(versionNum);
        for (Tuple2<ErasureServer.PayloadMetaType, AggregateFileMetadata> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(versionNum);
            }
        }

        return updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.getNamespace()), oldVersionNum, key, Json.encode(metaData), UPDATE_AGGREGATION_META, ERROR_PUT_AGGREGATION_META,
                nodeList, null, null);
    }

    public static Mono<Boolean> updateMetaData(MetaData metaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[] res,
                                               String aggregationKey, AggregateFileMetadata fileMetadata, long offset) {
        String oldFileName = metaData.fileName;
        // 更新元数据的聚合key
        metaData.setAggregationKey(aggregationKey);
        // 更新元数据在聚合文件中的偏移量
        metaData.setOffset(offset);
        // 更新元数据的文件名
        metaData.setFileName(fileMetadata.getFileName());
        // 更新元数据的存储池位置
        metaData.setStorage(fileMetadata.getStorage());
        // 更新聚合文件的大小
        metaData.setAggSize(fileMetadata.fileSize);
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
        Tuple2<String, String> tuple1 = metaStoragePool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
        String bucketVnodeId = tuple1.var1;
        String migrateVnodeId = tuple1.var2;
        Mono<Boolean> just = Mono.just(true);
        if (StringUtils.isNotEmpty(migrateVnodeId)) {
            String migrateMetaKey = Utils.getVersionMetaDataKey(migrateVnodeId, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
            just = metaStoragePool.mapToNodeInfo(migrateVnodeId)
                    .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(metaData.bucket, metaData.key, metaData.versionId, migrateVnodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(migrateVnodeList)))
                    .flatMap(resTuple2 -> ErasureClient.updateMetaData(migrateMetaKey, metaData.clone(), resTuple2.getT2(), null, resTuple2.getT1().var2))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> r == 1)
                    .onErrorReturn(false);
        }
        return just.
                flatMap(b -> {
                    if (b) {
                        String versionKey = Utils.getVersionMetaDataKey(bucketVnodeId, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
                        return metaStoragePool.mapToNodeInfo(bucketVnodeId)
                                .flatMap(nodeList -> ErasureClient.updateMetaData(versionKey, metaData, nodeList, null, res).zipWith(Mono.just(nodeList)))
                                .timeout(Duration.ofSeconds(30))
                                .flatMap(rt2 -> {
                                    int r = rt2.getT1();
                                    if (r != 2) {
                                        return Mono.just(r == 1);
                                    }
                                    // 若发生覆盖，进行一次重试
                                    return ErasureClient.getObjectMetaVersionResUnlimited(metaData.bucket, metaData.key, metaData.versionId, rt2.getT2(), null, metaData.snapshotMark, null)
                                            .flatMap(resTuple2 -> {
                                                MetaData metaData1 = resTuple2.var1;
                                                if (!metaData1.isAvailable() || !oldFileName.equals(metaData1.fileName) && !metaData.fileName.equals(metaData1.fileName)) {
                                                    return Mono.just(false);
                                                }
                                                metaData1.setAggregationKey(aggregationKey);
                                                // 更新元数据在聚合文件中的偏移量
                                                metaData1.setOffset(offset);
                                                // 更新元数据的文件名
                                                metaData1.setFileName(fileMetadata.getFileName());
                                                // 更新元数据的存储池位置
                                                metaData1.setStorage(fileMetadata.getStorage());
                                                // 更新聚合文件的大小
                                                metaData1.setAggSize(fileMetadata.fileSize);
                                                return ErasureClient.updateMetaData(versionKey, metaData1, rt2.getT2(), null, resTuple2.var2)
                                                        .map(r1 -> r1 == 1);
                                            });
                                });
                    }
                    return Mono.just(false);
                });
    }

    public static Mono<Boolean> aggregate(MetaData metaData, AggregateContainer aggregation, Runnable runnable) {
        // 加入聚合容器
        StoragePool pool = StoragePoolFactory.getStoragePool(metaData);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Tuple3<String, String, String>> getNodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(metaData)).block();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();
        Buffer buffer = Buffer.buffer();
        ECUtils.getObject(pool, metaData.fileName, false, 0, metaData.endIndex, metaData.endIndex + 1,
                        getNodeList, streamController, null, null)
                .doOnError(e -> res.onNext(false))
                .doOnComplete(() -> {
                    try {
                        String key = metaData.bucket + "/" + metaData.key + "/" + metaData.versionId;
                        boolean append = aggregation.append(key, buffer.getBytes(), runnable);
                        res.onNext(append);
                    } catch (Exception e) {
                        res.onNext(false);
                    }
                })
                .subscribe(bytes -> {
                    buffer.appendBuffer(Buffer.buffer(bytes));
                    streamController.onNext(1L);
                });
        return res;
    }

    public static Mono<Boolean> aggregate(String fileName, String storage, String bucket, String object, String versionId,
                                          long offset, long length, AggregateContainer container, Runnable runnable) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, null);
        if (storagePool == null) {
            return Mono.just(true);
        }
        List<Tuple3<String, String, String>> getNodeList = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(fileName)).block();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();
        Buffer buffer = Buffer.buffer();
        ECUtils.getObject(storagePool, fileName, false, offset, offset + length - 1, length,
                        getNodeList, streamController, null, null)
                .doOnError(e -> res.onNext(false))
                .doOnComplete(() -> {
                    try {
                        String key = bucket + "/" + object + "/" + versionId;
                        boolean append = container.append(key, buffer.getBytes(), runnable);
                        res.onNext(append);
                    } catch (Exception e) {
                        res.onNext(false);
                    }
                })
                .subscribe(bytes -> {
                    buffer.appendBuffer(Buffer.buffer(bytes));
                    streamController.onNext(1L);
                });
        return res;
    }

    public static Mono<Boolean> flushContainer(AggregateFileMetadata metaData, Flux<byte[]> content, StoragePool targetPool, MonoProcessor<Boolean> recoverDataProcessor) {
        Digest digest = new Md5Digest();
        Encoder ecEncodeHandler = targetPool.getEncoder();
        content
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(() -> {
                    ecEncodeHandler.complete();
                    metaData.setEtag(Hex.encodeHexString(digest.digest()));
                })
                .subscribe(bytes -> {
                    ecEncodeHandler.put(bytes);
                    digest.update(bytes);
                });
        // 按照存储策略、聚合容器 ID 生成 新的fileName
        String objectVnodeId = targetPool.getObjectVnodeId(metaData.getFileName());
        List<Tuple3<String, String, String>> putNodeList = targetPool.mapToNodeInfo(objectVnodeId).block();
        String bucketVnodeId = StoragePoolFactory.getMetaStoragePool(metaData.getNamespace()).getBucketVnodeId(metaData.getAggregationId());
        String metaKey = AggregationUtils.getAggregationKey(bucketVnodeId, metaData.getNamespace(), metaData.getAggregationId());
        List<UnicastProcessor<Payload>> publisher = putNodeList.stream()
                .map(t -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", metaData.getFileName())
                            .put("metaKey", metaKey)
                            .put("lun", t.var2)
                            .put("vnode", t.var3)
                            .put("compression", targetPool.getCompression());

                    return msg;
                })
                .map(msg0 -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create();
                    processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));
                    return processor;
                })
                .collect(Collectors.toList());

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            ecEncodeHandler.data()[index].subscribe(bytes -> {
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

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, putNodeList);

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());

        Disposable disposable = responseInfo.responses
                .subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                    }
                }, e -> log.error("", e), () -> {

                    if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= targetPool.getK()) {
                        res.onNext(true);

                        //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                        recoverDataProcessor.subscribe(b -> {
                            if (b) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("namespace", metaData.getNamespace())
                                        .put("aggregateId", metaData.getAggregationId())
                                        .put("fileName", metaData.getFileName())
                                        .put("storage", targetPool.getVnodePrefix())
                                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                                        .put("poolQueueTag", poolQueueTag);

                                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_PUT_AGGREGATION_OBJECT_FILE);
                            }
                        });

                    } else {
                        res.onNext(false);
                        //响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", "");
                        errorMsg.put("storage", targetPool.getVnodePrefix());
                        errorMsg.put("object", metaData.getAggregationId());
                        errorMsg.put("fileName", metaData.getFileName());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        return res;
    }

    protected static MSRocksDB mqDB = MSRocksDB.getRocksDB(Utils.getMqRocksKey());

    public static void putMq(String key, String value) {
        try {
            mqDB.put(key.getBytes(), value.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
