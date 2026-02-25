package com.macrosan.ec.restore;/**
 * @author niechengxing
 * @create 2023-06-21 16:03
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.DedupMeta;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.snapshot.utils.SnapshotUtil;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.SysConstants.ROCKS_DEDUPLICATE_KEY;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.storage.move.CacheMove.isEnableCacheOrderFlush;

/**
 * @program: MS_Cloud
 * @description: 重新存储对象数据任务
 * @author: niechengxing
 * @create: 2023-06-21 16:03
 */
@Log4j2
public class RestoreObjectTask {
    private final StoragePool oldPool;
    private final StoragePool newPool;
    private final String requestId;
    private final boolean dup;

    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public RestoreObjectTask(StoragePool oldPool, StoragePool newPool, String requestId, boolean dup) {
        this.oldPool = oldPool;
        this.newPool = newPool;
        this.requestId = requestId;
        this.dup = dup;
    }

    private enum RESTORE_TYPE {
        RESTORE_OBJECT(new Tuple2<>(new ErasureServer.PayloadMetaType[]{START_PUT_OBJECT
                , PUT_OBJECT, ERROR
                , COMPLETE_PUT_OBJECT}, ERROR_PUT_OBJECT_FILE)),

        RESTORE_PART(new Tuple2<>(new ErasureServer.PayloadMetaType[]{START_PART_UPLOAD
                , PART_UPLOAD, ERROR
                , COMPLETE_PART_UPLOAD}, ERROR_PART_UPLOAD_FILE));

        public Tuple2<ErasureServer.PayloadMetaType[], ErrorConstant.ECErrorType> value;

        RESTORE_TYPE(Tuple2<ErasureServer.PayloadMetaType[], ErrorConstant.ECErrorType> value) {
            this.value = value;
        }
    }

    /**
     * 重新存储对象
     *
     * @param metaData
     * @return
     */
    public Mono<reactor.util.function.Tuple2<Boolean, String[]>> restoreObject(MetaData metaData) {
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        String md5 = sysMetaMap.get(ETAG);
        if (metaData.partInfos == null) {
            String[] filenameArray = new String[]{null};
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(md5);
            String vnodeId = metaStoragePool.getBucketVnodeId(md5);
            String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, newPool.getVnodePrefix(), requestId);
            String firstKey = Utils.getDeduplicatMetaKey(vnodeId, md5, newPool.getVnodePrefix());
            List<Tuple3<String, String, String>> targetNodeList = metaStoragePool.mapToNodeInfo(vnodeId).block();
            return Mono.just(StringUtils.isEmpty(metaData.duplicateKey))
                    .flatMap(b -> {
                        if (b) {
                            return Mono.just(metaData.fileName);
                        } else {
                            return ErasureClient.getDeduplicateMeta(md5, metaData.storage, metaData.duplicateKey, targetNodeList, null)
                                    .timeout(Duration.ofSeconds(30))
                                    .flatMap(dupMeta -> {
                                        if (StringUtils.isNotEmpty(dupMeta.fileName)) {
                                            return Mono.just(dupMeta.fileName);
                                        } else {
                                            return Mono.just(metaData.fileName);
                                        }
                                    });
                        }
                    })
                    .flatMap(oldFileName -> {
                        if (!dup) {
                            return restore(RESTORE_TYPE.RESTORE_OBJECT, oldFileName, metaData.bucket, metaData.key,
                                    metaData.versionId, metaData.startIndex, metaData.endIndex, null, null, metaData.crypto, metaData.snapshotMark)
                                    .flatMap(tuple2 -> {
                                        if (tuple2.getT1()) {
                                            filenameArray[0] = tuple2.getT2();
                                        }
                                        return Mono.just(tuple2.getT1()).zipWith(Mono.just(filenameArray));
                                    });
                        }
                        return ErasureClient.getDeduplicateMeta(md5, newPool.getVnodePrefix(), firstKey, targetNodeList, null)
                                .timeout(Duration.ofSeconds(30))
                                .flatMap(dupMeta -> {
                                    if (StringUtils.isNotEmpty(dupMeta.fileName)) {
                                        //存在重删信息只需要更新重删信息
                                        metaData.storage = newPool.getVnodePrefix();
                                        return ErasureClient.getAndUpdateDeduplicate(metaStoragePool, dedupKey, targetNodeList, null, null, metaData, md5)
                                                .timeout(Duration.ofSeconds(30)).zipWith(Mono.just(filenameArray));
                                    } else {
                                        if (dupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                            return Mono.just(false).zipWith(Mono.just(filenameArray));
                                        }
                                        return restore(RESTORE_TYPE.RESTORE_OBJECT, oldFileName, metaData.bucket, metaData.key,
                                                metaData.versionId, metaData.startIndex, metaData.endIndex, null, null, metaData.crypto, metaData.snapshotMark)
                                                .flatMap(tuple2 -> {
                                                    String sourceStorage = metaData.storage;
                                                    if (tuple2.getT1()) {
                                                        filenameArray[0] = tuple2.getT2();
                                                        metaData.setFileName(tuple2.getT2());
                                                        metaData.storage = newPool.getVnodePrefix();
                                                        return ErasureClient.getAndUpdateDeduplicate(metaStoragePool, dedupKey, targetNodeList, null, null, metaData, md5)
                                                                .timeout(Duration.ofSeconds(30)).zipWith(Mono.just(filenameArray));
                                                    } else {
                                                        metaData.storage = sourceStorage;
                                                        return Mono.just(tuple2.getT1()).zipWith(Mono.just(filenameArray));
                                                    }
                                                });
                                    }
                                });
                    });

//            return restore(RESTORE_TYPE.RESTORE_OBJECT, metaData.fileName, metaData.bucket, metaData.key,
//                    metaData.versionId, metaData.startIndex, metaData.endIndex, null, null, metaData.crypto);
        }
        MonoProcessor<Boolean> res = MonoProcessor.create();
        UnicastProcessor<Integer> partProcessor = UnicastProcessor.create();
        AtomicInteger totalNum = new AtomicInteger(metaData.partInfos.length);
        int partSize = metaData.partInfos.length;
        PartInfo[] partInfos = new PartInfo[totalNum.get()];
        String[] partFileArray = new String[totalNum.get()];
        partProcessor.doOnComplete(() -> res.onNext(true))
                .doOnNext(parNum -> {
                    if (parNum >= partSize) {
                        metaData.setPartInfos(partInfos);
                        partProcessor.onComplete();
                        return;
                    }
                    PartInfo partInfo = metaData.partInfos[parNum];
                    partInfo.setInitSnapshotMark(metaData.snapshotMark);
                    restorePart(partInfo, metaData.crypto)
                            .subscribe(tuple2 -> {
                                if (tuple2.getT1()) {
                                    partFileArray[parNum] = tuple2.getT2();
                                    partInfos[parNum] = partInfo;
                                    partProcessor.onNext(parNum + 1);
                                } else {
                                    partProcessor.onError(new RuntimeException());
                                }
                            }, e -> {
                                log.error("part restore error", e);
                                partProcessor.onError(e);
                            });
                })
                .doOnError(e -> res.onNext(false))
                .subscribe();
        partProcessor.onNext(0);
        return res.zipWith(Mono.just(partFileArray));
    }

    /**
     * 重新存储分块
     *
     * @param partInfo
     * @param crypto
     * @return
     */
    public Mono<reactor.util.function.Tuple2<Boolean, String>> restorePart(PartInfo partInfo, String crypto) {
        String md5 = partInfo.etag;
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(md5);
        String vnodeId = metaStoragePool.getBucketVnodeId(md5);
        List<Tuple3<String, String, String>> targetNodeList = metaStoragePool.mapToNodeInfo(vnodeId).block();

        return Mono.just(StringUtils.isEmpty(partInfo.deduplicateKey))
                .flatMap(b -> {
                    if (b) {
                        return Mono.just(partInfo.fileName);
                    } else {
                        return ErasureClient.getDeduplicateMeta(partInfo.etag, partInfo.storage, partInfo.deduplicateKey, targetNodeList, null)
                                .timeout(Duration.ofSeconds(30))
                                .flatMap(dupMeta -> {
                                    if (StringUtils.isNotEmpty(dupMeta.fileName)) {
                                        return Mono.just(dupMeta.fileName);
                                    }
                                    return Mono.just(partInfo.fileName);
                                });
                    }
                })
                .flatMap(oldFileName -> {
                    if (!dup) {
                        return restore(RESTORE_TYPE.RESTORE_PART, oldFileName, partInfo.bucket, partInfo.object,
                                partInfo.versionId, 0, partInfo.getPartSize() - 1, partInfo.uploadId, partInfo.partNum, crypto, partInfo.initSnapshotMark);
                    }
                    String dupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, newPool.getVnodePrefix(), requestId);
                    String firstKey = Utils.getDeduplicatMetaKey(vnodeId, md5, newPool.getVnodePrefix());
                    return ErasureClient.getDeduplicateMeta(md5, newPool.getVnodePrefix(), firstKey, targetNodeList, null)
                            .timeout(Duration.ofSeconds(30))
                            .flatMap(dedupMeta -> {
                                if (StringUtils.isNotEmpty(dedupMeta.fileName)) {
                                    partInfo.storage = newPool.getVnodePrefix();
                                    return ErasureClient.putPartDeduplicate(metaStoragePool, dupKey, targetNodeList, null, null, partInfo)
                                            .zipWith(Mono.just(""));
                                } else {
                                    if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                        return Mono.just(false).zipWith(Mono.just(""));
                                    }
                                    return restore(RESTORE_TYPE.RESTORE_PART, oldFileName, partInfo.bucket, partInfo.object,
                                            partInfo.versionId, 0, partInfo.getPartSize() - 1, partInfo.uploadId, partInfo.partNum, crypto, partInfo.initSnapshotMark)
                                            .flatMap(tuple2 -> {
                                                String sourceStorage = partInfo.storage;
                                                if (tuple2.getT1()) {
                                                    partInfo.setFileName(tuple2.getT2());
                                                    partInfo.storage = newPool.getVnodePrefix();
                                                    return ErasureClient.putPartDeduplicate(metaStoragePool, dupKey, targetNodeList, null, null, partInfo).zipWith(Mono.just(tuple2.getT2()));
                                                } else {
                                                    partInfo.storage = sourceStorage;
                                                    return Mono.just(tuple2);
                                                }
                                            });
                                }
                            });
                });

//        return restore(RESTORE_TYPE.RESTORE_PART, partInfo.fileName, partInfo.bucket, partInfo.object,
//                partInfo.versionId, 0, partInfo.getPartSize() - 1, partInfo.uploadId, partInfo.partNum, crypto);

    }

    public Mono<reactor.util.function.Tuple2<Boolean, String>> restore(RESTORE_TYPE restoreType, String oldFileName, String bucket, String object,
                                                                       String versionId, long startIndex, long endIndex, String uploadId, String partNum, String crypto, String snapshotMark) {
        //获取原始数据字节流，放入ecEncodeHanlder进行encode，生成dataFluxes，dataFluxes包含k+m个数据流
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Encoder ecEncodeHandler = newPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        //获取数据块的存放位置,只修改ec的话获取到的vnode会发生改变
        List<Tuple3<String, String, String>> sourceNodeList = oldPool.mapToNodeInfo(oldPool.getObjectVnodeId(oldFileName)).block();

        String newFileName = RESTORE_TYPE.RESTORE_OBJECT.equals(restoreType) ? Utils.getObjFileName(newPool, bucket, object, requestId)
                : Utils.getPartFileName(newPool, bucket, object, uploadId, partNum, requestId);//使用新的requestId生成新的filename，避免后续删除旧对象数据时filename不变的话可能删除掉新对象的数据块

        List<Tuple3<String, String, String>> targetNodeList = newPool.mapToNodeInfo(newPool.getObjectVnodeId(newFileName)).block();
        int ontPutBytes = newPool.getK() * newPool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(oldPool.getK());
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);

//        String strategyName = "storage_" + newPool.getVnodePrefix();
//        String poolQueueTag = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(newPool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + newPool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;

        next.subscribe(t -> {
            if (t.var1 > 0) {
                exceptGetNum.decrementAndGet();
                waitEncodeBytes.addAndGet(t.var2);
                int n = waitEncodeBytes.get() / ontPutBytes;
                exceptPutNum.addAndGet(n);
                waitEncodeBytes.addAndGet(-n * ontPutBytes);
            } else {
                exceptPutNum.decrementAndGet();
            }

            while (exceptPutNum.get() == 0 && exceptGetNum.get() <= 0) {
                for (int j = 0; j < oldPool.getK(); j++) {
                    streamController.onNext(-1L);
                }
                exceptGetNum.addAndGet(oldPool.getK());
            }
        });

//        long endIndex0 = Long.parseLong(endIndex);
        ECUtils.getObject(oldPool, oldFileName, false, startIndex, endIndex, endIndex + 1, sourceNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    ecEncodeHandler.put(bytes);
                });

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("recover", "1")
                .put("fileName", newFileName)
                .put("compression", newPool.getCompression());
        // 判断是否开启缓冲池 按序下刷
        if (isEnableCacheOrderFlush(newPool)) {
            msg.put("flushStamp", String.valueOf(System.currentTimeMillis()));
        }
        CryptoUtils.generateKeyPutToMsg(crypto, msg);

        List<UnicastProcessor<Payload>> publisher = targetNodeList.stream()
                .map(tuple -> {
                            SocketReqMsg msg1 = msg.copy()
                                    .put("lun", tuple.var2)
                                    .put("vnode", tuple.var3);

                            return msg1;
                        }
                )
                .map(msg0 -> {
                    UnicastProcessor<Payload> process = UnicastProcessor.create();
                    process.onNext(DefaultPayload.create(Json.encode(msg0), restoreType.value.var1[0].name()));
                    return process;
                })
                .collect(Collectors.toList());
        for (int i = 0; i < targetNodeList.size(); i++) {
            int index = i;
            ecEncodeHandler.data()[index].subscribe(bytes -> {
                        publisher.get(index).onNext(DefaultPayload.create(bytes, restoreType.value.var1[1].name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", restoreType.value.var1[2].name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        publisher.get(index).onNext(DefaultPayload.create("", restoreType.value.var1[3].name()));
                        publisher.get(index).onComplete();
                    }
            );
        }
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, targetNodeList);
        List<Integer> errorChunkList = new ArrayList<>(newPool.getM());
        AtomicInteger putNum = new AtomicInteger();

        responseInfo.responses.subscribe(s -> {
                    if (ERROR.equals(s.var2)) {
                        errorChunkList.add(s.var1);
                    }

                    if (putNum.incrementAndGet() == targetNodeList.size()) {//只要成功上传k个数据块那么本次put成功，exceptPutNum减1
                        next.onNext(new Tuple2<>(-1, 0));
                        putNum.set(errorChunkList.size());
                    }
                },
                e -> log.error("send data error", e),
                () -> {
                    if (responseInfo.successNum == newPool.getK() + newPool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= newPool.getK()) {
                        res.onNext(true);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("storage", newPool.getVnodePrefix())
                                .put("bucket", bucket)
                                .put("object", object)
                                .put("uploadId", uploadId)
                                .put("partNum", partNum)
                                .put("fileName", newFileName)
                                .put("updateEC", oldPool.getVnodePrefix())
                                .put("endIndex", String.valueOf(endIndex))
                                .put("errorChunksList", Json.encode(errorChunkList))
                                .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                                .put("versionId", versionId)
                                .put("poolQueueTag", poolQueueTag);
                        Optional.ofNullable(snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                        Optional.ofNullable(msg.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                        CryptoUtils.putCryptoInfoToMsg(msg.get("crypto"), msg.get("secretKey"), errorMsg);

                        publishEcError(responseInfo.res, targetNodeList, errorMsg, restoreType.value.var2);
                    } else {
                        res.onNext(false);
                        //响应成功数量达不到k,发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", bucket);
                        errorMsg.put("object", object);
                        errorMsg.put("fileName", newFileName);
                        errorMsg.put("storage", newPool.getVnodePrefix());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                }
        );
        return res.zipWith(Mono.just(newFileName));
    }

    /**
     * 重新存储初始化未合并的分段
     *
     * @return
     */
    public Mono<Boolean> restoreInitPartObj(InitPartInfo initPartInfo) {
        //首先根据InitPartInfo获取到所有的part，可以调用complete_part获取
        String bucket = initPartInfo.getBucket();
        String object = initPartInfo.getObject();
        String uploadId = initPartInfo.getUploadId();
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);//元数据
        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucket, object);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        List<String> delFileName = new ArrayList<>();
        List<String> oldDedupFileName = new ArrayList<>();
        AtomicBoolean noPart = new AtomicBoolean(false);

        SocketReqMsg msg0 = new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("object", object)
                .put("uploadId", uploadId);
        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    if (initPartInfo.snapshotMark == null) {
                        return Mono.just(nodeList);
                    }
                    return SnapshotUtil.fetchBucketSnapshotInfo(bucket, msg0).thenReturn(nodeList);
                })
                .flatMap(nodeList -> {
                    MonoProcessor<Map<String, PartInfo>> res = MonoProcessor.create();
                    msg0.put("vnode", nodeList.get(0).var3);

                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple -> msg0.copy().put("lun", tuple.var2))
                            .collect(Collectors.toList());
                    Map<String, PartInfo> partInfos = new HashMap<>();

                    ClientTemplate.ResponseInfo<PartInfo[]> responseInfo = ClientTemplate.oneResponse(msgs, COMPLETE_PART_CHECK, PartInfo[].class, nodeList);
                    Disposable subscribe = responseInfo.responses
                            .doOnNext(t -> {
                                if (t.var2 == SUCCESS) {
                                    PartInfo[] curPartInfos = t.var3;
                                    for (PartInfo curPartInfo : curPartInfos) {
                                        String key;
                                        if (curPartInfo.snapshotMark != null) {
                                            // 如果存在快照，则多个快照下可能存在相同partNum的分段，因此key中添加mark，防止被覆盖
                                            key = curPartInfo.snapshotMark + File.separator + curPartInfo.partNum;
                                        } else {
                                            key = curPartInfo.partNum;
                                        }
                                        PartInfo partInfo = partInfos.get(key);
                                        if (partInfo == null || partInfo.versionNum.compareTo(curPartInfo.versionNum) < 0) {
                                            partInfos.put(key, curPartInfo);
                                        }
                                    }
                                }
                            })
                            .doOnComplete(() -> {
                                if (responseInfo.successNum < metaStoragePool.getK()) {
                                    res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "Get Part Info Fail"));
                                    return;
                                }
                                res.onNext(partInfos);
                            }).subscribe();
                    return res;
                })
                .flatMap(partInfos -> {
//                    log.info("partInfos: {}", partInfos);
                    if (partInfos.isEmpty()) {
                        noPart.set(true);
                        return Mono.just(true);
                    }
                    List<String> keyList = new ArrayList<>(partInfos.keySet());
                    MonoProcessor<Boolean> res0 = MonoProcessor.create();
                    UnicastProcessor<Integer> partProcessor = UnicastProcessor.create();
                    for (String key : keyList) {
                        oldDedupFileName.add(partInfos.get(key).fileName);
                    }
                    int partSize = partInfos.size();
                    partProcessor.doOnComplete(() -> res0.onNext(true))
                            .doOnNext(index -> {
                                if (index >= partSize) {
                                    partProcessor.onComplete();
                                    return;
                                }
                                PartInfo partInfo = partInfos.get(keyList.get(index));
                                if (newPool.getVnodePrefix().equals(partInfo.storage)) {
                                    partProcessor.onNext(index + 1);
                                    return;
                                }
                                restorePart(partInfo, initPartInfo.metaData.crypto)
                                        .flatMap(tuple2 -> {
                                            if (tuple2.getT1()) {
                                                //重新存储partInfo成功后更新partInfo
                                                if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                                    delFileName.add(partInfo.deduplicateKey);
                                                    if (!dup) {
                                                        partInfo.setDeduplicateKey(null);
                                                        partInfo.setFileName(tuple2.getT2());
                                                    }
                                                } else {
                                                    if (!dup) {
                                                        delFileName.add(partInfo.fileName);
                                                        partInfo.setFileName(tuple2.getT2());
                                                    } else {
                                                        delFileName.add(oldDedupFileName.get(index));
                                                    }
                                                }

                                                partInfo.setStorage(newPool.getVnodePrefix());
                                                if (dup) {
                                                    String vnodeId = metaStoragePool.getBucketVnodeId(partInfo.etag);
                                                    String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, partInfo.etag, newPool.getVnodePrefix(), requestId);
                                                    partInfo.setDeduplicateKey(dedupKey);
                                                }
//                                                partInfo.setFileName(Utils.getPartFileName(newPool, bucket, object, partInfo.uploadId, partInfo.partNum, requestId));
                                                return Mono.just(migrateVnode != null)
                                                        .flatMap(b1 -> {
                                                            if (b1) {
                                                                return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                                        .flatMap(migrateVnodeList -> PartClient.getPartInfoRes(bucket, object, uploadId, partInfo.partNum, partInfo.getPartKey(migrateVnode), migrateVnodeList, null, partInfo.snapshotMark).zipWith(Mono.just(migrateVnodeList)))
                                                                        .flatMap(resTuple -> ErasureClient.updatePartInfo(partInfo.getPartKey(migrateVnode), partInfo, resTuple.getT2(), null, resTuple.getT1().var2, null))
                                                                        .timeout(Duration.ofSeconds(30))
                                                                        .doOnNext(i -> {
                                                                            if (i != 1) {
                                                                                log.error("update new mapping part info error! {}", i);
                                                                            }
                                                                        })
                                                                        .map(i -> i == 1)
                                                                        .doOnError(e -> log.error("", e))
                                                                        .onErrorReturn(false);
                                                            }
                                                            return Mono.just(true);
                                                        })
                                                        .flatMap(writeSuccess -> {
                                                            if (writeSuccess) {
                                                                return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                                        .flatMap(nodeList -> PartClient.getPartInfoRes(bucket, object, uploadId, partInfo.partNum, partInfo.getPartKey(bucketVnode), nodeList, null, partInfo.snapshotMark).zipWith(Mono.just(nodeList)))
                                                                        .flatMap(resTuple -> ErasureClient.updatePartInfo(partInfo.getPartKey(bucketVnode), partInfo, resTuple.getT2(), null, resTuple.getT1().var2, null))
                                                                        .timeout(Duration.ofSeconds(30))
                                                                        .doOnNext(i -> {
                                                                            if (i != 1) {
                                                                                log.error("update part info error! {}", i);
                                                                            }
                                                                        })
                                                                        .map(i -> i == 1)
                                                                        .doOnError(e -> log.error("", e))
                                                                        .onErrorReturn(false);
                                                            }
                                                            return Mono.just(false);
                                                        });
                                            }
                                            return Mono.just(false);
                                        })
                                        .subscribe(b -> {
                                            if (b) {
                                                partProcessor.onNext(index + 1);
                                            } else {
                                                partProcessor.onError(new RuntimeException());
                                            }
                                        }, e -> {
                                            log.error("part restore error", e);
                                            partProcessor.onError(e);
                                        });
                            })
                            .doOnError(e -> res0.onNext(false))
                            .subscribe();
                    partProcessor.onNext(0);
                    return res0;
                })
                .flatMap(res1 -> {
                    if (res1) {//当前InitPartInfo相关的part均重新存储完成,开始删除旧的part数据
                        if (noPart.get()) {
                            log.info("there are no parts in the initPartInfo! bucket:{} object:{} uploadId:{}", bucket, object, uploadId);
                            return Mono.just(true);
                        }
                        //开始删除原ec存储的对象数据
                        log.debug("delete file {} {}", Arrays.toString(delFileName.toArray(new String[0])), initPartInfo.versionNum);
                        boolean deDup = false;
                        Set<String> allFile = new HashSet<>();
                        for (String name : delFileName) {
                            if (name.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                deDup = true;
                                break;
                            }
                        }
                        for (String files : delFileName) {
                            if (!files.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                allFile.add(files);
                            }
                        }
                        if (deDup) {
                            return ErasureClient.deleteDedupObjectFile(metaStoragePool, delFileName.toArray(new String[0]), null, false)
                                    .flatMap(bb -> {
                                        if (bb && !allFile.isEmpty()) {
                                            return ErasureClient.restoreDeleteObjectFile(oldPool, allFile.toArray(new String[0]), null);
                                        }
                                        return Mono.just(bb);
                                    });
                        }
                        return ErasureClient.restoreDeleteObjectFile(oldPool, delFileName.toArray(new String[0]), null);
                    } else {
                        log.error("restore init part from {} to {} error bucket:{} object:{} uploadId:{}", oldPool.getVnodePrefix(), newPool.getVnodePrefix(), initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId);
                        return Mono.just(false);
                    }
                });

    }

}

