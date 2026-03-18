package com.macrosan.lifecycle;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.DedupMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.ServerConstants.SNAPSHOT_LINK;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.lifecycle.LifecycleCommandConsumer.*;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.storage.move.CacheMove.isEnableCacheAccessTimeFlush;
import static com.macrosan.storage.move.CacheMove.isEnableCacheOrderFlush;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;

/**
 * @author admin
 */
public class LifecycleMoveTask {

    private static final Logger logger = LoggerFactory.getLogger(LifecycleMoveTask.class);

    private final StoragePool sourcePool;
    private final StoragePool targetPool;
    private final String requestId;
    private final boolean targetDup;
    protected static MSRocksDB mqDB = MSRocksDB.getRocksDB(Utils.getMqRocksKey());
    //单位为KB
    public static final long MAX_MOVE_DATA_SIZE = 5 * 1024 * 1024L;
    public static final AtomicLong TOTAL_MOVE_SIZE = new AtomicLong(0);
    private static final String MQ_KEY_PREFIX = "life_move_";

    private LifecycleMoveTask(StoragePool sourcePool, StoragePool targetPool, String requestId, boolean targetDup) {
        this.sourcePool = sourcePool;
        this.targetPool = targetPool;
        this.requestId = requestId;
        this.targetDup = targetDup;
    }

    /**
     * 迁移对象
     *
     * @param metaData 对象元数据
     */
    private Mono<Boolean> move(MetaData metaData, String currentSnapshotMark) {
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        String md5 = sysMetaMap.get(ETAG);

        if (metaData.partInfos == null) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(md5);
            String vnodeId = metaStoragePool.getBucketVnodeId(md5);
            String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, targetPool.getVnodePrefix(), requestId);
            String firstKey = Utils.getDeduplicatMetaKey(vnodeId, md5, targetPool.getVnodePrefix());
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
                        if (!targetDup) {
                            return move(MOVE_TYPE.MOVE_OBJECT, oldFileName
                                    , metaData.bucket, metaData.key
                                    , metaData.versionId, metaData.startIndex + metaData.offset
                                    , metaData.endIndex + metaData.offset, null, null, metaData.crypto, currentSnapshotMark);
                        }
                        return ErasureClient.getDeduplicateMeta(md5, targetPool.getVnodePrefix(), firstKey, targetNodeList, null)
                                .timeout(Duration.ofSeconds(30))
                                .flatMap(dupMeta -> {
                                    if (StringUtils.isNotEmpty(dupMeta.fileName)) {
                                        //存在重删信息只需要更新重删信息
                                        metaData.storage = targetPool.getVnodePrefix();
//                                        metaData.fileName = dupMeta.fileName;
                                        return ErasureClient.getAndUpdateDeduplicate(targetPool, dedupKey, targetNodeList, null, null, metaData, md5)
                                                .timeout(Duration.ofSeconds(30));
                                    } else {
                                        if (dupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                            return Mono.just(false);
                                        }
                                        return move(MOVE_TYPE.MOVE_OBJECT, oldFileName
                                                , metaData.bucket, metaData.key
                                                , metaData.versionId, metaData.startIndex + metaData.offset
                                                , metaData.endIndex + metaData.offset, null, null, metaData.crypto, currentSnapshotMark)
                                                .flatMap(b -> {
                                                    String sourceStorage = metaData.storage;
                                                    if (b) {
                                                        String objectWithVersionId = "null".equals(metaData.getVersionId()) ? metaData.getKey() : metaData.getKey() + File.separator + metaData.getVersionId();
                                                        metaData.setFileName(Utils.getObjFileName(targetPool, metaData.bucket, objectWithVersionId, requestId));
                                                        metaData.storage = targetPool.getVnodePrefix();
                                                        return ErasureClient.getAndUpdateDeduplicate(metaStoragePool, dedupKey, targetNodeList, null, null, metaData, md5)
                                                                .timeout(Duration.ofSeconds(30));
                                                    } else {
                                                        metaData.storage = sourceStorage;
                                                        return Mono.just(false);
                                                    }
                                                });
                                    }
                                });
                    });
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();
        AtomicInteger totalNum = new AtomicInteger(metaData.partInfos.length);
        UnicastProcessor<Integer> partProcessor = UnicastProcessor.create();
        int partSize = metaData.partInfos.length;
        PartInfo[] parts = new PartInfo[totalNum.get()];
        partProcessor
                .doOnComplete(() -> res.onNext(true))
                .doOnNext(partNum -> {
                    if (partNum >= partSize) {
                        metaData.setPartInfos(parts);
                        partProcessor.onComplete();
                        return;
                    }
                    PartInfo info = metaData.partInfos[partNum];
                    info.setInitSnapshotMark(currentSnapshotMark);
                    move(info, metaData.crypto)
                            .subscribe(b -> {
                                if (b) {
                                    parts[partNum] = info;
                                    partProcessor.onNext(partNum + 1);
                                } else {
                                    partProcessor.onError(new RuntimeException());
                                }
                            }, e -> {
                                logger.error("part move error", e);
                                partProcessor.onError(e);
                            });
                })
                .doOnError(e -> res.onNext(false))
                .subscribe();
        partProcessor.onNext(0);

        return res;
    }

    /**
     * 迁移分段
     *
     * @param partInfo 分段信息
     */
    private Mono<Boolean> move(PartInfo partInfo, String crypto) {
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
                    if (!targetDup) {
                        return move(MOVE_TYPE.MOVE_PART, oldFileName
                                , partInfo.bucket, partInfo.object
                                , partInfo.versionId, 0, partInfo.getPartSize() - 1
                                , partInfo.uploadId, partInfo.partNum, crypto, partInfo.initSnapshotMark);
                    }
                    String dupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, targetPool.getVnodePrefix(), requestId);
                    String firstKey = Utils.getDeduplicatMetaKey(vnodeId, md5, targetPool.getVnodePrefix());
                    return ErasureClient.getDeduplicateMeta(md5, metaStoragePool.getVnodePrefix(), firstKey, targetNodeList, null)
                            .timeout(Duration.ofSeconds(30))
                            .flatMap(dedupMeta -> {
                                if (StringUtils.isNotEmpty(dedupMeta.fileName)) {
                                    partInfo.storage = targetPool.getVnodePrefix();
                                    return ErasureClient.putPartDeduplicate(metaStoragePool, dupKey, targetNodeList, null, null, partInfo);
                                } else {
                                    if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                        return Mono.just(false);
                                    }
                                    return move(MOVE_TYPE.MOVE_PART, oldFileName
                                            , partInfo.bucket, partInfo.object
                                            , partInfo.versionId, 0
                                            , partInfo.getPartSize() - 1, partInfo.uploadId, partInfo.partNum, crypto, partInfo.initSnapshotMark)
                                            .flatMap(b -> {
                                                String sourceStorage = partInfo.storage;
                                                if (b) {
                                                    String newFileName = Utils.getPartFileName(targetPool, partInfo.bucket, partInfo.object, partInfo.uploadId, partInfo.partNum, requestId);
                                                    partInfo.setFileName(newFileName);
                                                    partInfo.storage = targetPool.getVnodePrefix();
                                                    return ErasureClient.putPartDeduplicate(metaStoragePool, dupKey, targetNodeList, null, null, partInfo);
                                                } else {
                                                    partInfo.storage = sourceStorage;
                                                    return Mono.just(false);
                                                }
                                            });
                                }
                            });
                });
    }

    private Mono<Boolean> move(
            MOVE_TYPE moveType
            , String oldFileName
            , String bucket, String object, String versionId
            , long startIndex, long endIndex
            , String uploadId, String partNum, String crypto, String snapshotMark) {

        //获取原始数据字节流，放入ecEncodeHanlder进行encode，生成dataFluxes，dataFluxes包含k+m个数据流
        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Tuple3<String, String, String>> sourceNodeList = sourcePool.mapToNodeInfo(sourcePool.getObjectVnodeId(oldFileName)).block();
        String objectWithVersionId = "null".equals(versionId) ? object : object + File.separator + versionId;
        String newFileName = MOVE_TYPE.MOVE_OBJECT.equals(moveType) ? Utils.getObjFileName(targetPool, bucket, objectWithVersionId, requestId)
                : Utils.getPartFileName(targetPool, bucket, object, uploadId, partNum, requestId);

        String newObjectVnodeId = targetPool.getObjectVnodeId(newFileName);
        List<Tuple3<String, String, String>> targetNodeList = targetPool.mapToNodeInfo(newObjectVnodeId).block();
        int ontPutBytes = targetPool.getK() * targetPool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(sourcePool.getK());
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);

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
                for (int j = 0; j < sourcePool.getK(); j++) {
                    streamController.onNext(-1L);
                }
                exceptGetNum.addAndGet(sourcePool.getK());
            }
        });

        ECUtils.getObject(sourcePool, oldFileName, false, startIndex, endIndex, endIndex + 1, sourceNodeList, streamController, null, null)
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
                .put("fileName", newFileName)
                .put("compression", targetPool.getCompression());

        // 判断是否开启缓冲池 按序下刷
        if (isEnableCacheOrderFlush(targetPool)) {
            msg.put("flushStamp", String.valueOf(System.currentTimeMillis()));
        }

        if (isEnableCacheAccessTimeFlush(targetPool)) {//生命周期迁移如果准备迁到缓存池，那么访问时间需要更新
            msg.put("lastAccessStamp", String.valueOf(System.currentTimeMillis()));
        }
        CryptoUtils.generateKeyPutToMsg(crypto, msg);

        List<UnicastProcessor<Payload>> processors = new ArrayList<>();
        for (int i = 0; i < targetNodeList.size(); i++) {
            Tuple3<String, String, String> tuple = targetNodeList.get(i);
            int index = i;
            SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2).put("recover", "1");  // 迁移落盘流量限流
            UnicastProcessor<Payload> processor = UnicastProcessor.create();
            processor.onNext(DefaultPayload.create(Json.encode(msg0), moveType.value.var1[0].name()));

            ecEncodeHandler.data()[index].subscribe(bytes -> {
                        processor.onNext(DefaultPayload.create(bytes, moveType.value.var1[1].name().getBytes()));
                    },
                    e -> {
                        logger.error("", e);
                        processor.onNext(DefaultPayload.create("", moveType.value.var1[2].name()));
                        processor.onComplete();
                    },
                    () -> {
                        processor.onNext(DefaultPayload.create("", moveType.value.var1[3].name()));
                        processor.onComplete();
                    });

            processors.add(processor);
        }

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(processors, String.class, targetNodeList);
        Set<Integer> errorChunksList = new HashSet<>(targetPool.getM());
//        String storageName = "storage_" + targetPool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + targetPool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        AtomicInteger putNum = new AtomicInteger();

        responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                    }

                    if (putNum.incrementAndGet() == targetNodeList.size()) {
                        next.onNext(new Tuple2<>(-1, 0));
                        putNum.set(errorChunksList.size());
                    }
                },
                e -> logger.error("send data error", e),
                () -> {
                    if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= targetPool.getK()) {
                        res.onNext(true);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("storage", targetPool.getVnodePrefix())
                                .put("bucket", bucket)
                                .put("object", object)
                                .put("uploadId", uploadId)
                                .put("partNum", partNum)
                                .put("fileName", newFileName)
                                .put("endIndex", String.valueOf(endIndex))
                                .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                                .put("errorChunksList", Json.encode(new ArrayList<>(errorChunksList)))
                                .put("versionId", versionId)
                                .put("poolQueueTag", poolQueueTag);
                        Optional.ofNullable(snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                        Optional.ofNullable(msg.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                        Optional.ofNullable(msg.get("lastAccessStamp")).ifPresent(v -> errorMsg.put("lastAccessStamp", v));

                        publishEcError(responseInfo.res, targetNodeList, errorMsg, moveType.value.var2());
                    } else {
                        res.onNext(false);
                        // 响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", bucket);
                        errorMsg.put("object", object);
                        errorMsg.put("fileName", newFileName);
                        errorMsg.put("storage", targetPool.getVnodePrefix());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        return res;
    }

    private enum MOVE_TYPE {
        MOVE_OBJECT(new Tuple2<>(new ErasureServer.PayloadMetaType[]{START_PUT_OBJECT
                , PUT_OBJECT, ERROR
                , COMPLETE_PUT_OBJECT}, ERROR_PUT_OBJECT_FILE)),

        MOVE_PART(new Tuple2<>(new ErasureServer.PayloadMetaType[]{START_PART_UPLOAD
                , PART_UPLOAD, ERROR
                , COMPLETE_PART_UPLOAD}, ERROR_PART_UPLOAD_FILE));

        public Tuple2<ErasureServer.PayloadMetaType[], ErrorConstant.ECErrorType> value;

        MOVE_TYPE(Tuple2<ErasureServer.PayloadMetaType[], ErrorConstant.ECErrorType> value) {
            this.value = value;
        }
    }

    public static Mono<Integer> move(String bucket, String object, String versionId, String targetStorageStrategy) {
        try {
            // todo 当前对象为小对象，且目标存储策略若开启小文件理应进入缓存池中等待聚合处理
            StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, object, Long.MAX_VALUE);
            StoragePool targetPool = StoragePoolFactory.getStoragePoolByStrategyName(targetStorageStrategy, operate);
            StorageStrategy storageStrategy1 = POOL_STRATEGY_MAP.get(targetStorageStrategy);
            String dataStr = storageStrategy1.redisMap.get("data");
            List<String> targetStorageList = new ArrayList<>();
            boolean[] isDup = new boolean[2];
            if (StringUtils.isNotEmpty(dataStr)) {
                targetStorageList.addAll(Arrays.asList(Json.decodeValue(dataStr, String[].class)));
            }

            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
            Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucket, object);
            String bucketVnode = bucketVnodeIdTuple.var1;
            String migrateVnode = bucketVnodeIdTuple.var2;
            List<String> oldFileName = new ArrayList<>();
            List<String> oldDedupFileName = new ArrayList<>();
            final String[] currentSnapshotMark = new String[]{null};
            final String[] snapshotLink = new String[]{null};
            return metaStoragePool.mapToNodeInfo(bucketVnode)
                    .flatMap(infoList -> RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK)
                            .collectList()
                            .doOnNext(keyValues -> keyValues.forEach(kv -> {
                                if (kv.getKey().equals(CURRENT_SNAPSHOT_MARK)) {
                                    currentSnapshotMark[0] = kv.getValueOrElse(null);
                                    return;
                                }
                                if (kv.getKey().equals(SNAPSHOT_LINK)) {
                                    snapshotLink[0] = kv.getValueOrElse(null);
                                }
                            }))
                            .map(b -> infoList))
                    .flatMap(bucketNodeList -> ErasureClient.getLifecycleMetaVersionRes(bucket, object, versionId, bucketNodeList, null, currentSnapshotMark[0], snapshotLink[0]))
                    .timeout(Duration.ofSeconds(30))
                    .flatMap(tuple2 -> {
                        MetaData meta = tuple2.var1;
                        if (meta.equals(MetaData.ERROR_META)) {
                            return Mono.just(ACK_FALSE);
                        }

                        if (meta.equals(MetaData.NOT_FOUND_META) || meta.deleteMark || meta.deleteMarker || targetStorageList.contains(meta.storage) || meta.isUnView(currentSnapshotMark[0])) {
                            return Mono.just(ACK_TURE);
                        } else {
                            Map<String, String> sysMetaMap = Json.decodeValue(meta.sysMetaData, new TypeReference<Map<String, String>>() {
                            });
                            String contentLength;
                            if (sysMetaMap.containsKey(CONTENT_LENGTH)) {
                                contentLength = sysMetaMap.get(CONTENT_LENGTH);
                            } else if (sysMetaMap.containsKey(CONTENT_LENGTH.toLowerCase())) {
                                contentLength = sysMetaMap.get(CONTENT_LENGTH.toLowerCase());
                            } else {
                                contentLength = String.valueOf(meta.endIndex - meta.startIndex <= 0 ? 0 : meta.endIndex - meta.startIndex + 1);
                            }

                            long dataSize = Long.parseLong(contentLength) / 1024;
                            // 超过5GB，进行限流
                            if (TOTAL_MOVE_SIZE.get() > MAX_MOVE_DATA_SIZE) {
                                return Mono.just(ACK_LIMIT);
                            }
                            long curSize = TOTAL_MOVE_SIZE.addAndGet(dataSize);
                            if (curSize > MAX_MOVE_DATA_SIZE && curSize != dataSize) {
                                TOTAL_MOVE_SIZE.addAndGet(-dataSize);
                                return Mono.just(ACK_LIMIT);
                            }

                            StoragePool sourcePool = StoragePoolFactory.getStoragePool(meta);
                            String requestId = getRequestId();
                            return StoragePoolFactory.getDeduplicateByStrategy(targetStorageStrategy)
                                    .doOnNext(dup -> isDup[0] = dup)
                                    .doOnNext(dup -> {

                                        if (meta.partUploadId != null) {
                                            for (PartInfo partInfo : meta.partInfos) {
                                                oldDedupFileName.add(partInfo.fileName);
                                            }
                                        } else {
                                            oldDedupFileName.add(meta.fileName);
                                        }
                                    })
                                    .flatMap(dup -> new LifecycleMoveTask(sourcePool, targetPool, requestId, dup)
                                            .move(meta, currentSnapshotMark[0])
                                            .flatMap(b -> {
                                                logger.debug("end new file = {}", meta.fileName);
                                                if (b) {
                                                    meta.setStorage(targetPool.getVnodePrefix());
                                                    if (meta.partUploadId != null) {
                                                        int index = 0;
                                                        for (PartInfo partInfo : meta.partInfos) {
                                                            if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                                                if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                    oldFileName.add(partInfo.deduplicateKey);
                                                                }
                                                                if (!isDup[0]) {
                                                                    partInfo.setDeduplicateKey(null);
                                                                    partInfo.setFileName(Utils.getPartFileName(targetPool, bucket, object, partInfo.uploadId, partInfo.partNum, requestId));
                                                                }
                                                            } else {
                                                                //move目标重删会修改了fileName,源对象非重删对象
                                                                if (!isDup[0]) {
                                                                    if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                        oldFileName.add(partInfo.fileName);
                                                                    }
                                                                    partInfo.setFileName(Utils.getPartFileName(targetPool, bucket, object, partInfo.uploadId, partInfo.partNum, requestId));
                                                                } else {
                                                                    if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                        oldFileName.add(oldDedupFileName.get(index));
                                                                    }
                                                                    index++;
                                                                }

                                                            }

                                                            partInfo.setStorage(targetPool.getVnodePrefix());
                                                            if (isDup[0]) {
                                                                String vnodeId = metaStoragePool.getBucketVnodeId(partInfo.etag);
                                                                String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, partInfo.etag, targetPool.getVnodePrefix(), requestId);
                                                                partInfo.setDeduplicateKey(dedupKey);
                                                            }
                                                        }
                                                    } else {
                                                        String objectWithVersionId = "null".equals(versionId) ? object : object + File.separator + versionId;
                                                        logger.debug("filename = {}, duplicateKey = {}", meta.fileName, meta.duplicateKey);
                                                        if (StringUtils.isNotEmpty(meta.duplicateKey)) {
                                                            if (meta.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                oldFileName.add(meta.duplicateKey);
                                                            }
                                                            //目标桶没开重删,需要去掉重删字段,加上自己的fileName
                                                            if (!isDup[0]) {
                                                                meta.setDuplicateKey(null);
                                                                meta.fileName = Utils.getObjFileName(targetPool, bucket, objectWithVersionId, requestId);
                                                            }
                                                        } else {
                                                            if (!isDup[0]) {
                                                                meta.setDuplicateKey(null);
                                                                if (meta.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                    oldFileName.add(meta.fileName);
                                                                }
                                                                meta.fileName = Utils.getObjFileName(targetPool, bucket, objectWithVersionId, requestId);
                                                            } else {
                                                                if (meta.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                    oldFileName.add(oldDedupFileName.get(0));
                                                                }
                                                            }
                                                        }

                                                        if (isDup[0]) {
                                                            String md5 = sysMetaMap.get(ETAG);
                                                            String vnodeId = metaStoragePool.getBucketVnodeId(md5);
                                                            String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, targetPool.getVnodePrefix(), requestId);
                                                            meta.setDuplicateKey(dedupKey);
                                                        }

                                                    }
                                                    final boolean[] isCurrentSnapshotObject = new boolean[]{meta.isCurrentSnapshotObject(currentSnapshotMark[0])};
                                                    return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                                                            .flatMap(notEmpty -> {
                                                                if (isCurrentSnapshotObject[0]) {
                                                                    return Mono.just(notEmpty);
                                                                }
                                                                // 不是当前快照下的对象，则修改元数据为当前快照
                                                                return VersionUtil.getVersionNum(bucket, object)
                                                                        .doOnNext(versionNum -> {
                                                                            meta.setVersionNum(versionNum);
                                                                            meta.setSnapshotMark(currentSnapshotMark[0]);
                                                                        }).thenReturn(notEmpty);
                                                            })
                                                            .flatMap(notEmpty -> {
                                                                if (notEmpty) {
                                                                    return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                                            .flatMap(migrateVnodeList -> {
                                                                                if (isCurrentSnapshotObject[0]) {
                                                                                    // 迁移对象是当前快照下的，则更新对象元数据
                                                                                    return ErasureClient.getLifecycleMetaVersionResOnlyRead(meta.bucket, meta.key, meta.versionId, migrateVnodeList, null, meta.snapshotMark).zipWith(Mono.just(migrateVnodeList))
                                                                                            .flatMap(resTuple -> ErasureClient.lifecycleMetaDataAcl(Utils.getVersionMetaDataKey(migrateVnode, meta.bucket, meta.key, meta.versionId, meta.snapshotMark), meta.clone(), resTuple.getT2(), null, resTuple.getT1().var2()))
                                                                                            .flatMap(getFinalUpdateMetaResult(migrateVnodeList, meta, currentSnapshotMark, targetStorageList));
                                                                                } else {
                                                                                    // 迁移对象不是当前快照下的，则为该对象put一个属于当前快照下的对象元数据
                                                                                    return ErasureClient.putMetaData(Utils.getMetaDataKey(migrateVnode, bucket, object, meta.versionId, meta.stamp, meta.snapshotMark), meta.clone(), migrateVnodeList, null, null, null, null, snapshotLink[0])
                                                                                            .map(r -> r ? 1 : 0);
                                                                                }
                                                                            })
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .doOnNext(r -> {
                                                                                if (r != 1) {
                                                                                    logger.error("update new mapping meta storage error! {}", r);
                                                                                }
                                                                            })
                                                                            .map(r -> r == 1)
                                                                            .doOnError(e -> logger.error("", e))
                                                                            .onErrorReturn(false);
                                                                }
                                                                return Mono.just(true);
                                                            })
                                                            .flatMap(writeSuccess -> {
                                                                if (writeSuccess) {
                                                                    return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                                            .flatMap(bucketNodeList -> {
                                                                                if (isCurrentSnapshotObject[0]) {
                                                                                    // 迁移对象是当前快照下的，则更新对象元数据
                                                                                    return ErasureClient.lifecycleMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, meta.bucket, meta.key, meta.versionId, meta.snapshotMark), meta, bucketNodeList, null, tuple2.var2)
                                                                                            .flatMap(getFinalUpdateMetaResult(bucketNodeList, meta, currentSnapshotMark, targetStorageList));
                                                                                } else {
                                                                                    // 迁移对象不是当前快照下的，则为该对象put一个属于当前快照下的对象元数据
                                                                                    return ErasureClient.putMetaData(Utils.getMetaDataKey(bucketVnode, bucket, object, meta.versionId, meta.stamp, meta.snapshotMark), meta.clone(), bucketNodeList, null, null, null, null, snapshotLink[0])
                                                                                            .map(r -> r ? 1 : 0);
                                                                                }
                                                                            })
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .doOnNext(r -> {
                                                                                if (r != 1) {
                                                                                    logger.error("update meta storage error! {}", r);
                                                                                }
                                                                            })
                                                                            .map(r -> r == 1)
                                                                            .doOnError(e -> logger.error("", e))
                                                                            .onErrorReturn(false);
                                                                }
                                                                return Mono.just(false);
                                                            })
                                                            .flatMap(res -> {
                                                                if (res) {
                                                                    if (isCurrentSnapshotObject[0] && !oldFileName.isEmpty()) {
                                                                        Map<String, String> map = new HashMap<>();
                                                                        map.put("object", object);
                                                                        map.put("bucket", bucket);
                                                                        map.put("versionId", versionId);
                                                                        map.put("storage", sourcePool.getVnodePrefix());
                                                                        map.put("versionNum", meta.getVersionNum());
                                                                        map.put("fileName", oldFileName.toString());
                                                                        map.put("storageStrategy", targetStorageStrategy);
                                                                        Optional.ofNullable(meta.snapshotMark).ifPresent(v -> map.put("snapshotMark", v));
                                                                        String jsonValue = JSON.toJSONString(map);
                                                                        putMq(MQ_KEY_PREFIX + requestId, jsonValue);
                                                                    }
                                                                    return Mono.just(ACK_TURE);
                                                                } else {
                                                                    logger.error("update meta storage error! {}:{}:{}:{}", meta.bucket, meta.key, meta.versionId, meta.storage);
                                                                    return Mono.just(ACK_FALSE);
                                                                }
                                                            });
                                                } else {
                                                    logger.error("from {} move bucket:{}, object:{}, versionId:{} to {} error", sourcePool.getVnodePrefix(), bucket, object, versionId, targetPool.getVnodePrefix());
                                                    return Mono.just(ACK_FALSE);
                                                }
                                            })
                                            .doOnNext(m -> {
                                                TOTAL_MOVE_SIZE.addAndGet(-dataSize);
                                            })
                                            .doOnError(b -> {
                                                TOTAL_MOVE_SIZE.addAndGet(-dataSize);
                                            }));
                        }
                    })
                    .onErrorReturn(ACK_FALSE);
        } catch (Exception e) {
            return Mono.just(ACK_FALSE);
        }
    }

    /**
     * 获取 更新元数据的真实结果
     * updateDataMetaResult==2时，会出现 success>k writeNum>1 的情况
     * 如果success节点版本号较大，则也属于更新成功，此时通过get对元数据进行修复，通过最新元数据判断是否更新成功
     */
    private static Function<Integer, Mono<? extends Integer>> getFinalUpdateMetaResult(List<Tuple3<String, String, String>> bucketNodeList, MetaData meta, String[] currentSnapshotMark, List<String> targetStorageList) {
        return updateDataMetaResult -> {
            if (updateDataMetaResult != 2) {
                return Mono.just(updateDataMetaResult);
            }
            return ErasureClient.getLifecycleMetaVersionRes(meta.bucket, meta.key, meta.versionId, bucketNodeList, null, currentSnapshotMark[0], meta.snapshotMark)
                    .flatMap(resTuple -> {
                        MetaData latestMeta = resTuple.var1;
                        if (latestMeta.equals(MetaData.NOT_FOUND_META) || latestMeta.deleteMark || latestMeta.deleteMarker || targetStorageList.contains(latestMeta.storage) || latestMeta.isUnView(currentSnapshotMark[0])) {
                            return Mono.just(1);
                        } else {
                            return Mono.just(updateDataMetaResult);
                        }
                    });
        };
    }

    private static void putMq(String key, String value) {
        try {
            mqDB.put(key.getBytes(), value.getBytes());
        } catch (Exception e) {
            logger.error("lifecycle object put mqDB error !!!", e);
        }
    }

}
