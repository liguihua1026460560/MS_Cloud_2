package com.macrosan.storage.coder;

import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.md5.PartMd5Digest;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PART_UPLOAD_FILE;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_ROLL_BACK_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.getDiskName;

@Log4j2
public class DataDivision {
    UnicastProcessor<byte[]>[][] division;
    PartInfo[] partInfos;
    LimitEncoder encoder;
    StoragePool storagePool;
    MonoProcessor<Boolean> recoverDataProcessor;
    SocketReqMsg baseReqMsg;
    List<List<UnicastProcessor<Payload>>> publishers;
    int divisionNum;

    AtomicBoolean putEnd = new AtomicBoolean(false);
    AtomicInteger successPart = new AtomicInteger(0);
    List<Tuple3<Tuple2<ErasureServer.PayloadMetaType, String>[], List<Tuple3<String, String, String>>,
            SocketReqMsg>> errorMsgList = new LinkedList<>();
    SocketReqMsg errorMsg;
    AtomicBoolean[] firstPut;
    List<Disposable> disposableList = new LinkedList<>();
    MonoProcessor<Boolean> res = MonoProcessor.create();

    private String secretKey;

    public DataDivision(LimitEncoder encoder, PartInfo[] partInfos, StoragePool storagePool, MetaData meta,
                        MonoProcessor<Boolean> recoverDataProcessor) {
        this.storagePool = storagePool;
        this.encoder = encoder;
        this.partInfos = partInfos;
        this.recoverDataProcessor = recoverDataProcessor;

        long divisionSize = storagePool.getDivisionSize();
        long packageSize = storagePool.getPackageSize();
        int k = storagePool.getK();
        divisionNum = (int) (divisionSize / k / packageSize);

        prepare(meta);
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolQueueTag)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
        errorMsg = new SocketReqMsg("", 0)
                .put("bucket", meta.bucket)
                .put("object", meta.key)
                .put("uploadId", meta.partUploadId)
                .put("versionId", meta.versionId)
                .put("storage", storagePool.getVnodePrefix())
                .put("stamp", meta.stamp)
                .put("poolQueueTag", poolQueueTag);

        CryptoUtils.putCryptoInfoToMsg(meta.getCrypto(), secretKey, errorMsg);
    }

    private void prepare(MetaData meta) {
        String bucketVnode = StoragePoolFactory.getMetaStoragePool(meta.bucket).getBucketVnodeId(meta.bucket, meta.key);
        String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, meta.bucket, meta.key, meta.versionId, meta.snapshotMark);

        baseReqMsg = new SocketReqMsg("", 0)
                // 自动分段上传可能会耗时很长，metaData尚未写入将可能导致后台校验误删fileMeta。规避。
//                .put("metaKey", versionMetaKey)
                .put("noGet", "1")
                .put("compression", storagePool.getCompression());

        CryptoUtils.generateKeyPutToMsg(meta.getCrypto(), baseReqMsg);
        secretKey = baseReqMsg.get("secretKey");

        publishers = new ArrayList<>(partInfos.length);
        firstPut = new AtomicBoolean[partInfos.length];

        for (int i = 0; i < partInfos.length; i++) {
            String vnode = storagePool.getObjectVnodeId(partInfos[i].fileName);
            List<Tuple3<String, String, String>> curNodeList = storagePool.mapToNodeInfo(vnode).block();
            List<UnicastProcessor<Payload>> publisher = new ArrayList<>(curNodeList.size());
            for (int j = 0; j < curNodeList.size(); j++) {
                publisher.add(UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get()));
            }
            publishers.add(publisher);
            firstPut[i] = new AtomicBoolean(true);
        }
    }

    public Mono<Boolean> putParts(MsHttpRequest request, MonoProcessor<Boolean> recoverData, PartMd5Digest digest, String requestID, boolean dedup) {
        UnicastProcessor<byte[]>[] source = encoder.data();
        HashMap<Integer, AtomicBoolean> dedupFlags = new HashMap<>(8);

        for (int i = 0; i < source.length; i++) {
            int index = i;

            encoder.request(i, 1L);
            AtomicInteger bytesNum = new AtomicInteger();

            source[index].subscribe(bytes -> {
                        if (putEnd.get()) {
                            return;
                        }

                        int n = bytes.length == 0 ? bytesNum.get() : bytesNum.getAndIncrement();
                        int partN = n / divisionNum;
                        boolean first = n % divisionNum == 0;

                        if (first) {
                            if (partN > 0) {
                                publishers.get(partN - 1).get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                                publishers.get(partN - 1).get(index).onComplete();
                            }

                            //first时处理上一段
                            if (dedup) {
                                dedupFlags.put(partN, new AtomicBoolean(false));
                            }
                            if (firstPut[partN].compareAndSet(true, false)) {
                                Disposable disposable = putPart(partN, publishers.get(partN), getNodeListForPart(partN))
                                        .filter(b -> b)
                                        .flatMap(b -> {
                                            if (dedup && !dedupFlags.get(partN).get()) {
                                                String md5 = digest.getPartMd5(partN);
                                                partInfos[partN].setEtag(md5);
                                                StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                                                String dedupVnode = dedupPool.getBucketVnodeId(md5);
                                                String suffix = Utils.getDeduplicatMetaKey(dedupVnode, md5, partInfos[partN].storage, requestID);
                                                partInfos[partN].deduplicateKey = suffix;
                                                return dedupPool.mapToNodeInfo(dedupVnode)
                                                        .flatMap(nodeList -> ErasureClient.putPartDeduplicate(dedupPool, suffix,
                                                                nodeList, request, recoverData, partInfos[partN]))
                                                        .publishOn(ErasureServer.DISK_SCHEDULER)
                                                        .flatMap(bool -> {
                                                            if (!bool) {
                                                                throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                                                            }
                                                            dedupFlags.get(partN).compareAndSet(false, true);
                                                            partInfos[partN].deduplicateKey = suffix;
                                                            return Mono.just(true);
                                                        });
                                            } else {
                                                return Mono.just(true);
                                            }
                                        })
                                        .subscribe();
                                disposableList.add(disposable);
                            }

                            publishers.get(partN).get(index).onNext(DefaultPayload.create(Json.encode(getMsgForPart(partN).get(index)), START_PUT_OBJECT.name()));
                            publishers.get(partN).get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                        } else {
                            publishers.get(partN).get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                        }
                    },
                    e -> {
                        log.error("", e);
                        tryEnd();
                    },
                    () -> {
                        publishers.get(partInfos.length - 1).get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        publishers.get(partInfos.length - 1).get(index).onComplete();
                    }
            );
        }

        request.addResponseCloseHandler(s -> {
            tryEnd();
        });

        return res;
    }

    private Mono<Boolean> putPart(int partN, List<UnicastProcessor<Payload>> publisher, List<Tuple3<String, String, String>> nodeList) {
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList);
        List<Integer> errorChunksList = new LinkedList<>();
        MonoProcessor<Boolean> res = MonoProcessor.create();

        int[] requestNum = new int[nodeList.size()];
        Arrays.fill(requestNum, 0);

        Disposable disposable = responseInfo.responses
                .subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                        encoder.request(s.var1, 1);
                        ++requestNum[s.var1];
                    } else if (s.var2.equals(CONTINUE)) {
                        encoder.request(s.var1, 1L);
                        int cur = ++requestNum[s.var1];
                        if (!errorChunksList.isEmpty()) {
                            for (int index : errorChunksList) {
                                if (cur > requestNum[index]) {
                                    encoder.request(index, 1L);
                                    ++requestNum[index];
                                }
                            }
                        }
                    }
                }, e -> {
                    log.error("", e);
                    tryEnd();
                }, () -> {
                    boolean success = false;
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        success = true;
                    } else if (responseInfo.successNum >= storagePool.getK()) {
                        success = true;
                        SocketReqMsg errorMsg = this.errorMsg.copy()
                                .put("partNum", partInfos[partN].partNum)
                                .put("endIndex", String.valueOf(partInfos[partN].partSize - 1))
                                .put("errorChunksList", Json.encode(errorChunksList))
                                .put("fileName", partInfos[partN].fileName);
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        Optional.ofNullable(partInfos[partN].snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                        errorMsgList.add(new Tuple3<>(responseInfo.res, nodeList, errorMsg));
                    } else {
                        tryEnd();
                    }

                    if (success) {
                        log.debug("put part success, partN: {}, partInfoLength:{}, successNum:{}", partN, partInfos.length, successPart.get() + 1);
                        if (partInfos.length == successPart.incrementAndGet()) {
                            putEnd();
                        }
                        res.onNext(true);
                    }
                });

        disposableList.add(disposable);
        return res;
    }

    private void tryEnd() {
        if (putEnd.compareAndSet(false, true)) {
            putEnd();
        }
    }

    private void putEnd() {
        for (List<UnicastProcessor<Payload>> list : publishers) {
            for (UnicastProcessor<Payload> publish : list) {
                publish.onNext(DefaultPayload.create("put file error", ERROR.name()));
                publish.onComplete();
            }
        }

        for (int i = 0; i < encoder.data().length; i++) {
            encoder.request(i, Long.MAX_VALUE);
        }

        ListIterator<Disposable> listIterator = disposableList.listIterator();
        while (listIterator.hasNext()) {
            Disposable disposable = listIterator.next();
            disposable.dispose();
            listIterator.remove();
        }

        if (successPart.get() == partInfos.length) {
            if (!errorMsgList.isEmpty()) {
                recoverDataProcessor.subscribe(s -> {
                    if (s) {
                        errorMsgList.forEach(arg -> {
                            publishEcError(arg.var1, arg.var2, arg.var3, ERROR_PART_UPLOAD_FILE);
                        });
                    }
                });
            }

            res.onNext(true);
        } else {
            for (int i = 0; i < partInfos.length; i++) {
                SocketReqMsg errorMsg = this.errorMsg.copy()
                        .put("fileName", partInfos[i].fileName);
                if (StringUtils.isNotEmpty(partInfos[i].deduplicateKey)) {
                    errorMsg = this.errorMsg.copy()
                            .put("fileName", partInfos[i].deduplicateKey);
                }

                for (Tuple3<String, String, String> tuple3 : getNodeListForPart(i)) {
                    SocketReqMsg msg = errorMsg.copy()
                            .put("lun", getDiskName(tuple3.var1, tuple3.var2));
                    ObjectPublisher.publish(tuple3.var1, msg, ERROR_ROLL_BACK_FILE);
                }
            }

            res.onNext(false);
        }
    }

    // 保证同一个分段所有节点获取到的vnodeList和msg是一致的
    ConcurrentHashMap<Integer, Tuple2<List<Tuple3<String, String, String>>, List<SocketReqMsg>>> consistencyMap = new ConcurrentHashMap<>();

    public List<Tuple3<String, String, String>> getNodeListForPart(int partN) {
        return getConsistentData(partN).var1;
    }

    public List<SocketReqMsg> getMsgForPart(int partN) {
        return getConsistentData(partN).var2;
    }

    private Tuple2<List<Tuple3<String, String, String>>, List<SocketReqMsg>> getConsistentData(int partN) {
        return consistencyMap.computeIfAbsent(partN, k -> getLatestVnodeListAndMsgForPart(partN));
    }

    /**
     * 获取分段最新的vnodeList和msg
     *
     * @param partN 分段
     */
    private Tuple2<List<Tuple3<String, String, String>>, List<SocketReqMsg>> getLatestVnodeListAndMsgForPart(int partN) {
        String vnode = storagePool.getObjectVnodeId(partInfos[partN].fileName);
        List<Tuple3<String, String, String>> curNodeList = storagePool.mapToNodeInfo(vnode).block();
        List<SocketReqMsg> msg = new ArrayList<>(curNodeList.size());

        for (Tuple3<String, String, String> tuple3 : curNodeList) {
            msg.add(baseReqMsg.copy()
                    .put("fileName", partInfos[partN].fileName)
                    .put("lun", tuple3.var2)
                    .put("vnode", tuple3.var3));
        }
        return new Tuple2<>(curNodeList, msg);
    }

}
