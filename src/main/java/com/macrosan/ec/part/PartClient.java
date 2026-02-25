package com.macrosan.ec.part;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.ec.*;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.CompleteMultipartUpload;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.client.ListPartsClientHandler;
import com.macrosan.storage.client.channel.ListMultiPartClientMergeChannel;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.coder.LimitEncoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.cache.FastMd5DigestPool;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.FastMd5Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import com.macrosan.utils.serialize.JaxbUtils;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.INVALED_PART_ORDER;
import static com.macrosan.constants.ErrorNo.MALFORMED_XML;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.SYNC_COMPRESS;
import static com.macrosan.ec.ECUtils.processHttpBody;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOT_FOUND;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.message.jsonmsg.PartInfo.*;
import static com.macrosan.utils.msutils.MsException.dealException;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class PartClient {
    /**
     * 初始化分段上传
     *
     * @param metaData    分段的元数据
     * @param uploadId    uploadId
     * @param accountName 账户名
     * @param nodeList    需要连接的节点信息
     * @param request     请求
     * @return 初始化是否成功
     */
    public static Mono<Boolean> initPartUpload(MetaData metaData, String uploadId, String accountName,
                                               List<Tuple3<String, String, String>> nodeList, String storage, MsHttpRequest request, String snapshotLink) {
        String bucket = request.getBucketName();
        String obj = request.getObjectName();
        String vnode = nodeList.get(0).var3;

        metaData.setStorage(storage);

        InitPartInfo initPartInfo = new InitPartInfo()
                .setBucket(bucket)
                .setInitAccountName(accountName)
                .setInitAccount(request.getUserId())
                .setObject(obj)
                .setStorage(storage)
                .setUploadId(uploadId)
                .setMetaData(metaData)
                .setInitiated(MsDateUtils.stampToISO8601(metaData.stamp))
                .setSnapshotMark(metaData.snapshotMark);
//                .setVersionNum(VersionUtil.getVersionNum(bucket));

        return VersionUtil.getVersionNum(bucket, obj)
                .doOnNext(initPartInfo::setVersionNum)
                .flatMap(versionNum -> ECUtils.putRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), initPartInfo.getPartKey(vnode), Json.encode(initPartInfo),
                        INIT_PART_UPLOAD, ERROR_INIT_PART_UPLOAD, nodeList, request, snapshotLink))
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "InitiateMultipartUpload fail");
                    }
                });
    }

    public static Mono<Boolean> initPartUpload(InitPartInfo initPartInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String snapshotLink) {
        return initPartUpload(initPartInfo, nodeList, request, false, snapshotLink);
    }

    public static Mono<Boolean> initPartUpload(InitPartInfo initPartInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, boolean isMigrate, String snapshotLink) {
        String vnode = nodeList.get(0).var3;
        return ECUtils.putRocksKey(StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket), initPartInfo.getPartKey(vnode), Json.encode(initPartInfo),
                        INIT_PART_UPLOAD, ERROR_INIT_PART_UPLOAD, nodeList, request, isMigrate, snapshotLink)
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "InitiateMultipartUpload fail");
                    }
                });
    }

    public static final TypeReference<Tuple3<Boolean, String, InitPartInfo>[]>
            LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE =
            new TypeReference<Tuple3<Boolean, String, InitPartInfo>[]>() {
            };

    public static void listMultiPartUploads(Owner uploadOwner, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, Disposable[] disposables) {
        String bucketName = request.getBucketName();
        String maxUploads = request.getParam(MAX_UPLOADS, "1000");
        String prefix = request.getParam(PREFIX, "");
        String marker = request.getParam(KEY_MARKER, "");
        String delimiter = request.getParam(DELIMITER, "");
        String uploadIdMarker = request.getParam(UPLOAD_ID_MARKER, "");
        int maxUploadsInt = 1000;

        try {
            maxUploadsInt = Integer.parseInt(maxUploads);
            if (maxUploadsInt < 1 || maxUploadsInt > 1000) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "listMultiPartUploads error, max_uploads param error, " +
                        "must be int in [1, 1000]");
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "listMultiPartUploads error, max_uploads param error, " +
                    "must be int in [1, 1000]");
        }

        if (StringUtils.isBlank(marker) && StringUtils.isNotBlank(uploadIdMarker)) {
            uploadIdMarker = "";
        }

        ListMultipartUploadsResult listMultiUploadsRes = new ListMultipartUploadsResult()
                .setBucket(bucketName)
                .setDelimiter(delimiter)
                .setKeyMarker(marker)
                .setUploadIdMarker(uploadIdMarker)
                .setPrefix(prefix)
                .setMaxUploads(maxUploadsInt);

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("vnode", nodeList.get(0).var3)
                .put("bucket", bucketName)
                .put("maxUploads", String.valueOf(maxUploadsInt))
                .put("prefix", prefix)
                .put("marker", marker)
                .put("delimiter", delimiter)
                .put("uploadIdMarker", uploadIdMarker);
        Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
        Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2))
                .collect(Collectors.toList());

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        ListMultiPartClientMergeChannel listMultiPartClientMergeChannel = (ListMultiPartClientMergeChannel) new ListMultiPartClientMergeChannel(uploadOwner, listMultiUploadsRes, storagePool, bucketVnodeList, request, currentSnapshotMark)
                .withBeginPrefix(prefix, marker, delimiter);
        listMultiPartClientMergeChannel.request(msg);

//        ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo =
//                ClientTemplate.oneResponse(msgs, LIST_MULTI_PART_UPLOAD, LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE, nodeList);
//
//        ListMultiPartClientHandler handler = new ListMultiPartClientHandler(uploadOwner, listMultiUploadsRes, responseInfo, nodeList, request);
//
//        disposables[1] = responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);

        disposables[2] = listMultiPartClientMergeChannel.response().doOnNext(b -> {
            if (!b) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List MultiPart Uploads fail");
            }
        }).subscribe(b -> {
            try {
                byte[] res = JaxbUtils.toByteArray(listMultiUploadsRes);
                addPublicHeaders(request, getRequestId())
                        .putHeader(CONTENT_TYPE, "application/xml")
                        .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                        .write(Buffer.buffer(res));
                addAllowHeader(request.response()).end();
            } catch (Exception e) {
                log.error(e);
                dealException(request, e);
            }

        }, e -> dealException(request, e));
    }

    public static Mono<String> partUpload(StoragePool storagePool, String fileName, List<Tuple3<String, String, String>> nodeList,
                                          MsHttpRequest request, PartInfo partInfo, MonoProcessor<Boolean> recoverDataProcessor, String crypto, boolean isChunked) {
        return partUpload(storagePool, fileName, nodeList, request, partInfo, recoverDataProcessor, crypto, isChunked, false);
    }

    public static Mono<String> partUpload(StoragePool storagePool, String fileName, List<Tuple3<String, String, String>> nodeList,
                                          MsHttpRequest request, PartInfo partInfo, MonoProcessor<Boolean> recoverDataProcessor, String crypto, boolean isChunked, boolean replace) {
        LimitEncoder handler = storagePool.getLimitEncoder(request, true);
        Digest digest;
        try {
            if (FastMd5Digest.isAvailable() && StringUtils.isBlank(request.getHeader(SYNC_COMPRESS))) {
                digest = FastMd5DigestPool.getInstance().borrow();
                Digest finalDigest = digest;
                request.addResponseEndHandler(v -> {
                    if (request.getDataHandlerCompleted().get()) {
                        FastMd5DigestPool.getInstance().returnDigest(finalDigest);
                    } else {
                        FastMd5DigestPool.getInstance().invalidateDigest(finalDigest);
                    }
                });
            } else {
                digest = new Md5Digest();
            }
        } catch (Exception e) {
            digest = new Md5Digest();
        }
        String[] md5 = new String[1];
        Long[] actualPartSize = new Long[1];
        MonoProcessor<String> processor = MonoProcessor.create();

        if (!isChunked) {
            long objSize = partInfo.partSize;
            if (objSize == 0) {
                processor.onNext(SysConstants.ZERO_ETAG);
                return processor;
            }
        }

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", fileName)
                .put("compression", storagePool.getCompression());

        if (replace) {
            msg.put("replace", "1");
        }
        CryptoUtils.generateKeyPutToMsg(crypto, msg);

        return processHttpBody(request, handler, digest, md5, actualPartSize, processor, new JsonObject())
                .doOnNext(b -> {
                    if (request.headers().contains("Expect")) {
                        request.response().writeContinue();
                    }
                })
                .flatMap(b -> PartUtils.partUpload(storagePool, msg, nodeList, request, handler.data(), partInfo, handler, recoverDataProcessor))
                .flatMap(b -> {
                    if (b) {
                        partInfo.setPartSize(actualPartSize[0]);
                        processor.onNext(md5[0]);
                    } else {
                        processor.onNext("");
                    }
                    return processor;
                });
    }

    public static Mono<String> getThenPartUpload(StoragePool targetPool, String targetFileName, MetaData sourceMeta, long startIndex,
                                                 long endIndex, List<Tuple3<String, String, String>> targetNodeList,
                                                 MsHttpRequest request, PartInfo partInfo, String crypto, MonoProcessor<Boolean> recoverDataProcessor) {
        //获取原始数据字节流，放入ecEncodeHanlder进行encode，生成dataFluxes，dataFluxes包含k+m个数据流
        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        MonoProcessor<String> res = MonoProcessor.create();
        MessageDigest digest = DigestUtils.getMd5Digest();
        final String[] md5 = new String[1];
        StoragePool sourcePool = StoragePoolFactory.getStoragePool(sourceMeta);
        List<Tuple3<String, String, String>> sourceNodeList = sourcePool.mapToNodeInfo(sourcePool.getObjectVnodeId(sourceMeta)).block();

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

        ErasureClient.getObject(sourcePool, sourceMeta, startIndex, endIndex, sourceNodeList, streamController, request, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    digest.update(bytes);
                    ecEncodeHandler.put(bytes);
                }, e -> log.error("", e), () -> md5[0] = Hex.encodeHexString(digest.digest()));

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", targetFileName)
                .put("compression", targetPool.getCompression());

        CryptoUtils.generateKeyPutToMsg(crypto, msg);

        List<UnicastProcessor<Payload>> processors = new ArrayList<>();

        for (int i = 0; i < targetNodeList.size(); i++) {
            Tuple3<String, String, String> tuple = targetNodeList.get(i);
            int index = i;
            SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

            UnicastProcessor<Payload> processor = UnicastProcessor.create();
            processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PART_UPLOAD.name()));


            ecEncodeHandler.data()[index].subscribe(bytes -> {
                        processor.onNext(DefaultPayload.create(bytes, PART_UPLOAD.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        processor.onNext(DefaultPayload.create("", ERROR.name()));
                        processor.onComplete();
                    },
                    () -> {
                        processor.onNext(DefaultPayload.create("", COMPLETE_PART_UPLOAD.name()));
                        processor.onComplete();
                    });

            processors.add(processor);
        }

        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(processors, String.class, targetNodeList);
        List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
//        String storageName = "storage_" + targetPool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + targetPool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        AtomicInteger putNum = new AtomicInteger();

        //控制单次发送数据流大小，防止占用大量内存
        Disposable disposable = responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                    }

                    if (putNum.incrementAndGet() == targetNodeList.size()) {
                        next.onNext(new Tuple2<>(-1, 0));
                        putNum.set(errorChunksList.size());
                    }
                },
                e -> log.error("send data error", e),
                () -> {
                    if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                        res.onNext(md5[0]);
                    } else if (responseInfo.successNum >= targetPool.getK()) {
                        res.onNext(md5[0]);
                        recoverDataProcessor.subscribe(s -> {
                            //若至少成功写了一个partInfo，发送修复数据的消息
                            if (s) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("storage", targetPool.getVnodePrefix())
                                        .put("bucket", partInfo.bucket)
                                        .put("object", partInfo.object)
                                        .put("uploadId", partInfo.uploadId)
                                        .put("partNum", partInfo.partNum)
                                        .put("fileName", partInfo.fileName)
                                        .put("endIndex", String.valueOf(partInfo.partSize - 1))
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("versionId", partInfo.versionId)
                                        .put("poolQueueTag", poolQueueTag);
                                Optional.ofNullable(partInfo.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                CryptoUtils.putCryptoInfoToMsg(msg.get("crypto"), msg.get("secretKey"), errorMsg);

                                ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_PART_UPLOAD_FILE);
                            }
                        });
                    } else {
                        res.onNext("");
                        //响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", partInfo.bucket);
                        errorMsg.put("object", partInfo.object);
                        errorMsg.put("fileName", partInfo.fileName);
                        errorMsg.put("storage", targetPool.getVnodePrefix());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        request.addResponseCloseHandler(v -> {
            for (UnicastProcessor<Payload> processor : processors) {
                processor.onNext(ERROR_PAYLOAD);
                processor.onComplete();
            }
            disposable.dispose();
        });

        return res;
    }

    public static Mono<PartInfo> getPartInfo(String bucket, String object, String uploadId, String partNum) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket),
                        PartInfo.getPartKey(bucketVnode, bucket, object, uploadId, partNum, null), PartInfo.class
                        , GET_PART_UPLOAD_META, NOT_FOUND_PART_INFO, ERROR_PART_INFO, null, PartInfo::getVersionNum,
                        Comparator.comparing(a -> a.versionNum), (a, b, c, d) -> Mono.just(1), nodeList, null))
                .flatMap(tuple -> Mono.just(tuple.var1));
    }

    public static Mono<Boolean> partUploadMeta(PartInfo partInfo, List<Tuple3<String, String, String>> nodeList,
                                               MonoProcessor<Boolean> recoverDataProcessor, MsHttpRequest request, String snapshotLink) {
        return partUploadMeta(partInfo, nodeList, recoverDataProcessor, request, false, snapshotLink);
    }

    public static Mono<Boolean> partUploadMeta(PartInfo partInfo, List<Tuple3<String, String, String>> nodeList,
                                               MonoProcessor<Boolean> recoverDataProcessor, MsHttpRequest request, boolean isMigrate, String snapshotLink) {
        String vnode = nodeList.get(0).var3;
        String partKey = partInfo.getPartKey(vnode);

        boolean[] overWrite = new boolean[]{false};
        final PartInfo[] validInfo = {null};
        List<String> overWriteFiles = new ArrayList<>();
        Consumer<Tuple3<Integer, ErasureServer.PayloadMetaType, String>> responseHandler = s -> {
            if (s.var2 == SUCCESS) {
                if (StringUtils.isNotBlank(s.var3)) {
                    Tuple2<String, String> tuple2 = Json.decodeValue(s.var3, Tuple2.class);
                    if (StringUtils.isNotBlank(tuple2.var1)) {
                        overWriteFiles.add(tuple2.var1);
                        overWrite[0] = true;
                    }
                    if (StringUtils.isNotBlank(tuple2.var2)) {
                        PartInfo oldPartInfo = Json.decodeValue(tuple2.var2, PartInfo.class);
                        if (oldPartInfo != null) {
                            if (validInfo[0] == null || oldPartInfo.versionNum.compareTo(validInfo[0].versionNum) > 0) {
                                validInfo[0] = oldPartInfo;
                            }
                        }
                    }
                }
            }
        };

        return ECUtils.putRocksKey(StoragePoolFactory.getMetaStoragePool(partInfo.bucket), partKey, Json.encode(partInfo),
                        PART_UPLOAD_META, ERROR_PART_UPLOAD_META, responseHandler, nodeList, recoverDataProcessor, request, null, null, null, isMigrate, snapshotLink)
                .doOnNext(b -> {
                    if (b && overWrite[0]) {
                        StoragePool dataPool = StoragePoolFactory.getStoragePool(partInfo);
                        dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(validInfo[0]))
                                .flatMap(list -> {
                                    //防止出现重复upload时已merge导致原upload的fileName被删掉问题
                                    return ErasureClient.getObjectMetaVersionUnlimited(partInfo.bucket, partInfo.object, partInfo.versionId, nodeList, null, partInfo.snapshotMark, null)
                                            .flatMap(meta -> {
                                                if (meta.equals(ERROR_META)) {
                                                    log.error("delete overwrite data {} error!", Arrays.toString(overWriteFiles.toArray(new String[0])));
                                                }
                                                if (meta.equals(NOT_FOUND_META) || meta.deleteMark || meta.deleteMarker
                                                        || StringUtils.isEmpty(meta.partUploadId) || !meta.partUploadId.equals(partInfo.uploadId)) {
                                                    return ErasureClient.deleteObjectFile(dataPool, overWriteFiles.toArray(new String[0]), request);
                                                } else {
                                                    for (PartInfo info : meta.partInfos) {
                                                        if (info.partNum.equals(partInfo.partNum)) {
                                                            if (info.fileName.equals(partInfo.fileName)) {
                                                                return ErasureClient.deleteObjectFile(dataPool, overWriteFiles.toArray(new String[0]), request);
                                                            } else {
                                                                overWriteFiles.clear();
                                                                overWriteFiles.add(partInfo.fileName);
                                                                log.info("delete overwrite data {}", Arrays.toString(overWriteFiles.toArray(new String[0])));
                                                                return ErasureClient.deleteObjectFile(dataPool, overWriteFiles.toArray(new String[0]), request);
                                                            }
                                                        }
                                                    }
                                                    return ErasureClient.deleteObjectFile(dataPool, overWriteFiles.toArray(new String[0]), request);

                                                }
                                            });
                                })
                                .subscribe(s -> {
                                }, e -> log.error("delete overwrite data error!"));
                    }
                });
    }

    public static Mono<Integer> repairInitPartInfo(String partKey, InitPartInfo info, List<Tuple3<String, String, String>> nodeList,
                                                   MsHttpRequest request, Tuple2<ErasureServer.PayloadMetaType, InitPartInfo>[] res) {
        return repairInitPartInfo(partKey, info, nodeList, request, res, null);
    }

    /**
     * 修复initPart--桶快照版
     * 修复时会根据snapshotLink对快照前的同名分段进行逻辑删除
     */
    public static Mono<Integer> repairInitPartInfo(String partKey, InitPartInfo info, List<Tuple3<String, String, String>> nodeList,
                                                   MsHttpRequest request, Tuple2<ErasureServer.PayloadMetaType, InitPartInfo>[] res, String snapshotLink) {
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<ErasureServer.PayloadMetaType, InitPartInfo> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    if (!Objects.equals(tuple2.var2.snapshotMark, info.snapshotMark)) {
                        // get到的数据和待更新数据不是同一快照下，则说明当前快照下元数据为not found
                        oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                    } else {
                        oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                    }
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(info.bucket);
        return VersionUtil.getLastVersionNum(info.getVersionNum(), info.bucket, info.object)
                .doOnNext(info::setVersionNum)
                .flatMap(lastVersionNum -> ECUtils.updateRocksKey(pool, oldVersionNum, partKey, Json.encode(info),
                        REPAIR_INIT_PART_UPLOAD, ERROR_INIT_PART_UPLOAD, nodeList, request, null, snapshotLink));
    }

    public static Mono<InitPartInfo> getInitPartInfo(String bucket, String object, String uploadId,
                                                     List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        String vnode = nodeList.get(0).var3;
        String partKey = InitPartInfo.getPartKey(vnode, bucket, object, uploadId);

        InitPartInfo deleteMark = new InitPartInfo()
                .setBucket(bucket)
                .setObject(object)
                .setUploadId(uploadId)
                .setDelete(true);

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), partKey, InitPartInfo.class,
                        GET_PART_META, NO_SUCH_UPLOAD_ID_INIT_PART_INFO, ERROR_INIT_PART_INFO,
                        deleteMark, InitPartInfo::getVersionNum, Comparator.comparing(a -> a.versionNum),
                        (partKey1, info, nodeList1, res) -> repairInitPartInfo(partKey1, info, nodeList1, request, res), nodeList, request)
                .flatMap(tuple -> Mono.just(tuple.var1));
    }

    /**
     * 获取initPart---桶快照版
     * 会先去currentSnapshotMark下获取数据，获取不到再去snapshotLink下获取
     */
    public static Mono<InitPartInfo> getInitPartInfo(String bucket, String object, String uploadId,
                                                     List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        String vnode = nodeList.get(0).var3;
        String partKey = InitPartInfo.getPartKey(vnode, bucket, object, uploadId, currentSnapshotMark);

        InitPartInfo deleteMark = new InitPartInfo()
                .setBucket(bucket)
                .setObject(object)
                .setUploadId(uploadId)
                .setSnapshotMark(currentSnapshotMark)
                .setDelete(true);
        Comparator<InitPartInfo> comparator = (o1, o2) -> {
            if (snapshotLink != null && !Objects.equals(o1.snapshotMark, o2.snapshotMark)) {
                return Objects.equals(o1.snapshotMark, currentSnapshotMark) ? 1 : -1;
            }
            return o1.versionNum.compareTo(o2.versionNum);
        };
        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), partKey, InitPartInfo.class,
                        GET_PART_META, NO_SUCH_UPLOAD_ID_INIT_PART_INFO, ERROR_INIT_PART_INFO,
                        deleteMark, InitPartInfo::getVersionNum, comparator,
                        (partKey1, info, nodeList1, res) -> repairInitPartInfo(partKey1, info, nodeList1, request, res, snapshotLink), nodeList, request,
                        currentSnapshotMark, snapshotLink)
                .flatMap(tuple -> Mono.just(tuple.var1));
    }

    public static Mono<Tuple2<InitPartInfo, Tuple2<ErasureServer.PayloadMetaType, InitPartInfo>[]>> getInitPartInfoRes(String bucket, String object, String uploadId,
                                                                                                                       List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String snapshotMark) {
        String vnode = nodeList.get(0).var3;
        String partKey = InitPartInfo.getPartKey(vnode, bucket, object, uploadId, snapshotMark);//生成!开头的key

        InitPartInfo deleteMark = new InitPartInfo()
                .setBucket(bucket)
                .setObject(object)
                .setUploadId(uploadId)
                .setDelete(true)
                .setSnapshotMark(snapshotMark);


        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), partKey, InitPartInfo.class,
                GET_PART_META, NO_SUCH_UPLOAD_ID_INIT_PART_INFO, ERROR_INIT_PART_INFO,
                deleteMark, InitPartInfo::getVersionNum, Comparator.comparing(a -> a.versionNum),
                (partKey1, info, nodeList1, res) -> repairInitPartInfo(partKey1, info, nodeList1, request, res), nodeList, request);
    }

    public static Mono<PartInfo> getPartInfo(String bucket, String object, String uploadId, String partNum, String partKey,
                                             List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, String updateDirList) {
        PartInfo deleteMark = new PartInfo()
                .setBucket(bucket)
                .setObject(object)
                .setUploadId(uploadId)
                .setPartNum(partNum)
                .setDelete(true)
                .setSnapshotMark(currentSnapshotMark);

        Comparator<PartInfo> comparator = (o1, o2) -> {
            if (snapshotLink != null && !Objects.equals(o1.snapshotMark, o2.snapshotMark)) {
                return Objects.equals(o1.snapshotMark, currentSnapshotMark) ? 1 : -1;
            }
            return o1.versionNum.compareTo(o2.versionNum);
        };

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), partKey,
                PartInfo.class, GET_PART_INFO, NO_SUCH_UPLOAD_ID_PART_INFO, ERROR_PART_INFO,
                deleteMark, PartInfo::getVersionNum, comparator,
                (partKey1, partInfo, nodelist1, res) -> {
                    if (StringUtils.isNotBlank(updateDirList)) {
                        partInfo.tmpUpdateQuotaKeyStr = updateDirList;
                    }
                    return ErasureClient.updatePartInfo(partKey1, partInfo, nodelist1, request, res, snapshotLink);
                }
                , nodeList, request, currentSnapshotMark, snapshotLink).flatMap(tuple -> Mono.just(tuple.var1));
    }

    public static Mono<Tuple2<PartInfo, Tuple2<ErasureServer.PayloadMetaType, PartInfo>[]>> getPartInfoRes(String bucket, String object, String uploadId, String partNum, String partKey,
                                                                                                           List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String snapshotMark) {
        PartInfo deleteMark = new PartInfo()
                .setBucket(bucket)
                .setObject(object)
                .setUploadId(uploadId)
                .setPartNum(partNum)
                .setDelete(true)
                .setSnapshotMark(snapshotMark);

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), partKey,
                PartInfo.class, GET_PART_INFO, NO_SUCH_UPLOAD_ID_PART_INFO, ERROR_PART_INFO,
                deleteMark, PartInfo::getVersionNum, Comparator.comparing(a -> a.versionNum),
                (partKey1, partInfo, nodelist1, res) -> ErasureClient.updatePartInfo(partKey1, partInfo, nodelist1, request, res, null)
                , nodeList, request, snapshotMark, null);
    }


    public static Mono<String> completeMultiPartUpload(String bucket, String object, String uploadId, InitPartInfo info,
                                                       List<Tuple3<String, String, String>> bucketNodeList, String migrateVnodeId,
                                                       MsHttpRequest request, boolean esEnable, boolean isHardLink, Inode oldInode, String currentSnapshotMark, String snapshotLink) {
        MonoProcessor<String> processor = MonoProcessor.create();
        Buffer buffer = Buffer.buffer();
        MetaData[] metaData = new MetaData[]{null};
        String stamp = StringUtils.isBlank(request.getHeader("stamp")) ?
                String.valueOf(System.currentTimeMillis()) : request.getHeader("stamp");
        request.handler(buffer::appendBuffer);

        Disposable[] disposables = new Disposable[1];
        request.endHandler(v -> {
            List<Part> pastList;
            int code = AuthorizeV4.checkManageStreamPayloadSHA256(request, buffer.getBytes());
            if (code != ErrorNo.SUCCESS_STATUS) {
                processor.onError(new MsException(code, "The Authorization was wrong."));
                return;
            }

            try {
                CompleteMultipartUpload completeMultipartUpload = (CompleteMultipartUpload) JaxbUtils.toObject(new String(buffer.getBytes()));
                verifyCompleteMultipartUpload(completeMultipartUpload);
                pastList = completeMultipartUpload.getParts();
            } catch (Exception e) {
                processor.onError(new MsException(ErrorNo.PART_INVALID, "The request body is error."));
                return;
            }

            Part[] parts = pastList.toArray(new Part[0]);

            disposables[0] = PartUtils.checkCompletePart(parts, bucket, object, uploadId, bucketNodeList, request, currentSnapshotMark, snapshotLink)
                    .zipWith(VersionUtil.getVersionNum(bucket, object))
                    .doOnError(processor::onError)
                    .flatMap(tuple2 -> {
                        Tuple3<String, PartInfo[], String> t = tuple2.getT1();
                        String versionNum = tuple2.getT2();
                        //info.setDelete(true);
                        info.setVersionNum(versionNum);
                        info.metaData.setVersionNum(versionNum);
                        info.metaData.setDeleteMark(false);
                        info.metaData.setPartInfos(t.var2);
                        JsonObject sysMeta = new JsonObject(info.metaData.sysMetaData);
                        long objectSize = 0L;
                        for (PartInfo partInfo : t.var2) {
                            objectSize += partInfo.partSize;
                        }
                        String syncStamp = StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? versionNum : request.getHeader(SYNC_STAMP);
                        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));
                        sysMeta.put(ETAG, t.var1);
                        sysMeta.put(CONTENT_LENGTH, String.valueOf(objectSize));
                        sysMeta.put(LAST_MODIFY, lastModify);
                        info.metaData.setStartIndex(0L);
                        info.metaData.setEndIndex(objectSize - 1);
                        info.metaData.setStamp(stamp);
                        info.metaData.setSysMetaData(sysMeta.encode());
                        info.metaData.setSyncStamp(syncStamp);
                        info.metaData.setShardingStamp(VersionUtil.getVersionNum());
                        metaData[0] = info.metaData;
//                        if (isHardLink) {
//                            Inode newInode = Inode.defaultInode(info.metaData);
//                            info.metaData.inode = oldInode.getNodeId();
//                            info.metaData.cookie = oldInode.getCookie();
//                            oldInode.setInodeData(newInode.getInodeData());
//                            int secNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
//                            oldInode.setRootTime(Long.parseLong(info.metaData.stamp) / 1000, secNano);
//                            oldInode.setVersionNum(info.metaData.getVersionNum());
//                            oldInode.setSize(newInode.getSize());
//                            oldInode.setMode(oldInode.getMode());
//                            oldInode.setCifsMode(oldInode.getCifsMode());
//                            CifsUtils.setDefaultCifsMode(oldInode);
//                            oldInode.setMajorDev(oldInode.getMajorDev());
//                            oldInode.setMinorDev(oldInode.getMinorDev());
//                            oldInode.setStorage(info.metaData.storage);
//                            info.metaData.tmpInodeStr = Json.encode(oldInode);
//                        } else if (oldInode != null) {
//                            info.metaData.tmpInodeStr = Json.encode(oldInode);
//                        }
                        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
                        return Mono.just(true)
                                .flatMap(b -> {
                                    if (StringUtils.isNotEmpty(migrateVnodeId)) {
                                        return metaStoragePool.mapToNodeInfo(migrateVnodeId)
                                                .flatMap(nodeList -> PartUtils.completeMultiPart(t.var1, info, nodeList, request, snapshotLink));
                                    }
                                    return Mono.just("1");
                                })
                                .flatMap(md5 -> {
                                    if (StringUtils.isBlank(md5)) {
                                        log.error("complete meta to new mapping error!");
                                        return Mono.just(false);
                                    }
                                    return Mono.just(true);
                                }).flatMap(b -> {
                                    if (b) {
                                        return PartUtils.completeMultiPart(t.var1, info, bucketNodeList, request, snapshotLink);
                                    }
                                    return Mono.just("");
                                });
                    })
                    .subscribe(processor::onNext);
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));
        request.resume();

        return processor.flatMap(md5 -> {
            if (StringUtils.isNotBlank(md5) && esEnable) {
                MonoProcessor<String> esRes = MonoProcessor.create();
                EsMeta esMeta = new EsMeta()
                        .setUserId(request.getUserId())
                        .setSysMetaData(metaData[0].sysMetaData)
                        .setUserMetaData(metaData[0].userMetaData)
                        .setBucketName(metaData[0].bucket)
                        .setObjName(metaData[0].key)
                        .setVersionId(info.metaData.versionId)
                        .setStamp(stamp)
                        .setObjSize(String.valueOf(metaData[0].endIndex + 1))
                        .setInode(metaData[0].inode);
                Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).subscribe(b -> esRes.onNext(md5));
                return esRes;
            } else {
                return Mono.just(md5);
            }
        });
    }

    public static Mono<Boolean> listParts(ListPartsResult listPartsResult, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("vnode", nodeList.get(0).var3)
                .put("bucket", listPartsResult.getBucket())
                .put("uploadId", listPartsResult.getUploadId())
                .put("object", listPartsResult.getKey())
                .put("maxParts", String.valueOf(listPartsResult.getMaxParts()))
                .put("partNumberMarker", String.valueOf(listPartsResult.getPartNumberMarker()));
        Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
        Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2))
                .collect(Collectors.toList());

        ResponseInfo<PartInfo[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_PART, PartInfo[].class, nodeList);
        ListPartsClientHandler handler = new ListPartsClientHandler(listPartsResult, responseInfo, nodeList, request, currentSnapshotMark, snapshotLink);

        Disposable subscribe = responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return handler.res;
    }

    public static Mono<List<PartInfo>> getPartInfoList(ListPartsResult listPartsResult, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("vnode", nodeList.get(0).var3)
                .put("bucket", listPartsResult.getBucket())
                .put("uploadId", listPartsResult.getUploadId())
                .put("object", listPartsResult.getKey())
                .put("maxParts", String.valueOf(listPartsResult.getMaxParts()))
                .put("partNumberMarker", String.valueOf(listPartsResult.getPartNumberMarker()));

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2))
                .collect(Collectors.toList());

        ResponseInfo<PartInfo[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_PART, PartInfo[].class, nodeList);
        ListPartsClientHandler handler = new ListPartsClientHandler(listPartsResult, responseInfo, nodeList, request, null, null);

        Disposable subscribe = responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return handler.res.map(s -> {
            if (s) {
                return handler.partInfoList;
            } else {
                return new ArrayList<>();
            }
        });
    }

    /**
     * 中断分段上传
     * 分段任务删除标记
     * 删除分段相关数据
     *
     * @param bucketNodeList      bucket node list
     * @param initPartInfo        init part info
     * @param request
     * @param currentSnapshotMark 中断分段上传时的快照标记
     * @return 是否成功
     */
    public static Mono<Boolean> abortMultiPartUpload(StoragePool dataPool, List<Tuple3<String, String, String>> bucketNodeList, InitPartInfo initPartInfo,
                                                     MsHttpRequest request, String currentSnapshotMark) {
        return VersionUtil.getVersionNum(initPartInfo.bucket, initPartInfo.object)
                .flatMap(versionNum -> abortMultiPartUpload(dataPool, bucketNodeList, initPartInfo, request, versionNum, currentSnapshotMark, false));
    }

    public static Mono<Boolean> abortMultiPartUpload(StoragePool dataPool, List<Tuple3<String, String, String>> bucketNodeList, InitPartInfo initPartInfo,
                                                     MsHttpRequest request, String currentSnapshotMark, boolean deletePartMetaOnly, Integer... ignoreDeletePartNums) {
        return VersionUtil.getVersionNum(initPartInfo.bucket, initPartInfo.object)
                .flatMap(versionNum -> abortMultiPartUpload(dataPool, bucketNodeList, initPartInfo, request, versionNum, currentSnapshotMark, deletePartMetaOnly, ignoreDeletePartNums));
    }

    @SuppressWarnings("CallingSubscribeInNonBlockingScope")
    private static Mono<Boolean> abortMultiPartUpload(StoragePool dataPool, List<Tuple3<String, String, String>> bucketNodeList, InitPartInfo initPartInfo,
                                                      MsHttpRequest request, String versionNum, String currentSnapshotMark, boolean deletePartMetaOnly, Integer... ignoreDeletePartNums) {
        String vnode = bucketNodeList.get(0).var3;
        if (currentSnapshotMark != null && !Objects.equals(currentSnapshotMark, initPartInfo.snapshotMark)) {
            // 快照创建后中断快照创建前的分段，则对快照前分段进行逻辑删除
            Set<String> unView = new HashSet<>(1);
            unView.add(currentSnapshotMark);
            initPartInfo.setUnView(unView);
        } else {
            initPartInfo.setDelete(true);
            if (initPartInfo.metaData != null) {
                initPartInfo.metaData.setDeleteMark(true);
            }
        }
        initPartInfo.setVersionNum(versionNum);

        List<SocketReqMsg> msgs = ECUtils.mapToMsg(initPartInfo.getPartKey(vnode), Json.encode(initPartInfo), bucketNodeList);

        //复用init写数据接口
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, MARK_ABORT_PARTS, String.class, bucketNodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        Disposable[] disposables = new Disposable[2];
        disposables[0] = responseInfo.responses
                .doOnComplete(() -> {
                    if (responseInfo.writedNum > 0) {
                        initPartInfo.metaData = null;
                    }
                    if (responseInfo.errorNum == 0) {
                        if (!deletePartMetaOnly) {
                            DelDeleteMark.putDeleteKey(initPartInfo.getPartKey(vnode), Json.encode(initPartInfo));
                            res.onNext(true);
                        } else {
                            disposables[1] = PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, bucketNodeList, true, initPartInfo.snapshotMark, currentSnapshotMark)
                                    .subscribe(s -> res.onNext(true));
                        }
                    } else if (responseInfo.errorNum <= metaPool.getM()) {
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(metaPool.getVnodePrefix());
                        if (deletePartMetaOnly) {
                            disposables[1] = PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, bucketNodeList, true, initPartInfo.snapshotMark, currentSnapshotMark)
                                    .subscribe(s -> res.onNext(true));
                        } else {
                            res.onNext(true);
                        }
                        // 故障节点恢复后开启修复
                        log.debug("abort {}/{}?{} multiple part successfully. but produce an exception to MQ. maybe some nodes occurred internal error or detached cluster.", initPartInfo.bucket,
                                initPartInfo.object, initPartInfo.uploadId);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("initPartInfo", Json.encode(initPartInfo))
                                .put("poolQueueTag", poolQueueTag)
                                .put("deletePartMetaOnly", String.valueOf(deletePartMetaOnly));
                        Optional.ofNullable(currentSnapshotMark).ifPresent(v -> errorMsg.put("currentSnapshotMark", v));
                        Optional.ofNullable(ignoreDeletePartNums).ifPresent(v -> errorMsg.put("ignoreDeletePartNums", Json.encode(Arrays.asList(v))));
                        ECUtils.publishEcError(responseInfo.res, bucketNodeList, errorMsg, ERROR_ABORT_MULTI_PART);
                    } else {
                        //未成功，但小于k个节点成功，不返回结果
                        log.error("abort {}/{}?{} failed. cannot response. maybe more than m nodes occurred internal error or detached cluster.", initPartInfo.bucket, initPartInfo.object,
                                initPartInfo.uploadId);
                    }
                })
                .doOnError(throwable -> log.error("", throwable))
                .subscribe();

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));
        return res;
    }

    /**
     * 验证CompleteMultipartUpload参数有效性
     *
     * @param completeMultipartUpload
     */
    private static void verifyCompleteMultipartUpload(CompleteMultipartUpload completeMultipartUpload) {
        final boolean b = completeMultipartUpload == null || completeMultipartUpload.getParts() == null;
        if (b) {
            throw new MsException(MALFORMED_XML, "The XML is not well-formed");
        }
        final List<Part> parts = completeMultipartUpload.getParts();
        int curPartNumber = 0;
        for (Part part : parts) {
            if (part == null || part.getPartNumber() == null || part.getEtag() == null) {
                throw new MsException(MALFORMED_XML, "The XML is not well-formed");
            }

            if (part.getPartNumber() <= curPartNumber) {
                throw new MsException(INVALED_PART_ORDER, "The list of parts was not in ascending order.");
            }
            curPartNumber = part.getPartNumber();
        }
    }
}
