package com.macrosan.ec.part;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.BaseEncoding;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.*;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.client.ListPartsClientHandler;
import com.macrosan.storage.coder.LimitEncoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.IS_SYNCING;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class PartUtils {
    public static boolean firstLoad = true;

    public static void init() {
        try {
            Long redisPartSize = getPartSizeFromRedis();

            if (redisPartSize != null && isValidPartSize(redisPartSize)) {
                if (redisPartSize > UPLOAD_PART_MIN_SIZE || firstLoad) {
                    log.info("updating part size limit from {} to {}", UPLOAD_PART_MIN_SIZE, redisPartSize);
                    UPLOAD_PART_MIN_SIZE = redisPartSize;
                    if (UPLOAD_PART_MIN_SIZE < 5 * 1024 * 1024) {
                        RedisConnPool.getInstance()
                                .getShortMasterCommand(REDIS_SYSINFO_INDEX)
                                .set(PART_CONFIG_SIGN, "1");
                    }
                }
                firstLoad = false;
            }
            if (redisPartSize == null && !firstLoad){
                log.info("part size limit reset to 5242880");
                UPLOAD_PART_MIN_SIZE = 5 * 1024 * 1024;
                firstLoad = true;
            }
        } catch (Exception e) {
            log.error("part config init failed, please check the config in redis.", e);
        } finally {
            // 定时重新加载
            DISK_SCHEDULER.schedule(() -> {
                try {
                    init();
                } catch (Exception e) {
                    log.error("part config reload failed.", e);
                }
            }, 30, TimeUnit.SECONDS);
        }
    }
    private static Long getPartSizeFromRedis() {
        try {
            String sizeStr = RedisConnPool.getInstance()
                    .getShortMasterCommand(REDIS_SYSINFO_INDEX)
                    .hget(PART_CONFIG, PART_SIZE);
            return sizeStr != null ? Long.parseLong(sizeStr) : null;
        } catch (Exception e) {
            log.error("Failed to get part size from Redis.", e);
            return null;
        }
    }
    private static boolean isValidPartSize(long size) {
        return size >= 100 * 1024 && size <= 5 * 1024 * 1024;
    }
    public static Mono<Boolean> partUpload(StoragePool storagePool, SocketReqMsg msg, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                           Flux<byte[]>[] dataFlux, PartInfo partInfo, LimitEncoder encoder, MonoProcessor<Boolean> recoverDataProcessor) {
        List<UnicastProcessor<Payload>> processors = new ArrayList<>();

        for (int i = 0; i < nodeList.size(); i++) {
            Tuple3<String, String, String> tuple = nodeList.get(i);
            int index = i;
            SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

            UnicastProcessor<Payload> processor = UnicastProcessor.create();
            processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PART_UPLOAD.name()));
            encoder.request(index, 1L);
            dataFlux[index].subscribe(bytes -> {
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

        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(processors, String.class, nodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Set<Integer> errorChunksList = new HashSet<>(storagePool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        Disposable disposable = responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                        encoder.request(s.var1, Long.MAX_VALUE);
                    } else {
                        encoder.request(s.var1, 1L);
                    }
                },
                e -> log.error("", e),
                () -> {
                    if (responseInfo.successNum >= storagePool.getK()) {
                        res.onNext(true);
                        if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
                            res.onNext(true);
                            recoverDataProcessor.subscribe(s -> {
                                //若至少成功写了一个partInfo，发送修复数据的消息
                                if (s) {
                                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                            .put("storage", storagePool.getVnodePrefix())
                                            .put("bucket", partInfo.bucket)
                                            .put("object", partInfo.object)
                                            .put("uploadId", partInfo.uploadId)
                                            .put("partNum", partInfo.partNum)
                                            .put("fileName", partInfo.fileName)
                                            .put("endIndex", String.valueOf(partInfo.partSize - 1))
                                            .put("errorChunksList", Json.encode(new ArrayList<>(errorChunksList)))
                                            .put("versionId", partInfo.versionId)
                                            .put("poolQueueTag", poolQueueTag);
                                    Optional.ofNullable(partInfo.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                    Optional.ofNullable(partInfo.initSnapshotMark).ifPresent(v -> errorMsg.put("initSnapshotMark", v));
                                    Optional.ofNullable(partInfo.tmpUpdateQuotaKeyStr).ifPresent(v -> errorMsg.put("quotaDirList", v));
                                    CryptoUtils.putCryptoInfoToMsg(msg.get("crypto"), msg.get("secretKey"), errorMsg);

                                    ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_PART_UPLOAD_FILE);
                                }
                            });
                        }
                    } else {
                        res.onNext(false);
                        //响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", partInfo.bucket);
                        errorMsg.put("object", partInfo.object);
                        errorMsg.put("fileName", partInfo.fileName);
                        errorMsg.put("storage", storagePool.getVnodePrefix());
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_ROLL_BACK_FILE);
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

    public static Mono<Boolean> deleteMultiPartUploadData(StoragePool dataPool, String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList,
                                                          MsHttpRequest request, String initSnapshotMark, String currentSnapshotMark, Integer... ignoreDeletePartNums) {
        String vnode = bucketNodeList.get(0).var3;
        String partKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, "", currentSnapshotMark);// 只删除当前快照下分段数据块
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(partKey, "", bucketNodeList);
        msgs.forEach(msg -> {
            if (ignoreDeletePartNums.length > 0) {
                msg.put("ignoreDeletePartNums", Arrays.toString(ignoreDeletePartNums));
            }
        });
        ResponseInfo<String[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_PART_INFO_FILE, String[].class, bucketNodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();

        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        AtomicBoolean isDeduplicate = new AtomicBoolean(false);

        Disposable subscribe = responseInfo.responses.doOnComplete(() -> {
            if (responseInfo.successNum >= bucketPool.getK()) {
                Set<String> partFileNames = new HashSet<>();
                Set<String> allFileNames = new HashSet<>();
                for (int i = 0; i < bucketPool.getK() + bucketPool.getM(); i++) {
                    if (responseInfo.res != null && responseInfo.res[i] != null && responseInfo.res[i].var2 != null) {
                        String[] fileNames = responseInfo.res[i].var2;
                        for (String fileName : fileNames) {
                            if (fileName.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                isDeduplicate.set(true);
                            }
                            partFileNames.add(fileName);
                        }
                    }
                }

                if (isDeduplicate.get()) {
                    ErasureClient.deleteDedupObjectFile(bucketPool, partFileNames.stream().toArray(String[]::new), request, false)
                            .flatMap(b -> {
                                if (allFileNames.size() > 0) {
                                    return ErasureClient.deleteObjectFile(dataPool, allFileNames.stream().toArray(String[]::new), request);
                                }
                                return Mono.just(b);
                            })
                            .subscribe(b -> res.onNext(true));
                } else {
                    ErasureClient.deleteObjectFile(dataPool, partFileNames.stream().toArray(String[]::new), request)
                            .subscribe(b -> res.onNext(true));
                }
            } else {
                res.onNext(false);
            }
        }).subscribe();
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res.doOnNext(b -> {
            if (!b) {
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(dataPool.getVnodePrefix());
                SocketReqMsg msg = new SocketReqMsg("", 0)
                        .put("bucket", bucket)
                        .put("object", object)
                        .put("uploadId", uploadId)
                        .put("storage", dataPool.getVnodePrefix())
                        .put("bucketNodeList", Json.encode(bucketNodeList))
                        .put("poolQueueTag", poolQueueTag);
                Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
                Optional.ofNullable(initSnapshotMark).ifPresent(v -> msg.put("initSnapshotMark", v));

                ECUtils.publishEcError(responseInfo.res, bucketNodeList, msg, ERROR_DELETE_PART_DATA);
            }
        });
    }

    public static Mono<Boolean> deleteMultiPartUploadMeta(String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList,
                                                          boolean isAbortPart, String initPartSnapshotMark, String currentSnapshotMark) {
        return deleteMultiPartUploadMeta(bucket, object, uploadId, bucketNodeList, isAbortPart, null, initPartSnapshotMark, currentSnapshotMark);
    }

    public static Mono<Boolean> deleteMultiPartUploadMeta(String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList,
                                                          boolean isAbortPart, List<String> partInfoNums, String initPartSnapshotMark, String currentSnapshotMark) {
        return deleteMultiPartUploadMeta(bucket, object, uploadId, bucketNodeList, isAbortPart, partInfoNums, null, initPartSnapshotMark, currentSnapshotMark);
    }

    public static Mono<Boolean> deleteMultiPartUploadMeta(String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList,
                                                          boolean isAbortPart, List<String> partInfoNums, InitPartInfo initPartInfo, String initPartSnapshotMark, String currentSnapshotMark) {
        return deleteMultiPartUploadMeta(bucket, object, uploadId, bucketNodeList, isAbortPart, partInfoNums, initPartInfo, initPartSnapshotMark, currentSnapshotMark, false);
    }

    /**
     * 删除InitPartInfo和partInfo
     */
    public static Mono<Boolean> deleteMultiPartUploadMeta(String bucket, String object, String uploadId,
                                                          List<Tuple3<String, String, String>> bucketNodeList,
                                                          boolean isAbortPart, List<String> partInfoNums, InitPartInfo initPartInfo, String initPartSnapshotMark, String currentSnapshotMark, boolean discard) {
        String vnode = bucketNodeList.get(0).var3;
        String initPartKey = InitPartInfo.getPartKey(vnode, bucket, object, uploadId, currentSnapshotMark);
        String partKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, "", currentSnapshotMark);
        boolean markDel = initPartInfo != null && initPartInfo.delete;
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(initPartKey, partKey, bucketNodeList);
        msgs = msgs.stream().map(msg -> {
            msg.put("isAbortPart", isAbortPart ? "1" : "0");
            msg.put("bucket", bucket);
            msg.put("vnode", vnode);
            if (markDel) {
                initPartInfo.setVersionNum(VersionUtil.getLastVersionNum(initPartInfo.versionNum));
                msg.put("initPartInfo", Json.encode(initPartInfo));
            }
            Optional.ofNullable(partInfoNums).ifPresent(v -> msg.put("partInfoNums", StringUtils.join(partInfoNums, ",")));
            Optional.ofNullable(initPartSnapshotMark).ifPresent(v -> msg.put("initPartSnapshotMark", v));
            Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
            return msg;
        }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_PART, String.class, bucketNodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        responseInfo.responses.doOnComplete(() -> {
            if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                // 将key写入mqRocks异步处理
                if (markDel) {
                    DelDeleteMark.putDeleteKey(initPartKey, Json.encode(initPartInfo));
                }
                res.onNext(true);
            } else {
                SocketReqMsg msg = new SocketReqMsg("", 0)
                        .put("bucket", bucket)
                        .put("object", object)
                        .put("vnode", vnode)
                        .put("uploadId", uploadId)
                        .put("poolQueueTag", poolQueueTag);
                Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
                if (markDel) {
                    msg.put("initPartInfo", Json.encode(initPartInfo));
                    ECUtils.publishEcError(responseInfo.res, bucketNodeList, msg, ERROR_MARK_DELETE_PART_META);
                } else {
                    Optional.ofNullable(initPartSnapshotMark).ifPresent(v -> msg.put("initPartSnapshotMark", v));
                    if (!discard) {
                        ECUtils.publishEcError(responseInfo.res, bucketNodeList, msg, ERROR_DELETE_PART_META);
                    }
                }
                res.onNext(false);
            }
        }).subscribe();

        return res;
    }

    public static Mono<Tuple3<String, PartInfo[], String>> checkCompletePart(Part[] checkParts, String bucket, String object, String uploadId,
                                                                             List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                                             String currentSnapshotMark, String snapshotLink) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        MonoProcessor<Tuple3<String, PartInfo[], String>> res = MonoProcessor.create();
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("object", object)
                .put("uploadId", uploadId)
                .put("vnode", nodeList.get(0).var3)
                .put("checkParts", Json.encode(checkParts));
        Optional.ofNullable(currentSnapshotMark).ifPresent(v -> msg.put("currentSnapshotMark", v));
        Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2))
                .collect(Collectors.toList());
        // 保留返回结果的fileName相同数量满足k个以上，且versionNum最大的partInfo
        Map<Integer, PartInfo> partInfoMap = new HashMap<>();

        ResponseInfo<PartInfo[]> responseInfo = ClientTemplate.oneResponse(msgs, COMPLETE_PART_CHECK, PartInfo[].class, nodeList);
        ListPartsResult listPartsResult = new ListPartsResult()
                .setMaxParts(MAX_UPLOAD_PART_NUM);
        ListPartsClientHandler handler = new ListPartsClientHandler(listPartsResult, responseInfo, nodeList, request, false, currentSnapshotMark, snapshotLink);

        responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);
        Disposable subscribe = handler.res
                .doOnNext(b -> {
                    if (!b) {
                        res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "complete part check error!"));
                    }
                })
                .subscribe(b -> {
                    for (PartInfo curPartInfo : handler.partInfoList) {
                        // 分段 unview 则说明当前快照下上传过相同partNum的分段
                        if (curPartInfo.isUnView(currentSnapshotMark)) {
                            continue;
                        }
                        Integer partNum = Integer.parseInt(curPartInfo.partNum);
                        PartInfo partInfo = partInfoMap.get(partNum);
                        if (partInfo == null || partInfo.versionNum.compareTo(curPartInfo.versionNum) < 0) {
                            partInfoMap.put(partNum, curPartInfo);
                        }
                    }

                    PartInfo[] partInfos = new PartInfo[checkParts.length];

                    MessageDigest digest = Md5DigestPool.acquire();
                    try {
                        long redunMetaLength = 0;
                        for (int i = 0; i < checkParts.length; i++) {
                            Part checkPart = checkParts[i];
                            if (null == checkPart || null == partInfoMap.get(checkPart.getPartNumber())) {
                                res.onError(new MsException(ErrorNo.PART_INVALID, "Complete multi part fail, invalid part."));
                                return;
                            }
                            PartInfo partInfo = partInfoMap.get(checkPart.getPartNumber());
                            String requestEtag = checkPart.getEtag().startsWith("\"") ? '"' + partInfo.getEtag() + '"' : partInfo.getEtag();

                            if ((!requestEtag.equals(checkPart.getEtag()) || partInfo.delete) &&
                                    !PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                                res.onError(new MsException(ErrorNo.PART_INVALID, "Complete multi part fail, invalid part."));
                                return;
                            }

                            if (!request.headers().contains(IS_SYNCING) && i != checkParts.length - 1 && partInfo.partSize < UPLOAD_PART_MIN_SIZE) {
                                res.onError(new MsException(ErrorNo.TOO_SMALL_PART, "Complete multi part fail, too small part."));
                                return;
                            }

                            digest.update(BaseEncoding.base16().decode(partInfo.getEtag().toUpperCase()));
                            partInfos[i] = partInfo;
                            partInfoMap.remove(checkPart.getPartNumber());
                            redunMetaLength = redunMetaLength + partInfo.getPartKey(nodeList.get(0).var3).length() + Json.encode(partInfo).length();
                        }
                        redunMetaLength = redunMetaLength + partInfoMap.values().stream().mapToLong(info -> info.getPartKey(nodeList.get(0).var3).length() + Json.encode(info).length()).count();
                        long redunPartLength = partInfoMap.values().stream().mapToLong(info -> info.partSize).count();
                        res.onNext(new Tuple3<>(Hex.encodeHexString(digest.digest()) + "-" + checkParts.length, partInfos, redunMetaLength + "_" + redunPartLength));
                        Md5DigestPool.release(digest);
                    } catch (Exception e) {
                        log.error("", e);
                        res.onError(e);
                    }
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res;
    }

    private static final TypeReference<Tuple3<String[], String[], String>> COMPLETE_MULTI_PART_RESPONSE_TYPE = new TypeReference<Tuple3<String[], String[], String>>() {
    };

    public static Mono<String> completeMultiPart(String md5, InitPartInfo initPartInfo,
                                                 List<Tuple3<String, String, String>> bucketNodeList,
                                                 MsHttpRequest request, String snapshotLink) {
        return MsObjVersionUtils.versionStatusReactive(initPartInfo.bucket)
                .flatMap(status -> completeMultiPart(md5, initPartInfo, bucketNodeList, request, status, snapshotLink));
    }

    public static Mono<String> completeMultiPart(String md5, InitPartInfo initPartInfo,
                                                 List<Tuple3<String, String, String>> bucketNodeList,
                                                 MsHttpRequest request, String versionStatus, String snapshotLink) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        String vnode = bucketNodeList.get(0).var3;
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("vnode", vnode)
                .put("initPartInfo", Json.encode(initPartInfo));
        Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));
        List<SocketReqMsg> msgs = bucketNodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2).put("status", versionStatus))
                .collect(Collectors.toList());

        ResponseInfo<Tuple3<String[], String[], String>> responseInfo = ClientTemplate.oneResponse(msgs, MARK_COMPLETE_PARTS, COMPLETE_MULTI_PART_RESPONSE_TYPE, bucketNodeList);
        MonoProcessor<String> res = MonoProcessor.create();
        Disposable subscribe = responseInfo.responses
                .doOnComplete(() -> {
                    if (responseInfo.successNum >= bucketPool.getK()) {
                        // 发布合并分段修复消息
                        if (responseInfo.successNum != bucketPool.getK() + bucketPool.getM()) {
                            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(bucketPool.getVnodePrefix());
                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                    .put("initPartInfo", Json.encode(initPartInfo)).put("poolQueueTag", poolQueueTag);
                            Optional.ofNullable(snapshotLink).ifPresent(v -> errorMsg.put("snapshotLink", snapshotLink));
                            ECUtils.publishEcError(responseInfo.res, bucketNodeList, errorMsg, ERROR_COMPLETE_MULTI_PART);
                        }

                        Set<String> extraPartFile = new HashSet<>();
                        Set<String> allPartFile = new HashSet<>();
                        for (int i = 0; i < bucketPool.getK() + bucketPool.getM(); i++) {
                            if (responseInfo.res[i] != null && responseInfo.res[i].var2 != null && responseInfo.res[i].var2.var2 != null) {
                                Collections.addAll(extraPartFile, responseInfo.res[i].var2.var2);
                            }
                        }
                        // 将InitPartInfo设置为delete
                        initPartInfo.setDelete(true);
                        //删除InitPartInfo和partInfo
                        List<String> partInfoNums = Arrays.stream(initPartInfo.metaData.getPartInfos()).map(PartInfo::getPartNum).collect(Collectors.toList());
                        deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, bucketNodeList, true, partInfoNums, initPartInfo, initPartInfo.snapshotMark, initPartInfo.metaData.snapshotMark).subscribe();

                        boolean hasDuplicate = false;
                        for (String fileKey : extraPartFile) {
                            if (StringUtils.isEmpty(fileKey)) {
                                continue;
                            }
                            if (fileKey.contains("^")) {
                                hasDuplicate = true;
                            } else {
                                allPartFile.add(fileKey);
                            }
                        }
                        StoragePool dataPool = StoragePoolFactory.getStoragePool(initPartInfo);
                        if (extraPartFile.size() > 0 && hasDuplicate) {
                            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
                            ErasureClient.deleteDedupObjectFile(metaStoragePool, extraPartFile.toArray(new String[0]), request, false)
                                    .flatMap(b -> {
                                        if (allPartFile.size() > 0) {
                                            return ErasureClient.deleteObjectFile(dataPool, allPartFile.toArray(new String[0]), request);
                                        }
                                        return Mono.just(b);
                                    })
                                    .subscribe();
                        } else {
                            ErasureClient.deleteObjectFile(dataPool, extraPartFile.toArray(new String[0]), request).subscribe();
                        }
                        res.onNext(md5);
                    } else if (responseInfo.successNum == 0) {
                        res.onNext("");
                    } else {
                        //未成功，但小于k个节点成功，不返回结果
                    }
                })
                .subscribe();

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res.doOnNext(s -> {
            if (md5.equals(s)) {
                boolean overWrite = false;
                SocketReqMsg overWriteMsg = new SocketReqMsg("", 0)
                        .put("bucket", initPartInfo.bucket)
                        .put("object", initPartInfo.object);
                Set<String> storages = new HashSet<>();
                StoragePool dataPool = StoragePoolFactory.getStoragePool(initPartInfo);
                for (int i = 0; i < bucketPool.getK() + bucketPool.getM(); i++) {
                    if (responseInfo.res[i].var1 == SUCCESS && responseInfo.res[i].var2.var1.length > 0) {
                        overWriteMsg.put("overWrite" + i, Json.encode(responseInfo.res[i].var2.var1));
                        if (responseInfo.res[i].var2.var3 == null) {
                            storages.add(dataPool.getVnodePrefix());
                        } else {
                            storages.add(responseInfo.res[i].var2.var3);
                        }
                        overWrite = true;
                    }
                }

                if (overWrite) {
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(dataPool.getVnodePrefix());
                    overWriteMsg.put("storages", Json.encode(storages));
                    overWriteMsg.put("poolQueueTag", poolQueueTag);
                    ObjectPublisher.basicPublish(bucketNodeList.get(0).var1, overWriteMsg, OVER_WRITE);
                }
            }
        });
    }

    /**
     * 所有节点是否存在一个相关的partInfo或者meta。
     * 由于merge前存在相同的uploadId覆盖上传的情况，需要判断文件是否已被覆盖，即fileName是否匹配。
     */
    public static Mono<Boolean> hasPartInfoOrMeta(String bucket, String object, String fileName, String uploadId, String partNum,
                                                  List<Tuple3<String, String, String>> bucketNodeList, String versionId, String storage, String updateEC, String snapshotMark, String initSnapshotMark) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        String bucketVnode = bucketNodeList.get(0).var3;
        String metaDataKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);
        List<SocketReqMsg> msgs = bucketNodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("partInfoKey", PartInfo.getPartKey(bucketVnode, bucket, object, uploadId, partNum, snapshotMark))
                        .put("initPartInfoKey", InitPartInfo.getPartKey(bucketVnode, bucket, object, uploadId, initSnapshotMark == null ? snapshotMark : initSnapshotMark))
                        .put("metaKey", metaDataKey)
                        .put("lun", tuple.var2)
                        .put("fileName", fileName)
                        .put("partNum", partNum)
                        .put("storage", storage)
                        .put("updateEC", updateEC)
                )
                .collect(Collectors.toList());

        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, HAS_PART_INFO_OR_META, String.class, bucketNodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum > 0) {
                res.onNext(true);
            } else if (responseInfo.errorNum > storagePool.getM()) {
                res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "hasPartInfoOrMeta failed. "));
            } else {
                res.onNext(false);
            }
        });
        return res;
    }

    /**
     * initPartInfo中保存的partInfo与metaData保存的partInfo，它们的filename一一对应
     */
    public static boolean checkPartInfo(InitPartInfo info, MetaData metaData) {
        if (metaData.deleteMark) {
            return false;
        }

        PartInfo[] partInfos0 = info.metaData.partInfos;
        PartInfo[] partInfos = metaData.partInfos;
        if (partInfos == null) {
            return false;
        }
        if (partInfos0.length != partInfos.length) {
            return false;
        }
        int bound = partInfos.length;
        for (int i = 0; i < bound; i++) {
            if (!partInfos0[i].fileName.equals(partInfos[i].fileName)) {
                return false;
            }
        }
        return true;
    }

    public static void checkPartInfo(PartInfo partInfo) {
        if (partInfo.equals(PartInfo.ERROR_PART_INFO)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Upload Part Info fail");
        }

        if (partInfo.equals(PartInfo.NOT_FOUND_PART_INFO)) {
            throw new MsException(ErrorNo.NO_SUCH_UPLOAD_ID, "No such partInfo. " + partInfo.toString());
        }
    }

}
