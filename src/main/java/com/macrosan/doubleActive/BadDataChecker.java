package com.macrosan.doubleActive;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListOriginPartHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanStream;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.ec.part.PartClient.LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_MULTI_PART_UPLOAD;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;

/**
 * 定期处理多余的数据
 */
@Log4j2
public class BadDataChecker {
    private static BadDataChecker instance;

    public static BadDataChecker getInstance() {
        if (instance == null) {
            instance = new BadDataChecker();
        }
        return instance;
    }

    private static final RedisConnPool POOL = RedisConnPool.getInstance();

//    public static final int processor_num = 1;
//    public static UnicastProcessor<SyncRequest<Upload>>[] processors = new UnicastProcessor[processor_num];
//    private final AtomicLong countScanUpload = new AtomicLong();
//    private final int maxScanUpload = 1;

    public static UnicastProcessor<String> scanBucketProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());

    private final AtomicInteger countScan = new AtomicInteger();
    private final int maxScan = 1;
    public static Set<String> SCANNING_BUCKET_SET = new ConcurrentSkipListSet<>();


    public void init() {
        scanBucketProcessor.publishOn(SCAN_SCHEDULER)
                .filter(bucket -> {
                    if (countScan.incrementAndGet() > maxScan) {
                        countScan.decrementAndGet();
                        Mono.delay(Duration.ofSeconds(5)).publishOn(SCAN_SCHEDULER).subscribe(s -> scanBucketProcessor.onNext(bucket));
                        return false;
                    }
                    return true;
                })
                .flatMap(bucket -> listMultiPartInfo(bucket)
                        .doFinally(s -> {
                            countScan.decrementAndGet();
                            SCANNING_BUCKET_SET.remove(bucket);
                        }))
                .doOnError(e -> log.error("scanBucketProcessor err, ", e))
                .subscribe();


        SCAN_SCHEDULER.schedule(this::partInitCheck, 60, TimeUnit.SECONDS);
    }


    private void partInitCheck() {
        if (!MainNodeSelector.checkIfSyncNode()) {
            SCAN_SCHEDULER.schedule(this::partInitCheck, 60, TimeUnit.SECONDS);
            return;
        }
        ScanStream.sscan(POOL.getReactive(REDIS_SYSINFO_INDEX), SYNC_BUCKET_SET)
                .publishOn(SCAN_SCHEDULER)
                .subscribe(bucket -> {
                    if (SCANNING_BUCKET_SET.add(bucket)) {
                        scanBucketProcessor.onNext(bucket);
                    }
                }, e -> log.error("partInitCheck err, ", e));

        SCAN_SCHEDULER.schedule(this::partInitCheck, 60, TimeUnit.SECONDS);
    }


    private static Mono<Boolean> listMultiPartInfo(String bucketName) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        log.debug("bucket {} start list part", bucketName);
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaPool.getBucketVnodeList(bucketName);
        UnicastProcessor<String> bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        AtomicInteger nextRunNumber = new AtomicInteger(1);

        bucketVnodeProcessor.publishOn(SCAN_SCHEDULER).subscribe(bucketVnode -> {
            UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
            listController
                    .publishOn(SCAN_SCHEDULER)
                    .doOnComplete(() -> {
                        int next = nextRunNumber.getAndIncrement();
                        if (next < bucketVnodeList.size()) {
                            bucketVnodeProcessor.onNext(bucketVnodeList.get(next));
                        } else {
                            log.debug("listMultiPartInfo complete, {}", bucketName);
                            bucketVnodeProcessor.onComplete();
                            res.onNext(true);
                        }
                    }).doOnNext(marker -> {
                        POOL.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                                .defaultIfEmpty(new HashMap<>(0))
                                .flatMap(bucketInfo ->
                                        POOL.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                                                .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                                                .zipWith(metaPool.mapToNodeInfo(bucketVnode)))
                                .doOnNext(tuple2 -> {
                                    Owner owner = tuple2.getT1();
                                    List<Tuple3<String, String, String>> nodeList = tuple2.getT2();
                                    int maxUploadsInt = 1000;

                                    ListMultipartUploadsResult listMultiUploadsRes = new ListMultipartUploadsResult()
                                            .setBucket(bucketName)
                                            .setMaxUploads(maxUploadsInt)
                                            .setKeyMarker(marker);

                                    SocketReqMsg msg = new SocketReqMsg("", 0)
                                            .put("vnode", nodeList.get(0).var3)
                                            .put("bucket", bucketName)
                                            .put("maxUploads", String.valueOf(maxUploadsInt))
                                            .put("prefix", "")
                                            .put("marker", marker)
                                            .put("delimiter", "")
                                            .put("uploadIdMarker", "");

                                    List<SocketReqMsg> msgs = nodeList.stream()
                                            .map(tuple -> msg.copy().put("lun", tuple.var2))
                                            .collect(Collectors.toList());

                                    ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo =
                                            ClientTemplate.oneResponse(msgs, LIST_MULTI_PART_UPLOAD, LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE, nodeList);
                                    ListOriginPartHandler handler = new ListOriginPartHandler(owner, listMultiUploadsRes, responseInfo, nodeList, null, bucketName);
                                    responseInfo.responses.subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);

                                    handler.res.publishOn(SCAN_SCHEDULER)
                                            .doOnNext(b -> {
                                                if (!b) {
                                                    Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(s -> listController.onNext(marker));
                                                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "List MultiPart Uploads fail");
                                                }
                                            })
                                            .doOnNext(b -> {
                                                if (listMultiUploadsRes.getUploads().isEmpty()) {
                                                    listController.onComplete();
                                                } else {
                                                    UnicastProcessor<Integer> bucketIndexSignal = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                                                    bucketIndexSignal.publishOn(SCAN_SCHEDULER)
                                                            .doOnComplete(() -> {
                                                                if (!listMultiUploadsRes.isTruncated()) {
                                                                    listController.onComplete();
                                                                } else {
                                                                    String nextMarker = listMultiUploadsRes.getNextKeyMarker();
                                                                    listController.onNext(nextMarker);
                                                                }
                                                            })
                                                            .subscribe(index -> {
                                                                if (index >= listMultiUploadsRes.getUploads().size()) {
                                                                    bucketIndexSignal.onComplete();
                                                                    return;
                                                                }
                                                                Upload upload = listMultiUploadsRes.getUploads().get(index);
                                                                String bucketVnodeId = metaPool.getBucketVnodeId(bucketName, upload.getKey());
                                                                POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS)
                                                                        .defaultIfEmpty("")
                                                                        .doFinally(s -> bucketIndexSignal.onNext(index + 1))
                                                                        .flatMap(v -> {
                                                                            if (StringUtils.isEmpty(v)) {
                                                                                return metaPool.mapToNodeInfo(bucketVnodeId)
                                                                                        .flatMap(nlist -> ErasureClient.getObjectMetaVersion(bucketName, upload.getKey(), "null", nlist));
                                                                            } else {
                                                                                return metaPool.mapToNodeInfo(metaPool.getBucketVnodeId(bucketName, upload.getKey()))
                                                                                        .flatMap(nlist -> ErasureClient.getObjectMetaByUploadId(bucketName, upload.getKey(),
                                                                                                upload.getUploadId()
                                                                                                , nlist));
                                                                            }
                                                                        })
                                                                        .filter(meta -> meta.isAvailable() && upload.getUploadId().equals(meta.partUploadId))
                                                                        .flatMap(BadDataChecker::getObject)
                                                                        .filter(get -> get)
                                                                        .flatMap(bool -> metaPool.mapToNodeInfo(bucketVnodeId))
                                                                        .flatMap(nList ->
                                                                                PartClient.getInitPartInfo(bucketName, upload.getKey(), upload.getUploadId(), nList, null)
                                                                                        .filter(info -> !info.equals(ERROR_INIT_PART_INFO) && !info.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) && !info.delete)
                                                                                        .flatMap(info -> PartUtils.deleteMultiPartUploadMeta(bucketName, upload.getKey(),
                                                                                                upload.getUploadId(), nList, false, null, info.setDelete(true),null,null))
                                                                                        .doOnNext(aBoolean -> {
                                                                                            if (aBoolean) {
                                                                                                log.info("deleteMultiPartUploadMeta complete, {}", upload);
                                                                                            }
                                                                                        })
                                                                        )
                                                                        .timeout(Duration.ofSeconds(10))
//                                                                            .filter(info -> !info.equals(ERROR_INIT_PART_INFO) && !info.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) && !info.delete)
//                                                                            .zipWith(metaPool.mapToNodeInfo(bucketVnodeId))
//                                                                            .flatMap(tuple -> PartUtils.deleteMultiPartUploadMeta(bucketName, upload.getKey(), upload.getUploadId(), tuple.getT2(),
//                                                                                    false, null, tuple.getT1().setDelete(true)))
                                                                        .doOnError(e -> log.error("compare upload {} {} error", upload.getKey(), upload.getUploadId(), e))
                                                                        .subscribe();

                                                            });
                                                    bucketIndexSignal.onNext(0);

                                                }
                                            })
                                            .doOnError(e -> log.error("listMultiPartInfo err2, {}", bucketName, e))
                                            .subscribe();
                                })
                                .doOnError(e -> {
                                    log.error("listMultiPartInfo err3, {}", bucketName, e);
                                    listController.onComplete();
                                })
                                .subscribe();
                    }
            )
                    .doOnError(e -> log.error("listMultiPartInfo err1, {}", bucketName, e))
                    .subscribe();
            listController.onNext("");
        });

        bucketVnodeProcessor.onNext(bucketVnodeList.get(0));
        return res;
    }

    private static Mono<Boolean> getObject(MetaData metaData) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData))
                .flatMapMany(nodeList ->
                        ErasureClient.getObject(storagePool, metaData, 0, metaData.endIndex - metaData.startIndex, nodeList, streamController, null, null)
                )
                .publishOn(SCAN_SCHEDULER)
                .timeout(Duration.ofSeconds(30))
                .subscribe(bytes -> streamController.onNext(-1L),
                        e -> {
                            log.error("getobj error, {} {} {}", metaData.bucket, metaData.key, metaData.versionId, e);
                            res.onNext(false);
                        },
                        () -> res.onNext(true));
        return res;
    }

}
