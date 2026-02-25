package com.macrosan.doubleActive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.BaseEncoding;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DataSyncSignHandler.SignType;
import com.macrosan.doubleActive.compress.SyncCompressProcessor;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import io.netty.channel.ConnectTimeoutException;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.macrosan.constants.ErrorNo.GET_OBJECT_TIME_OUT;
import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_INDEX_IPS_ENTIRE_MAP;
import static com.macrosan.storage.client.GetObjectClientHandler.DATA_CORRUPTED_MESSAGE;
import static com.macrosan.storage.compressor.CompressorFactory.getCompressor;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

/**
 * @auther wuhaizhong
 * @date 2021/4/7
 */
@Log4j2
public class DataSyncHandler {

    public static HttpClient client = MossHttpClient.getClient();

    public static DataSyncHandler instance;

    public static DataSyncHandler getInstance() {
        if (instance == null) {
            return new DataSyncHandler();
        }
        return instance;
    }

    public static Mono<Boolean> recoverPut(UnSynchronizedRecord record, MetaData metaData) {
        return recoverPut(record, metaData, false);
    }

    /**
     * 修复时向对端发请求获取修复用数据。
     * 如果发送请求时遇到socketException。将进行重试。
     */
    public static Mono<Boolean> recoverPut(UnSynchronizedRecord record, MetaData metaData, boolean append) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        MsHttpRequest syncHttpRequest = new MsHttpRequest(null);
        syncHttpRequest.setSyncTag(IS_SYNCING);
        syncHttpRequest.setBucketName(record.bucket);
        syncHttpRequest.addMember("target-cluster", String.valueOf(record.index));

        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData);
        long fileSize = metaData.endIndex - metaData.startIndex + 1;
        long start = 0;
        long end = fileSize - 1;

        Disposable[] disposables = new Disposable[2];

        // V4签名的uri会有许多无用参数，影响minio复制的鉴权
        MultiMap params = RestfulVerticle.params(record.uri);
        StringBuilder stringBuilder = new StringBuilder().append(record.uri.split("\\?")[0]).append("?");
        params.forEach(entry -> {
            if (!PoolingRequest.escapeHeaders.contains(entry.getKey().toLowerCase())) {
                if (SIG_INCLUDE_PARAM_LIST.contains(entry.getKey().hashCode()) && StringUtils.isEmpty(entry.getValue())) {
                    stringBuilder.append(entry.getKey()).append("&");
                } else {
                    stringBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
                }
            }
        });
        String uri = stringBuilder.substring(0, stringBuilder.toString().length() - 1);
        if (append && params.contains("position") && record.headers != null && record.headers.containsKey(CONTENT_LENGTH)) {
            fileSize = Long.parseLong(record.headers.get(CONTENT_LENGTH));
            start = Long.parseLong(params.get("position"));
            end = start + Long.parseLong(record.headers.get(CONTENT_LENGTH));
            record.headers.remove(CONTENT_MD5);
        }
        PoolingRequest poolingRequest = new PoolingRequest(append ? HttpMethod.POST : HttpMethod.PUT, record.index, uri, MossHttpClient.getClient());
        AtomicBoolean waitForFetch = new AtomicBoolean(false);
        AtomicLong writeTotal = new AtomicLong();
        AtomicLong fetchTotal = new AtomicLong(1L);

        long finalStart = start;
        long finalEnd = end;
        long finalFileSize = fileSize;
        record.headers.put(CONTENT_LENGTH, String.valueOf(finalFileSize));
        record.headers.put(ORIGIN_INDEX_CRYPTO, metaData.getCrypto() == null ? "" : metaData.getCrypto());
        AtomicBoolean checkErr = new AtomicBoolean(true);
        boolean isExtra = EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(poolingRequest.clusterIndex);
        disposables[1] = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(record.bucket, SYNC_COMPRESS)
                .defaultIfEmpty("none")
                .zipWith(poolingRequest.request0(record))
                .publishOn(SCAN_SCHEDULER)
                .subscribe(tuple2 -> {
                    String syncCompress = tuple2.getT1();
                    MsClientRequest request1 = tuple2.getT2();

                    List<Disposable> signDisposableList = new ArrayList<>();
                    MonoProcessor<Boolean> requestSendProcessor = MonoProcessor.create();

                    request1.setWriteQueueMaxSize(WriteQueueMaxSize)
                            .exceptionHandler(e -> {
                                log.debug("send {} request to {} error!{}, {}", poolingRequest.method.name(), request1.getHost(), e.getClass().getName(), e.getMessage());
                                if (e instanceof TimeoutException || "Connection was closed".equals(e.getMessage())
                                        || e instanceof ConnectTimeoutException || e instanceof ConnectionPoolTooBusyException) {
//                                    SyncRecordLimiter.sizeLimiter.decLimit(e.getMessage());
                                }
                                closeConn(request1);
                                if (checkErr.get()) {
                                    res.onNext(false);
                                }
//                                DoubleActiveUtil.streamDispose(disposables);
                            })
                            .handler(response -> {
                                String resEtag = response.headers().get(ETAG);
                                if (DataSynChecker.isDebug) {
                                    log.info("ip:{}, uri:{}, statusCode:{}, msg:{}, resEtag:{}", request1.getHost(), uri, response.statusCode(), response.statusMessage(), resEtag);
                                } else {
                                    log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}, resEtag:{}", request1.getHost(), uri, response.statusCode(), response.statusMessage(), resEtag);
                                }
                                if (response.statusCode() != SUCCESS) {
                                    response.pause();
                                    MossHttpClient.vertx.runOnContext(v -> {
                                        response.bodyHandler(body -> {
                                            Error errorObj = (Error) JaxbUtils.toErrorObject(new String(body.getBytes()));
                                            log.debug("{}, {}", uri, Json.encode(errorObj));
                                            closeConn(request1);
                                            res.onNext(false);
                                        });
                                        response.resume();
                                    });
                                    return;
                                }
                                res.onNext(response.statusCode() == SUCCESS);
//                                closeConn(request1);
                            });

                    requestSendProcessor.doOnNext(b -> {
                        if ((request1.headers().contains(CONTENT_LENGTH) && Long.parseLong(request1.headers().get(CONTENT_LENGTH)) > 0)
                                || request1.isChunked()) {
                            request1.putHeader(EXPECT, EXPECT_100_CONTINUE);
                            request1.sendHead();
                        } else {
                            request1.end();
                        }
                    }).doOnError(e -> {
                        streamDispose(signDisposableList);
                        closeConn(request1);
                        if (checkErr.get()) {
                            res.onNext(false);
                        }
                    }).subscribe();

                    if (CompressorUtils.checkCompressEnable(syncCompress)
                            && !CompressorUtils.checkCompressEnable(storagePool.getCompression())
                            && getCompressor(syncCompress) != null
                            && finalFileSize > 0
                            && !isExtra) {
                        request1.setChunked(true);
                        request1.headers().remove(CONTENT_LENGTH);
                        request1.putHeader(SYNC_COMPRESS, syncCompress);

                        UnicastProcessor<Long> streamController0 = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                        MonoProcessor<Boolean> completeSignal = MonoProcessor.create();
                        Function<Void, Boolean> fetchFunction = v -> {
                            streamController0.onNext(-1L);
                            return true;
                        };
                        SyncCompressProcessor compressProcessor = new SyncCompressProcessor(SyncCompressProcessor.DEF_LENGTH, syncCompress, fetchFunction, completeSignal);
                        AtomicLong fileSizeCount = new AtomicLong();
                        Disposable disposable0 = compressProcessor.outputProcessor
                                .map(compressProcessor::compressBytes)
                                .doOnNext(dealCompressedBytesStream(request1, waitForFetch, writeTotal, fetchTotal, compressProcessor))
                                .doOnError(e -> {
                                    if ("Disposed".equals(e.getMessage())) {
                                        return;
                                    }
                                    log.error("compress error2, {}, {}", record.bucket, record.object, e);
                                    closeConn(request1);
                                    res.onNext(false);
                                })
                                .doOnComplete(() -> {
                                    // 2519: end(->write->endReqeust)会锁HttpClientRequestImpl和Http1xClientConnection，
                                    // handleResponseBegin要锁HttpClientRquestBase，beginResponse的handleEnd要锁conn
                                    synchronized (request1.getDelegate().connection()) {
                                        request1.end();
                                    }
                                })
                                .subscribe();
                        signDisposableList.add(disposable0);

                        request1.setWriteQueueMaxSize(WriteQueueMaxSize)
                                .continueHandler(v -> {
                                    disposables[0] = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData))
                                            .flatMapMany(nodeList ->
                                                    ErasureClient.getObject(storagePool, metaData, finalStart, finalEnd, nodeList, streamController0, syncHttpRequest, request1)
                                            )
                                            .publishOn(SCAN_SCHEDULER)
                                            .timeout(Duration.ofSeconds(30))
                                            .doOnNext(compressProcessor::formatBytesStream)
                                            .doOnNext(bytes -> countFileSize(bytes, finalFileSize, fileSizeCount, completeSignal))
                                            .doOnError(e -> {
                                                log.error("getObject err, {}", record.recordKey, e);
                                                checkErrorMessage(e, record, res, checkErr);
                                                closeConn(request1);
                                            })
                                            .subscribe();
                                    request1.addResponseCloseHandler(s -> {
                                        compressProcessor.close();
                                        DoubleActiveUtil.streamDispose(disposables);
                                    });
                                });

                        Disposable disposable = poolingRequest.sign(record, request1, isExtra)
                                .doOnNext(b -> {
                                    if (poolingRequest.signHandler.signType == SignType.AWS_V4_SINGLE) {
                                        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                                        MonoProcessor<Boolean> completeSignal0 = MonoProcessor.create();
                                        Function<Void, Boolean> fetchFunction0 = v -> {
                                            streamController.onNext(-1L);
                                            return true;
                                        };
                                        SyncCompressProcessor compressProcessor0 = new SyncCompressProcessor(SyncCompressProcessor.DEF_LENGTH, syncCompress, fetchFunction0, completeSignal0);
                                        AtomicLong fileSizeCount0 = new AtomicLong();

                                        Flux<byte[]> flux = compressProcessor0.outputProcessor
                                                .map(compressProcessor0::compressBytes);
                                        poolingRequest.signHandler.singleCheckSHA256Flux(record.headers.get(AUTHORIZATION), flux, fetchFunction0, request1.headers().contains(IS_SYNCING), isExtra)
                                                .doOnError(e -> {
                                                    if ("Disposed".equals(e.getMessage())) {
                                                        return;
                                                    }
                                                    log.error("compress error1, {}, {}", record.bucket, record.object, e);
                                                    requestSendProcessor.onError(e);
                                                })
                                                .subscribe(b0 -> requestSendProcessor.onNext(true));

                                        Disposable disposable1 = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData))
                                                .flatMapMany(nodeList ->
                                                        ErasureClient.getObject(storagePool, metaData, finalStart, finalEnd, nodeList, streamController, syncHttpRequest, null)
                                                )
                                                .publishOn(SCAN_SCHEDULER)
                                                .timeout(Duration.ofSeconds(30))
                                                .doOnNext(compressProcessor0::formatBytesStream)
                                                .doOnNext(bytes -> countFileSize(bytes, finalFileSize, fileSizeCount0, completeSignal0))
                                                .doOnError(e -> {
                                                    log.error("getObject err1, {}", record.recordKey, e);
                                                    checkErrorMessage(e, record, res, checkErr);
                                                    requestSendProcessor.onError(e);
                                                })
                                                .subscribe();
                                        signDisposableList.add(disposable1);
                                    } else {
                                        requestSendProcessor.onNext(true);
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("sign2 err, ", e);
                                    requestSendProcessor.onError(e);
                                })
                                .subscribe();
                        signDisposableList.add(disposable);

                    } else {
                        request1.continueHandler(v -> {
                            UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                            disposables[0] = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData))
                                    .flatMapMany(nodeList ->
                                            ErasureClient.getObject(storagePool, metaData, finalStart, finalEnd, nodeList, streamController, syncHttpRequest, request1)
                                    )
                                    .publishOn(SCAN_SCHEDULER)
                                    .timeout(Duration.ofSeconds(30))
                                    .doOnNext(dealBytesStream(request1, waitForFetch, writeTotal, fetchTotal, streamController))
                                    .doOnError(e -> {
                                        log.error("getObj error, {}", e.getMessage());
                                        checkErrorMessage(e, record, res, checkErr);
                                        closeConn(request1);
//                                            DoubleActiveUtil.streamDispose(disposables);
                                    })
                                    .doOnComplete(() -> {
                                        // 2519: end(->write->endReqeust)会锁HttpClientRequestImpl和Http1xClientConnection，
                                        // handleResponseBegin要锁HttpClientRquestBase，beginResponse的handleEnd要锁conn
                                        synchronized (request1.getDelegate().connection()) {
                                            request1.end();
                                        }
                                    })
                                    .subscribe();
                            request1.addResponseCloseHandler(s -> DoubleActiveUtil.streamDispose(disposables));
                        });

                        Disposable subscribe = poolingRequest.sign(record, request1, isExtra)
                                .flatMap(b -> {
                                    if (poolingRequest.signHandler.signType == SignType.AWS_V4_SINGLE) {
                                        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                                        Flux<byte[]> flux = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData))
                                                .flatMapMany(nodeList ->
                                                        ErasureClient.getObject(storagePool, metaData, finalStart, finalEnd, nodeList, streamController, syncHttpRequest, null)
                                                );
                                        Function<Void, Boolean> fetchFunction = v1 -> {
                                            streamController.onNext(-1L);
                                            return true;
                                        };
                                        return poolingRequest.signHandler.singleCheckSHA256Flux(record.headers.get(AUTHORIZATION), flux, fetchFunction, request1.headers().contains(IS_SYNCING),
                                                isExtra);
                                    } else {
                                        return Mono.just(true);
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("sign err, ", e);
                                    checkErrorMessage(e, record, res, checkErr);
                                    requestSendProcessor.onError(e);
                                })
                                .subscribe(b -> requestSendProcessor.onNext(true));
                        signDisposableList.add(subscribe);
                    }
                }, e -> {
                    log.error("recoverPut err0, ", e);
                    res.onNext(false);
                });
        return res;
    }

    private static void checkErrorMessage(Throwable ex, UnSynchronizedRecord record, MonoProcessor<Boolean> res, AtomicBoolean checkErr) {
        if (ex.getMessage() != null && ex.getMessage().contains(DATA_CORRUPTED_MESSAGE) ||
                (ex instanceof MsException && ((MsException) ex).getErrCode() == GET_OBJECT_TIME_OUT)) {
            checkErr.set(false);
            StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
            bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                    .flatMap(nodeList -> rewriteSyncRecord(bucketPool, record, nodeList))
                    .doOnError(e -> {
                        log.error("rewriteSyncRecord error", e);
                        res.onNext(false);
                    })
                    .subscribe(res::onNext);
        } else {
            checkErr.set(false);
            res.onNext(false);
        }
    }

    public static Consumer<byte[]> dealCompressedBytesStream(HttpClientRequest request, AtomicBoolean waitForFetch, AtomicLong writeTotal,
                                                             AtomicLong fetchTotal, SyncCompressProcessor processor) {
        return bytes -> {
            request.write(Buffer.buffer(bytes), done -> {
//                                                log.info("write:{}, fetch:{}", writeTotal.get(), fetchTotal.get());
                if (!waitForFetch.get() && writeTotal.incrementAndGet() >= fetchTotal.get()) {
                    processor.fetch();
                    fetchTotal.incrementAndGet();
                }
            });

            if (!waitForFetch.get() && request.writeQueueFull()) {
                waitForFetch.set(true);
                request.drainHandler(done -> {
                    waitForFetch.set(false);
                    processor.fetch();
                    fetchTotal.incrementAndGet();
                });
            }
        };
    }

    private static void countFileSize(byte[] bytes, long fileSize, AtomicLong count, MonoProcessor<Boolean> completeSignal) {
        if (count.addAndGet(bytes.length) == fileSize) {
            completeSignal.onNext(true);
        }
    }

    public static Consumer<byte[]> dealBytesStream(HttpClientRequest request, AtomicBoolean waitForFetch, AtomicLong writeTotal,
                                                   AtomicLong fetchTotal, UnicastProcessor<Long> streamController) {

        return bytes -> {
            request.write(Buffer.buffer(bytes), done -> {
//                                                log.info("write:{}, fetch:{}", writeTotal.get(), fetchTotal.get());
                if (!waitForFetch.get() && writeTotal.incrementAndGet() >= fetchTotal.get()) {
                    streamController.onNext(-1L);
                    fetchTotal.incrementAndGet();
                }
            });

            if (!waitForFetch.get() && request.writeQueueFull()) {
                waitForFetch.set(true);
                request.drainHandler(done -> {
                    waitForFetch.set(false);
                    streamController.onNext(-1L);
                    fetchTotal.incrementAndGet();
                });
            }
        };
    }

    /**
     * 将copy的分段记录按init/upload/merge拆成多条记录
     *
     * @param record   原记录
     * @param metaData copy后对象的元数据
     * @param nodeList bucket node list
     * @return 操作结果
     */
    public Mono<Boolean> rewritePartCopyRecord(StoragePool storagePool, UnSynchronizedRecord record, MetaData metaData, List<Tuple3<String, String, String>> nodeList) {
        record.getHeaders().put(UPLOAD_ID, metaData.partUploadId);
        record.getHeaders().put("referencedKey", metaData.referencedKey);
        record.getHeaders().put("referencedBucket", metaData.referencedBucket);
        String recordUri = record.uri.contains("?") ? record.uri.split("\\?")[0] : record.uri;
        String daVersion = getDaVersion(record.recordKey);
        return ECUtils.rewritePartCopyRecord(storagePool, recordUri, Json.encode(record), daVersion, Json.encode(metaData), nodeList)
                .doOnNext(b -> record.headers.put("overWriteFlag", "1"));
    }

    public static Mono<Boolean> recoverUploadPart(UnSynchronizedRecord record, PartInfo partInfo) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        Disposable[] disposables = new Disposable[2];

        AtomicBoolean waitForFetch = new AtomicBoolean(false);
        AtomicLong writeTotal = new AtomicLong();
        AtomicLong fetchTotal = new AtomicLong(1L);
        AtomicBoolean checkErr = new AtomicBoolean(true);

        PoolingRequest[] poolingRequest = new PoolingRequest[1];
        record.headers.put(CONTENT_LENGTH, String.valueOf(partInfo.partSize));
        boolean isExtra = EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index);
        disposables[1] = Mono.just(isExtra)
                .flatMap(extra -> {
                    if (extra) {
                        return ErasureClient.getTargetUploadId(record.bucket, record.afterInitRecordKey())
                                .flatMap(targetUploadId -> {
                                    if ("-1".equals(targetUploadId)) {
                                        throw new RuntimeException("ERROR_INIT_PART_INFO, " + targetUploadId);
                                    }
                                    if ("-2".equals(targetUploadId)) {
                                        throw new RuntimeException("ERROR_INIT_PART_INFO, " + targetUploadId);
                                    }
                                    // 往非moss的集群同步时使用AFTER_INIT_RECORD里的uploadId。
                                    if (!partInfo.uploadId.equals(targetUploadId)) {
                                        String partUploadUri = "/" + UrlEncoder.encode(record.bucket, "UTF-8")
                                                + "/" + UrlEncoder.encode(record.object, "UTF-8")
                                                + "?partNumber=" + partInfo.partNum
                                                + "&uploadId=" + targetUploadId;
                                        record.setUri(partUploadUri);
                                    }
                                    return Mono.just(true);
                                });
                    } else {
                        String uri = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object, "UTF-8") + "?versionId=" + record.versionId;
                        Map<String, String> headerMap = new HashMap<>();
                        headerMap.put(IS_SYNCING, "1");
                        headerMap.put(AUTHORIZATION, record.headers.get(AUTHORIZATION));
//                        headerMap.put(ID, record.headers.get(ID));
//                        headerMap.put(USERNAME, record.headers.get(USERNAME));
                        return MossHttpClient.getInstance().sendRequest(record.index,
                                record.bucket, record.object, uri, HttpMethod.HEAD, headerMap, null)
                                .flatMap(tuple3 -> {
                                    if (tuple3.var1 == INTERNAL_SERVER_ERROR) {
                                        return Mono.error(new RuntimeException("headPartObjErr"));
                                    }
                                    if (tuple3.var1 == SUCCESS && tuple3.var3.contains("uploadId") && partInfo.uploadId.equals(tuple3.var3.get("uploadId"))) {
                                        //对象已合并
                                        record.headers.put(SYNC_VERSION_ID, record.versionId);
                                        return Mono.just(false);
                                    }
                                    return Mono.just(true);
                                });
                    }
                })
                .publishOn(SCAN_SCHEDULER)
                .flatMap(b -> {
                    poolingRequest[0] = new PoolingRequest(HttpMethod.PUT, record.index, record.uri, MossHttpClient.getClient());
                    return poolingRequest[0].request0(record).doOnNext(r -> {
                        if (!b) {
                            // 对象已合并
                            r.headers().add(SYNC_PART_DATA, "1");
                        }
                    }).zipWith(RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(record.bucket, SYNC_COMPRESS).defaultIfEmpty("-1"));
                })
                .subscribe(tuple2 -> {
                    MsClientRequest request1 = tuple2.getT1();
                    String syncCompress = tuple2.getT2();

                    request1.setWriteQueueMaxSize(WriteQueueMaxSize)
                            .exceptionHandler(e -> {
                                log.debug("send {} request to {} error! {}, {}", HttpMethod.PUT.name(), request1.getHost(), e.getClass().getName(), e.getMessage());
                                if (e instanceof TimeoutException || "Connection was closed".equals(e.getMessage())
                                        || e instanceof ConnectTimeoutException || e instanceof ConnectionPoolTooBusyException) {
//                                    SyncRecordLimiter.sizeLimiter.decLimit(e.getMessage());
                                }
                                closeConn(request1);
                                if (checkErr.get()) {
                                    res.onNext(false);
                                }
//                    DoubleActiveUtil.streamDispose(disposables);
                            })
                            .handler(response -> {
                                String resEtag = response.headers().get(ETAG);
                                if (DataSynChecker.isDebug) {
                                    log.info("ip:{}, uri:{}, statusCode:{}, msg:{}, resEtag:{}", request1.getHost(), request1.uri(), response.statusCode(), response.statusMessage(), resEtag);
                                } else {
                                    log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}, resEtag:{}", request1.getHost(), request1.uri(), response.statusCode(), response.statusMessage(), resEtag);
                                }
                                if (response.statusCode() == NOT_FOUND) {
                                    response.pause();
                                    MossHttpClient.vertx.runOnContext(v -> {
                                        response.bodyHandler(body -> {
                                            Error errorObj = (Error) JaxbUtils.toErrorObject(new String(body.getBytes()));
                                            if (errorObj != null && ("NoSuchUpload".equals(errorObj.getCode()))) {
                                                isSynced(record, true).subscribe(b -> {
                                                    res.onNext(b);
                                                    closeConn(request1);
                                                }, e -> res.onNext(false));
                                            } else {
                                                closeConn(request1);
                                                res.onNext(false);
                                            }
                                        });
                                        response.resume();
                                    });

                                } else if (response.statusCode() != SUCCESS) {
//                                    response.bodyHandler(buffer -> log.debug(new String(buffer.getBytes())));
                                    res.onNext(false);
                                } else {
                                    closeConn(request1);
                                    res.onNext(response.statusCode() == SUCCESS || response.statusCode() == DEL_SUCCESS);
                                }
                            });

                    MonoProcessor<Boolean> requestSendProcessor = MonoProcessor.create();
                    List<Disposable> signDisposableList = new ArrayList<>();
                    requestSendProcessor.doOnNext(b -> {
                        if ((request1.headers().contains(CONTENT_LENGTH) && Long.parseLong(request1.headers().get(CONTENT_LENGTH)) > 0)
                                || request1.isChunked()) {
                            request1.putHeader(EXPECT, EXPECT_100_CONTINUE);
                            request1.sendHead();
                        } else {
                            request1.end();
                        }
                    }).doOnError(e -> {
                        streamDispose(signDisposableList);
                        closeConn(request1);
                        if (checkErr.get()) {
                            res.onNext(false);
                        }
                        res.onNext(false);
                    }).subscribe();


                    if (CompressorUtils.checkCompressEnable(syncCompress)
                            && !CompressorUtils.checkCompressEnable(StoragePoolFactory.getStoragePool(partInfo).getCompression())
                            && getCompressor(syncCompress) != null
                            && partInfo.partSize > 0
                            && !isExtra) {
                        request1.setChunked(true);
                        request1.headers().remove(CONTENT_LENGTH);
                        request1.putHeader(SYNC_COMPRESS, syncCompress);

                        MonoProcessor<Boolean> completeSignal = MonoProcessor.create();
                        UnicastProcessor<Long> streamController0 = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                        Function<Void, Boolean> fetchFunction = v -> {
                            streamController0.onNext(-1L);
                            return true;
                        };
                        SyncCompressProcessor compressProcessor = new SyncCompressProcessor(SyncCompressProcessor.DEF_LENGTH, syncCompress, fetchFunction, completeSignal);
                        AtomicLong fileSizeCount = new AtomicLong();
                        Disposable disposable0 = compressProcessor.outputProcessor
                                .map(compressProcessor::compressBytes)
                                .doOnNext(dealCompressedBytesStream(request1, waitForFetch, writeTotal, fetchTotal, compressProcessor))
                                .doOnError(e -> {
                                    if ("Disposed".equals(e.getMessage())) {
                                        return;
                                    }
                                    log.error("compress error, {}, {}", record.bucket, record.object, e);
                                    closeConn(request1);
                                    res.onNext(false);
                                })
                                .doOnComplete(() -> {
                                    // 2519: end(->write->endReqeust)会锁HttpClientRequestImpl和Http1xClientConnection，
                                    // handleResponseBegin要锁HttpClientRquestBase，beginResponse的handleEnd要锁conn
                                    synchronized (request1.getDelegate().connection()) {
                                        request1.end();
                                    }
                                })
                                .subscribe();
                        signDisposableList.add(disposable0);

                        request1.continueHandler(v -> {
                            disposables[0] = getLocalSinglePart(record, partInfo, request1, streamController0)
                                    .publishOn(SCAN_SCHEDULER)
                                    .timeout(Duration.ofSeconds(30))
                                    .doOnNext(compressProcessor::formatBytesStream)
                                    .doOnNext(bytes -> countFileSize(bytes, partInfo.partSize, fileSizeCount, completeSignal))
                                    .doOnError(e -> {
                                        log.error("getLocalSinglePart err, {}", record.recordKey, e);
                                        checkErrorMessage(e, record, res, checkErr);
                                        closeConn(request1);
                                    })
                                    .subscribe();
                            request1.addResponseCloseHandler(s -> {
                                compressProcessor.close();
                                DoubleActiveUtil.streamDispose(disposables);
                            });
                        });

                        Disposable disposable = poolingRequest[0].sign(record, request1, isExtra)
                                .subscribe(b -> {
                                    UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                                    if (poolingRequest[0].signHandler.signType == SignType.AWS_V4_SINGLE) {
                                        MonoProcessor<Boolean> completeSignal0 = MonoProcessor.create();
                                        Function<Void, Boolean> fetchFunction0 = v -> {
                                            streamController.onNext(-1L);
                                            return true;
                                        };
                                        SyncCompressProcessor compressProcessor0 = new SyncCompressProcessor(SyncCompressProcessor.DEF_LENGTH, syncCompress, fetchFunction0, completeSignal0);
                                        AtomicLong fileSizeCount0 = new AtomicLong();

                                        Flux<byte[]> flux = compressProcessor0.outputProcessor
                                                .map(compressProcessor0::compressBytes);
                                        poolingRequest[0].signHandler.singleCheckSHA256Flux(record.headers.get(AUTHORIZATION), flux, fetchFunction0, request1.headers().contains(IS_SYNCING), isExtra)
                                                .doOnError(e -> {
                                                    if ("Disposed".equals(e.getMessage())) {
                                                        return;
                                                    }
                                                    log.error("compress error1, {}, {}", record.bucket, record.object, e);
                                                    requestSendProcessor.onError(e);
                                                })
                                                .subscribe(b0 -> requestSendProcessor.onNext(true));
                                        Disposable disposable1 = getLocalSinglePart(record, partInfo, null, streamController)
                                                .publishOn(SCAN_SCHEDULER)
                                                .timeout(Duration.ofSeconds(30))
                                                .doOnNext(compressProcessor0::formatBytesStream)
                                                .doOnNext(bytes -> countFileSize(bytes, partInfo.partSize, fileSizeCount0, completeSignal0))
                                                .doOnError(e -> {
                                                    log.error("getLocalSinglePart err1, {}", record.recordKey, e);
                                                    checkErrorMessage(e, record, res, checkErr);
                                                    requestSendProcessor.onError(e);
                                                })
                                                .subscribe();
                                        signDisposableList.add(disposable1);
                                    } else {
                                        requestSendProcessor.onNext(true);
                                    }
                                });
                        signDisposableList.add(disposable);
                    } else {
                        request1.continueHandler(v -> {
                            UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                            disposables[0] = getLocalSinglePart(record, partInfo, request1, streamController)
                                    .publishOn(SCAN_SCHEDULER)
                                    .timeout(Duration.ofSeconds(30))
                                    .subscribe(
                                            dealBytesStream(request1, waitForFetch, writeTotal, fetchTotal, streamController),
                                            e -> {
                                                log.error("get single part error, recordKey: {}, {}", record.rocksKey(), e.getMessage());
                                                checkErrorMessage(e, record, res, checkErr);
                                                closeConn(request1);
//                                        DoubleActiveUtil.streamDispose(disposables);
                                            },
                                            () -> {
                                                synchronized (request1.getDelegate().connection()) {
                                                    request1.end();
                                                }
                                            });
                            request1.addResponseCloseHandler(s -> DoubleActiveUtil.streamDispose(disposables));
                        });

                        Disposable subscribe = poolingRequest[0].sign(record, request1, isExtra)
                                .flatMap(b -> {
                                    UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                                    if (poolingRequest[0].signHandler.signType == SignType.AWS_V4_SINGLE) {
                                        Flux<byte[]> flux = getLocalSinglePart(record, partInfo, null, streamController);
                                        Function<Void, Boolean> fetchFunction = v1 -> {
                                            streamController.onNext(-1L);
                                            return true;
                                        };
                                        return poolingRequest[0].signHandler.singleCheckSHA256Flux(record.headers.get(AUTHORIZATION), flux, fetchFunction, request1.headers().contains(IS_SYNCING),
                                                isExtra);
                                    } else {
                                        return Mono.just(true);
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("sign err, ", e);
                                    checkErrorMessage(e, record, res, checkErr);
                                    requestSendProcessor.onError(e);
                                })
                                .subscribe(b -> requestSendProcessor.onNext(true));
                        signDisposableList.add(subscribe);
                    }

                }, e -> {
                    if (e == null || StringUtils.isBlank(e.getMessage())) {
                        res.onNext(false);
                        return;
                    }
                    if (e.getMessage().contains("ERROR_INIT_PART_INFO")) {
                        isSynced(record, true).subscribe(b -> {
                            res.onNext(b);
                        }, e1 -> res.onNext(false));
                    } else {
                        if (e.getMessage().contains("headPartObjErr")) {
                            log.debug("", e);
                        } else {
                            log.error("", e);
                        }
                        res.onNext(false);
                    }
                });
        return res;
    }

    public static Mono<Boolean> recoverCompletePart(UnSynchronizedRecord record, byte[] requestBody) {
        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket,
                record.object, record.uri, record.method, record.headers, requestBody)
                .flatMap(tuple3 -> {
                    boolean noUploadId = tuple3.var1 == NOT_FOUND && "NoSuchUpload".equals(tuple3.var2);
                    boolean invalidPart = tuple3.var1 == BAD_REQUEST_REQUEST && "InvalidPart".equals(tuple3.var2);
                    if (noUploadId || invalidPart) {
                        return isSynced(record, false);
                    }
                    return Mono.just(tuple3.var1 == SUCCESS);
                });
    }


    public static Mono<Boolean> recoverInitPart(UnSynchronizedRecord record) {
        String uri = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object, "UTF-8") + "?versionId=" + record.versionId;
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(IS_SYNCING, "1");
        headerMap.put(AUTHORIZATION, record.headers.get(AUTHORIZATION));
        headerMap.put(ID, record.headers.get(ID));
        headerMap.put(USERNAME, record.headers.get(USERNAME));
        String uploadId = record.headers.get(UPLOAD_ID);

        if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
            // minio复制init_part时查询是否已有相应的AFTER_INIT记录。有则说明part_init已完成，无需再同步本条init记录
            // 缺陷：如果节点挂了一个，期间合并了对象，后续又修复回了init、partUpload记录，此时依然会初始化或上传同名的新的分段。AFTER_INIT也可能在之前或之后修复回来
            return ErasureClient.getTargetUploadId(record.bucket, record.afterInitRecordKey())
                    .flatMap(targetUploadId -> {
                        if (targetUploadId.equals("-1")) {
                            return Mono.just(false);
                        } else if (targetUploadId.equals("-2")) {
                            return MossHttpClient.getInstance().sendRequest(record)
                                    .map(tuple -> tuple.var1 == SUCCESS);
                        } else {
                            return Mono.just(true);
                        }
                    });
        }

        return MossHttpClient.getInstance().sendRequest(record.index,
                record.bucket, record.object, uri, HttpMethod.HEAD, headerMap, null)
                .flatMap(tuple3 -> {
                    if (tuple3.var1 == SUCCESS && tuple3.var3.contains("uploadId") && uploadId.equals(tuple3.var3.get("uploadId"))) {
                        return Mono.just(true);
                    } else if (tuple3.var1 != INTERNAL_SERVER_ERROR) {
                        String partListUri = UrlEncoder.encode(File.separator + record.bucket + File.separator
                                + record.object, "UTF-8") + "?uploadId=" + uploadId + "&max-parts=1";
                        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket,
                                record.object, partListUri, HttpMethod.GET, headerMap, null)
                                .flatMap(resTuple3 -> {
                                    if (resTuple3.var1 == SUCCESS) {
                                        return Mono.just(true);
                                    }
                                    if (resTuple3.var1 == NOT_FOUND && "NoSuchUpload".equals(resTuple3.var2)) {
                                        return MossHttpClient.getInstance().sendRequest(record)
                                                .map(tuple -> tuple.var1 == SUCCESS);
                                    }
                                    return Mono.just(false);
                                });
                    } else {
                        return Mono.just(false);
                    }
                });
    }

    public static Mono<Boolean> isSynced(UnSynchronizedRecord record, boolean upload) {
        String uri = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object, "UTF-8") + "?versionId=" + record.versionId;
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(IS_SYNCING, "1");
        headerMap.put(AUTHORIZATION, record.headers.get(AUTHORIZATION));
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        String uploadId = getOringinUploadId(record);

        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket, record.object))
                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object,
                        record.versionId, bucketVnodeList).zipWith(Mono.just(bucketVnodeList)))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1();
                    if (uploadId.equals(metaData.partUploadId)) {
                        return MossHttpClient.getInstance().sendRequest(record.index,
                                record.bucket, record.object, uri, HttpMethod.HEAD, headerMap, null)
                                .flatMap(tuple3 -> {
                                    if (tuple3.var1 == INTERNAL_SERVER_ERROR) {
                                        return Mono.just(false);
                                    }

                                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                                        if (tuple3.var1 == SUCCESS && tuple3.var3.contains(ETAG)) {
                                            Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData,
                                                    new TypeReference<Map<String, String>>() {
                                                    });
                                            String localEtag = sysMetaMap.get(ETAG).replace("\"", "");
                                            String respEtag = tuple3.var3.get(ETAG).replace("\"", "");

                                            if (localEtag.equals(respEtag)) {
                                                return Mono.just(true);
                                            }
                                            // 因为moss修改过分段合并后的etag计算方式，老版本的对象需要重新按照aws标准计算一遍。
                                            MessageDigest digest = Md5DigestPool.acquire();
                                            for (PartInfo partInfo : metaData.partInfos) {
                                                digest.update(BaseEncoding.base16().decode(partInfo.getEtag().toUpperCase()));
                                            }
                                            localEtag = Hex.encodeHexString(digest.digest()) + "-" + metaData.partInfos.length;
                                            Md5DigestPool.release(digest);

                                            if (localEtag.equals(respEtag)) {
                                                return Mono.just(true);
                                            }
                                        }
                                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                                .flatMap(nodeList -> rewriteSyncRecord(bucketPool, record, nodeList)
                                                        .doOnNext(b -> record.headers.put("minio-rewrite", "1")));
                                    } else {
                                        if (tuple3.var1 == SUCCESS && tuple3.var3.contains("uploadId")
                                                && uploadId.equals(tuple3.var3.get("uploadId"))) {
                                            return Mono.just(true);
                                        }
                                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                                .flatMap(nodeList -> rewriteSyncRecord(bucketPool, record, nodeList));
                                    }

                                });
                    } else if (upload) {
                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket))
                                .flatMap(nodeList -> rewriteSyncRecord(bucketPool, record, nodeList));
                    }
                    return Mono.just(false);
                });
    }

    public static Mono<Boolean> rewriteSyncRecord(StoragePool storagePool, UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList) {
        UnSynchronizedRecord newRecord = (UnSynchronizedRecord) record.clone();
        // record根据字典排序
        String rocksKey = newRecord.getRecordRocksKey(VersionUtil.getVersionNum(false));
        // 旧差异标记已被重写
        return ErasureClient.rewriteRecord(storagePool, rocksKey, newRecord, nodeList)
                .doOnNext(b -> record.headers.put("overWriteFlag", "1"));
    }

    /**
     * 获取本地的单个分段。
     */
    private static Flux<byte[]> getLocalSinglePart(UnSynchronizedRecord record, PartInfo info, MsClientRequest clientRequest, UnicastProcessor<Long> streamController) {
        String bucketName = record.bucket;
        String objName = record.object;

        Mono<PartInfo> partInfoMono;
        if (info == null) {
            String[] split = record.headers.get(GET_SINGLE_PART).split(",");
            String uploadId = split[0];
            String partNum = split[1];
            partInfoMono = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                    .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket. bucket name :" + bucketName)))
                    .flatMap(bucketInfo -> PartClient.getPartInfo(bucketName, objName, uploadId, partNum))
                    .doOnNext(PartUtils::checkPartInfo);
        } else {
            partInfoMono = Mono.just(info);
        }
        StoragePool[] storagePool = new StoragePool[]{null};

        return partInfoMono
                .flatMap(partInfo -> {
                    storagePool[0] = StoragePoolFactory.getStoragePool(partInfo);
                    return storagePool[0].mapToNodeInfo(storagePool[0].getObjectVnodeId(partInfo)).zipWith(Mono.just(partInfo));
                })
                .flatMapMany(tuple2 -> {
                    PartInfo partInfo = tuple2.getT2();
                    long partSize = partInfo.partSize;

                    if (partSize == 0) {
                        MonoProcessor<byte[]> res = MonoProcessor.create();
                        res.onNext(new byte[0]);
                        return res;
                    }

                    // 设置异步复制限流
                    MsHttpRequest msHttpRequest = new MsHttpRequest(null);
                    msHttpRequest.setSyncTag(IS_SYNCING);
                    msHttpRequest.setBucketName(bucketName);

                    return ECUtils.getObject(storagePool[0], partInfo.fileName, false, 0, partSize - 1,
                            partSize, tuple2.getT1(), streamController, msHttpRequest, clientRequest);
                });
    }

    /**
     * 根据同步记录重新构造对应api的http请求
     *
     * @param record      同步记录
     * @param requestBody 请求体内容
     * @return 是否修复成功
     */
    public static Mono<Boolean> recoverRequest(UnSynchronizedRecord record, byte[] requestBody) {
        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket,
                record.object, record.uri, record.method, record.headers, requestBody)
                .flatMap(tuple3 -> Mono.just(tuple3.var1 == SUCCESS));
    }

    public static Mono<Boolean> deleteObjRequest(UnSynchronizedRecord record) {
        int otherIndex = getOtherSiteIndex();
        //双活环境下 两个双活站点都删成功再删复制站点
        if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index) || otherIndex == -1 || record.index == otherIndex) {
            return MossHttpClient.getInstance().sendSyncRequest(record);
        }
        String uri = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object, "UTF-8") + "?versionId=" + record.versionId;
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(IS_SYNCING, "1");
        headerMap.put(AUTHORIZATION, record.headers.get(AUTHORIZATION));

        return MossHttpClient.getInstance().sendRequest(otherIndex, record.bucket, record.object, uri, HttpMethod.HEAD, headerMap, null)
                .flatMap(tuple3 -> {
                    if (tuple3.var1 == NOT_FOUND) {
                        return MossHttpClient.getInstance().sendSyncRequest(record.setUri(uri));
                    }
                    return Mono.just(false);
                });
    }

    public static Mono<Boolean> abortObjRequest(UnSynchronizedRecord record) {
        return MossHttpClient.getInstance().sendRequest(record.index, record.bucket,
                record.object, record.uri, HttpMethod.DELETE, record.headers, null)
                .flatMap(tuple3 -> Mono.just(tuple3.var1 != INTERNAL_SERVER_ERROR));
    }

    // 获取历史数据同步记录生成时的versionNum，其他记录获取的是差异生成时的syncStamp
    private String getDaVersion(String recordKey) {
        String[] split = recordKey.split(File.separator);
        if (recordKey.contains("async")) {
            return split[5];
        } else {
            return split[3];
        }
    }

}
