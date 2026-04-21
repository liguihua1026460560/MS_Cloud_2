package com.macrosan.component.compression;


import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.ModuleDebug;
import com.macrosan.utils.authorize.AuthorizeV2;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.COMPONENT_USER_ID;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.storage.client.GetObjectClientHandler.DATA_CORRUPTED_MESSAGE;


/**
 * @author zhaoyang
 * @date 2026/02/05
 * @description 压缩处理器，负责处理压缩和解压
 **/
@Log4j2
public class ImageCompressionProcessor {

    @Getter
    private static final ImageCompressionProcessor instance = new ImageCompressionProcessor();

    /**
     * 压缩服务实例
     **/
    private final ImageCompressionService imageCompressionService;
    private final HttpClient httpClient;

    public static final Payload RATE_LIMIT_ERROR_PAYLOAD = DefaultPayload.create("RATE_LIMIT_ERROR", ErasureServer.PayloadMetaType.ERROR.name());

    private final ConcurrentSkipListMap<String, Tuple2<ComponentRecord, MonoProcessor<Payload>>> waitMap = new ConcurrentSkipListMap<>();

    public static final int MAX_WAIT_QUEUE_SIZE = 50;
    /**
     * 系统元数据字段集合
     **/
    public static final Set<String> SYS_META_DATA = new HashSet<>(Arrays.asList(
            "cache-control",
            "content-disposition",
            "content-encoding",
            "content-language",
            "expires",
            "content-type"
    ));

    private ImageCompressionProcessor() {
        Vertx vertx = ServerConfig.getInstance().getVertx().getDelegate();
        this.imageCompressionService = new ImageCompressionService(vertx);

        HttpClientOptions options = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                .setMaxPoolSize(100)
                .setConnectTimeout(30000)
                .setIdleTimeout(300);
        this.httpClient = vertx.createHttpClient(options);
    }

    public Mono<Payload> compressImage(Payload payload) {
        ComponentRecord record = Json.decodeValue(payload.getDataUtf8(), ComponentRecord.class);
        // 获取可用压缩服务
        CompressionServerManager.AcquiredServer availableServer = CompressionServerManager.getInstance().getAvailableServer();
        if (availableServer == null) {
            // 超过最大并发，进入队列等待
            if (waitMap.size() > MAX_WAIT_QUEUE_SIZE) {
                if (ModuleDebug.mediaComponentDebug()) {
                    log.info("Not available server and waitQueue is full bucket:{} object:{} versionId:{}", record.getBucket(), record.getObject(), record.getVersionId());
                }
                // 进入等待队列失败，则直接返回error
                return Mono.just(RATE_LIMIT_ERROR_PAYLOAD);
            } else {
                synchronized (waitMap) {
                    if (waitMap.size() > MAX_WAIT_QUEUE_SIZE) {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("Not available server and waitQueue is full bucket:{} object:{} versionId:{}", record.getBucket(), record.getObject(), record.getVersionId());
                        }
                        // 进入等待队列失败，则直接返回error
                        return Mono.just(RATE_LIMIT_ERROR_PAYLOAD);
                    } else {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("put in waitQueue queueSize:{} bucket:{} object:{} versionId:{}", waitMap.size(), record.getBucket(), record.getObject(), record.getVersionId());
                        }
                        MonoProcessor<Payload> res = MonoProcessor.create();
                        waitMap.put(RandomStringUtils.randomAlphanumeric(8), new Tuple2<>(record, res));
                        return res;
                    }
                }
            }
        }
        return doCompressImage(record, availableServer);
    }

    private Mono<Payload> doCompressImage(ComponentRecord record, CompressionServerManager.AcquiredServer availableServer) {
        AtomicBoolean needRelease = new AtomicBoolean(true);
        try {
            if (ModuleDebug.mediaComponentDebug()) {
                log.info("Start processing DICOM record: bucket={}, object={} version:{}", record.bucket, record.object, record.versionId);
            }
            StoragePool srcPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
            return getObjectMetadata(record, srcPool)
                    .filter(metaData -> {
                        if (metaData.equals(MetaData.ERROR_META)) {
                            throw new RuntimeException("Failed to get object metadata");
                        }
                        // 对象已压缩过或不存在或已删除，则不进行压缩
                        return !metaData.equals(MetaData.NOT_FOUND_META) && !metaData.isDeleteMarker() && !metaData.isDeleteMark() && !ComponentUtils.hasCompressed(metaData, record.getStrategy().getType());
                    })
                    // 开始处理压缩数据，返回压缩结果
                    .flatMap(metaData -> doCompress(record, metaData, availableServer))
                    .map(result -> {
                        if (result) {
                            if (ModuleDebug.mediaComponentDebug()) {
                                log.info("DICOM compression success: bucket={}, object={} version:{}", record.bucket, record.object, record.versionId);
                            }
                            return SUCCESS_PAYLOAD;
                        } else {
                            log.error("DICOM compression failed: bucket={}, object={} version:{}", record.bucket, record.object, record.versionId);
                            return ERROR_PAYLOAD;
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("DICOM compression failed: bucket={}, object={} version:{}", record.bucket, record.object, record.versionId, e);
                        return Mono.just(ERROR_PAYLOAD);
                    })
                    .switchIfEmpty(Mono.just(SUCCESS_PAYLOAD))
                    .doFinally(s -> {
                        if (needRelease.compareAndSet(true, false)) {
                            notifyWaitRecord(availableServer);
                        }
                    });
        } catch (Exception e) {
            if (needRelease.compareAndSet(true, false) && availableServer != null) {
                notifyWaitRecord(availableServer);
            }
            log.error("Process DICOM record error", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    /**
     * 一个请求处理结束，如果等待队列有请求，则优先处理等待队列的请求
     * @param availableServer 可用服务
     */
    private void notifyWaitRecord(CompressionServerManager.AcquiredServer availableServer) {

        if (waitMap.isEmpty()) {
            availableServer.release();
            return;
        }
        Map.Entry<String, Tuple2<ComponentRecord, MonoProcessor<Payload>>> waiter = waitMap.pollFirstEntry();
        if (waiter == null) {
            availableServer.release();
            return;
        }

        Tuple2<ComponentRecord, MonoProcessor<Payload>> tuple2 = waiter.getValue();
        if (ModuleDebug.mediaComponentDebug()) {
            log.info("notify waiter record bucket:{} object:{} versionId:{}", tuple2.var1.getBucket(), tuple2.var1.getObject(), tuple2.var1.getVersionId());
        }
        doCompressImage(tuple2.var1(), availableServer)
                .doOnError(e -> tuple2.var2().onError(e))
                .subscribe(payload -> tuple2.var2().onNext(payload));

    }

    public Flux<Buffer> decompressImage(String compressionType, long dataSize, Flux<byte[]> dataFlux, UnicastProcessor<Long> streamController, UnicastProcessor<Long> decompressStreamController, CompressionServerManager.ServerInfo server) {
        return imageCompressionService.decompressImage(dataFlux, dataSize, streamController, decompressStreamController, server);
    }

    /**
     * 对压缩后数据流进行解压
     *
     * @param compressionType            压缩类型
     * @param dataSize                   压缩后数据大小
     * @param dataFlux                   压缩后数据流
     * @param streamController           读取数据控制器
     * @param decompressStreamController 解压流控制器
     * @return 解压后数据流
     */
    public Flux<Buffer> decompressImage(String compressionType, long dataSize, Flux<byte[]> dataFlux, UnicastProcessor<Long> streamController, UnicastProcessor<Long> decompressStreamController) {
        CompressionServerManager.AcquiredServer availableServer = CompressionServerManager.getInstance().getAvailableServer();
        if (availableServer == null) {
            return Flux.error(new MsException(ErrorNo.COMPRESS_SERVER_ERROR, "No available server"));
        }
        return decompressImage(compressionType, dataSize, dataFlux, streamController, decompressStreamController, availableServer.getServer())
                .doFinally(s -> {
                    availableServer.release();
                });
    }

    /**
     * 对压缩后数据流进行解压，并返回指定范围的数据
     *
     * @param compressionType            压缩类型
     * @param dataSize                   压缩后数据大小
     * @param decompressedSize           解压后数据总大小
     * @param dataFlux                   压缩后数据流
     * @param streamController           读取数据控制器
     * @param decompressStreamController 解压流控制器（用于控制上游拉取速率）
     * @param startIndex                 起始字节位置（包含）
     * @param endIndex                   结束字节位置（包含）
     * @return 解压后指定范围的数据流
     */
    public Flux<Buffer> decompressImage(String compressionType, long dataSize, long decompressedSize, Flux<byte[]> dataFlux,
                                        UnicastProcessor<Long> streamController, UnicastProcessor<Long> decompressStreamController,
                                        long startIndex, long endIndex) {
        Flux<Buffer> decompressedFlux = decompressImage(compressionType, dataSize, dataFlux, streamController, decompressStreamController);

        // 如果不需要范围裁剪，直接返回解压后的原始流
        if (startIndex <= 0 && endIndex >= decompressedSize - 1) {
            return decompressedFlux;
        }

        return sliceFluxRange(decompressedFlux, decompressStreamController, startIndex, endIndex);
    }

    /**
     * 对数据流进行范围裁剪，只返回 startIndex 到 endIndex 之间的数据，发送完后立即结束流
     *
     * @param flux             原始数据流
     * @param streamController 流控制器（用于控制上游拉取速率，跳过数据时需通知）
     * @param startIndex       起始字节位置（包含）
     * @param endIndex         结束字节位置（包含）
     * @return 裁剪后的数据流
     */
    private Flux<Buffer> sliceFluxRange(Flux<Buffer> flux, UnicastProcessor<Long> streamController, long startIndex, long endIndex) {
        long[] bytesRead = {0};
        MonoProcessor<Void> endSignal = MonoProcessor.create();

        // takeUntilOther ：收到结束信号后，向上游发送cancel信息，向下游发送complete信号
        return flux.takeUntilOther(endSignal)
                .flatMap(buffer -> {
                    long currentStart = bytesRead[0];
                    int bufferLength = buffer.length();
                    long currentEnd = currentStart + bufferLength - 1;
                    bytesRead[0] += bufferLength;

                    // 完全在范围之前，跳过
                    if (currentEnd < startIndex) {
                        notifyController(streamController);
                        return Mono.empty();
                    }

                    // 完全在范围内，直接返回
                    if (currentStart >= startIndex && currentEnd <= endIndex) {
                        // 已到达范围末尾，发送数据后立即结束流
                        if (currentEnd == endIndex) {
                            endSignal.onComplete();
                        }
                        return Mono.just(buffer);
                    }

                    // 跨越边界，需要切割
                    Buffer slicedBuffer = sliceBuffer(buffer, currentStart, bufferLength, startIndex, endIndex);

                    // 只有到达或超过范围末尾时才结束流
                    if (currentEnd >= endIndex) {
                        endSignal.onComplete();
                    }

                    return Mono.just(slicedBuffer);
                });
    }

    /**
     * 对跨越边界的 buffer 进行切割
     *
     * @param buffer       原始 buffer
     * @param currentStart 当前 buffer 在整个数据流中的起始位置
     * @param bufferLength buffer 长度
     * @param startIndex   请求范围的起始位置
     * @param endIndex     请求范围的结束位置
     * @return 切割后的 buffer
     */
    private Buffer sliceBuffer(Buffer buffer, long currentStart, int bufferLength, long startIndex, long endIndex) {
        int sliceStart = (int) Math.max(0, startIndex - currentStart);
        // slice 的 end 参数是结束位置，左闭右开 [start, end)
        int sliceEnd = (int) Math.min(bufferLength, endIndex - currentStart + 1);
        return buffer.slice(sliceStart, sliceEnd);
    }

    /**
     * 通知控制器继续拉取数据，避免数据流阻塞
     *
     * @param streamController 流控制器
     */
    private void notifyController(UnicastProcessor<Long> streamController) {
        if (streamController != null) {
            streamController.onNext(1L);
        }
    }

    /**
     * 获取解压后图片的md5和长度
     *
     * @param buffers 压缩后数据流
     * @param server  压缩服务信息
     * @return Tuple2<md5, length>
     */
    public Mono<Tuple2<String, Long>> getDecompressImageMd5(List<Buffer> buffers, CompressionServerManager.ServerInfo server) {
        int dataSize = buffers.stream().mapToInt(Buffer::length).sum();
        Iterator<Buffer> bufferIterator = buffers.iterator();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();
        UnicastProcessor<byte[]> dataFlux = UnicastProcessor.create();
        streamController
                .subscribe(amount -> {
                    if (bufferIterator.hasNext()) {
                        dataFlux.onNext(bufferIterator.next().getBytes());
                    } else {
                        dataFlux.onComplete();
                    }
                });
        streamController.onNext(1L);
        Md5Digest md5Digest = new Md5Digest();
        return decompressImage("dcm", dataSize, dataFlux, streamController, null, server)
                .map(bytes -> {
                    md5Digest.update(bytes.getBytes());
                    return bytes.length();
                })
                .collectList()
                .map(lengths -> {
                    long decompressSize = 0;
                    for (Integer length : lengths) {
                        decompressSize += length;
                    }
                    byte[] digest = md5Digest.digest();
                    return new Tuple2<>(Hex.encodeHexString(digest), decompressSize);
                });
    }


    private Mono<MetaData> getObjectMetadata(ComponentRecord record, StoragePool srcPool) {
        String bucketVnode = srcPool.getBucketVnodeId(record.bucket, record.object);
        return srcPool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(
                        record.bucket, record.object, record.versionId, nodeList, null))
                .map(Tuple2::var1);
    }

    private Flux<byte[]> getDataFlux(MetaData metaData, UnicastProcessor<Long> streamController) {
        StoragePool dataPool = StoragePoolFactory.getStoragePool(metaData);

        return dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(metaData))
                .flatMapMany(nodeList -> {
                    long start = 0;
                    long end = metaData.endIndex;
                    return ErasureClient.getObject(dataPool, metaData, start, end,
                            nodeList, streamController, null, null);
                });
    }

    private Mono<Boolean> doCompress(ComponentRecord record, MetaData metaData, CompressionServerManager.AcquiredServer availableServer) {
        long originalSize = metaData.endIndex + 1;
        UnicastProcessor<Long> streamController = UnicastProcessor.create();
        Flux<byte[]> dataFlux = getDataFlux(metaData, streamController);
        AtomicReference<List<Buffer>> compressedBuffers = new AtomicReference<>();
        return imageCompressionService.compressImage(dataFlux, originalSize, streamController, availableServer.getServer())
                .collectList()
                .doOnNext(compressedBuffers::set)
                .filter(buffers -> {
                    long compressedSize = compressedBuffers.get().stream().mapToInt(Buffer::length).sum();
                    if (compressedSize >= originalSize) {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("Compressed size {} >= original size {} skipping upload, bucket={}, object={} version:{}",
                                    compressedSize, originalSize, record.bucket, record.object, record.versionId);
                        }
                        return false;
                    }
                    return true;
                })
                .flatMap(buffers -> getDecompressImageMd5(buffers, availableServer.getServer()))
                .flatMap(result -> uploadCompressedData(record, metaData, compressedBuffers.get(), result.var1(), result.var2()))
                .onErrorResume(e -> {
                    if (e.getMessage().contains(DATA_CORRUPTED_MESSAGE)) {
                        // 数据块损坏，则返回成功，不再对当前对象进行压缩处理
                        log.error("Data corrupted during compression, bucket:{} object:{} version:{}", record.bucket, record.object, record.versionId);
                        return Mono.just(true);
                    }
                    return Mono.error(e);
                });
    }

    private Mono<Boolean> uploadCompressedData(ComponentRecord record, MetaData metaData, List<Buffer> compressedFlux, String decompressedEtag, long decompressedSize) {
        String uri;
        if (StringUtils.isNotEmpty(record.strategy.destination)) {
            uri = UrlEncoder.encode(File.separator + record.strategy.destination + File.separator + record.object, "UTF-8");
        } else {
            uri = UrlEncoder.encode(File.separator + record.bucket + File.separator + record.object, "UTF-8");
        }
        HttpClientRequest request = httpClient.request(HttpMethod.PUT, Integer.parseInt(ServerConfig.getInstance().getHttpPort()), "localhost", uri);
        copyUserMetaData(record, metaData, request);
        request.putHeader(DATE, MsDateUtils.stampToGMT(System.currentTimeMillis()));
        request.putHeader(DECOMPRESSED_ETAG, decompressedEtag);
        request.putHeader(DECOMPRESSED_LENGTH, String.valueOf(decompressedSize));
        request.putHeader(COMPRESSION_TYPE, DCMX);
        request.putHeader(VERSIONID, metaData.getVersionId());
        request.putHeader("stamp", metaData.getStamp());
        if (record.headerMap.containsKey("userId")) {
            request.putHeader(COMPONENT_USER_ID, record.headerMap.get("userId"));
        }
        String authorizationHeader = AuthorizeV2.getAuthorizationHeader(record.ak, record.sk, uri, HttpMethod.PUT.name(), "UTF-8", request.headers());
        request.putHeader(AUTHORIZATION, authorizationHeader);

        request.setChunked(true);
        return uploadObject(request, compressedFlux)
                .map(resp -> {
                    if (resp.statusCode() == 200) {
                        return true;
                    } else {
                        String message = resp.statusMessage();
                        log.error("Upload compressed DICOM failed: {}", message);
                        return false;
                    }
                })
                .onErrorResume(e -> {
                    log.error("Upload compressed DICOM failed: {}", uri, e);
                    return Mono.just(false);
                });
    }

    private Mono<HttpClientResponse> uploadObject(HttpClientRequest request, List<Buffer> dataFlux) {
        MonoProcessor<HttpClientResponse> responseMono = MonoProcessor.create();
        request.setTimeout(30_000)
                .handler(responseMono::onNext)
                .continueHandler(v -> {
                    try {
                        for (Buffer buffer : dataFlux) {
                            request.write(buffer);
                        }
                        request.end();
                    } catch (Exception e) {
                        responseMono.onError(e);
                    }
                })
                .exceptionHandler(responseMono::onError)
                .putHeader(EXPECT, EXPECT_100_CONTINUE)
                .sendHead();
        return responseMono;
    }

    /**
     * 复制用户元数据
     *
     * @param record   压缩任务记录
     * @param metaData 源对象元数据
     * @param request  请求
     */
    private void copyUserMetaData(ComponentRecord record, MetaData metaData, HttpClientRequest request) {

        if (record.getStrategy().isCopyUserMetaData()) {
            Map<String, String> userMetaDataMap = new HashMap<>();
            if (metaData.getUserMetaData() != null) {
                userMetaDataMap.putAll(Json.decodeValue(metaData.getUserMetaData(), new TypeReference<Map<String, String>>() {
                }));
            }
            if (metaData.getSysMetaData() != null) {
                userMetaDataMap.putAll(Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                }));
            }

            userMetaDataMap.forEach((key, value) -> {
                String lowerKey = key.toLowerCase();
                if (lowerKey.startsWith("x-amz-meta-") || SYS_META_DATA.contains(lowerKey)) {
                    request.putHeader(key, value);
                }
            });
        }
    }


}
