package com.macrosan.component.compression;


import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ECUtils;
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
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
        AtomicBoolean needRelease = new AtomicBoolean(true);
        // 获取可用压缩服务
        CompressionServerManager.AcquiredServer availableServer = CompressionServerManager.getInstance().getAvailableServer();
        try {
            if (availableServer == null) {
                return Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
                        .thenReturn(RATE_LIMIT_ERROR_PAYLOAD);
            }
            ComponentRecord record = Json.decodeValue(payload.getDataUtf8(), ComponentRecord.class);
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
                            availableServer.release();
                        }
                    });
        } catch (Exception e) {
            if (needRelease.compareAndSet(true, false) && availableServer != null) {
                availableServer.release();
            }
            log.error("Process DICOM record error", e);
            return Mono.just(ERROR_PAYLOAD);
        }
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
