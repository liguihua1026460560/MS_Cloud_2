package com.macrosan.inventory.transmitter;

import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.ec.ECUtils;
import com.macrosan.inventory.InventoryService;
import com.macrosan.inventory.datasource.DataSource;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.StoragePoolType;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.authorize.AuthorizeV2;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.quota.QuotaRecorder;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.SUCCESS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSyncHandler.dealBytesStream;
import static com.macrosan.doubleActive.DoubleActiveUtil.WriteQueueMaxSize;
import static com.macrosan.doubleActive.DoubleActiveUtil.closeConn;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_OBJECT_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;

@Log4j2
public class InventoryTransmitter {

    /**
     * Inventory API存储至默认存储池中
     */
    private final MessageDigest digest = DigestUtils.getMd5Digest();

    /**
     * 每隔一定大小生成清单文件
     */
    public static final long MAX_PART_SIZE = 1024 * 1024L * 1024L;

    private DataSource dataSource;

    private final List<Disposable> disposables = new ArrayList<>();

    public InventoryTransmitter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 记录已上传数据流的大小
     */
    private final AtomicLong contentLength = new AtomicLong(0);

    /**
     * 计算数据流的md5值
     */
    private String[] md5 = new String[1];

    public Long getContentLength() {
        return contentLength.get();
    }

    public String getMd5() {
        return md5[0];
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * 置换数据源
     */
    public void replaceSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 将数据源写入到目标桶中
     *
     * @return 是否成功
     */
    public Mono<Boolean> transport(Destination destination, StoragePool pool) {
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(destination.getBucket(), destination.getObject()).var1).block();
        StoragePoolType type = pool.getType();
        InventoryEncoder[] encoder = new InventoryEncoder[]{null};
        switch (type) {
            case ERASURE:
                encoder[0] = new InventoryEcLimitEncoder(pool.getK(), pool.getM(), pool.getPackageSize(), pool.getCodc(), dataSource);
                break;
            case REPLICA:
                encoder[0] = new InventoryReplicaEncoder(pool.getK(), pool.getM(), pool.getPackageSize(), dataSource);
                break;
            default:
                log.error("No such storage strategy.");
                return Mono.just(false);
        }

        AtomicBoolean needRequest = new AtomicBoolean(true);
        Disposable disposable = dataSource.data().doOnCancel(() -> {
            log.error("transmitter is canceled.");
            for (int i = 0; i < encoder[0].data().length; i++) {
                encoder[0].data()[i].onError(new RuntimeException("transmitter dispose"));
            }
        }).doOnError(e -> {
            for (int i = 0; i < encoder[0].data().length; i++) {
                encoder[0].data()[i].onError(e);
            }
        }).doOnComplete(() -> {
            encoder[0].complete();
            md5[0] = Hex.encodeHexString(digest.digest());
        }).subscribe(bytes -> {
            contentLength.getAndAdd(bytes.var2.length);
            if (contentLength.get() >= MAX_PART_SIZE && needRequest.get()) {
                needRequest.set(false);
                dataSource.complete();
            }
            if (bytes.var1 != null) {
                dataSource.cursor(bytes.var1);
            }
            digest.update(bytes.var2);
            // 从上流获取了一次数据
            encoder[0].put(bytes.var2);
        });

        disposables.add(disposable);

        List<UnicastProcessor<Payload>> publisher = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("fileName", destination.getFileName())
                        .put("lun", t.var2)
                        .put("vnode", t.var3)
                        .put("compression", pool.getCompression())
                )
                .map(msg0 -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create();
                    processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));
                    return processor;
                })
                .collect(Collectors.toList());

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            encoder[0].request(i, 1L);
            encoder[0].data()[index].subscribe(bytes -> {
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

        Optional.ofNullable(dataSource.keepalive())
                .ifPresent(longFlux -> longFlux
                        .doFinally(v -> log.info("bucket {} keepalive:{}", destination.getBucket(), v))
                        .subscribe(l -> {
                            log.debug("send keepalive");
                            for (UnicastProcessor<Payload> payloads : publisher) {
                                payloads.onNext(DefaultPayload.create("", KEEPALIVE.name()));
                            }
                        }));

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList);

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>();
        AtomicInteger errorNum = new AtomicInteger(0);

        responseInfo.responses.publishOn(InventoryService.INVENTORY_SCHEDULER).doOnNext(s -> {
            if (s.var2.equals(ERROR)) {
                // 如果所有节点返回的ERROR数量大于K则，直接结束任务
                if (errorNum.incrementAndGet() > pool.getK()) {
                    for (UnicastProcessor<Payload> payloads : publisher) {
                        payloads.onNext(DefaultPayload.create("put file error", ERROR.name()));
                        payloads.onComplete();
                    }
                    return;
                }

                errorChunksList.add(s.var1);
                if (needRequest.get()) {
                    encoder[0].request(s.var1, Long.MAX_VALUE);
                }
            } else {
                if (needRequest.get()) {
                    encoder[0].request(s.var1, 1L);
                }
            }

        }).doOnComplete(() -> {
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                QuotaRecorder.addCheckBucket(destination.getBucket());
                res.onNext(true);
            } else if (responseInfo.successNum >= pool.getK()) {
                QuotaRecorder.addCheckBucket(destination.getBucket());
                res.onNext(true);

//                String storageName = "storage_" + pool.getVnodePrefix();
//                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                if (StringUtils.isEmpty(poolQueueTag)) {
//                    String strategyName = "storage_" + pool.getVnodePrefix();
//                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                }
                //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("errorChunksList", Json.encode(errorChunksList))
                        .put("storage", pool.getVnodePrefix())
                        .put("bucket", destination.getBucket())
                        .put("object", destination.getObject())
                        .put("fileName", destination.getFileName())
                        .put("versionId", destination.getVersionId())
                        .put("fileSize", String.valueOf(encoder[0].size()))
                        .put("stamp", destination.getTimestamp())
                        .put("poolQueueTag", poolQueueTag)
                        .put("fileOffset", "");
                for (int index : errorChunksList) {
                    if (pool.getK() + pool.getM() <= index) {
                        log.error("publish error {} {}", pool, errorChunksList);
                    }
                }

                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
            } else {
                res.onNext(false);
            }
        }).doOnError(e -> log.error("", e)).subscribe();

        return res;
    }

    /**
     * 重置数据传输，便于该对象可以多次利用
     */
    public void reset() {
        this.contentLength.set(0);
        this.dataSource.reset();
        this.md5 = new String[1];
    }

    /**
     * 关闭流资源
     */
    public void dispose() {
        for (Disposable disposable : disposables) {
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
        }
    }

    private static final HttpClient HTTP_CLIENT;

    static {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(PROC_NUM)
                .setPreferNativeTransport(true));
        HttpClientOptions options = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                .setMaxPoolSize(200)
                .setConnectTimeout(3000)
                .setIdleTimeout(30);
        HTTP_CLIENT = vertx.createHttpClient(options);
    }

    /**
     * 从本地读取指定对象上传至目标位置
     *
     * @param sourceAccountAk 请求账户的ak
     * @param host            目的ip
     * @param storagePool     对象所在存储池
     * @param bucket          目标桶名
     * @param object          目标对象名
     * @param fileName        对象文件名
     * @param contentLength   对象大小
     * @param headers         自定义请求头信息
     */
    public static Mono<Boolean> readAndWriteObject(String sourceAccountAk, String host, int port, StoragePool storagePool,
                                                   String bucket, String object, String fileName, long contentLength,
                                                   Map<String, String> headers, Handler<io.vertx.reactivex.core.http.HttpClientResponse> handler) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        String uri = UrlEncoder.encode("/" + bucket + "/" + object, "UTF-8");
        HttpClientRequest httpClientRequest = HTTP_CLIENT.request(HttpMethod.PUT, port, host, uri);
        MsClientRequest request = new MsClientRequest(httpClientRequest);
        request.setTimeout(30_000);
        request.setHost(host);
        Disposable[] disposables = new Disposable[2];
        AtomicBoolean waitForFetch = new AtomicBoolean(false);
        AtomicLong writeTotal = new AtomicLong();
        AtomicLong fetchTotal = new AtomicLong(1L);
        request.putHeader(EXPECT, EXPECT_100_CONTINUE).putHeader(CONTENT_LENGTH, String.valueOf(contentLength));
        headers.forEach(request::putHeader);
        String authString = "AWS " + sourceAccountAk + ":0000";
        disposables[1] = AuthorizeV2.getAuthorizationHeader(bucket, object, authString, request, false, false)
                .doOnNext(authHeader -> request.putHeader(AUTHORIZATION, authHeader))
                .publishOn(SCAN_SCHEDULER)
                .subscribe(o -> {
                    UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                    request.setWriteQueueMaxSize(WriteQueueMaxSize)
                            .continueHandler(v -> {
                                disposables[0] = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(fileName))
                                        .flatMapMany(nodeList -> ECUtils.getObject(storagePool, fileName, false, 0, contentLength - 1, contentLength, nodeList, streamController, null, request))
                                        .publishOn(SCAN_SCHEDULER)
                                        .timeout(Duration.ofSeconds(60))
                                        .doOnNext(dealBytesStream(request, waitForFetch, writeTotal, fetchTotal, streamController))
                                        .doOnError(e -> {
                                            log.error("getobj error, {}", e.getMessage());
                                            closeConn(request);
                                            res.onNext(false);
                                        })
                                        .doOnComplete(() -> {
                                            synchronized (request.getDelegate().connection()) {
                                                request.end();
                                            }
                                        })
                                        .subscribe();

                                request.addResponseCloseHandler(s -> DoubleActiveUtil.streamDispose(disposables));
                            })
                            .exceptionHandler(e -> {
                                log.error("send {} request to {} error!{}, {}", request.method().name(), request.getHost(), e.getClass().getName(), e.getMessage());
                                closeConn(request);
                                res.onNext(false);
                            })
                            .handler(response -> {
                                if (handler != null) {
                                    handler.handle(response);
                                }
                                String resEtag = response.headers().get(ETAG);
                                log.info("ip:{}, statusCode:{}, resEtag:{}", request.getHost(), response.statusCode(), resEtag);
                                res.onNext(response.statusCode() == SUCCESS);
                                closeConn(request);
                            })
                            .sendHead();

                });
        return res;
    }
}
