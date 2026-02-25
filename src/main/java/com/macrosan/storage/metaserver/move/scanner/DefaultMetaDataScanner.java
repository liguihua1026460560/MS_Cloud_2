package com.macrosan.storage.metaserver.move.scanner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.migrate.Scanner;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListShardingMetaHandler;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_SHARDING_META_OBJ;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.PART_UPLOAD;

/**
 * @author Administrator
 */
@Log4j2
public class DefaultMetaDataScanner implements com.macrosan.storage.metaserver.move.scanner.Scanner<Tuple2<byte[], byte[]>> {
    /**
     * 桶名
     */
    private final String bucketName;
    /**
     * 桶分片所对应的vnode
     */
    private final String vnode;
    /**
     * 需要扫描的元数据类型，如: *,!,@，-等前缀开头的元数据
     */
    private final String prefixType;
    /**
     * 起始扫描标记
     */
    private final String startMarker;
    /**
     * 扫描顺序，是向后扫描还是向前扫描
     */
    private final SCAN_SEQUENCE sequence;

    /**
     * 扫描是否已经结束
     */
    private final AtomicBoolean end = new AtomicBoolean(false);

    private final AtomicStampedReference<Boolean> hasRemaining = new AtomicStampedReference<>(true, 0);

    private final StoragePool storagePool;

    private final MemoryLimiter memoryLimiter = new MemoryLimiter();

    public DefaultMetaDataScanner(String bucketName, String vnode, String prefixType, String startMarker, SCAN_SEQUENCE sequence) {
        this.bucketName = bucketName;
        this.vnode = vnode;
        this.prefixType = prefixType;
        this.startMarker = startMarker;
        this.sequence = sequence;
        storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
    }

    /**
     * 开始扫描数据
     * @return 对象元数据流
     */
    @Override
    public Flux<Tuple2<byte[], byte[]>> scan() {
        log.info("bucket:{} vnode:{} prefix:{} scanner begin running.", bucketName, vnode, prefixType);
        return storagePool.mapToNodeInfo(vnode)
                .flatMapMany(this::iterator)
                .doOnNext(tuple2 -> memoryLimiter.releaseMemory(tuple2.var1.length() + tuple2.var2.toString().length()))
                .doFinally(v -> log.debug("bucket {} vnode {} prefix {} memoryLimiter usage mem:{}", bucketName, vnode, prefixType, memoryLimiter.getCurrentMemoryUsage()))
                .flatMap(tuple2 -> Mono.just(new Tuple2<>(tuple2.var1.getBytes(), Json.encode(tuple2.var2).getBytes())));
    }

    /**
     * 用于保存数据流
     */
    private final UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create(Queues.<Tuple2<String, JsonObject>>unboundedMultiproducer().get());

    private static final int HIGH_THRESHOLD = 20_000;
    private static final int LOW_THRESHOLD = 10_000;

    public Flux<Tuple2<String, JsonObject>> iterator(List<Tuple3<String, String, String>> nodeList) {
        UnicastProcessor<String> listController = UnicastProcessor.create();
        List<SocketReqMsg> msg = nodeList.stream().map(info -> new SocketReqMsg("", 0)
                .put("lun", info.var2)
                .put("bucket", bucketName)
                .put("sourceVnode", vnode)
                .put("startMarker", startMarker)
                .put("prefix", prefixType)
                .put("sequence", sequence.name())).collect(Collectors.toList());
        listController.publishOn(ErasureServer.DISK_SCHEDULER)
                .subscribe(marker -> {
                    if (StringUtils.isEmpty(marker)) {
                        listFlux.onComplete();
                        listController.onComplete();
                    } else {
                        if (listFlux.size() <= LOW_THRESHOLD && !memoryLimiter.isOverLimit()) {
                            if (!isEnd()) {
                                next(marker, msg, nodeList, listController);
                            } else {
                                listController.onComplete();
                            }
                        } else {
                            Disposable[] disposables = new Disposable[]{null};
                            disposables[0] = Flux.interval(Duration.ofSeconds(1))
                                    .subscribe(l -> {
                                        if (listFlux.size() <= HIGH_THRESHOLD && !memoryLimiter.isOverLimit()) {
                                            disposables[0].dispose();
                                            if (!isEnd()) {
                                                next(marker, msg, nodeList, listController);
                                            } else {
                                                listController.onComplete();
                                            }
                                        } else {
                                            log.debug("Bucket {} sharding wait list next {}." +
                                                            " Because the cache has reached the maximum limit. size:{}, mem:{}",
                                                    bucketName, marker, listFlux.size(), memoryLimiter.getCurrentMemoryUsage() );
                                        }
                                    });
                        }
                    }
                }, e -> log.error("", e));

        next("", msg, nodeList, listController);

        return listFlux;
    }


    private void next(String marker, List<SocketReqMsg> msg, List<Tuple3<String, String, String>> nodeList, UnicastProcessor<String> listController) {
        for (SocketReqMsg socketReqMsg : msg) {
            socketReqMsg.put("marker", marker);
        }
        TypeReference<Tuple2<String, JsonObject>[]> reference = new TypeReference<Tuple2<String, JsonObject>[]>() {
        };

        ClientTemplate.ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo =
                ClientTemplate.oneResponse(msg, ErasureServer.PayloadMetaType.LIST_SHARDING_META_OBJ, reference, nodeList);

        final ListShardingMetaHandler clientHandler = new ListShardingMetaHandler(storagePool, responseInfo, nodeList, sequence);
        responseInfo.responses.subscribe(clientHandler::handleResponse, listFlux::onError, clientHandler::handleComplete);
        clientHandler.listFlux.subscribe(tuple2 -> {
            // 记录使用的估计内存。
            long len = tuple2.var1.length() + tuple2.var2.toString().length();
            memoryLimiter.allocateMemory(len);
            listFlux.onNext(tuple2);
        }, e -> {
            log.error("", e);
            listFlux.onError(e);
        });
        clientHandler.res.subscribe(b -> {
            if (!b) {
                listFlux.onError(new RuntimeException("list shard meta error!"));
                return;
            }
            if (StringUtils.isNotEmpty(clientHandler.getNextMarker())) {
                listController.onNext(clientHandler.getNextMarker());
            } else {
                boolean compareAndSet = hasRemaining.compareAndSet(true, false, 0, 1);
                if (compareAndSet) {
                    listFlux.onComplete();
                }
                listController.onComplete();
                log.info("list prefix:{} complete remaining:{}!", prefixType, hasRemaining.getReference());
            }
        });
    }

    /**
     * 停止扫描数据
     */
    @Override
    public void tryEnd() {
        log.info("tryEnd");
        hasRemaining.set(true, 1);
        listFlux.onComplete();
        this.end.set(true);
    }

    @Override
    public boolean isEnd() { return this.end.get(); }

    @Override
    public boolean hasRemaining() {
        return hasRemaining.getReference();
    }

    /**
     * rocksdb的扫描顺序
     */
    public enum SCAN_SEQUENCE {
        /**
         * 向后扫描
         */
        NEXT,
        /**
         * 向前扫描
         */
        PREV
    }
}
