package com.macrosan.storage.aggregation.transaction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListAggregationUndoLogHandler;
import com.macrosan.storage.client.ListMultiMediaTaskHandler;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_AGGREGATION_UNDO_LOG;
import static com.macrosan.storage.aggregation.AggregateFileGcScheduler.GC_SCHEDULER;

@Log4j2
public class UndoLogHandler implements Runnable {

    public static final ConcurrentHashSet<String> running = new ConcurrentHashSet<>();

    @Getter
    @Setter
    private NameSpace nameSpace;

    // 间隔时间
    public static long interval = 15 * 60 * 1000;

    public UndoLogHandler(NameSpace nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override
    public void run() {
        Mono.delay(Duration.ofSeconds(0))
                .doOnNext(l -> log.debug(nameSpace.getNameSpaceIdentifier() + " start to deal undo log"))
                .flatMapMany(l -> listUndoLog())
                .flatMap(this::handle, 1, 1)
                .doFinally(v -> GC_SCHEDULER.schedule(this, 5, TimeUnit.MINUTES))
                .doOnComplete(() -> log.debug(nameSpace.getNameSpaceIdentifier() + " finish deal undo log"))
                .subscribe();
    }

    private Flux<UndoLog> listUndoLog() {
        UnicastProcessor<UndoLog> listFlux = UnicastProcessor.create(Queues.<UndoLog>unboundedMultiproducer().get());
        UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        TypeReference<Tuple2<String, UndoLog>[]> reference = new TypeReference<Tuple2<String, UndoLog>[]>() {
        };

        String startStamp = "0";
        String endStamp = String.valueOf(System.currentTimeMillis() - interval);
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(nameSpace.getNameSpaceIdentifier());
        String vnodeId = metaPool.getBucketVnodeId(nameSpace.getNameSpaceIdentifier());
        String nodeUuid = ServerConfig.getInstance().getHostUuid();
        listController
                .publishOn(GC_SCHEDULER)
                .flatMap(marker -> Mono.just(marker).zipWith(metaPool.mapToNodeInfo(vnodeId)))
                .doFinally(v -> listFlux.onComplete())
                .subscribe(tuple -> {
                    String marker = tuple.getT1();
                    List<Tuple3<String, String, String>> nodeList = tuple.getT2();

                    List<SocketReqMsg> msgs = nodeList.stream().map(info ->
                            new SocketReqMsg("", 0)
                                    .put("namespace", nameSpace.getNameSpaceIdentifier())
                                    .put("maxKeys", String.valueOf(ListAggregationUndoLogHandler.MAX_KEY))
                                    .put("marker", marker)
                                    .put("lun", MSRocksDB.getAggregateLun(info.var2))
                                    .put("nodeUuid", nodeUuid)
                                    .put("startStamp", startStamp)
                                    .put("endStamp", endStamp)
                    ).collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<Tuple2<String, UndoLog>[]> responseInfo =
                            ClientTemplate.oneResponse(msgs, LIST_AGGREGATION_UNDO_LOG, reference, nodeList);
                    ListAggregationUndoLogHandler listHandler = new ListAggregationUndoLogHandler(metaPool, responseInfo, nodeList);
                    responseInfo.responses.subscribe(listHandler::handleResponse, listController::onError, listHandler::handleComplete);
                    listHandler.listProcessor
                            .subscribe(logList -> {
                                if (logList.isEmpty()) {
                                    listController.onComplete();
                                    return;
                                } else {
                                    logList.forEach(undoLog -> {
                                        if (!running.contains(undoLog.aggregateId) && System.currentTimeMillis() - undoLog.timestamp > interval) {
                                            listFlux.onNext(undoLog);
                                        }
                                    });
                                }
                                if (StringUtils.isNotEmpty(listHandler.getNextMarker())) {
                                    if (listFlux.size() >= 10000) {
                                        Disposable[] disposables = new Disposable[]{null};
                                        disposables[0] = Flux.interval(Duration.ofSeconds(1))
                                                .subscribe(l -> {
                                                    if (listFlux.size() < 10000) {
                                                        disposables[0].dispose();
                                                        listController.onNext(listHandler.getNextMarker());
                                                    }
                                                });
                                    } else {
                                        listController.onNext(listHandler.getNextMarker());
                                    }
                                } else {
                                    listController.onComplete();
                                }
                            });
                });

        listController.onNext("");
        return listFlux;
    }

    private Mono<Boolean> handle(UndoLog undoLog) {
        return handle0(undoLog)
                .flatMap(b -> {
                    if (b) {
                        return commitUndoLog(undoLog);
                    }
                    return Mono.just(false);
                });
    }

    private Mono<Boolean> handle0(UndoLog undoLog) {
        int operatorType = undoLog.operatorType;
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(undoLog.namespace);
        if (!undoLog.committed) {
            if (operatorType <= 2) {
                String vnodeId = metaStoragePool.getBucketVnodeId(undoLog.aggregateId);
                String realKey = AggregationUtils.getAggregationKey(vnodeId, undoLog.namespace, undoLog.aggregateId);
                return metaStoragePool.mapToNodeInfo(vnodeId)
                        .flatMap(nodeList -> AggregateFileClient.getAggregationMeta(undoLog.namespace, undoLog.aggregateId, nodeList).zipWith(Mono.just(nodeList)))
                        .flatMap(tuple2 -> {
                            AggregateFileMetadata aggregateFileMetadata = tuple2.getT1();
                            if (aggregateFileMetadata.equals(AggregateFileMetadata.ERROR_AGGREGATION_META)) {
                                return Mono.just(false);
                            }
                            if (aggregateFileMetadata.equals(AggregateFileMetadata.NOT_FOUND_AGGREGATION_META)
                                    || aggregateFileMetadata.deleteMark) {
                                if (operatorType <= 1) {
                                    String[] split = undoLog.content.split("=");
                                    if (split.length == 2) {
                                        String storage = split[0];
                                        String fileName = split[1];
                                        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, "");
                                        if (storagePool != null) {
                                            return ErasureClient.deleteObjectFile(storagePool, new String[]{fileName}, null);
                                        }
                                    }
                                }
                                return Mono.just(true);
                            }
                            BitSet deserialize = AggregationUtils.deserialize("");
                            if (operatorType == 2) {
                                deserialize = AggregationUtils.deserialize(undoLog.content);
                            }
                            return AggregateFileClient.calculateAggregateFileHoleRate(aggregateFileMetadata, deserialize)
                                    .flatMap(t2 -> {
                                        BitSet hole = t2.var1;
                                        BitSet successRate = t2.var2;
                                        return Mono.just(hole.cardinality() == aggregateFileMetadata.segmentKeys.length)
                                                .flatMap(b -> {
                                                    if (!b) {
                                                        log.debug("aggregationId: {} fileName:{} hole:{} successRate:{}", undoLog.aggregateId, aggregateFileMetadata.fileName, hole.cardinality(), successRate.cardinality());
                                                        return AggregateFileClient.freeAggregationSpace(realKey, tuple2.getT2(), hole);
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .flatMap(b -> {
                                                    if (successRate.cardinality() != aggregateFileMetadata.segmentKeys.length) {
                                                        undoLog.setOperatorType(2);
                                                        undoLog.setTimestamp(System.currentTimeMillis());
                                                        undoLog.setContent(AggregationUtils.serialize(successRate));
                                                        return recordUndoLog(undoLog);
                                                    } else {
                                                        return Mono.just(true);
                                                    }
                                                });
                                    });
                        });
            } else {
                log.error("operatorType :{} error!", operatorType);
                return Mono.just(false);
            }
        } else {
            return Mono.just(true);
        }
    }

    public static Mono<Boolean> recordUndoLog(UndoLog undoLog) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool("");
        String vnodeId = storagePool.getBucketVnodeId(undoLog.namespace);
        undoLog.versionNum = VersionUtil.getVersionNum();
        return storagePool.mapToNodeInfo(vnodeId)
                .flatMap(nodeList -> AggregateFileClient.recordAggregationUndoLog(undoLog.rocksKey(), undoLog, nodeList));
    }

    public static Mono<Boolean> commitUndoLog(UndoLog undoLog) {
        undoLog.committed = true;
        undoLog.setContent(null);
        return recordUndoLog(undoLog);
    }
}
