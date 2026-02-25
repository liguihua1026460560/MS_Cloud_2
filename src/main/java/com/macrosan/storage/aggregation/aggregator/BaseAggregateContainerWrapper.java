package com.macrosan.storage.aggregation.aggregator;

import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.message.consturct.RequestBuilder;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainerPool;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.aggregation.transaction.UndoLog;
import com.macrosan.storage.aggregation.transaction.UndoLogHandler;
import com.macrosan.utils.aggregation.AggregationUtils;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.BitSet;
import java.util.Map;

@Log4j2
public abstract class BaseAggregateContainerWrapper<E> {

    /**
     * 对相同命名空间下的小对象进行聚合操作
     */
    protected NameSpace nameSpace;

    @Getter
    protected AggregateContainer container;

    public BaseAggregateContainerWrapper(NameSpace nameSpace, AggregateContainer container) {
        this.nameSpace = nameSpace;
        this.container = container;
    }

    /**
     * 向聚合容器中追加数据
     *
     * @param key 小对象唯一标识
     * @param e   小对象
     */
    public abstract Mono<Boolean> append(String key, E e);

    /**
     * 根据生成的聚合文件更新小对象的元数据
     */
    protected abstract Mono<BitSet> update(String aggregateKey, AggregateFileMetadata metaData);

    /**
     * 将聚合容器中的数据进行下刷
     */
    public synchronized Mono<Boolean> flush(boolean force) {
        if (container == null || container.isEmpty()) {
            return Mono.error(new IllegalStateException("container is empty"));
        }
        return container.flush(force)
                .flatMap(flushSuccess -> {
                    if (!flushSuccess) {
                        return Mono.error(new IllegalStateException("force:" + force + " container immutable:" + isImmutable() + ", flushing:" + isFlushing() + ", isEmpty:" + isEmpty()));
                    }
                    String nameSpaceIdentifier = nameSpace.getNameSpaceIdentifier();
                    StoragePool storagePool = nameSpace.getStoragePool(new StorageOperate(StorageOperate.PoolType.DATA, nameSpaceIdentifier, Long.MAX_VALUE));
                    MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
                    String aggregationId = AggregationUtils.generateAggregationId();
                    int size = container.getIndexMap().size();
                    String[] k = new String[size];
                    long[] o = new long[size];
                    int index = 0;
                    for (Map.Entry<String, AggregateContainer.FileIndex> entry : container.getIndexMap().entrySet()) {
                        k[index] = entry.getKey();
                        o[index] = (int) entry.getValue().offset;
                        index++;
                    }
                    AggregateFileMetadata aggregatedMetaData = new AggregateFileMetadata()
                            .setNamespace(nameSpaceIdentifier)
                            .setAggregationId(aggregationId)
                            .setNamespace(nameSpaceIdentifier)
                            .setFileName(Utils.getObjFileName(storagePool, nameSpaceIdentifier, aggregationId, RequestBuilder.getRequestId()))
                            .setFileSize(container.getSize().get())
                            .setStorage(storagePool.getVnodePrefix())
                            .setVersionNum(VersionUtil.getVersionNum())
                            .setSegmentKeys(k)
                            .setStamp(String.valueOf(System.currentTimeMillis()))
                            .setDeltaOffsets(o);

                    StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(nameSpaceIdentifier);
                    String bucketVnodeId = metaStoragePool.getBucketVnodeId(aggregationId);
                    String key = AggregationUtils.getAggregationKey(bucketVnodeId, nameSpaceIdentifier, aggregationId);
                    // 创建一条聚合日志，保证聚合失败的时候不会出现数据块残留
                    UndoLog undoLog = new UndoLog();
                    undoLog.namespace = nameSpaceIdentifier;
                    undoLog.aggregateId = aggregationId;
                    undoLog.timestamp = DateChecker.getCurrentTime();
                    undoLog.content = aggregatedMetaData.storage + "=" + aggregatedMetaData.fileName;
                    undoLog.operatorType = 0;
                    UndoLogHandler.running.add(undoLog.aggregateId);
                    return UndoLogHandler.recordUndoLog(undoLog)
                            .flatMap(b -> b ? AggregateFileClient.flushContainer(aggregatedMetaData, container.asFlux(), storagePool, recoverDataProcessor) : Mono.just(false))
                            .timeout(Duration.ofMinutes(10))
                            .onErrorReturn(false)
                            .flatMap(b -> {
                                if (b) {
                                    return metaStoragePool.mapToNodeInfo(bucketVnodeId)
                                            .flatMap(nodeList -> AggregateFileClient.putAggregationMeta(key, aggregatedMetaData, nodeList, recoverDataProcessor))
                                            .doOnNext(success -> {
                                                if (success) {
                                                    log.debug("The aggregation container {} is successfully down brushed! fileName:{}", aggregationId, aggregatedMetaData.getFileName());
                                                }
                                            });
                                }
                                return Mono.just(false);
                            })
                            .flatMap(b -> {
                                if (b) {
                                    return update(key, aggregatedMetaData)
                                            .flatMap(bitSet -> {
                                                int cardinality = bitSet.cardinality();
                                                if (cardinality == aggregatedMetaData.getSegmentKeys().length) {
                                                    log.debug("aggregation meta flush success! aggregationId: {} fileName:{}", aggregationId, aggregatedMetaData.getFileName());
                                                    return Mono.just(true);
                                                } else {
                                                    // 存在元数据更新失败,则进行一次检查,释放更新失败占用的存储空间
                                                    log.debug("Metadata update failed. Procedure aggregationId: {}, use: {} bits:{} total:{} fileName:{}", aggregationId, cardinality, bitSet.length()
                                                            , aggregatedMetaData.getSegmentKeys().length, aggregatedMetaData.getFileName());
                                                    undoLog.operatorType = 2;
                                                    undoLog.content = AggregationUtils.serialize(bitSet);
                                                    return UndoLogHandler.recordUndoLog(undoLog).map(b1 -> false);
                                                }
                                            });
                                }
                                return Mono.just(false);
                            })
                            .flatMap(b -> b ? UndoLogHandler.commitUndoLog(undoLog) : Mono.just(false))
                            .doFinally(b -> UndoLogHandler.running.remove(undoLog.aggregateId));
                });
    }

    /**
     * 聚合容器是否已满
     */
    public synchronized boolean isImmutable() {
        return container != null && container.isImmutable();
    }

    /**
     * 聚合容器是否正在下刷
     */
    public synchronized boolean isFlushing() {
        return container != null && container.isFlushing();
    }

    /**
     * 清空聚合容器
     */
    public synchronized void clear() {
        if (container == null) {
            return;
        }
        AggregateContainer oldContainer = container;
        container.clear();
        container = null; // help gc
        AggregateContainerPool.getInstance().releaseContainer(oldContainer); // release container
        log.debug(this + " clear()");
    }


    public synchronized boolean isEmpty() {
        return container == null || container.isEmpty();
    }
}
