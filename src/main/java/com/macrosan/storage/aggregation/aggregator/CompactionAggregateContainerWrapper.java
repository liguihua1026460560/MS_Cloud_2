package com.macrosan.storage.aggregation.aggregator;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.utils.aggregation.ConcurrentBitSet;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

@Log4j2
public class CompactionAggregateContainerWrapper extends BaseAggregateContainerWrapper<Tuple3<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[], Runnable>> {

    private final Map<String, Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]>> metaMap = new LinkedHashMap<>();
    private final Map<String, Runnable> hookMap = new LinkedHashMap<>();

    public CompactionAggregateContainerWrapper(NameSpace nameSpace, AggregateContainer container) {
        super(nameSpace, container);
    }

    @Override
    public Mono<Boolean> append(String key, Tuple3<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[], Runnable> tuple3) {
        if (isFlushing() || isImmutable() || container == null) {
            return Mono.error(new IllegalStateException("Append error, container immutable:" + isImmutable() + ", flushing:" + isFlushing()));
        }
        MetaData metaData = tuple3.var1;
        Runnable runnable = getRunnable(key, tuple3, metaData);
        return AggregateFileClient.aggregate(metaData.fileName, metaData.storage, metaData.bucket, metaData.key
                , metaData.versionId, metaData.offset, (metaData.endIndex - metaData.startIndex + 1), container, runnable)
                .doOnNext(next -> {
                    if (isImmutable()) {
                        flush(false)
                                .doOnError(t -> {
                                    if (!(t instanceof IllegalStateException)) {
                                        log.error(container.getContainerId() + " Flush error", t);
                                        clear();
                                    }
                                })
                                .subscribe(b -> clear());
                    }
                });
    }

    private Runnable getRunnable(String key, Tuple3<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[], Runnable> tuple3, MetaData metaData) {
        Tuple2<ErasureServer.PayloadMetaType, MetaData>[] res = tuple3.var2;
        Runnable hook = tuple3.var3;
        return () -> {
            metaMap.put(key, new Tuple2<>(metaData, res));
            hookMap.put(key, hook);
        };
    }

    @Override
    protected Mono<BitSet> update(String aggregateKey, AggregateFileMetadata metaData) {
        Map<String, Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]>> metaMap = this.metaMap;
        long[] offsets = metaData.getDeltaOffsets();
        ConcurrentBitSet bitSet = new ConcurrentBitSet(offsets.length);
        bitSet.set(0, offsets.length);
        return Flux.fromIterable(metaMap.entrySet())
                .flatMap(tuple -> {
                    Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]> value = tuple.getValue();
                    MetaData var1 = value.var1;
                    String k = var1.bucket + "/" + var1.key + "/" + var1.versionId;
                    long offset = container.get(k).offset;
                    int totalParts = offsets.length;
                    int partNum = Arrays.binarySearch(offsets, offset);
                    String key = aggregateKey + ":" + totalParts + ":" + partNum;
                    return AggregateFileClient.updateMetaData(var1, value.var2, key, metaData, offset)
                            .doOnNext(b -> {
                                if (b) {
                                    Runnable hook = hookMap.get(k);
                                    if (hook != null) {
                                        hook.run();
                                    }
                                } else {
                                    bitSet.clear(partNum);
                                }
                            });
                }, 4)
                .collectList()
                .map(list -> bitSet);
    }

    @Override
    public void clear() {
        super.clear();
        metaMap.clear();
        hookMap.clear();
    }
}
