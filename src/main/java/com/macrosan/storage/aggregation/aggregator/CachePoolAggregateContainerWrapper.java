package com.macrosan.storage.aggregation.aggregator;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.utils.aggregation.ConcurrentBitSet;
import com.macrosan.utils.functional.Tuple2;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.ROCKS_OBJ_META_DELETE_MARKER;

@Getter
@Log4j2
public class CachePoolAggregateContainerWrapper extends BaseAggregateContainerWrapper<Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]>> {

    /**
     * 用于暂存小对象的元数据，key为对象元数据的metaKey
     */
    private final Map<String, Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]>> metaMap = new LinkedHashMap<>();

    public CachePoolAggregateContainerWrapper(NameSpace nameSpace, AggregateContainer container) {
        super(nameSpace, container);
    }

    @Override
    public Mono<Boolean> append(String taskKey, Tuple2<MetaData, Tuple2<ErasureServer.PayloadMetaType, MetaData>[]> value) {
        if (isFlushing() || isImmutable() || container == null) {
            return Mono.error(new IllegalStateException("Append error, container immutable:" + isImmutable() + ", flushing:" + isFlushing()));
        }
        Runnable runnable = () -> metaMap.put(taskKey, value);
        return AggregateFileClient.aggregate(value.var1, container, runnable)
                .doOnNext(next -> {
                    if (isImmutable()) {
                        flush(false)
                                .doOnError(t -> {
                                    if (!(t instanceof IllegalStateException)) {
                                        log.error("Flush error", t);
                                        clear();
                                    }
                                })
                                .subscribe(b -> clear());
                    }
                });
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
                    // 需要删除的数据文件
                    String needDeleteFile = var1.fileName;
                    String k = var1.bucket + "/" + var1.key + "/" + var1.versionId;
                    long offset = container.get(k).offset;
                    int totalParts =offsets.length;
                    int partNum = Arrays.binarySearch(offsets, offset);
                    String key = aggregateKey + ":" + totalParts + ":" + partNum;
                    return AggregateFileClient.updateMetaData(var1, value.var2, key, metaData, offset)
                            .doOnNext(b -> {
                                if (b) {
                                    AggregateFileClient.putMq(tuple.getKey(), ROCKS_OBJ_META_DELETE_MARKER + needDeleteFile);
                                } else {
                                    bitSet.clear(partNum);
                                }
                            });
                }, 4)
                .collectList()
                .map(list -> list.get(0))
                .map(b -> bitSet);
    }

    @Override
    public void clear() {
        super.clear();
        metaMap.clear();
    }

    @Override
    public boolean isEmpty() {
        if (container == null || metaMap.isEmpty()) {
            return true;
        }
        return super.isEmpty();
    }
}
