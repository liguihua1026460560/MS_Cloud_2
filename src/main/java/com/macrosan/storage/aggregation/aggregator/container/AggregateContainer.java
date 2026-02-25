package com.macrosan.storage.aggregation.aggregator.container;

import com.macrosan.utils.functional.Tuple2;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Log4j2
public class AggregateContainer{
    /**
     * 用于记录小对象在聚合块中的偏移量和长度
     */
    public static class FileIndex {
        public final long offset;
        public final long length;

        public FileIndex(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }
    }

    /**
     * 聚合块的唯一标识
     */
    @Getter
    @Setter
    private String containerId;

    /**
     * 聚合块的总容量，单位为字节
     */
    @Getter
    private int capacity;

    /**
     * 聚合块的数据大小，单位为字节
     */
    @Getter
    private final AtomicLong size = new AtomicLong(0);

    /**
     * 用于暂存小对象的数据，key为对象元数据的metaKey，value为小文件在大文件中的偏移量、长度信息
     */
    @Getter
    private final Map<String, FileIndex> indexMap = new LinkedHashMap<>();

    /**
     * 存储聚合块的数据
     */
    private ByteBuffer aggregateBuffer;

    /**
     * 聚合块是否为只读状态，如果为只读状态，则不允许修改聚合块的数据
     */
    private volatile boolean immutable = false;

    /**
     * 聚合块是否正在刷新，如果正在刷新，则不允许修改聚合块的数据
     */
    private volatile boolean isFlushing = false;

    /**
     * 聚合块的锁，用于控制并发访问
     */
    private final ReentrantLock lock = new ReentrantLock();


    @Getter
    private final int maxFilesLimit;


    AggregateContainer(String containerId, int maxFilesLimit, int capacity, boolean isDirect) {
        this.maxFilesLimit = maxFilesLimit;
        this.containerId = containerId;
        this.capacity = capacity;
        if (isDirect) {
            this.aggregateBuffer = ByteBuffer.allocateDirect(capacity);
        } else {
            this.aggregateBuffer = ByteBuffer.allocate(capacity);
        }
    }

    /**
     * 追加数据到聚合块中
     *
     * @param metaKey 元数据的key
     * @param data    数据
     * @return 是否追加成功
     */
    public boolean append(String metaKey, byte[] data, Runnable runnable) {
        lock.lock();
        try {
            if (immutable || isFlushing) {
                return false;
            }
            // 达到最大文件数量限制
            if (indexMap.size() >= maxFilesLimit) {
                immutable = true;
                return false;
            }
            // 达到最大容量限制
            if (aggregateBuffer.remaining() < data.length) {
                immutable = true;
                boolean b = expandBuffer(data.length);
                if (!b) {
                    return false;
                }
            }
            int offset = aggregateBuffer.position();
            aggregateBuffer.put(data);
            if (indexMap.containsKey(metaKey)) {
                this.indexMap.put(metaKey, new FileIndex(offset, data.length));
                // 处理同名对象
                reassemble();
            } else {
                this.indexMap.put(metaKey, new FileIndex(offset, data.length));
                this.size.addAndGet(data.length);
            }
            if (indexMap.size() == maxFilesLimit) {
                immutable = true;
            }
            if (aggregateBuffer.remaining() <= 0) {
                immutable = true;
            }
            if (runnable != null) {
                runnable.run();
            }
            return true;
        } catch (Exception e) {
            log.error(metaKey + " append error", e);
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取小文件的索引信息
     *
     * @param metaKey 元数据的key
     * @return 小文件的索引信息
     */
    public FileIndex get(String metaKey) {
        lock.lock();
        try {
            return indexMap.get(metaKey);
        } catch (Exception e) {
            log.error(metaKey + " get error", e);
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * 将聚合块的数据以流形式返回
     *
     * @return 聚合块的数据流
     */
    public Flux<byte[]> asFlux() {
        lock.lock();
        try {
            List<byte[]> list = indexMap.values().stream().map(index -> {
                aggregateBuffer.position((int) index.offset);
                byte[] bytes = new byte[(int) index.length];
                aggregateBuffer.get(bytes);
                return bytes;
            }).collect(Collectors.toList());
            return Flux.fromIterable(list);
        } catch (Exception e) {
            log.error("asFlux error", e);
        } finally {
            lock.unlock();
        }
        return Flux.fromIterable(Collections.emptyList());
    }

    public Mono<Boolean> flush(boolean force) {
        lock.lock();
        try {
            if ((!force && !immutable) || isFlushing || indexMap.isEmpty()) {
                return Mono.error(new IllegalStateException("container aggregate is flushing immutable:" + isImmutable() + ", flushing:" + isFlushing() + ", isFlushing:"
                + isFlushing + ", isEmpty:" + indexMap.isEmpty() + ", force:" + force));
            }
            isFlushing = true;
        } finally {
            lock.unlock();
        }
        return Mono.just(true);
    }

    /**
     * 判断聚合块是否为只读状态
     */
    public boolean isImmutable() {
        lock.lock();
        try {
            return immutable;
        } finally {
            lock.unlock();
        }
    }

    public boolean isFlushing() {
        lock.lock();
        try {
            return isFlushing;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重新组装聚合块，将索引信息中的偏移量和长度信息应用到聚合块中
     */
    private void reassemble() {
        try {
            List<Tuple2<String, byte[]>> list = indexMap.entrySet().stream().map(entry -> {
                aggregateBuffer.position((int) entry.getValue().offset);
                byte[] bytes = new byte[(int) entry.getValue().length];
                aggregateBuffer.get(bytes);
                return new Tuple2<>(entry.getKey(), bytes);
            }).collect(Collectors.toList());
            aggregateBuffer.clear();
            indexMap.clear();
            size.set(0);
            immutable = false;
            list.forEach(tuple -> this.append(tuple.var1, tuple.var2, null));
        } catch (Exception e) {
            log.error("reassemble error", e);
        }
    }

    /**
     * 扩展聚合块的大小
     *
     * @param additionalSize 需要扩展的大小
     * @return 扩展后的聚合块是否成功
     */
    private boolean expandBuffer(int additionalSize) {
        int newSize = aggregateBuffer.capacity() + additionalSize;
        if (newSize > capacity) {
            return false; // 超出最大容量
        }
        ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
        aggregateBuffer.flip();
        newBuffer.put(aggregateBuffer);
        aggregateBuffer = newBuffer;
        this.capacity = newSize;
        return true;
    }

    /**
     * 清空聚合容器
     */
    public void clear() {
        lock.lock();
        try {
            indexMap.clear();
            aggregateBuffer.clear();
            size.set(0);
            immutable = false;
            isFlushing = false;
        } catch (Exception e) {
            log.error(containerId + " clear error", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断聚合容器是否为空
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return indexMap.isEmpty();
        } finally {
            lock.unlock();
        }
    }

}
