package com.macrosan.inventory.datasource.scanner;

import com.macrosan.inventory.InventoryService;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.channel.ListInventoryIncrementalMergeChannel;
import com.macrosan.utils.functional.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class InventoryListStampScanner implements Scanner<Tuple2<byte[], byte[]>>, AutoCloseable {
    private final BlockingQueue<Tuple2<byte[], byte[]>> queue;
    private final String bucket;
    private final StoragePool storagePool;
    private final List<String> shardNodes;
    private final String endStamp;
    private volatile String currentSnapshotMark;

    private final ScannerConfig config;

    private final CompletableFuture<Void> scannerFuture = new CompletableFuture<>();


    private final AtomicReference<ScannerState> state = new AtomicReference<>(ScannerState.CLOSED);
    private final AtomicReference<String> seekMarker;

    private final AtomicReference<UnicastProcessor<String>> listSink = new AtomicReference<>();

    public InventoryListStampScanner(String bucket, String stamp, boolean isCurrent) {
        this.config = new ScannerConfig(1000, isCurrent,5000, 30000, 30000);
        this.queue = new LinkedBlockingQueue<>(config.getQueueCapacity());
        this.bucket = bucket;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        this.shardNodes = storagePool.getBucketVnodeList(bucket);
        this.endStamp = String.valueOf(System.currentTimeMillis());
        this.seekMarker = new AtomicReference<>(initializeSeekMarker(stamp));
    }

    private String initializeSeekMarker(String stamp) {
        if (StringUtils.isEmpty(stamp)) {
            return getStampKey(bucket, "0");
        }
        return stamp.startsWith(bucket) ? stamp : getStampKey(bucket, stamp);
    }

    @Override
    public void start(String stampKey) {
        if (StringUtils.isNotEmpty(stampKey)) {
            seekMarker.set(stampKey);
        }
        // 清空队列中旧的数据
        queue.clear();
        // 重置processor
        UnicastProcessor<String> processor = listSink.get();
        if (processor != null && !processor.hasCompleted()) {
            processor.onComplete();
        }
        state.set(ScannerState.INIT);
        UnicastProcessor<String> listProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        listSink.set(listProcessor);
        // 使用 Flux 处理流式数据
        listProcessor
                .flatMap(this::createListRequest)
                .flatMap(this::processRequest)
                .publishOn(InventoryService.INVENTORY_SCHEDULER)
                .subscribe(
                        this::handleSuccess,
                        this::handleError,
                        this::handleComplete
                );

        // 触发首次列举
        listProcessor.onNext(seekMarker.get());
    }

    private Mono<SocketReqMsg> createListRequest(String marker) {
        return Mono.just(new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("maxKeys", String.valueOf(config.getMaxKeys()))
                .put("beginStamp", marker)
                .put("endStamp", endStamp)
                .put("isCurrent", String.valueOf(config.isCurrent())));
    }

    private Mono<ListResult> processRequest(SocketReqMsg msg) {
        return SnapshotUtil.fetchBucketSnapshotInfo(bucket, msg)
                .then(Mono.defer(() -> {
                    currentSnapshotMark = msg.get("currentSnapshotMark");
                    ListInventoryIncrementalMergeChannel channel = createChannel(msg);
                    channel.request(msg);
                    return channel.response()
                            .map(success -> new ListResult(success, channel));
                }));
    }

    private ListInventoryIncrementalMergeChannel createChannel(SocketReqMsg msg) {
        return new ListInventoryIncrementalMergeChannel(
                bucket,
                storagePool,
                shardNodes,
                null,
                config.getMaxKeys(),
                msg.get("beginStamp"),
                msg.get("snapshotLink")
        );
    }

    private void handleSuccess(ListResult result) {
        if (!result.isSuccess()) {
            handleError(new RuntimeException("List operation failed"));
            return;
        }

        try {
            for (Tuple2<byte[], byte[]> item : result.getChannel().linkedList) {
                if (!queue.offer(item, config.getOfferTimeout(), TimeUnit.MILLISECONDS)) {
                    log.error("Queue offer timeout for item in bucket: {}", bucket);
                    throw new ScannerException("Queue offer timeout for item in bucket: " + bucket);
                }
            }

            String nextMarker = result.getChannel().getNextMarker();
            if (StringUtils.isNotEmpty(nextMarker)) {
                seekMarker.set(nextMarker);
                listSink.get().onNext(nextMarker);
            } else {
                state.set(ScannerState.COMPLETED);
                listSink.get().onComplete();
            }
        } catch (Exception e) {
            handleError(e);
        }
    }

    private void handleError(Throwable error) {
        log.error("Error in inventory scanner", error);
        state.set(ScannerState.ERROR);
        scannerFuture.completeExceptionally(error);
    }

    private void handleComplete() {
        // nothing to do
    }

    @Override
    public Tuple2<byte[], byte[]> next() throws Exception {
        if (state.get() == ScannerState.CLOSED) {
            throw new ScannerException("Scanner is closed");
        }

        if (state.get() == ScannerState.COMPLETED && queue.isEmpty()) {
            return null;
        }

        // 使用带超时的阻塞获取
        Tuple2<byte[], byte[]> item = queue.poll(config.getPollTimeout(), TimeUnit.MILLISECONDS);
        if (item != null) {
            return item;
        }

        if (state.get() == ScannerState.COMPLETED) {
            return null;
        }

        if (state.get() == ScannerState.ERROR) {
            throw new ScannerException("Scanner is in error state");
        }

        // 等待更多数据
        return next();
    }

    @Override
    public void seek(byte[] point) {
        // 实现seek逻辑
    }

    @Override
    public void release() {
        try {
            close();
        } catch (Exception e) {
            log.error("Error closing scanner", e);
        }
    }

    @Override
    public void close() {
        UnicastProcessor<String> processor = listSink.get();
        if (processor != null && !processor.hasCompleted()) {
            processor.onComplete();
        }
        queue.clear();
        state.set(ScannerState.CLOSED);
    }

    @Override
    public String getCurrentSnapshotMark() {
        return currentSnapshotMark;
    }

    @Data
    @AllArgsConstructor
    private static class ListResult {
        private final boolean success;
        private final ListInventoryIncrementalMergeChannel channel;
    }

    private enum ScannerState {
        INIT, RUNNING, COMPLETED, ERROR, CLOSED
    }

    private static String getStampKey(String bucket, String stamp) {
        return bucket + File.separator + stamp + File.separator;
    }

    @Data
    @Builder
    static class ScannerConfig {
        private final int maxKeys;

        private final boolean isCurrent;

        @Builder.Default
        private final int queueCapacity = 10000;

        @Builder.Default
        private final long offerTimeout = 300000; // 5min

        @Builder.Default
        private final long pollTimeout = 60000; // 1min
    }

    static class ScannerException extends RuntimeException {
        public ScannerException(String message) {
            super(message);
        }

        public ScannerException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
