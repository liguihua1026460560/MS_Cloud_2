package com.macrosan.inventory.datasource;

import com.macrosan.inventory.datasource.scanner.Scanner;
import com.macrosan.inventory.translator.AbstractStreamTranslator;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class InventoryDefaultDataSource implements DataSource {

    private final Scanner<Tuple2<byte[], byte[]>> rocksScanner;
    private final UnicastProcessor<Long> rocksScannerController = UnicastProcessor.create();
    private UnicastProcessor<Tuple2<byte[], byte[]>> byteFlux = UnicastProcessor.create(Queues.<Tuple2<byte[], byte[]>>unboundedMultiproducer().get());
    private AbstractStreamTranslator<MetaData> translator;
    private Disposable disposable;
    private volatile boolean isEnd = false;
    private final ReentrantLock lock = new ReentrantLock();

    public UnicastProcessor<Long> keepaliveProcessor = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());

    /**
     * 记录数据流导出的位置信息
     */
    private byte[] cursor;
    private boolean hasRemaining = true;

    private AtomicBoolean writeTitle = new AtomicBoolean(true);

    public InventoryDefaultDataSource(Scanner<Tuple2<byte[], byte[]>> rocksScanner) {
        this.rocksScanner = rocksScanner;
    }

    /**
     * 添加流转换器
     * @param translator 流转换器
     */
    public DataSource setTranslator(AbstractStreamTranslator<MetaData> translator) {
        this.translator = translator;
        return this;
    }


    @Override
    public Mono<Boolean> start() {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        try {
            AtomicLong skipCount = new AtomicLong(0);
            rocksScanner.start(cursor == null ? null : new String(cursor));
            disposable = rocksScannerController.doFinally((l) -> rocksScanner.release())
                    .publishOn(Schedulers.boundedElastic())
                    .subscribe(l -> {

                        if (writeTitle.get()) {
                            byteFlux.onNext(new Tuple2<>(null, translator.title));
                            writeTitle.set(false);
                            return;
                        }

                        Tuple2<byte[], byte[]> next = null;

                        try {
                            next = this.rocksScanner.next();
                        } catch (Exception e) {
                            log.error("", e);
                            byteFlux.onError(e);
                            return;
                        }

                        if (next != null) {
                            lock.lock();
                            try {
                                if (isEnd) {
                                    return;
                                }
                                byte[] bytes = next.var2;
                                //this.cursor = next.var1;

                                if (translator != null) {
                                    MetaData metaData = Json.decodeValue(new String(next.var2), MetaData.class);
                                    // 如果为deleteMark，则跳过
                                    if (metaData.deleteMark || metaData.isUnView(rocksScanner.getCurrentSnapshotMark())) {
                                        if (metaData.deleteMark) {
                                            log.debug("{} is deleteMark", metaData);
                                        } else {
                                            log.debug("{} is notVisible", metaData);
                                        }
                                        // 每次向下游发送一千条数据，发送一次心跳
                                        if (skipCount.getAndIncrement() >= 1000) {
                                            keepaliveProcessor.onNext(1L);
                                            skipCount.set(0);
                                        }
                                        fetch(1L);
                                        return;
                                    }
                                    bytes = translator.translate(metaData);
                                }
                                skipCount.set(0);
                                byteFlux.onNext(new Tuple2<>(next.var1, bytes));
                            } catch (Exception e) {
                                log.error("", e);
                                byteFlux.onError(e);
                            } finally {
                                lock.unlock();
                            }
                        } else {
                            hasRemaining = false;
                            byteFlux.onComplete();
                        }
                    });
            res.onNext(true);
        } catch (Exception e) {
            log.info("InventoryDataSource start fail!", e);
            res.onError(e);
        }
        return res;
    }

    @Override
    public void fetch(long n) {
        rocksScannerController.onNext(n);
    }

    @Override
    public Flux<Tuple2<byte[], byte[]>> data() {
        return byteFlux;
    }

    @Override
    public void complete() {
        lock.lock();
        try {
            isEnd = true;
            byteFlux.onComplete();
            keepaliveProcessor.onComplete();
            log.info("complete!");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release() {
        try {
            if (!rocksScannerController.hasCompleted()) {
                this.rocksScannerController.onComplete();
            }
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
            if (!byteFlux.hasCompleted() && !byteFlux.isDisposed()) {
                byteFlux.dispose();
            }
            if (!keepaliveProcessor.hasCompleted()) {
                keepaliveProcessor.onComplete();
            }
        } catch (Exception e) {
            log.error("InventoryDefaultDataSource release fail!", e);
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            this.isEnd = false;
            this.writeTitle.set(true);
            // 处置旧的数据通道
            if (!this.byteFlux.isDisposed()) {
                this.byteFlux.dispose();
            }
            // 处置就的keepalive通道
            if (!this.keepaliveProcessor.hasCompleted()) {
                this.keepaliveProcessor.onComplete();
            }
            // 设置新的数据通道
            this.byteFlux = UnicastProcessor.create(Queues.<Tuple2<byte[], byte[]>>unboundedMultiproducer().get());
            // 设置新的keepalive通道
            this.keepaliveProcessor = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        } catch (Exception e) {
            log.error("", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean hasRemaining() {
        return hasRemaining;
    }

    @Override
    public byte[] cursor(byte[] newCursor) {
        if (newCursor == null) {
            return cursor;
        }
        byte[] oldCursor = cursor;
        this.cursor = newCursor;
        return oldCursor != null ? oldCursor : new byte[]{0};
    }

    @Override
    public Scanner scanner() {
        return this.rocksScanner;
    }

    @Override
    public Flux<Long> keepalive() {
        return keepaliveProcessor;
    }
}
