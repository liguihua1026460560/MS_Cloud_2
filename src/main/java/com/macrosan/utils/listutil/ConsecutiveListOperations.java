package com.macrosan.utils.listutil;

import com.macrosan.utils.listutil.interpcy.ExhaustionPolicy;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public abstract class ConsecutiveListOperations<T> implements ListOperation<T> {
    protected InterruptPolicy<T> interruptPolicy = new ExhaustionPolicy<>();
    private final UnicastProcessor<T> flux = UnicastProcessor.create(Queues.<T>unboundedMultiproducer().get());
    private final AtomicReference<String> markerPoint = new AtomicReference<>("");
    @Getter
    @Setter
    protected ListOptions listOptions;
    protected AtomicLong bufferSize = new AtomicLong(0);

    private final Map<String, AtomicInteger> retryRecordMap = new HashMap<>();

    protected ConsecutiveListOperations() {
        this.listOptions = new ListOptions();
    }

    @Override
    public Flux<T> apply(String s) {
        s = s == null ? "" : s;
        markerPoint.set(s);
        UnicastProcessor<String> listController = UnicastProcessor.create();
        AtomicBoolean interrupt = new AtomicBoolean(false);
        listController.subscribe(marker -> {
            AtomicBoolean haveMore = new AtomicBoolean(false);
            listOnce(marker)
                    .timeout(Duration.ofMillis(listOptions.getMaxListTimeout()))
                    .subscribe(
                            item -> {
                                haveMore.set(true);
                                if (!interruptPolicy.shouldInterrupt(item)) {
                                    bufferSize.addAndGet(getSize(item));
                                    flux.onNext(item);
                                    markerPoint.set(getNextMarker(item));
                                } else {
                                    if (!interrupt.get()) {
                                        interrupt.set(true);
                                    }
                                }
                            },
                            e -> {
                                AtomicInteger tryNum = retryRecordMap.computeIfAbsent(marker, k -> new AtomicInteger(listOptions.getMaxListRetryCount()));
                                if (tryNum.decrementAndGet() > 0) {
                                    Mono.delay(Duration.ofMillis(1000)).subscribe(l -> listController.onNext(marker));
                                } else {
                                    flux.onError(e);
                                    listController.onComplete();
                                    retryRecordMap.remove(marker);
                                }
                            },
                            () -> {
                                retryRecordMap.remove(marker);
                                if (interrupt.get() || !haveMore.get()) {
                                    log.debug("list complete {} {}", interrupt.get(), haveMore.get());
                                    flux.onComplete();
                                    listController.onComplete();
                                } else {
                                    if (flux.size() >= listOptions.getBufferCount() || bufferSize.get() >= listOptions.getBufferSize()) {
                                        Disposable[] disposables = new Disposable[]{null};
                                        disposables[0] = Flux.interval(Duration.ofSeconds(1))
                                                .subscribe(l -> {
                                                    if (flux.size() < listOptions.getBufferCount() && bufferSize.get() < listOptions.getBufferSize()) {
                                                        disposables[0].dispose();
                                                        listController.onNext(markerPoint.get());
                                                    } else {
                                                        log.debug("wait bufferSize: " + bufferSize.get() + " bufferCount: " + flux.size());
                                                    }
                                                });
                                    } else {
                                        listController.onNext(markerPoint.get());
                                    }
                                }
                            }
                    );
        });
        listController.onNext(markerPoint.get());
        return flux.doOnNext(t -> bufferSize.addAndGet(-getSize(t)));
    }

    protected abstract Flux<T> listOnce(String marker);

    protected abstract String getNextMarker(T item);

    protected abstract long getSize(T item);
}
