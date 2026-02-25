package com.macrosan.rsocket;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class MsPublishOn<T> extends Mono<T> {
    Mono<? extends T> source;
    CoreSubscriber<? super T> actual;
    ScheduledExecutorService executor;

    public MsPublishOn(Mono<? extends T> source, ScheduledExecutorService executor) {
        this.source = source;
        this.executor = executor;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.actual = actual;
        source.subscribe(new MsPublishOnSub());

    }

    private class MsPublishOnSub implements CoreSubscriber<T>, Runnable {
        T value;
        AtomicBoolean done = new AtomicBoolean(false);
        Subscription s;
        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            if (done.compareAndSet(false, true)) {
                value = t;
                executor.submit(this);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done.compareAndSet(false, true)) {
                executor.submit(() -> {
                    actual.onSubscribe(s);
                    actual.onError(throwable);
                });
            }
        }

        @Override
        public void onComplete() {
            if (done.compareAndSet(false, true)) {
                executor.submit(this);
            }
        }

        @Override
        public void run() {
            T t;
            if ((t = value) != null) {
                actual.onNext(t);
            }
            actual.onComplete();
        }


    }


}
