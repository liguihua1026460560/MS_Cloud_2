package com.macrosan.filesystem.utils.timeout;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class TimeoutSubscriber<T> implements CoreSubscriber<T> {
    CoreSubscriber<T> actual;
    Subscription s;

    TimeoutSubscriber(CoreSubscriber<T> actual) {
        this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.s = s;
        actual.onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
        actual.onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
        actual.onError(throwable);
    }

    @Override
    public void onComplete() {
        actual.onComplete();
    }
}
