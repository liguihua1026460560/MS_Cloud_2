package com.macrosan.filesystem.utils.timeout;

import com.macrosan.filesystem.utils.timeout.MsLinkedList.Node;
import com.macrosan.filesystem.utils.timeout.TimeOutRunner.TimeOutTask;
import com.macrosan.utils.functional.Tuple2;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FastMonoTimeOut<T> extends MonoOperator<T, T> {
    TimeoutSubscriber<? super T> actual;
    Duration timeout;
    AtomicBoolean cancel = new AtomicBoolean(false);
    TimeOutRunner timeoutRunner;

    private FastMonoTimeOut(Mono<? extends T> source, Duration timeout) {
        super(source);
        this.timeout = timeout;
        this.timeoutRunner = TimeOutRunner.getInstance(timeout);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual0) {
        this.actual = new TimeoutSubscriber<>(actual0);

        Tuple2<Integer, Node<TimeOutTask>> tuple2 = timeoutRunner.addTimeOut(this::timeout);
        source.doFinally(s -> {
                    cancel(tuple2);
                })
                .subscribe(actual);

    }

    private void timeout() {
        if (cancel.compareAndSet(false, true)) {
            TimeoutException exception = new TimeoutException("Did not observe any item or terminal signal within "
                    + timeout.toMillis() + "ms" + " (and no fallback has been configured)");
            actual.s.cancel();
            actual.onError(exception);
        }
    }

    private void cancel(Tuple2<Integer, Node<TimeOutTask>> tuple2) {
        if (cancel.compareAndSet(false, true)) {
            timeoutRunner.cancelTimeOut(tuple2.var1, tuple2.var2);
        }
    }

    public static <T> Mono<T> fastTimeout(Mono<T> source, Duration timeout) {
        return new FastMonoTimeOut<>(source, timeout);
    }
}
