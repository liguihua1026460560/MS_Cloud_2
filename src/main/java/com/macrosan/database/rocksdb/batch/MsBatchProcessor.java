package com.macrosan.database.rocksdb.batch;

import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class MsBatchProcessor<T> implements Runnable {
    Scheduler scheduler;
    Queue<T> queue = Queues.<T>unboundedMultiproducer().get();
    AtomicInteger wip = new AtomicInteger();
    CoreSubscriber<List<T>> actual;

    MsBatchProcessor(Scheduler scheduler) {
        this.scheduler = scheduler;
    }


    public void onNext(T t) {
        queue.add(t);

        if (wip.getAndIncrement() != 0) {
            return;
        }

        scheduler.schedule(this);

    }

    public void subscribe(CoreSubscriber<List<T>> actual) {
        this.actual = actual;
    }

    @Override
    public void run() {
        int missed = 1;
        for (; ; ) {
            List<T> list = new LinkedList<>();
            T t;
            while (null != (t = queue.poll())) {
                list.add(t);

                if (list.size() > 1000) {
                    break;
                }
            }

            actual.onNext(list);

            missed = wip.addAndGet(-missed);
            if (missed == 0) {
                break;
            }
        }
    }
}
