package com.macrosan.storage.coder.fs;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.utils.ReadObjClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class FsReplicaDecoder extends Flux<Tuple2<Integer, byte[]>> {
    int readIndex;
    StoragePool pool;
    String fileName;
    long offset;
    long size;
    List<Tuple3<String, String, String>> nodeList;
    AtomicInteger index = new AtomicInteger();
    CoreSubscriber<? super Tuple2<Integer, byte[]>> actual;
    AtomicBoolean done = new AtomicBoolean();
    boolean isPrefetch = false;

    public static final Subscription EMPTY_SUBSCRIPTION = new Subscription() {
        @Override
        public void request(long l) {

        }

        @Override
        public void cancel() {

        }
    };

    public FsReplicaDecoder(int readIndex, StoragePool pool, String fileName, long offset, long size, boolean isPrefetch) {
        this.readIndex = readIndex;
        this.pool = pool;
        this.fileName = fileName;
        this.offset = offset;
        this.size = size;
        this.isPrefetch = isPrefetch;
        nodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(fileName)).block();
    }


    @Override
    public void subscribe(CoreSubscriber<? super Tuple2<Integer, byte[]>> actual) {
        this.actual = actual;
        actual.onSubscribe(EMPTY_SUBSCRIPTION);
        for (int i = 0; i < pool.getK(); i++) {
            read(index.getAndIncrement());
        }
    }

    private void read(int index0) {
        Tuple3<String, String, String> tuple3 = nodeList.get(index0);
        ReadObjClient.readOnce(tuple3.var1, tuple3.var2, fileName, offset, offset + size)
                .subscribe(t -> {
                    if (t.var1) {
                        if (done.compareAndSet(false, true)) {
                            for (byte[] bytes : t.var2) {
                                actual.onNext(new Tuple2<>(readIndex, bytes));
                                readIndex += bytes.length;
                            }

                            actual.onComplete();
                        }

                    } else {
                        int next = index.getAndIncrement();
                        if (next < nodeList.size()) {
                            read(next);
                        } else {
                            if (done.compareAndSet(false, true)) {
                                if (isPrefetch) {
                                    actual.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "pre-read data modified(replica)- " + fileName));
                                } else {
                                    log.error("read object {} fail", fileName);
                                    actual.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "read object fail"));
                                }
                            }
                        }
                    }
                }, e -> {
                    if (done.compareAndSet(false, true)) {
                        log.error("", e);
                        actual.onError(e);
                    }
                });
    }
}
