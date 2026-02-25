package com.macrosan.message.socketmsg;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;

/**
 * EmptySocket
 * <p>
 * 空对象模式
 *
 * @author liyixin
 * @date 2019/1/8
 */
public class EmptySocket extends io.vertx.reactivex.core.net.NetSocket {

    private static final EmptySocket INSTANCE = new EmptySocket(null);

    public static NetSocket getInstance() {
        return INSTANCE;
    }

    private EmptySocket(io.vertx.core.net.NetSocket delegate) {
        super(delegate);
    }

    @Override
    public io.vertx.core.net.NetSocket getDelegate() {
        return null;
    }

    @Override
    public NetSocket exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    @Override
    public NetSocket handler(Handler<Buffer> handler) {
        return this;
    }

    @Override
    public NetSocket pause() {
        return this;
    }

    @Override
    public NetSocket resume() {
        return this;
    }

    @Override
    public NetSocket fetch(long amount) {
        return this;
    }

    @Override
    public NetSocket endHandler(Handler<Void> endHandler) {
        return this;
    }

    @Override
    public Observable<Buffer> toObservable() {
        return null;
    }

    @Override
    public Flowable<Buffer> toFlowable() {
        return null;
    }

    @Override
    public NetSocket write(Buffer data) {
        return this;
    }

    @Override
    public void end() {

    }

    @Override
    public void end(Buffer buffer) {

    }

    @Override
    public NetSocket setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public NetSocket drainHandler(Handler<Void> handler) {
        return this;
    }

    @Override
    public void close() {
    }
}
