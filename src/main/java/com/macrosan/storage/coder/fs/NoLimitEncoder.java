package com.macrosan.storage.coder.fs;

import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.coder.LimitEncoder;
import io.vertx.core.streams.ReadStream;
import reactor.core.publisher.UnicastProcessor;

public class NoLimitEncoder implements LimitEncoder {
    ReadStream request;
    Encoder encoder;

    public NoLimitEncoder(Encoder encoder, ReadStream request) {
        this.encoder = encoder;
        this.request = request;
    }

    @Override
    public void put(byte[] bytes) {
        encoder.put(bytes);
    }

    @Override
    public void complete() {
        encoder.complete();
    }

    @Override
    public UnicastProcessor<byte[]>[] data() {
        return encoder.data();
    }

    @Override
    public long size() {
        return encoder.size();
    }

    @Override
    public void request(int index, long n) {
        request.resume();
    }
    @Override
    public void flush() {encoder.flush();}
}
