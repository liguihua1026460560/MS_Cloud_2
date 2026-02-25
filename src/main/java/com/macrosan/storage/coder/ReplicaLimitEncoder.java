package com.macrosan.storage.coder;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;

/**
 * @author gaozhiyuan
 */
public class ReplicaLimitEncoder extends ReplicaEncoder implements LimitEncoder {
    @Getter
    private Limiter limiter;

    public ReplicaLimitEncoder(int k, int m, int packageSize, ReadStream request) {
        super(k, m, packageSize);
        limiter = new Limiter(request, super.data().length, k);
    }

    @Override
    public void put(byte[] bytes) {
        super.put(bytes);
        limiter.addFetchN();
    }
    @Override
    public void put(Buffer bytes) {
        super.put(bytes);
        limiter.addFetchN();
    }

    @Override
    public void flush() {
        super.flush();
        limiter.addPublish();
    }

    @Override
    public void request(int index, long n) {
        limiter.request(index, n);
    }
}
