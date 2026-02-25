package com.macrosan.storage.coder;

import com.macrosan.constants.ServerConstants;
import com.macrosan.httpserver.DateChecker;
import io.vertx.core.buffer.Buffer;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

/**
 * @author gaozhiyuan
 */
public class ReplicaEncoder implements Encoder {
    private UnicastProcessor<byte[]>[] dataFluxes;

    private byte[] srcBytes;
    private int index = 0;
    private int packageSize;

    private long flushTime;

    @SuppressWarnings("unchecked")
    public ReplicaEncoder(int k, int m, int packageSize) {
        this.packageSize = packageSize;
        dataFluxes = new UnicastProcessor[k + m];

        for (int i = 0; i < k + m; i++) {
            dataFluxes[i] = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());
        }

        srcBytes = new byte[packageSize];
    }

    @Override
    public void put(byte[] bytes) {
        size += bytes.length;
        int i = 0;

        while (i < bytes.length) {
            int copyLen = Math.min(bytes.length - i, srcBytes.length - index);
            //将字节数据拷贝进srcBytes
            System.arraycopy(bytes, i, srcBytes, index, copyLen);
            index += copyLen;
            i += copyLen;

            //读完了所有数据，flush
            if (index == srcBytes.length) {
                flush();
            }
        }

        if (flushTime != 0 && DateChecker.getCurrentTime() - flushTime > 30_000L) {
            for (UnicastProcessor<byte[]> dataFlux : dataFluxes) {
                dataFlux.onNext(ServerConstants.DEFAULT_DATA);
            }
        }
    }

    @Override
    public void put(Buffer buffer) {
        int length = buffer.length();
        size += length;
        int i = 0;

        while (i < length) {
            int copyLen = Math.min(length - i, srcBytes.length - index);
            //将字节数据拷贝进srcBytes
            buffer.getBytes(i, i + copyLen, srcBytes, index);
            index += copyLen;
            i += copyLen;

            //读完了所有数据，flush
            if (index == srcBytes.length) {
                flush();
            }
        }

        if (flushTime != 0 && DateChecker.getCurrentTime() - flushTime > 30_000L) {
            for (UnicastProcessor<byte[]> dataFlux : dataFluxes) {
                dataFlux.onNext(ServerConstants.DEFAULT_DATA);
            }
        }
    }

    @Override
    public void flush() {
        flushTime = DateChecker.getCurrentTime();
        for (UnicastProcessor<byte[]> dataFlux : dataFluxes) {
            dataFlux.onNext(srcBytes);
        }

        index = 0;
        srcBytes = new byte[packageSize];
    }

    private void lastFlush() {
        byte[] bytes = new byte[index];
        System.arraycopy(srcBytes, 0, bytes, 0, index);
        for (UnicastProcessor<byte[]> dataFlux : dataFluxes) {
            dataFlux.onNext(bytes);
        }

        index = 0;
    }

    @Override
    public void complete() {
        if (index != 0) {
            lastFlush();
        }

        for (UnicastProcessor<byte[]> data : dataFluxes) {
            data.onComplete();
        }
    }

    @Override
    public UnicastProcessor<byte[]>[] data() {
        return dataFluxes;
    }

    long size = 0L;

    @Override
    public long size() {
        return size;
    }
}
