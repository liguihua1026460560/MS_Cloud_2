package com.macrosan.storage.coder.fs;

import com.macrosan.storage.codec.ErasureCodc;
import com.macrosan.storage.coder.Encoder;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

/**
 * 仅支持fileSize小于packetSize时使用
 * 仅支持一次put
 */
public class LowPackageErasureEncoder implements Encoder {
    private final byte[][] srcBytes;
    private final byte[][] encodeBytes;
    private final int packageSize;
    private final UnicastProcessor<byte[]>[] dataFluxes;
    private final int fileSize;
    ErasureCodc codc;
    int x = 0, y = 0;

    public LowPackageErasureEncoder(int k, int m, int fileSize, ErasureCodc codc) {
        this.fileSize = fileSize;
        packageSize = fileSize / 64 / k * 64 + 64;
        srcBytes = new byte[k][packageSize];
        encodeBytes = new byte[m][packageSize];
        dataFluxes = new UnicastProcessor[k + m];
        for (int i = 0; i < k + m; i++) {
            dataFluxes[i] = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());
        }

        this.codc = codc;
    }

    @Override
    public void put(byte[] bytes) {
        if (bytes.length > fileSize) {
            throw new UnsupportedOperationException("LowPackageErasureEncoder fileSize not match");
        } else if (bytes.length == fileSize) {
            for (int i = 0; i < srcBytes.length; i++) {
                int copyLen = Math.min(packageSize, bytes.length - i * packageSize);
                if (copyLen < 0) {
                    break;
                }
                System.arraycopy(bytes, i * packageSize, srcBytes[i], 0, copyLen);
            }

            codc.encode(srcBytes, encodeBytes, packageSize / 64);
            x = 0;
            y = 0;
        } else {
            int last = bytes.length;

            do {
                int copyLen = Math.min(packageSize - y, last);
                System.arraycopy(bytes, bytes.length - last, srcBytes[x], y, copyLen);
                last -= copyLen;
                y += copyLen;

                if (y == packageSize) {
                    x++;
                    y = 0;
                }
            } while (last > 0);

            if (x == srcBytes.length) {
                codc.encode(srcBytes, encodeBytes, packageSize / 64);
                x = 0;
                y = 0;
            }
        }
    }

    @Override
    public void complete() {
        if (!(x == 0 && y == 0)) {
            codc.encode(srcBytes, encodeBytes, packageSize / 64);
        }
        for (int i = 0; i < srcBytes.length; i++) {
            dataFluxes[i].onNext(srcBytes[i]);
        }

        for (int i = 0; i < encodeBytes.length; i++) {
            dataFluxes[i + srcBytes.length].onNext(encodeBytes[i]);
        }
    }

    @Override
    public UnicastProcessor<byte[]>[] data() {
        return dataFluxes;
    }

    @Override
    public long size() {
        return fileSize;
    }

    @Override
    public void flush() {}
}
