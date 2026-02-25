package com.macrosan.storage.coder.fs;

import com.macrosan.storage.coder.Encoder;
import reactor.core.publisher.UnicastProcessor;

public class LowPackageReplicaEncoder implements Encoder {
    private byte[] bytes;
    private final UnicastProcessor<byte[]>[] dataFluxes;
    private final int fileSize;
    int index = 0;

    @SuppressWarnings("unchecked")
    public LowPackageReplicaEncoder(int k, int m, int fileSize) {
        this.fileSize = fileSize;
        dataFluxes = new UnicastProcessor[k + m];
        for (int i = 0; i < dataFluxes.length; i++) {
            dataFluxes[i] = UnicastProcessor.create();
        }
    }

    @Override
    public void put(byte[] bytes) {
        if (bytes.length > fileSize) {
            throw new UnsupportedOperationException("LowPackageReplicaEncoder fileSize not match");
        } else if (bytes.length == fileSize) {
            this.bytes = bytes;
        } else {
            if (this.bytes == null) {
                this.bytes = new byte[fileSize];
            }

            System.arraycopy(bytes, 0, this.bytes, index, bytes.length);
            index += bytes.length;
        }
    }

    @Override
    public void complete() {
        for (UnicastProcessor<byte[]> dataFlux : dataFluxes) {
            dataFlux.onNext(bytes);
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
