package com.macrosan.storage.coder.direct;

import com.macrosan.storage.codec.ErasureCodc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import javafx.print.Collation;
import reactor.core.publisher.MonoProcessor;

import java.text.CollationKey;
import java.util.Collections;

public class ReplicaDirectEncoder  implements DirectEncoder{
    int k;
    int m;
    int packageSize;
    int size;
    ErasureCodc codc;
    private MonoProcessor<ByteBuf[]>[] dataFluxes;

    public ReplicaDirectEncoder(int k, int m, int packetSize, ErasureCodc codc, int size) {
        this.k = k;
        this.m = m;
        this.packageSize = packetSize;
        this.size = size;
        this.codc = codc;
        dataFluxes = new MonoProcessor[k + m];

        for (int i = 0; i < k + m; i++) {
            dataFluxes[i] = MonoProcessor.create();
        }
    }

    public void putAndComplete(ByteBuf buf) {
        for (int i = 0; i < k+m; i++) {
            dataFluxes[i].onNext(new ByteBuf[]{buf.retainedSlice()});
        }
    }

    public MonoProcessor<ByteBuf[]>[] data() {
        return dataFluxes;
    }
}
