package com.macrosan.storage.coder.direct;

import com.macrosan.storage.codec.ErasureCodc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import reactor.core.publisher.MonoProcessor;

public class ECDirectEncoder implements DirectEncoder {
    int k;
    int m;
    int packageSize;
    int size;
    ErasureCodc codc;
    private MonoProcessor<ByteBuf[]>[] dataFluxes;

    private static ByteBuf zeroBuf;

    static {
        zeroBuf = PooledByteBufAllocator.DEFAULT.directBuffer(128 << 10);
        zeroBuf.writerIndex(zeroBuf.capacity());
    }

    public ECDirectEncoder(int k, int m, int packetSize, ErasureCodc codc, int size) {
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
        int n = buf.readableBytes() / packageSize / k;
        int last = buf.readableBytes() - n * packageSize * k;
        int l = last > 0 ? last / 64 / k * 64 + 64 : 0;

        int num = l == 0 ? n : n + 1;

        ByteBuf[][] bufs = new ByteBuf[k + m][num];

        for (int i = 0; i < n; i++) {
            ByteBuf[] src = new ByteBuf[k];
            ByteBuf[] dst = new ByteBuf[m];

            for (int j = 0; j < k; j++) {
                src[j] = buf.retainedSlice(i * packageSize * k + j * packageSize, packageSize);
                bufs[j][i] = src[j];
            }

            for (int j = 0; j < m; j++) {
                dst[j] = PooledByteBufAllocator.DEFAULT.directBuffer(packageSize, packageSize);
                dst[j].writerIndex(packageSize);
                bufs[k + j][i] = dst[j];
            }

            codc.directEncode(src, dst);
        }

        if (l > 0) {
            ByteBuf[] src = new ByteBuf[k];
            ByteBuf[] dst = new ByteBuf[m];

            int start = n * packageSize * k;

            for (int j = 0; j < k; j++) {
                if (last >= l) {
                    src[j] = buf.retainedSlice(start, l);
                    start += l;
                    last -= l;
                } else if (last > 0) {
                    src[j] = PooledByteBufAllocator.DEFAULT.directBuffer(l, l);
                    src[j].writeBytes(buf.slice(start, last));
                    src[j].writerIndex(l);
                    start += last;
                    last = 0;
                }
                //全0
                else {
                    src[j] = zeroBuf.retainedSlice(0, l);
                }

                bufs[j][n] = src[j];
            }

            for (int j = 0; j < m; j++) {
                dst[j] = PooledByteBufAllocator.DEFAULT.directBuffer(l, l);
                dst[j].writerIndex(l);
                bufs[k + j][n] = dst[j];
            }

            codc.directEncode(src, dst);
        }


        for (int i = 0; i < k + m; i++) {
            dataFluxes[i].onNext(bufs[i]);
        }
    }

    public MonoProcessor<ByteBuf[]>[] data() {
        return dataFluxes;
    }
}
