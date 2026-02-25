package com.macrosan.storage.coder;

import com.macrosan.constants.ServerConstants;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.codec.ErasureCodc;

import com.macrosan.utils.cache.MsPackagePool;
import com.macrosan.utils.msutils.md5.Digest;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.UnicastProcessor;

import java.util.Arrays;

/**
 * 处理原始数据，形成纠删编码后的二元数组，每一行数组（数据块）各自由特定的fluxSink发出
 *
 * @author gaozhiyuan
 */
@Slf4j
public class ErasureEncoder implements Encoder {
    /**
     * 原始一元字节数组经过排列的二元数组，行数为原始数据块数，列数为数据块大小
     */
    private byte[][] srcBytes;

    /**
     * 保存纠删编码后的bytes，行数为校验数据块数，列数为数据块大小
     */
    private byte[][] encodeBytes;

    private UnicastProcessor<byte[]>[] dataFluxes;

    /**
     * 当前读到第几个数据块的指针，最大为k
     */
    private int curX = 0;

    /**
     * 在当前数据块中读到第几个字节的指针,最大为packetSize
     */
    private int curY = 0;

    private int k;
    private int m;
    private int packetSize;
    private ErasureCodc codc;
    private int index;
    private long flushTime;
    protected Digest digest;

    @SuppressWarnings("unchecked")
    public ErasureEncoder(int k, int m, int packetSize, ErasureCodc codc) {
        this.k = k;
        this.m = m;
        this.packetSize = packetSize;
        this.codc = codc;
        dataFluxes = new UnicastProcessor[k + m];

        for (int i = 0; i < k + m; i++) {
            dataFluxes[i] = UnicastProcessor.create();
        }

        if (packetSize == StoragePoolFactory.DEFAULT_PACKAGE_SIZE) {
            index = MsPackagePool.getIndex();
            srcBytes = MsPackagePool.acquire(k, index);
            encodeBytes = MsPackagePool.acquire(m, index);
        } else {
            srcBytes = new byte[k][packetSize];
            encodeBytes = new byte[m][packetSize];
        }
    }

    /**
     * srcBytes是一个k * packetSize的二维数组，将原始数据保存其中。
     * 保存完毕后，执行flush
     *
     * @param bytes 原始数据的字节数组
     */
    @Override
    public void put(byte[] bytes) {
        int index = 0;
        size += bytes.length;

        while (index < bytes.length) {
            int copyLen = Math.min(bytes.length - index, packetSize - curY);
            //将字节数据拷贝进srcBytes
            System.arraycopy(bytes, index, srcBytes[curX], curY, copyLen);
            index += copyLen;
            curY += copyLen;
            //读完了一个数据块后，读下一个数据块
            if (curY >= packetSize) {
                curY = 0;
                curX++;
            }
            //读完了所有数据，flush
            if (curX == k) {
                flush();
            }
        }

        if (flushTime != 0 && DateChecker.getCurrentTime() - flushTime > 30_000L) {
            for (int i = 0; i < dataFluxes.length; i++) {
                dataFluxes[i].onNext(ServerConstants.DEFAULT_DATA);
            }
        }
    }

    @Override
    public void put(Buffer buffer) {
        try {
            int index = 0;
            int length = buffer.length();
            size += length;

            while (index < length) {
                int copyLen = Math.min(length - index, packetSize - curY);
                //将字节数据拷贝进srcBytes
                buffer.getBytes(index, index + copyLen, srcBytes[curX], curY);
                index += copyLen;
                curY += copyLen;
                //读完了一个数据块后，读下一个数据块
                if (curY >= packetSize) {
                    curY = 0;
                    curX++;
                }
                //读完了所有数据，flush
                if (curX == k) {
                    flush();
                }
            }
            if (flushTime != 0 && DateChecker.getCurrentTime() - flushTime > 30_000L) {
                for (int i = 0; i < dataFluxes.length; i++) {
                    dataFluxes[i].onNext(ServerConstants.DEFAULT_DATA);
                }
            }
        } catch (Exception e) {
            log.info("flush error", e);
        }
    }


    /**
     * 原始和校验数据块的发出
     */
    @Override
    public void flush() {
        flushTime = DateChecker.getCurrentTime();
        codc.encode(srcBytes, encodeBytes);
        // 原始数据块由前k个FluxSink发出
        for (int i = 0; i < k; i++) {
            dataFluxes[i].onNext(srcBytes[i]);
        }
        // 校验数据块由后m个FluxSink发出
        for (int i = 0; i < m; i++) {
            dataFluxes[i + k].onNext(encodeBytes[i]);
        }

        curX = 0;
        curY = 0;
        srcBytes = new byte[k][packetSize];
        encodeBytes = new byte[m][packetSize];
    }

    /**
     * 所有数据都完成输入后的最后一次下刷
     * 下发的数据块根据实际大小切片
     */
    private void lastFlush() {
        //最后一次要下刷的数据总长度
        int curLen = curX * packetSize + curY;
        //每个原始的数据块期望的长度（srcBytes每行的长）。l一定小于等于packetSize
        int l = curLen / 64 / k * 64 + 64;
        //期望长度等于packetSize，此次下刷依然和前面一样处理
        if (l == packetSize) {
            flush();
            return;
        }

        //保存srcBytes切片后的数据，每行有效长度为l
        byte[][] realSrcBytes;
        if (packetSize == StoragePoolFactory.DEFAULT_PACKAGE_SIZE) {
            realSrcBytes = MsPackagePool.acquire(k, index);
        } else {
            realSrcBytes = new byte[k][packetSize];
        }

        // 此时curY为srcBytes最后一行的长度-1，curX为srcBytes行数-1。
        //  x,y为指针位置。x记录srcBytes中已经复制到第几行，y记录该行已复制到哪个位置。
        int x = 0, y = 0;
        for (int i = 0; i < k; i++) {
            //lastLen，表示sryBytes在x行还剩余多少长度没有复制给realSrcBytes
            int lastLen = x == curX ? curY - y : packetSize - y;
            if (lastLen > l) {
                // srcByetes该行待复制的长度超出了期望的行长度l，记下srcBytes在x行已复制的位置，剩余部分转到realSrcBytes的下一行（i+1）继续复制
                System.arraycopy(srcBytes[x], y, realSrcBytes[i], 0, l);
                y += l;
            } else if (lastLen == l) {
                //srcByetes该行待复制的长度正好等于期望的行长度，复制入realBytes，开始下一行数据处理
                System.arraycopy(srcBytes[x], y, realSrcBytes[i], 0, l);
                x++;
                y = 0;
            } else if (lastLen > 0) {
                //srcByetes该行待复制的长度小于期望的行长度，复制入realBytes，再去srcBytes的下一行(x++)取数据补全到l，记录y
                System.arraycopy(srcBytes[x], y, realSrcBytes[i], 0, lastLen);
                x++;
                System.arraycopy(srcBytes[x], 0, realSrcBytes[i], lastLen, l - lastLen);
                y = l - lastLen;
            }
        }

        codc.encode(realSrcBytes, encodeBytes, l / 64);

        // 发出的校验块是截取过的，每行长度为l
        for (int i = 0; i < k; i++) {
            dataFluxes[i].onNext(Arrays.copyOf(realSrcBytes[i], l));
        }

        for (int i = 0; i < m; i++) {
            dataFluxes[i + k].onNext(Arrays.copyOf(encodeBytes[i], l));
        }

        curX = 0;
        curY = 0;
        MsPackagePool.release(index, srcBytes);
        MsPackagePool.release(index, encodeBytes);
        MsPackagePool.release(index, realSrcBytes);
    }

    /**
     * 确保flush操作已完成。
     * <p>结束每个fluxSink。</p>
     */
    @Override
    public void complete() {
        try {
            // 两个指针若都为0，表明flush已结束。
            if (curX == 0 && curY == 0) {
                return;
            }
            lastFlush();
        } catch (Exception e) {
            for (int i = 0; i < k + m; i++) {
                dataFluxes[i].onError(e);
            }
        } finally {
            for (int i = 0; i < k + m; i++) {
                dataFluxes[i].onComplete();
            }
        }
    }

    @Override
    public UnicastProcessor<byte[]>[] data() {
        return dataFluxes;
    }

    long size = 0;

    @Override
    public long size() {
        return size;
    }
}
