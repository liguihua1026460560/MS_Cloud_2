package com.macrosan.storage.codec;

import com.macrosan.fs.AioChannel;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.utils.msutils.UnsafeUtils.unsafe;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ErasureCodc {
    private int k;
    private int m;
    private int packetSize;
    private int operateNum;

    private int[][] encoderScheduler;

    private long schdulerAddr;

    private static final Map<String, int[][]> SCHEDULER_MAP = new ConcurrentHashMap<>();

    private static int[][] getScheduler(int k, int m, int[] erasures) {
        String key = k + "+" + m + (erasures == null ? "" : Arrays.toString(erasures));
        int[][] res = SCHEDULER_MAP.get(key);

        if (null == res) {
            synchronized (SCHEDULER_MAP) {
                res = SCHEDULER_MAP.get(key);
                if (null == res) {
                    res = ErasureCodecFactory.loadEncodeScheduler(k, m, erasures);
                    SCHEDULER_MAP.put(key, res);
                }
            }
        }

        return res;
    }

    static {
        boolean c = DirectCoder.isSupport;
    }

    public ErasureCodc(int k, int m, int packetSize) {
        this.k = k;
        this.m = m;
        this.packetSize = packetSize;

        operateNum = packetSize / 64;

        encoderScheduler = getScheduler(k, m, null);
        initDirectScheduler();
    }

    private void initDirectScheduler() {
        //每个encoderScheduler有5个int
        schdulerAddr = unsafe.allocateMemory(encoderScheduler.length * 5 * 4);
        for (int i = 0; i < encoderScheduler.length; i++) {
            int[] opt = encoderScheduler[i];
            unsafe.putInt(schdulerAddr + i * 5 * 4, opt[0]);
            unsafe.putInt(schdulerAddr + i * 5 * 4 + 4, opt[1]);
            unsafe.putInt(schdulerAddr + i * 5 * 4 + 8, opt[2]);
            unsafe.putInt(schdulerAddr + i * 5 * 4 + 12, opt[3]);
            unsafe.putInt(schdulerAddr + i * 5 * 4 + 16, opt[4]);
        }
    }

    private void code(int[][] scheduler, byte[][] src, byte[][] dst) {
        code(scheduler, src, dst, operateNum);
    }

    private void code(int[][] scheduler, byte[][] src, byte[][] dst, int opNum) {
        long baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        for (int i = 0; i < opNum; i++, baseOffset += 64) {
            for (int[] opt : scheduler) {
                long srcIndex = baseOffset + opt[1] * 8;
                long dstIndex = baseOffset + opt[3] * 8;

                if (opt[4] == 0) {
                    if (opt[0] < k) {
                        unsafe.copyMemory(src[opt[0]], srcIndex, dst[opt[2]], dstIndex, 8);
                    } else {
                        unsafe.copyMemory(dst[opt[0] - k], srcIndex, dst[opt[2]], dstIndex, 8);
                    }
                } else {
                    long c = unsafe.getLong(src[opt[0]], srcIndex) ^ unsafe.getLong(dst[opt[2]], dstIndex);
                    unsafe.putLong(dst[opt[2]], dstIndex, c);
                }
            }
        }
    }

    public void encode(byte[][] src, byte[][] dst, int optNum) {
        if (src.length == k && dst.length == m) {
            code(encoderScheduler, src, dst, optNum);
        } else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    public void encode(byte[][] src, byte[][] dst) {
        if (src.length == k && dst.length == m && checkArrayLength(src) && checkArrayLength(dst)) {
            code(encoderScheduler, src, dst);
        } else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    public byte[][] fsDecode(byte[][] src, int[] index) {
        int optNum = src[0].length / 64;
        int[][] scheduler = getScheduler(k, m, index);
        byte[][] dst = new byte[k][src[0].length];
        code(scheduler, src, dst, optNum);
        return dst;
    }

    public byte[][] decode(byte[][] src, int[] index) {
        if (src.length == k && checkArrayLength(src)) {
            if (index.length == k + m) {
                int zeroCount = 0;

                for (int i = 0; i < index.length; i++) {
                    if (index[i] == 0) {
                        zeroCount++;
                    }
                }

                if (zeroCount == m) {
                    int[][] scheduler = getScheduler(k, m, index);
                    byte[][] dst = new byte[k][packetSize];
                    code(scheduler, src, dst);
                    return dst;
                }
            }
        }

        throw new ArrayIndexOutOfBoundsException();
    }


    private boolean checkArrayLength(byte[][] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i].length != packetSize) {
                return false;
            }
        }

        return true;
    }

    public void directEncode(ByteBuf[] src, ByteBuf[] dst) {
        int size = src[0].readableBytes();
        if (DirectCoder.isSupport && (size & DirectCoder.unit) == 0) {
            long[] srcAddr = new long[src.length];
            long[] dstAddr = new long[dst.length];

            for (int i = 0; i < src.length; i++) {
                if (src[i].nioBufferCount() != 1) {
                    throw new UnsupportedOperationException();
                }

                ByteBuffer buffers = src[i].nioBuffer();
                srcAddr[i] = AioChannel.getBufferAddress(buffers);
            }

            for (int i = 0; i < dst.length; i++) {
                if (dst[i].nioBufferCount() != 1) {
                    throw new UnsupportedOperationException();
                }

                ByteBuffer buffers = dst[i].nioBuffer();
                dstAddr[i] = AioChannel.getBufferAddress(buffers);
            }

            DirectCoder.directCode(schdulerAddr, encoderScheduler.length, size, srcAddr, src.length, dstAddr, dst.length);
        } else {
            for (int i = 0; i < size; i += 64) {
                for (int[] opt : encoderScheduler) {
                    int srcIndex = i + opt[1] * 8;
                    int dstIndex = i + opt[3] * 8;
                    if (opt[4] == 0) {
                        if (opt[0] < k) {
                            dst[opt[2]].setLong(dstIndex, src[opt[0]].getLong(srcIndex));
                        } else {
                            dst[opt[2]].setLong(dstIndex, dst[opt[0] - k].getLong(srcIndex));
                        }
                    } else {
                        long c = src[opt[0]].getLong(srcIndex)
                                ^ dst[opt[2]].getLong(dstIndex);
                        dst[opt[2]].setLong(dstIndex, c);
                    }
                }
            }
        }
    }

}
