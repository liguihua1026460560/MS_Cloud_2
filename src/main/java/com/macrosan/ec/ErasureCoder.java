package com.macrosan.ec;

import lombok.extern.log4j.Log4j2;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ErasureCoder {
    /**
     * 原始的数据块数
     * <p></p>
     * 待进行纠处理的数据块数；纠删解码完毕后的数据块数。
     */
    public static final int k;

    /**
     * 校验数据块数
     * 纠删编码完毕后的数据块数
     */
    public static final int m;

    private static final int w;

    /**
     * 数据块的大小。
     */
    public static final int packetSize;

    private static final int minCodeSize;

    static {
        int k0, m0, w0, packetSize0;

        try {
            System.load("/moss/jni/libec.so");
            k0 = getk();
            m0 = getm();
            w0 = getw();
            packetSize0 = getPacketSize();
        } catch (Throwable e) {
            log.info("load ec library fail, use default replica strategy");
            k0 = 1;
            m0 = 1;
            w0 = -1;
            packetSize0 = 128 * 1024;
        }

        k = k0;
        m = m0;
        w = w0;
        packetSize = packetSize0;
        //实际使用unsigned long long进行计算，相当于8个byte
        minCodeSize = 8 * w;
    }

    /**
     * 判断输入byte数组的长度，要求输入的byte数组和packetSize相等
     *
     * @param bytes
     * @return 长度是否符合要求
     */
    private static boolean checkArrayLength(byte[][] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i].length != packetSize) {
                return false;
            }
        }

        return true;
    }

    /**
     * 对输入的src数组进行纠删编码，并将编码结果res中。
     * 要求输入的byte[]个数为k，输出的byte[]个数为m。
     *
     * @param src 原始数据块k
     * @param res 冗余块
     * @return
     */
    public static boolean encode(byte[][] src, byte[][] res) {
        if (src.length == k && res.length == m && checkArrayLength(src) && checkArrayLength(res)) {
            encode0(src, res);
            return true;
        }

        return false;
    }

    /**
     * 对输入的src数组进行纠删解码，并将编码结果res中。
     * 要求输入的byte[]个数为k，输出的byte[]个数为k。
     * index长度为k+m
     * 输入的块需要按照k+m的顺序排序
     *
     * @param src   k+m个数据块中的任意k个
     * @param index 标志输入的src在整个k+m个块的位置，0表示该块缺失，1表示该块在src中
     * @param res   解码结果，原始的数据块k
     * @return
     */
    public static boolean decode(byte[][] src, int[] index, byte[][] res) {
        if (src.length == k && res.length == k && checkArrayLength(src) && checkArrayLength(res)) {
            if (index.length == k + m) {
                int zeroCount = 0;
                for (int i = 0; i < index.length; i++) {
                    if (index[i] == 0) {
                        zeroCount++;
                    }
                }

                if (zeroCount == m) {
                    decode0(src, index, res);
                    return true;
                }
            }
        }

        return false;
    }

    private static native int getk();

    private static native int getm();

    private static native int getw();

    private static native int getPacketSize();

    private static native void encode0(byte[][] src, byte[][] res);

    private static native void decode0(byte[][] src, int[] index, byte[][] res);
}
