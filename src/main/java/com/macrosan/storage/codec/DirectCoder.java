package com.macrosan.storage.codec;

import lombok.extern.log4j.Log4j2;

@Log4j2
class DirectCoder {
    public static boolean isSupport;
    public static int unit;

    static {
        try {
            System.load("/moss/jni/libcode.so");
            unit = unit() - 1;
            isSupport = true;
            log.error("direct ec native coder support {}", unit + 1);
        } catch (Throwable e) {
            isSupport = false;
            log.error("direct ec native coder not support");
        }
    }

    public static native int unit();

    public static native void directCode(long scheduler, int schedulerLength, int dataSize, long[] src, int sNum, long[] dst, int dNum);
}
