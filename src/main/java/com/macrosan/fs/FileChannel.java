package com.macrosan.fs;

import lombok.extern.log4j.Log4j2;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class FileChannel {
    static {
        try {
            System.load("/moss/jni/libfile.so");
        } catch (Throwable e) {
            log.error("load file library fail", e);
            System.exit(0);
        }
    }

    public static native int open(String path);

    public static native int write(int fd, long offset, byte[] bytes, long len);

    public static native int read(int fd, long offset, byte[] bytes, long len);

    public static native int fsync(int fd);
}
