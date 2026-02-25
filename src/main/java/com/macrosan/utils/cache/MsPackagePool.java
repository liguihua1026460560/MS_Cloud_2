package com.macrosan.utils.cache;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.macrosan.storage.StoragePoolFactory.DEFAULT_PACKAGE_SIZE;

/**
 * @author gaozhiyuan
 */
public class MsPackagePool {
    private static final ConcurrentLinkedQueue<byte[]>[] pool = new ConcurrentLinkedQueue[4];

    static {
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new ConcurrentLinkedQueue<>();
        }

        for (int i = 0; i < 1024 * 1024 * 1024 / DEFAULT_PACKAGE_SIZE; i++) {
            byte[] bytes = new byte[DEFAULT_PACKAGE_SIZE];
            int ii = Math.abs(bytes.hashCode() % pool.length);
            pool[ii].add(bytes);
        }
    }

    private static byte[] acquire(int index) {
        byte[] res = pool[index].poll();

        if (res != null) {
            return res;
        } else {
            return new byte[DEFAULT_PACKAGE_SIZE];
        }
    }

    public static byte[][] acquire(int n, int index) {
        byte[][] res = new byte[n][];
        for (int i = 0; i < n; i++) {
            res[i] = acquire(index);
        }

        return res;
    }

    public static void release(byte[] bytes, int index) {
        if (null == bytes || bytes.length != DEFAULT_PACKAGE_SIZE) {
            return;
        }

        Arrays.fill(bytes, (byte) 0);
        pool[index].add(bytes);
    }

    public static void release(int index, byte[]... bytes) {
        for (byte[] bs : bytes) {
            release(bs, index);
        }
    }

    private static long index0 = 0;

    public static int getIndex() {
        return (int) Math.abs(index0++ % pool.length);
    }
}
