package com.macrosan.filesystem.cache;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.macrosan.filesystem.cache.WriteCache.AVAILABLE_CACHE_NUM;
import static com.macrosan.filesystem.cache.WriteCache.MAX_TRUNK_CACHE_SIZE;

public class BytesPool {
    public static final int MAX_BYTE_POOL_SIZE = 64;
    private static final ConcurrentLinkedQueue<byte[]> pool = new ConcurrentLinkedQueue<>();

    static {
        for (int i = 0; i < MAX_BYTE_POOL_SIZE; i++) {
            byte[] arr = new byte[MAX_TRUNK_CACHE_SIZE];
            pool.add(arr);
        }
    }

    public static byte[] allocate() {
        byte[] bytes = pool.poll();

        if (bytes == null) {
            return null;
        } else {
            AVAILABLE_CACHE_NUM.decrementAndGet();
            return bytes;
        }
    }

    public static void release(byte[] bytes) {
        if (AVAILABLE_CACHE_NUM.incrementAndGet() <= MAX_BYTE_POOL_SIZE) {
            Arrays.fill(bytes, (byte) 0);
            pool.add(bytes);
        } else {
            AVAILABLE_CACHE_NUM.decrementAndGet();
            bytes = null;
        }
    }

}
