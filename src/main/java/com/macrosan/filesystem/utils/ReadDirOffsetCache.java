package com.macrosan.filesystem.utils;

import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class ReadDirOffsetCache {
    private static final Map<String, ReadDirOffsetCache> cacheMap = new ConcurrentHashMap<>();

    private static final long MAX_CACHE_MIL = 300_000;
    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("ReadDirOffsetCache-"));

    static {
        executor.submit(ReadDirOffsetCache::tryClearCache);
    }

    private static void tryClearCache() {
        cacheMap.values().forEach(cache -> cache.offsetCache.values().removeIf(t -> t.var2));
        cacheMap.values().forEach(cache -> cache.offsetCache.values().forEach(t -> t.var2 = true));

        executor.schedule(ReadDirOffsetCache::tryClearCache, MAX_CACHE_MIL, TimeUnit.MILLISECONDS);
    }

    AtomicLong offset = new AtomicLong(1);
    Map<Long, Tuple2<String, Boolean>> offsetCache = new ConcurrentHashMap<>();


    public ReadDirOffsetCache() {
    }

    public static long newOffset(String bucket) {
        return cacheMap.computeIfAbsent(bucket, k -> new ReadDirOffsetCache())
                .offset.incrementAndGet();
    }

    public static void addCache(String bucket, long offset, String cacheName) {
        cacheMap.computeIfAbsent(bucket, k -> new ReadDirOffsetCache())
                .offsetCache.put(offset, new Tuple2<>(cacheName, false));
    }

    public static String getCache(String bucket, long offset) {
        Tuple2<String, Boolean> cache = cacheMap.computeIfAbsent(bucket, k -> new ReadDirOffsetCache())
                .offsetCache.get(offset);

        if (cache == null) {
            return null;
        } else {
            return cache.var1;
        }
    }
}
