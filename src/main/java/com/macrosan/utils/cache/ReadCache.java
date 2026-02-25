package com.macrosan.utils.cache;

import com.macrosan.utils.functional.Tuple2;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ReadCache {
    private static final long CACHE_SIZE;
    private static final int CACHE_NUM;
    //比MAX_CACHE_FILE_SIZE小的文件都将被缓存
    public static final long MAX_CACHE_FILE_SIZE;

    static {
        long size = 1024 * 1024 * 100;
        try {
            size = Long.parseLong(System.getProperty("com.macrosan.cache.read.total"));
        } catch (Exception e) {

        }

        CACHE_SIZE = size;

        int num = 1024 * 100;
        try {
            size = Integer.parseInt(System.getProperty("com.macrosan.cache.read.num"));
        } catch (Exception e) {

        }

        CACHE_NUM = num;

        //默认情况下，关闭读缓存功能。
        size = Long.MIN_VALUE;
        try {
            size = Long.parseLong(System.getProperty("com.macrosan.cache.fileSize"));
        } catch (Exception e) {

        }

        MAX_CACHE_FILE_SIZE = size;
    }

    private static ReadCache instance = new ReadCache();

    public static ReadCache getInstance() {
        return instance;
    }

    @AllArgsConstructor
    private static class CacheKey {
        String fileName;
        long start;
        long end;

        @Override
        public int hashCode() {
            return fileName.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CacheKey) {
                CacheKey key = (CacheKey) o;
                return key.fileName.equalsIgnoreCase(fileName) && start == key.start && key.end == end;
            }

            return false;
        }
    }

    private static class Cache {
        CacheKey key;
        long cacheSize;
        List<byte[]> cache;

        Cache(String fileName, long start, long end, List<byte[]> cache) {
            this(new CacheKey(fileName, start, end), cache);
        }

        Cache(CacheKey key, List<byte[]> cache) {
            this.key = key;
            this.cache = cache;
            for (byte[] b : cache) {
                cacheSize += b.length;
            }
        }
    }

    @AllArgsConstructor
    private static class Limit {
        long size;
        long num;

        static final AtomicReferenceFieldUpdater<ReadCache, Limit> LIMIT_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(ReadCache.class, Limit.class, "limit");

        static boolean limit() {
            Limit l = LIMIT_UPDATER.get(instance);
            return l.size <= CACHE_SIZE && l.num <= CACHE_NUM;
        }

        static void update(long size, long num) {
            LIMIT_UPDATER.updateAndGet(instance, o -> new Limit(o.size += size, o.num += num));
        }
    }

    volatile Limit limit = new Limit(0L, 0L);

    private static class CacheQueue {
        Map<CacheKey, Cache> map = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<CacheKey> queue = new ConcurrentLinkedQueue<>();
        UnicastProcessor<Tuple2<CacheKey, Cache>>[] processor;
        final int CONSUMER_NUM = 32;

        CacheQueue() {
            processor = new UnicastProcessor[CONSUMER_NUM];
            for (int i = 0; i < processor.length; i++) {
                processor[i] = UnicastProcessor.create(Queues.<Tuple2<CacheKey, Cache>>unboundedMultiproducer().get());
                processor[i].publishOn(DISK_SCHEDULER).doOnNext(t -> {
                    try {
                        CacheKey key = t.var1;
                        Cache cache = map.get(key);
                        if (null == cache) {
                            map.put(key, t.var2);
                            queue.add(key);
                            Limit.update(t.var2.cacheSize, 1);

                            while (!Limit.limit()) {
                                removeFirst();
                            }
                        } else {
                            queue.remove(key);
                            queue.add(key);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }).subscribe();
            }
        }

        void removeFirst() {
            CacheKey key = queue.poll();
            if (null == key) {
                return;
            }

            Cache cache = map.remove(key);
            Limit.update(-cache.cacheSize, -1);
        }

        void addLast(CacheKey key, Cache cache) {
            int n = Math.abs(key.hashCode()) % CONSUMER_NUM;
            processor[n].onNext(new Tuple2<>(key, cache));
        }
    }

    private CacheQueue cacheQueue = new CacheQueue();

    public List<byte[]> getCache(String fileName, long start, long end) {
        CacheKey key = new CacheKey(fileName, start, end);
        Cache cache = cacheQueue.map.get(key);
        if (cache != null) {
            return cache.cache;
        }

        return null;
    }

    public void addCache(String fileName, long start, long end, List<byte[]> cache) {
        CacheKey key = new CacheKey(fileName, start, end);
        cacheQueue.addLast(key, new Cache(key, cache));
    }
}
