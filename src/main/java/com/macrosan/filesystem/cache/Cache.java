package com.macrosan.filesystem.cache;

import com.macrosan.message.jsonmsg.Inode;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class Cache<K, V> {
    @AllArgsConstructor
    private static class Item<V> {
        long id;
        V v;
    }

    Map<K, Item<V>> cache = new ConcurrentHashMap<>();

    static AtomicLong id = new AtomicLong(0);

    int maxCacheNum;
    Scheduler executor;

    public Cache(int maxCacheNum, Scheduler executor) {
        this.maxCacheNum = maxCacheNum;
        this.executor = executor;

        executor.schedule(this::clear);
    }

    long lastClear = 0;

    private void clear() {
        long maxID = id.get();
        if (maxID - lastClear > maxCacheNum) {
            long needClear = maxID - maxCacheNum;
            for (K key : cache.keySet()) {
                cache.compute(key, (k, i) -> {
                    if (i != null && i.id < needClear) {
                        return null;
                    }

                    return i;
                });
            }

            lastClear = needClear;
            executor.schedule(this::clear);
        } else {
            executor.schedule(this::clear, 10_000, TimeUnit.MILLISECONDS);
        }
    }

    public V get(K key) {
        Item<V> item = cache.computeIfPresent(key, (k, i) -> {
            i.id = id.incrementAndGet();
            return i;
        });

        if (item == null) {
            return null;
        } else {
            return item.v;
        }
    }

    public void put(K key, V v) {
        cache.compute(key, (k, i) -> {
            if (i == null) {
                return new Item<>(id.incrementAndGet(), v);
            } else {
                i.id = id.incrementAndGet();
                i.v = v;
                return i;
            }
        });
    }

    public void remove(K key) {
        cache.compute(key, (k, i) -> null);
    }

    /**
     * 用于打印当前vnode缓存中的指定数目的记录
     **/
    public void print(boolean isInode, int printNum) {
        Iterator<K> iterator = cache.keySet().iterator();
        int i = 0;
        while (iterator.hasNext()) {
            K key = iterator.next();
            Inode inode = (Inode) cache.get(key).v;
            if (isInode) {
                log.info("nodeId: {}, inode: {}", key, inode);
            } else {
                log.info("nodeId: {}, objName: {}", key, inode.getObjName());
            }

            if (i++ >= printNum) {
                break;
            }
        }
    }

    /**
     * 用于打印当前vnode缓存中指定的记录
     **/
    public void find(K key) {
        Item<V> item = cache.computeIfPresent(key, (k, i) -> {
            return i;
        });

        if (item == null) {
            Long nodeId = (Long) key;
            log.info("【find】 nodeId: {}, inode: null", nodeId);
        } else {
            Long nodeId = (Long) key;
            Inode inode = (Inode) item.v;
            log.info("【find】 nodeId: {}, inode: {}", nodeId, inode);
        }
    }
}
