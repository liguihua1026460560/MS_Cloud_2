package com.macrosan.filesystem.cache;

import lombok.AllArgsConstructor;
import reactor.core.scheduler.Scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class TimeCache<K, V> {
    @AllArgsConstructor
    private static class Item<V> {
        long time;
        V v;
    }

    Map<K, Item<V>> cache = new ConcurrentHashMap<>();

    long maxCacheTime;
    Scheduler executor;

    public TimeCache(long maxCacheTime, Scheduler executor) {
        this.maxCacheTime = maxCacheTime;
        this.executor = executor;

        executor.schedule(this::clear);
    }

    private void clear() {
        long cur = System.nanoTime();

        for (K key : cache.keySet()) {
            cache.compute(key, (k, i) -> {
                if (i != null && cur - i.time > maxCacheTime) {
                    return null;
                }

                return i;
            });
        }

        executor.schedule(this::clear, 10_000, TimeUnit.MILLISECONDS);
    }

    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        return compute(key, remappingFunction, 0);
    }

    public V compute(K key, BiFunction<K, V, V> remappingFunction, long timeout) {
        Item<V> item = cache.compute(key, (k, i) -> {
            V v;
            if (i == null) {
                v = remappingFunction.apply(k, null);
            } else {
                v = remappingFunction.apply(k, i.v);
            }

            if (v == null) {
                return null;
            } else {
                if (timeout > 0) {
                    return new Item<>(System.nanoTime() - maxCacheTime + timeout, v);
                } else {
                    return new Item<>(System.nanoTime(), v);
                }
            }
        });

        if (item == null) {
            return null;
        } else {
            return item.v;
        }
    }
}
