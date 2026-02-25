package com.macrosan.utils.functional;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhaoyang
 * @date 2024/11/12
 **/
public class ExpiringSet<V> {
    private final ConcurrentHashMap<V, Long> map = new ConcurrentHashMap<>();

    public void add(V value, long expireMs) {
        map.put(value, System.currentTimeMillis() + expireMs);
    }

    public boolean contains(V value) {
        return map.computeIfPresent(value, (v, expireMs) -> {
            if (isExpired(expireMs)) {
                return null;
            }
            return expireMs;

        }) != null;
    }

    public boolean remove(V value) {
        return map.remove(value) != null;
    }

    private boolean isExpired(long expireTime) {
        return System.currentTimeMillis() > expireTime;
    }
}