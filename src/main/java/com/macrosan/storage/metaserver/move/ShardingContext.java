package com.macrosan.storage.metaserver.move;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 记录散列的运行状态，供各个子任务共享
 */
public class ShardingContext {

    private final ConcurrentHashMap<String, Object> context = new ConcurrentHashMap<>();

    public void put(String key, Object value) {
        context.put(key, value);
    }

    public Object get(String key) {
        return context.get(key);
    }

    public void remove(String key) {
        context.remove(key);
    }

    public boolean contains(String key) {
        return context.containsKey(key);
    }

    public void clear() {
        context.clear();
    }
}
