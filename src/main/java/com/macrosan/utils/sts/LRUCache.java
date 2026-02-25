package com.macrosan.utils.sts;

import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class LRUCache<K, V> {
    
    /**
     * 基于LinkedHashMap实现的LRU缓存
     */
    private class LRUMap extends LinkedHashMap<K, V> {
        public LRUMap(int capacity) {
            // 初始容量，负载因子，访问顺序
            super(capacity, 0.75f, true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            // 当size超过容量时，移除最老的元素
            return size() > capacity;
        }
    }

    private final int capacity;                    // 缓存容量
    private final Map<K, V> cache;                // 缓存Map
    private final ReadWriteLock lock;             // 读写锁
    private final Lock readLock;                  // 读锁
    private final Lock writeLock;                 // 写锁

    /**
     * 构造函数
     */
    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.cache = new LRUMap(capacity);
        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    /**
     * 获取缓存值
     */
    public V get(K key) {
        // 先尝试读锁
        readLock.lock();
        try {
            V value = cache.get(key);
            if (value != null) {
                return value;
            }
        } finally {
            readLock.unlock();
        }
        return null;
    }

    /**
     * 放入缓存
     */
    public void put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key or value cannot be null");
        }
        
        writeLock.lock();
        try {
            cache.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 移除缓存
     */
    public V remove(K key) {
        writeLock.lock();
        try {
            return cache.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 清空缓存
     */
    public void clear() {
        writeLock.lock();
        try {
            cache.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 获取当前缓存大小
     */
    public int size() {
        readLock.lock();
        try {
            return cache.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 检查key是否存在
     */
    public boolean containsKey(K key) {
        readLock.lock();
        try {
            return cache.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取所有缓存的key
     */
    public Set<K> keySet() {
        readLock.lock();
        try {
            return new HashSet<>(cache.keySet());
        } finally {
            readLock.unlock();
        }
    }
    
    /**
     * 批量获取
     */
    public Map<K, V> getAll(Collection<K> keys) {
        if (keys == null) {
            throw new IllegalArgumentException("Keys cannot be null");
        }
        
        Map<K, V> result = new HashMap<>();
        readLock.lock();
        try {
            for (K key : keys) {
                V value = cache.get(key);
                if (value != null) {
                    result.put(key, value);
                } else {
                }
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }
    
    /**
     * 批量放入
     */
    public void putAll(Map<K, V> map) {
        if (map == null) {
            throw new IllegalArgumentException("Map cannot be null");
        }
        
        writeLock.lock();
        try {
            cache.putAll(map);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isEmpty() {
        readLock.lock();
        try {
            return cache.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public Collection<V> values() {
        readLock.lock();
        try {
            return cache.values();
        } finally {
            readLock.unlock();
        }
    }

}