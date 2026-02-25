package com.macrosan.utils.functional;

import java.util.Map;

/**
 * describe:
 *
 * @author chengyinfeng
 * @date 2019/01/29
 */
public class Entry<K, V> implements Map.Entry<K, V> {

    private K key;
    private V value;

    public Entry() {
    }

    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    public Entry<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        this.value = value;
        return value;
    }
}
