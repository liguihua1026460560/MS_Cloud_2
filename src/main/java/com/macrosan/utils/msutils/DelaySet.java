package com.macrosan.utils.msutils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DelaySet<E> {
    private Map<E, Long> map = new ConcurrentHashMap<>();

    //mill seconds
    public void add(E e, long timeout) {
        map.put(e, System.currentTimeMillis() + timeout);
    }

    public boolean contains(E e) {
        Long time = map.get(e);
        if (time == null) {
            return false;
        } else {
            long cur = System.currentTimeMillis();
            if (cur >= time) {
                map.remove(e, time);
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     *  判断当前是否到达重试的延时时间
     * @param e
     * @return
     */
    public boolean timeout(E e) {
        Long time = map.get(e);
        if (time == null) {
            return true;
        } else {
            long cur = System.currentTimeMillis();
            return cur > time;
        }
    }
}
