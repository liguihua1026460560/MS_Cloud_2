package com.macrosan.action.core;

import java.util.Map;

/**
 * MergeMap
 *
 * @author liyixin
 * @date 2018/12/29
 */
public class MergeMap<T, R> {

    private final Map<T, R> map1;

    private final Map<T, R> map2;

    public MergeMap(Map<T, R> map1, Map<T, R> map2) {
        this.map1 = map1;
        this.map2 = map2;
    }

    public R get(T t) {
        R r;
        return (r = map1.get(t)) != null ?
                r :
                map2.get(t);
    }

    @Override
    public String toString() {
        return "MergeMap{" +
                "map1=" + map1 +
                ", map2=" + map2 +
                '}';
    }
}
