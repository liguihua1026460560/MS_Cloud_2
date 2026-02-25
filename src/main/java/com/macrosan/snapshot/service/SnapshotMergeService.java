package com.macrosan.snapshot.service;


import io.vertx.core.json.Json;

import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhaoyang
 * @date 2024/06/27
 **/
public interface SnapshotMergeService<T> {

    /**
     * 将不同快照的同名对象数据进行合并
     *
     * @param snapshotMarks 快照链接集合---单快照中该集合中只有一个mark
     * @param nextMark      最新快照标记
     * @param listFunction  具体查询数据的函数
     * @return 合并多个快照中的同名对象后的数据集合
     */
    default List<T> listMerge(TreeSet<String> snapshotMarks, String nextMark, int maxKey, Function<String, List<T>> listFunction) {
        // 获取当前最新快照标记下的数据
        List<T> curSnap = listFunction.apply(nextMark);
        // 获取快照创建前的数据
        List<T> prevSnap = listFunction.apply(snapshotMarks.first());
        // 合并数据
        prevSnap.addAll(curSnap);
        return prevSnap.stream().sorted(this::compareTo).limit(maxKey + 1).collect(Collectors.toList());
    }

    default int compareTo(T t, T t1) {
        return 0;
    }

}
