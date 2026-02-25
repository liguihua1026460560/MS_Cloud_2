package com.macrosan.snapshot.service;

import java.util.TreeSet;
import java.util.function.Function;

/**
 * @author zhaoyang
 * @date 2024/06/28
 **/
public class SnapshotGetObjectService {

    public byte[] getMerge(TreeSet<String> snapshotMarks, String nextMark, Function<String, byte[]> getFunction) {
        // 查询当前快照下数据
        byte[] value = getFunction.apply(nextMark);
        if (value != null) {
            return value;
        }
        // 当前快照下没有有效数据，则去之前快照中查询
        return getFunction.apply(snapshotMarks.first());
    }
}
