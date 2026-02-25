package com.macrosan.snapshot.service;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple3;

import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * @author zhaoyang
 * @date 2024/07/04
 **/
public class SnapshotListLifeService implements SnapshotMergeService<Tuple3<Boolean, String, MetaData>> {
    @Override
    public List<Tuple3<Boolean, String, MetaData>> listMerge(TreeSet<String> snapshotMarks, String nextMark, int maxKey, Function<String, List<Tuple3<Boolean, String, MetaData>>> listFunction) {
        // 获取第一个标记和nextMark对应的列表
        // 将不同快照间的数据直接合并
        List<Tuple3<Boolean, String, MetaData>> prevSnap = listFunction.apply(snapshotMarks.first());
        if (prevSnap.size() > maxKey) {
            return prevSnap;
        }
        List<Tuple3<Boolean, String, MetaData>> curSnap = listFunction.apply(nextMark);
        curSnap.forEach(item -> {
            if (prevSnap.size() > maxKey) {
                return;
            }
            prevSnap.add(item);
        });
        return prevSnap;
    }
}
