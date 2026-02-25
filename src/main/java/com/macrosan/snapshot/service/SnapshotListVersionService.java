package com.macrosan.snapshot.service;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhaoyang
 * @date 2024/07/01
 **/
@Log4j2
public class SnapshotListVersionService implements SnapshotMergeService<Tuple3<Boolean, String, MetaData>> {
    @Override
    public int compareTo(Tuple3<Boolean, String, MetaData> o1, Tuple3<Boolean, String, MetaData> o2) {
        int keyCompare = o1.var2.compareTo(o2.var2);
        if (keyCompare == 0) {
            return o2.var3.stamp.compareTo(o1.var3.stamp);
        }
        return keyCompare;
    }


    @Override
    public List<Tuple3<Boolean, String, MetaData>> listMerge(TreeSet<String> snapshotMarks, String nextMark, int maxKey, Function<String, List<Tuple3<Boolean, String, MetaData>>> listFunction) {
        // 获取当前最新快照标记下的数据
        List<Tuple3<Boolean, String, MetaData>> curSnap = listFunction.apply(nextMark);
        // 获取快照创建前的数据
        List<Tuple3<Boolean, String, MetaData>> prevSnap = listFunction.apply(snapshotMarks.first());
        // 合并数据
        prevSnap.addAll(curSnap);
        Set<String> deleteMarkerSet = new HashSet<>(maxKey + 1);
        List<Tuple3<Boolean, String, MetaData>> result = prevSnap.stream().sorted(this::compareTo).limit(maxKey + 1)
                .peek(t3 -> {
                    if (t3.var3.deleteMarker && t3.var3.snapshotMark.equals(nextMark)) {
                        deleteMarkerSet.add(t3.var2);
                    }
                })
                .collect(Collectors.toList());
        if (deleteMarkerSet.isEmpty()) {
            return result;
        }
        result.forEach(t3 -> {
            if (deleteMarkerSet.contains(t3.var2) && !t3.var3.isCurrentSnapshotObject(nextMark)) {
                t3.var3.setLatest(false);
            }
        });
        return result;
    }
}
