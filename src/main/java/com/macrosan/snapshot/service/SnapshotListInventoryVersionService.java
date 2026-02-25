package com.macrosan.snapshot.service;

import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple3;

/**
 * @author zhaoyang
 * @date 2024/07/26
 **/
public class SnapshotListInventoryVersionService implements SnapshotMergeService<Tuple3<Boolean, String, MetaData>> {

    private String getKey(Tuple3<Boolean, String, MetaData> t) { // 不同快照间相同key的数据应该排列到一起，因此此处的key不包含 快照标记
        return Utils.getMetaDataKey("", t.var3.getBucket(), t.var3.getKey(), null);
    }

    private String getStampKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey("", t.var3.getBucket(), t.var3.getKey(), t.var3.versionId, t.var3.stamp);
    }

    @Override
    public int compareTo(Tuple3<Boolean, String, MetaData> o1, Tuple3<Boolean, String, MetaData> o2) {
        // 进行排序 相同key的对象排到一起，相同key对象按照stamp从小到大排序
        int cmp = getKey(o1).compareTo(getKey(o2));
        if (cmp == 0) {
            return getStampKey(o1).compareTo(getStampKey(o2));
        }
        return cmp;
    }
}
