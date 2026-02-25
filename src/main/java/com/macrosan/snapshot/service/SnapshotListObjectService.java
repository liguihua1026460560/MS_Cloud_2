package com.macrosan.snapshot.service;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple3;

/**
 * @author zhaoyang
 * @date 2024/06/27
 **/
public class SnapshotListObjectService implements SnapshotMergeService<Tuple3<Boolean, String, MetaData>> {

    @Override
    public int compareTo(Tuple3<Boolean, String, MetaData> o1, Tuple3<Boolean, String, MetaData> o2) {
        return o1.var3.key.compareTo(o2.var3.key);
    }

}
