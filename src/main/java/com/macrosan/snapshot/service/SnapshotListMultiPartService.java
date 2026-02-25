package com.macrosan.snapshot.service;

import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.utils.functional.Tuple3;


/**
 * @author zhaoyang
 * @date 2024/08/14
 **/
public class SnapshotListMultiPartService implements SnapshotMergeService<Tuple3<Boolean, String, InitPartInfo>> {

    @Override
    public int compareTo(Tuple3<Boolean, String, InitPartInfo> o1, Tuple3<Boolean, String, InitPartInfo> o2) {
        int res = o1.var3.object.compareTo(o2.var3.object);
        if (res == 0) {
            return o1.var3.uploadId.compareTo(o2.var3.uploadId);
        }
        return res;
    }
}
