package com.macrosan.snapshot.service;


import com.macrosan.message.jsonmsg.PartInfo;

/**
 * @author zhaoyang
 * @date 2024/07/15
 **/
public class SnapshotListPartInfoService implements SnapshotMergeService<PartInfo> {

    @Override
    public int compareTo(PartInfo o1, PartInfo o2) {
        int res = o1.object.compareTo(o2.object);
        if (res == 0) {
            return o1.partNum.compareTo(o2.partNum);
        }
        return res;
    }
}
