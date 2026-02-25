package com.macrosan.snapshot.utils;

import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import io.vertx.core.json.Json;

import java.util.Arrays;

/**
 * @author zhaoyang
 * @date 2024/08/23
 **/
public class SnapshotDoubleWriteUtil {

    public static MetaData replaceMetaSnapshotMark(MetaData metaData, String newSnapshotMark) {
        MetaData clone = metaData.clone();
        clone.setSnapshotMark(newSnapshotMark);
        if (metaData.partInfos != null) {
            for (PartInfo partInfo : metaData.partInfos) {
                partInfo.setSnapshotMark(newSnapshotMark);
            }
        }
        return clone;
    }

    public static PartInfo replacePartInfoSnapshotMark(PartInfo partInfo, String newSnapshotMark) {

        PartInfo clone = Json.decodeValue(Json.encode(partInfo), PartInfo.class);
        clone.setSnapshotMark(newSnapshotMark);
        return clone;
    }

    public static InitPartInfo replaceInitPartSnapshotMark(InitPartInfo initPartInfo, String newSnapshotMark) {
        InitPartInfo clone = Json.decodeValue(Json.encode(initPartInfo), InitPartInfo.class);
        clone.setSnapshotMark(newSnapshotMark);
        return clone;
    }
}
