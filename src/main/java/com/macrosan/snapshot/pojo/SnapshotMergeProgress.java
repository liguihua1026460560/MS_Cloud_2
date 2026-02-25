package com.macrosan.snapshot.pojo;

import com.macrosan.snapshot.enums.MergeStep;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author zhaoyang
 * @date 2024/08/26
 **/
@Data
@NoArgsConstructor
public class SnapshotMergeProgress {
    private MergeStep step;
    private String beginPrefix;
    private String keyMarker;
    private String uploadIdMarker;

    public SnapshotMergeProgress(Map<String, String> mergeProgressMap) {
        this.from(mergeProgressMap);
    }

    public SnapshotMergeProgress from(Map<String, String> map) {
        try {
            this.step = MergeStep.valueOf(map.get("step"));
        } catch (Exception e) {
            this.step = null;
        }
        this.beginPrefix = map.get("beginPrefix");
        this.keyMarker = map.get("beginKeyMarker");
        this.uploadIdMarker = map.get("beginUploadIdMarker");
        return this;
    }
}
