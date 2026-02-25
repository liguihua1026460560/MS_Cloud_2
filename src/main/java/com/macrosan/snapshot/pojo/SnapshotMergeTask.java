package com.macrosan.snapshot.pojo;

import com.macrosan.snapshot.enums.MergeTaskType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhaoyang
 * @date 2024/07/08
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotMergeTask {
    private String srcSnapshotMark;
    private String targetSnapshotMark;
    private String bucketName;
    private MergeTaskType type;
    private String snapshotLink;
    private Long ctime;
}
