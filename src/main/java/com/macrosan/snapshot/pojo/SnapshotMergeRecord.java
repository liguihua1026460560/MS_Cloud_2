package com.macrosan.snapshot.pojo;

import com.macrosan.snapshot.enums.SnapshotMergeStrategy;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @author zhaoyang
 * @date 2024/07/08
 **/
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
public class SnapshotMergeRecord<T> {
    private SnapshotMergeStrategy mergeType;
    private SnapshotMergeTask mergeTask;
    private T data;

    public static <T> SnapshotMergeRecord<T> error() {
        return new SnapshotMergeRecord<T>().setMergeType(SnapshotMergeStrategy.ERROR);
    }

    public static <T> SnapshotMergeRecord<T> none() {
        return new SnapshotMergeRecord<T>().setMergeType(SnapshotMergeStrategy.NONE);
    }
}
