package com.macrosan.snapshot.enums;

/**
 * @author zhaoyang
 * @date 2024/07/08
 **/
public enum SnapshotMergeStrategy {
    WRITE_BACK,
    DELETE_META,
    DELETE_ALL,
    UPDATE_META,
    NONE,
    ERROR;
}
