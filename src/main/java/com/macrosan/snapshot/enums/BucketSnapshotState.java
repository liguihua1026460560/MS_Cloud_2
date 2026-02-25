package com.macrosan.snapshot.enums;

import lombok.Getter;

/**
 * @author zhaoyang
 * @date 2024/07/08
 **/
@Getter
public enum BucketSnapshotState {
    CREATED("created"),
    DELETING("deleting");

    private final String state;

    BucketSnapshotState(String state) {
        this.state = state;
    }

}
