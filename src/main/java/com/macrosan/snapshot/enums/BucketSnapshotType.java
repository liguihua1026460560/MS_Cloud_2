package com.macrosan.snapshot.enums;

/**
 * @author zhaoyang
 * @date 2024/08/22
 **/
public enum BucketSnapshotType {
    /**
     * 用户手动创建的快照，可以进行回滚、删除
     */
     ManualSnapshot,
    /**
     * 用户回滚快照时为了保留当前数据而创建的快照，只能用于回滚
     */
    RollbackSnapshot,
}
