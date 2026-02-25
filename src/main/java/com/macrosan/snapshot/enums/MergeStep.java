package com.macrosan.snapshot.enums;


/**
 * @author zhaoyang
 */

public enum MergeStep {
    /**
     * 复制对象数据
     */
    COPY_OBJ,
    /**
     * 复制视图中的initPart和对应的part
     */
    COPY_INIT_PART,
    /**
     * 复制视图中的part
     */
    COPY_PART,
    /**
     * 清除数据
     */
    REMOVE,
    /**
     * 清理对象
     */
    CLEAN_UP_OBJ,
    /**
     * 清理当前快照下initPart和对应的part
     */
    CLEAN_UP_INIT_PART,
    /**
     * 清理其他快照下initPart对应的当前快照下的part
     */
    CLEAN_UP_PART

}