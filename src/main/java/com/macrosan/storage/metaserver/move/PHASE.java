package com.macrosan.storage.metaserver.move;

/**
 * @author Administrator
 */

public enum PHASE {

    /**
     * 起始，开启双写
     */
    START(0),

    /**
     * 复制阶段
     */
    COPY(1),

    /**
     * 更新映射
     */
    UPDATE(2),

    /**
     * 移除重复的数据
     */
    REMOVE(3),

    /**
     * 迁移成功
     */
    FINISH(4),

    /**
     * 弃置迁移任务
     */
    DISCARD(-1);


    public int value;

    PHASE(int value) {
        this.value = value;
    }
}
