package com.macrosan.storage.strategy.select;

/**
 * @author gaozhiyuan
 */
public enum SelectStrategy {
    /**
     * 不选择，永远返回第一个
     */
    NO,
    /**
     * 随机选择
     */
    RANDOM,
    /**
     * 根据存储池容量选择
     */
    SIZE,
    /**
     * 根据存储池健康状态选择
     */
    HEALTH;
}
