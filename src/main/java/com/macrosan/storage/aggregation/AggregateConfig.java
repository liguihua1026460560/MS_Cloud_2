package com.macrosan.storage.aggregation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregateConfig {
    /**
     * 单个聚合文件的最大大小（字节）
     */
    private long maxAggregatedFileSize = 64 * 1024 * 1024L; // 默认64MB

    /**
     * 聚合文件的数量阈值，当达到该阈值时进行聚合操作
     */
    private int maxAggregatedFileNum = 10000; // 默认最大聚合1w个小文件

    /**
     *  聚合文件的最大空洞率，超过该值时进行空间回收操作
     */
    private double maxHoleRate = 0.3;

    /**
     * 是否使用直接内存
     */
    private boolean useDirect = false;
}
