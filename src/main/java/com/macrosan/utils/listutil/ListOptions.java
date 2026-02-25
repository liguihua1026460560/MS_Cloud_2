package com.macrosan.utils.listutil;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ListOptions {
    // 单次List的条数
    private int maxListCount = 1000;

    // 单词List的超时时间 毫秒
    private long maxListTimeout = 100000;

    // 单次List失败的重试次数
    private int maxListRetryCount = 10;

    // 缓冲区缓存大小
    private int bufferSize = 64 * 1024 * 1024;

    // 最大缓存数量
    private int bufferCount = 10000;
}
