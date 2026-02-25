package com.macrosan.storage.metaserver.move.scanner;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryLimiter {
    private long maxMemory = 128L * 1024 * 1024; // 128MB
    private final AtomicLong currentMemoryUsage = new AtomicLong(0);

    public MemoryLimiter() {}

    public MemoryLimiter(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    // 尝试分配内存，返回是否成功
    public void allocateMemory(long size) {
        currentMemoryUsage.addAndGet(size);
    }

    // 释放内存
    public void releaseMemory(long size) {
        currentMemoryUsage.addAndGet(-size);
    }

    // 获取当前内存使用量
    public long getCurrentMemoryUsage() {
        return currentMemoryUsage.get();
    }

    public boolean isOverLimit() {
        return currentMemoryUsage.get() > maxMemory;
    }
}
