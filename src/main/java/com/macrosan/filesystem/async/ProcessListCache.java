package com.macrosan.filesystem.async;

import com.macrosan.doubleActive.DataSynChecker;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;

/**
 * 每个fsKey下有一个FIFO缓存队列。要求顺次处理队列中的元素
 */
@Log4j2
public abstract class ProcessListCache<T> {
    public static final Map<String, ProcessListCache<DataSynChecker.SyncRquest>> SYNC_CACHE_MAP;

    static {
        SYNC_CACHE_MAP = new ConcurrentHashMap<>();
        SCAN_SCHEDULER.schedule(() -> cleanFsRecordCache(SYNC_CACHE_MAP), 30, TimeUnit.SECONDS);
    }

    // getFSRecordMapKey
    String fsKey;
    // 保存recordkey和实例。此处注意和fsKey做区分
    ConcurrentLinkedQueue<T> queue;
    // 队列添加时设为true，队列为空设为false
    AtomicBoolean isProcessing = new AtomicBoolean();
    // 添加一个状态标记，记录最近活动时间。队列添加元素、队内元素开始处理都会更新
    long lastActiveTime = System.currentTimeMillis();

    ReentrantLock lock;

    ProcessListCache(String fsKey) {
        this.fsKey = fsKey;
        this.queue = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
    }

    // key为recordKey，不是fsKey
    public void add(T t) {
        lock.lock();
        try {
            lastActiveTime = System.currentTimeMillis();
            queue.add(t);
            startProcessingIfNeeded();
        } finally {
            lock.unlock();
        }
    }

    // 队列元素执行添加、处理等操作时需要更新操作时间，防止cache频繁移除
    // 加锁是为了防止cleanFsRecordCache定期移除的同时又有队列相关的操作，从而规避线程冲突引发的缓存错误移除
    public void updateActivityTime() {
        lock.lock();
        try {
            lastActiveTime = System.currentTimeMillis();
        } finally {
            lock.unlock();
        }
    }

    // 开始处理
    private void startProcessingIfNeeded() {
        if (isProcessing.compareAndSet(false, true)) {
            processQueue();
        }
    }

    /**
     * 开始处理缓冲队列中的元素。
     */
    public abstract void processQueue();

    /**
     * 处理完一个元素成功后调用，推送下一个元素
     */
    public abstract void onNext();

    /**
     * 处理元素失败调用
     */
    public abstract void onFail();

    /**
     * 缓存队列清空，判断可以清除缓存时调用，释放资源
     */
    public abstract void cleanCache();


    /**
     * 定期清理fsRecordCacheMap中已清空的FsRecordCache
     *
     * @param cacheMap cacheMap必须支持compute原子操作，如concurrentHashMap
     * @param <T>
     */
    public static <T> void cleanFsRecordCache(Map<String, ProcessListCache<T>> cacheMap) {
        try {
            for (Map.Entry<String, ProcessListCache<T>> next : cacheMap.entrySet()) {
                if (next == null) {
                    continue;
                }
                if (next.getValue() == null) {
                    continue;
                }
                cacheMap.compute(next.getKey(), (k, cache) -> {
                    if (cache.lock.tryLock()) {
                        try {
                            // 检查是否满足清理条件
                            if (cache.queue.isEmpty() &&
                                    !cache.isProcessing.get() &&
                                    System.currentTimeMillis() - cache.lastActiveTime > DataSynChecker.timeoutMinute * 1000) {
                                cache.cleanCache();
                                log.debug("Cleaned cache for fsKey: {}", next.getKey());
                                return null;
                            }
                        } finally {
                            cache.lock.unlock();
                        }
                    }
                    return cache;
                });
            }

        } catch (Exception e) {
            log.error("cleanFsRecordCache err, ", e);
        } finally {
            log.debug("cleanCache. {}", cacheMap.size());
            SCAN_SCHEDULER.schedule(() -> cleanFsRecordCache(cacheMap), 30, TimeUnit.SECONDS);
        }
    }
}
