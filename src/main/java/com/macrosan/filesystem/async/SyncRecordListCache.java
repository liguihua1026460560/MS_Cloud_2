package com.macrosan.filesystem.async;

import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.DataSynChecker.*;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import static com.macrosan.doubleActive.DataSynChecker.*;

/**
 * 文件的差异记录扫描处理时，需要对同一个nodeId下的记录按顺序处理。
 * 但是，原本S3的处理方式是每次拿到一批record后，随机发送到数个线程上进行dealrecord。
 * 文件需要上一个record处理完后再发送队列中的下一个record。处理方法为将recordList中全部内容放入队列，然后将队列中的顺序依次执行。
 * 如果期间一个record处理失败，需要重复处理直到成功。
 */
@Log4j2
public class SyncRecordListCache extends ProcessListCache<SyncRquest> {

    UnicastProcessor<Integer> streamController;

    SyncRecordListCache(String fsKey) {
        super(fsKey);
        streamController = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        streamController
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(i -> {
                    // 这里和add加锁，保证提前清空队列时还可以触发processQueue里的onNext
                    lock.lock();
                    try {
                        SyncRquest curSyncRquest = queue.poll();
                        log.debug("stream {} {}", i, curSyncRquest == null ? "null" : curSyncRquest.record.rocksKey());
                        if (curSyncRquest == null) {
                            if (isProcessing.compareAndSet(true, false)) {
                                if (isDebug) {
                                    log.info("complete queue {}", fsKey);
                                }
                            }
                            return;
                        }
                        if (i == -1) {
                            // 之前有过失败的record处理，后续的所有处理均返回失败
                            curSyncRquest.res.onNext(false);
                        } else {
                            allocSyncRequest(curSyncRquest);
                        }
                    } finally {
                        lock.unlock();
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                })
                .subscribe();
    }

    @Override
    public void processQueue() {
        try {
            onNext();
        } catch (Exception e) {
            log.error("processQueue err, ", e);
            throw e;
        }
    }

    @Override
    public void onNext() {
        updateActivityTime();
        streamController.onNext(1);
    }

    // 出现一个syncRquest处理失败且重试不成功，整个队列都丢弃，等待下一轮扫描重新开始。
    @Override
    public void onFail() {
        updateActivityTime();
        streamController.onNext(-1);
    }

    @Override
    public void cleanCache() {
        try {
            streamController.onComplete();
            streamController = null;
        } catch (Exception ignored) {
        }
    }


//    public static SyncRecordListCache getCache(String fsKey) {
//        synchronized (SYNC_CACHE_MAP) {
//            if (SYNC_CACHE_MAP.containsKey(fsKey)) {
//                ProcessListCache<SyncRquest> syncRquestProcessListCache = SYNC_CACHE_MAP.get(fsKey);
//                syncRquestProcessListCache.lastActiveTime = System.currentTimeMillis();
//                return (SyncRecordListCache) syncRquestProcessListCache;
//            }
//            return (SyncRecordListCache) SYNC_CACHE_MAP.computeIfAbsent(fsKey, k -> new SyncRecordListCache(fsKey));
//        }
//    }

    public static SyncRecordListCache getCache(String fsKey) {
        ProcessListCache<DataSynChecker.SyncRquest> cache =
                SYNC_CACHE_MAP.compute(fsKey, (key, existing) -> {
                    if (existing == null) {
                        return new SyncRecordListCache(key);
                    }
                    existing.updateActivityTime();
                    return existing;
                });
        return (SyncRecordListCache) cache;
    }

}
