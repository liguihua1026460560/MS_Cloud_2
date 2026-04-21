package com.macrosan.filesystem.tier;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.SysConstants.ROCKS_CACHE_BACK_STORE_KEY;
import static com.macrosan.constants.SysConstants.ROCKS_OBJ_META_DELETE_MARKER;
import static com.macrosan.filesystem.tier.FileTierMove.ONCE;
import static com.macrosan.filesystem.utils.FsTierUtils.FS_TIER_DEBUG;

/**
 * @author DaiFengtao
 * @date 2026年03月17日 13:54
 */
@Log4j2
public class FileTierCleanTask {
    MonoProcessor<Boolean> res = MonoProcessor.create();
    MSRocksDB mqDB;
    final MSRocksIterator iterator;
    AtomicBoolean closed = new AtomicBoolean(false);
    AtomicBoolean scanEnd = new AtomicBoolean(false);

    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    AtomicLong queueSize = new AtomicLong();

    private final AtomicLong total = new AtomicLong();

    public long getTotal() {
        return total.get();
    }

    FileTierCleanTask(MSRocksDB mqDB) {
        this.mqDB = mqDB;
        this.iterator = mqDB.newIterator();
        iterator.seek(ROCKS_CACHE_BACK_STORE_KEY.getBytes());
        getSomeTask();
    }

    private void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {

                String key = new String(iterator.key());
                if (!key.startsWith(ROCKS_CACHE_BACK_STORE_KEY)) {
                    scanEnd.set(true);
                }
                String value = new String(iterator.value());
                iterator.next();
                if (value.startsWith(ROCKS_OBJ_META_DELETE_MARKER)) {
                    queue.offer(new Tuple2<>(key, value));
                    total.incrementAndGet();
                    if (queueSize.incrementAndGet() > 2000) {
                        return;
                    }
                    if (total.get() >= ONCE) {
                        scanEnd.set(true);
                        return;
                    }
                }

            }

            if (!scanEnd.get() && !iterator.isValid()) {
                scanEnd.set(true);
            }
        }
    }

    void run() {
        try {
            Tuple2<String, String> task = queue.poll();
            if (null == task) {
                if (scanEnd.get()) {
                    tryEnd();
                } else {
                    getSomeTask();
                    FsUtils.fsExecutor.submit(this::run);
                }
            } else {
                queueSize.decrementAndGet();
                runTask(task.var1, task.var2)
                        .doFinally(s -> FsUtils.fsExecutor.submit(this::run))
                        .subscribe(t -> {
                        }, e -> log.error("", e));
            }
        } catch (Exception e) {
            if (e instanceof MsException && ((MsException) e).getErrCode() == NO_SUCH_BUCKET) {

            } else {
                log.error("run clear task error", e);
            }
            FsUtils.fsExecutor.submit(this::run);
        }

    }

    private void tryEnd() {
        synchronized (iterator) {
            if (!closed.get() && scanEnd.get() && queue.isEmpty() && queueSize.get() == 0L) {
                iterator.close();
                closed.set(true);
                res.onNext(true);
            }
        }
    }

    private Mono<Boolean> runTask(String taskKey, String value) {
        String[] split = taskKey.split("_");
        String storage = split[1];

        long nodeId = Long.parseLong(split[2]);
        String bucket = split[3];
        Node instance = Node.getInstance();
        String fileName = value.substring(ROCKS_OBJ_META_DELETE_MARKER.length());
        if (instance == null) {
            return Mono.just(false);
        }
        StoragePool storagePool = StoragePoolFactory.getStoragePool(storage, bucket);
        if (storagePool == null) {
            return Mono.just(true);
        }
        if (FS_TIER_DEBUG) {
            log.info("start to clean tier file, taskKey {}, fileName {},storage:{}", taskKey, fileName, storage);
        }
        return instance.getInode(bucket, nodeId)
                .flatMap(inode -> {
                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                        return Mono.just(false);
                    }
                    return ErasureClient.restoreDeleteObjectFile(storagePool, new String[]{fileName}, null)
                            .doOnNext(b -> {
                                if (b) {
                                    deleteMq(taskKey);
                                }
                            });
                });
    }

    private void deleteMq(String key) {
        try {
            mqDB.delete(key.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
