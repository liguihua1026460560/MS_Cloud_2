package com.macrosan.filesystem.lifecycle;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.SysConstants.ROCKS_OBJ_META_DELETE_MARKER;
import static com.macrosan.constants.SysConstants.ROCKS_VERSION_PREFIX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.lifecycle.FileLifecycleMove.MQ_LIFECYCLE_KEY_PREFIX;
import static com.macrosan.filesystem.lifecycle.TaskRunner.getMetaData;
import static com.macrosan.filesystem.lifecycle.TaskRunner.parseMetaKey;

@Log4j2
public class ClearTaskRunner {
    MonoProcessor<Boolean> res = MonoProcessor.create();
    MSRocksIterator iterator;
    MSRocksDB mqDB;
    AtomicBoolean closed = new AtomicBoolean(false);
    AtomicBoolean scanEnd = new AtomicBoolean(false);
    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    AtomicLong queueSize = new AtomicLong();

    ClearTaskRunner(MSRocksDB mqDB) {
        this.mqDB = mqDB;
        this.iterator = mqDB.newIterator();
        iterator.seek(MQ_LIFECYCLE_KEY_PREFIX.getBytes());
        getSomeTask();
    }

    private void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {
                String key = new String(iterator.key());
                if (!key.startsWith(MQ_LIFECYCLE_KEY_PREFIX)) {
                    scanEnd.set(true);
                } else {
                    String value = new String(iterator.value());
                    iterator.next();

                    if (value.startsWith(ROCKS_OBJ_META_DELETE_MARKER)) {
                        queue.offer(new Tuple2<>(key, value.substring(ROCKS_OBJ_META_DELETE_MARKER.length())));
                        if (queueSize.incrementAndGet() > 2000) {
                            return;
                        }
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
                    DISK_SCHEDULER.schedule(this::run);
                }
            } else {
                queueSize.decrementAndGet();
                runTask(task.var1, task.var2)
                        .doFinally(s -> DISK_SCHEDULER.schedule(this::run))
                        .subscribe(t -> {
                        }, e -> log.error("", e));
            }
        } catch (Exception e) {
            if (e instanceof MsException && ((MsException) e).getErrCode() == NO_SUCH_BUCKET) {

            } else {
                log.error("run clear task error", e);
            }
            DISK_SCHEDULER.schedule(this::run);
        }

    }

    private synchronized void tryEnd() {
        synchronized (iterator) {
            if (!closed.get() && scanEnd.get() && queue.isEmpty() && queueSize.get() == 0L) {
                iterator.close();
                closed.set(true);
                res.onNext(true);
            }
        }
    }

    private Mono<Boolean> runTask(String taskKey, String value) {
        String[] s = taskKey.split("_");
        String poolName = s[1];
        String fileName = taskKey.substring(taskKey.indexOf(poolName) + poolName.length() + 1);

        JsonObject jsonValue = new JsonObject(value);
        String metaKey = jsonValue.getString("metaKey");
        Tuple3<String, String, String> t3 = parseMetaKey(metaKey);
        String vnode = t3.var1;
        String bucket = t3.var2;
        long nodeId = Long.parseLong(t3.var3);
        MonoProcessor<Long> nodeIdMono = MonoProcessor.create();

        if (metaKey.startsWith(ROCKS_VERSION_PREFIX)) {
            getMetaData(metaKey, bucket)
                    .subscribe(meta -> {
                        nodeIdMono.onNext(meta.getInode());
                    });
        } else {
            nodeIdMono.onNext(nodeId);
        }


        StoragePool pool = StoragePoolFactory.getStoragePool(poolName, bucket);

        return nodeIdMono
                .flatMap(nodeId0 -> Node.getInstance().getInode(bucket, nodeId))
                .flatMap(inode -> {
                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                        return Mono.just(false);
                    }
                    return ErasureClient.deleteObjectFile(pool, new String[]{fileName}, null)
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
