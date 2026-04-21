package com.macrosan.storage.move;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.ServerConstants.DATA_MERGE_MAPPING;
import static com.macrosan.constants.ServerConstants.SNAPSHOT_SWITCH;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.ZERO_STR;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.storage.move.CacheMove.getKeyPrefix;
import static com.macrosan.storage.move.TaskRunner.getTaskKey;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ClearTaskRunner {
    MonoProcessor<Boolean> res = MonoProcessor.create();
    StoragePool pool;
    MSRocksDB mqDB;
    final MSRocksIterator iterator;
    AtomicBoolean closed = new AtomicBoolean(false);
    AtomicBoolean scanEnd = new AtomicBoolean(false);

    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    AtomicLong queueSize = new AtomicLong();

    public static final String DEL_CACHE_AND_DATA_VALUE_PREFIX = ROCKS_OBJ_META_DELETE_MARKER + ROCKS_OBJ_META_DELETE_MARKER;

    ClearTaskRunner(StoragePool pool, MSRocksDB mqDB) {
        this.pool = pool;
        this.mqDB = mqDB;
        this.iterator = mqDB.newIterator();
        iterator.seek(getKeyPrefix(pool.getVnodePrefix()).getBytes());
        getSomeTask();
    }

    private void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {
                String key = new String(iterator.key());
                if (!key.startsWith(getKeyPrefix(pool.getVnodePrefix()))) {
                    scanEnd.set(true);
                } else {
                    String value = new String(iterator.value());
                    iterator.next();
                    if (value.startsWith(ROCKS_OBJ_META_DELETE_MARKER)) {
                        queue.offer(new Tuple2<>(key, value));
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
        String key = getTaskKey(taskKey);
        int indexInodeKeyPrefix = key.indexOf(ROCKS_INODE_PREFIX);
        boolean isInodeKey = taskKey.split("/")[0].contains(ROCKS_INODE_PREFIX);
        int index = key.indexOf(File.separator);
        String[] vnode = new String[1];
        if (isInodeKey) {
            vnode[0] = key.substring(indexInodeKeyPrefix + ROCKS_INODE_PREFIX.length(), index);
        } else {
            int start = key.indexOf(ROCKS_VERSION_PREFIX);
            vnode[0] = key.substring(start + ROCKS_VERSION_PREFIX.length(), index);
        }

        key = key.substring(index + File.separator.length());
        index = key.indexOf(File.separator);
        String bucekt = key.substring(0, index);

        String fileName;
        if (value.startsWith(DEL_CACHE_AND_DATA_VALUE_PREFIX)) {
            // ~~开头的value后续不会存在，此处只是为了兼容之前已存在~~开头的数据
            int valueIndex = value.indexOf(File.separator);
            fileName = value.substring(valueIndex);
        } else {
            // 只删除缓冲池中数据块
            fileName = value.substring(ROCKS_OBJ_META_DELETE_MARKER.length());
        }
        String[] object = new String[1];
        String[] versionId = new String[1];
        long[] nodeId = new long[1];
        if (!isInodeKey) {
            key = key.substring(index + File.separator.length());
            index = key.indexOf(ZERO_STR);
            object[0] = key.substring(0, index);
            key = key.substring(index + ZERO_STR.length());
            versionId[0] = key;
        } else {
            key = key.substring(index + File.separator.length());
            nodeId[0] = Long.parseLong(key);
        }

        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucekt);

        String bucketVnodeId;
        String bucketMigrateVnodeId;
        String[] oldSnapshotMark = new String[]{null};
        String snapshotSwitch = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucekt, SNAPSHOT_SWITCH).block();
        if ("on".equals(snapshotSwitch)) {
            // 如果开启了桶快照，则此处object中包含有snapshotmark，重新获取真正的object
            String[] splits = object[0].split(File.separator, 2);
            oldSnapshotMark[0] = splits[0];
            object[0] = splits[1];
        }
        try {
            final Tuple2<String, String> tuple = bucketPool.getBucketVnodeIdTuple(bucekt, object[0]);
            bucketVnodeId = tuple.var1;
            bucketMigrateVnodeId = tuple.var2;
        } catch (MsException e) {
            // 只有确认桶不存在时才将缓存池中的数据块走删除流程
            Long exists = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).exists(bucekt);
            if (exists == 0) {
                return ErasureClient.deleteObjectFile(pool, new String[]{fileName}, null)
                        .doOnNext(b -> {
                            if (b) {
                                deleteMq(taskKey);
                            }
                        });
            } else {
                return Mono.just(false);
            }
        }

        // 桶散列迁移过程中，若开启了双写，获取新分片上的元数据更新的结果
        Mono<Tuple3<Boolean, Boolean, Boolean>> migrationResults = StringUtils.isNotBlank(bucketMigrateVnodeId) ? (obtainMigrationResults(bucketPool, bucketMigrateVnodeId, bucekt, object[0], versionId[0], oldSnapshotMark[0])
                .flatMap(metaData -> {
                    boolean error = metaData.equals(ERROR_META);
                    boolean normalMove = !metaData.deleteMark && fileName.equals(metaData.fileName)
                            && !pool.getVnodePrefix().equalsIgnoreCase(metaData.storage);
                    boolean overWrite = !fileName.equals(metaData.fileName);
                    return Mono.just(new Tuple3<>(error, normalMove, overWrite));
                })) : Mono.just(new Tuple3<>(false, true, true));
        if (!isInodeKey) {
            return bucketPool.mapToNodeInfo(bucketVnodeId)
//                    .flatMap(nodeList -> ErasureClient.getObjectMetaVersion(bucekt, object[0], versionId[0], nodeList, null, oldSnapshotMark[0], null).zipWith(Mono.just(nodeList)))
                    .flatMap(nodeList -> ErasureClient.getObjectMetaVersionRes(bucekt, object[0], versionId[0], nodeList, null, oldSnapshotMark[0], null).zipWith(Mono.just(nodeList)))
                    .flatMap(tuple -> {
                        String vnode1 = tuple.getT2().get(0).var3;
                        String versionKey = Utils.getVersionMetaDataKey(vnode1, bucekt, object[0], versionId[0], oldSnapshotMark[0]);
                        return ErasureClient.updateMetaData(versionKey, tuple.getT1().var1, tuple.getT2(), null, tuple.getT1().var2)
                                .timeout(Duration.ofSeconds(60))
                                .flatMap(n -> {
                                    if (n == 1) {
                                        return Mono.just(tuple.getT1().var1).zipWith(Mono.just(tuple.getT2()))
                                                .flatMap(tuple2 -> {
                                                    if (StringUtils.isBlank(oldSnapshotMark[0]) || (!tuple2.getT1().deleteMark && !tuple2.getT1().equals(NOT_FOUND_META))) {
                                                        return Mono.just(tuple2.getT1());
                                                    }
                                                    // 获取迁移映射
                                                    return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucekt, DATA_MERGE_MAPPING)
                                                            .flatMap(dataMapping -> {
                                                                if (oldSnapshotMark[0].compareTo(dataMapping) >= 0) {
                                                                    return Mono.just(tuple2.getT1());
                                                                }
                                                                // 查询迁移后的对象元数据
                                                                return ErasureClient.getObjectMetaVersion(bucekt, object[0], versionId[0], tuple2.getT2(), null, dataMapping, null);
                                                            }).switchIfEmpty(Mono.just(tuple2.getT1()));
                                                })
                                                .zipWith(migrationResults)
                                                .flatMap(tuple2 -> {
                                                    MetaData metaData = tuple2.getT1();
                                                    boolean error = metaData.equals(ERROR_META);
                                                    boolean normalMove = !metaData.deleteMark && fileName.equals(metaData.fileName)
                                                            && !pool.getVnodePrefix().equalsIgnoreCase(metaData.storage);
                                                    boolean overWrite = !fileName.equals(metaData.fileName);

                                                    // 桶散列迁移开启双写过程中，综合新旧分片上元数据的更新结果来判断是否需要将数据块删除
                                                    error = error || tuple2.getT2().var1;
                                                    normalMove = normalMove && tuple2.getT2().var2;
                                                    overWrite = overWrite && tuple2.getT2().var3;

                                                    if (!error && (overWrite || normalMove)) {
                                                        return ErasureClient.deleteObjectFile(pool, new String[]{fileName}, null)
                                                                .doOnNext(b -> {
                                                                    if (b) {
                                                                        deleteMq(taskKey);
                                                                    }
                                                                });
                                                    } else {
                                                        if (!error) {
                                                            deleteMq(taskKey);
                                                        }

                                                        return Mono.just(false);
                                                    }
                                                });
                                    } else {
//                                        log.info("can not update metaData versionNum");
                                        return Mono.just(false);//更新versionNum失败不再删除，说明此时存在其他更新元数据的操作，暂不删除等待下次删除
                                    }
                                });
                    });
        } else {
            Node instance = Node.getInstance();
            if (instance == null) {
                return Mono.just(false);
            }
            return instance.getInode(bucekt, nodeId[0])
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
    }

    /**
     * 获取对象元数据在新分片上的结果
     */
    private static Mono<MetaData> obtainMigrationResults(StoragePool pool, String migrateVnodeId, String bucket, String object, String versionId, String snapshotMark) {
        return pool.mapToNodeInfo(migrateVnodeId)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucket, object, versionId, nodeList, null, snapshotMark, null))
                .flatMap(tuple2 -> Mono.just(tuple2.var1));
    }

    private void deleteMq(String key) {
        try {
            mqDB.delete(key.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
