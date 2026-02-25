package com.macrosan.filesystem.lifecycle;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ServerConstants.DATA_MERGE_MAPPING;
import static com.macrosan.constants.ServerConstants.SNAPSHOT_SWITCH;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.ZERO_STR;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.lifecycle.FileLifecycleMove.MQ_LIFECYCLE_KEY_PREFIX;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.storage.move.FileSystemRunner.*;

@Log4j2
public class TaskRunner {
    MSRocksDB mqDB;
    final MSRocksIterator iterator;
    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    AtomicLong queueSize = new AtomicLong();
    AtomicBoolean scanEnd = new AtomicBoolean(false);
    AtomicBoolean closed = new AtomicBoolean(false);
    MonoProcessor<Boolean> res = MonoProcessor.create();

    TaskRunner(MSRocksDB mqDB) {
        this.mqDB = mqDB;
        iterator = mqDB.newIterator();
        iterator.seek(MQ_LIFECYCLE_KEY_PREFIX.getBytes());
        getSomeTask();
    }

    private synchronized void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {
                String key = new String(iterator.key());
                if (!key.startsWith(MQ_LIFECYCLE_KEY_PREFIX)) {
                    scanEnd.set(true);
                } else {
                    String value = new String(iterator.value());
                    iterator.next();

                    if (!value.startsWith(ROCKS_OBJ_META_DELETE_MARKER)) {
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
                    }, e -> {
                        if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("syncStamp")) {
                            return;
                        }
                        log.error("", e);
                    });
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
        long fileOffset = jsonValue.getLong("fileOffset");
        long fileSize = jsonValue.getLong("fileSize");
        String targetPoolName = jsonValue.getString("targetPool");

        int retryNum = jsonValue.containsKey("retryNum") ? jsonValue.getInteger("retryNum") : 10;

        String metaKey = jsonValue.getString("metaKey");
        Tuple3<String, String, String> t3 = parseMetaKey(metaKey);
        String vnode = t3.var1;
        String bucket = t3.var2;
        long nodeId = Long.parseLong(t3.var3);
        MonoProcessor<Long> nodeIdMono = MonoProcessor.create();

        if (metaKey.startsWith(ROCKS_VERSION_PREFIX)) {
            getMetaData(metaKey, bucket)
                    .subscribe(meta -> {
//                        log.info("{}", meta);
                        nodeIdMono.onNext(meta.getInode());
                    });
        } else {
            nodeIdMono.onNext(nodeId);
        }

//        log.info("poolPrefix : {}, metaKey : {}, fileName : {}", poolName, metaKey, fileName);
//        log.info("fileOffset : {}, size : {} ,targetPool :{}", fileOffset, fileSize, targetPoolName);

        StoragePool sourcePool = StoragePoolFactory.getStoragePool(poolName, bucket);
        StoragePool targetPool = StoragePoolFactory.getStoragePool(targetPoolName, bucket);

        AtomicBoolean isErrorInode = new AtomicBoolean(false);
        AtomicBoolean fileExist = new AtomicBoolean(false);
        AtomicBoolean updateProcess = new AtomicBoolean(false);


        return nodeIdMono
                .flatMap(nodeId0 -> Node.getInstance().getInode(bucket, nodeId0))
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        isErrorInode.set(true);
                        return Mono.just(false);
                    }

                    return searchInodeData(fileName, inode.getInodeData(), fileOffset, fileSize, sourcePool)
                            .flatMap(t2 -> {
//                                log.info("{} {}",inode.getObjName(), t2);
                                if (t2.var2 == -1) {
                                    return Mono.just(false);
                                }
                                fileExist.set(true);
                                return moveFile(inode, fileName, fileSize, targetPool, vnode, fileOffset, sourcePool);
                            }).flatMap(b -> {
                                if (b) {
                                    return updateInodeData(inode, fileName, targetPool, fileOffset, fileSize, updateProcess)
                                            .doOnNext(b0 -> {
                                                // 设置清理任务
                                                putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + jsonValue.encode());
                                            });
                                }
                                return Mono.just(b);
                            });
                })
                .doOnNext(b -> {
                    if (!fileExist.get() && !b && !isErrorInode.get()) {
                        if (!updateProcess.get()) {
                            if (retryNum <= 0) {
                                putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + jsonValue.encode());
                            } else {
                                jsonValue.put("retryNum", retryNum - 1);
                                putMq(taskKey, jsonValue.encode());
                            }
                        } else {
                            //处理升级，升级期间不删除
                            jsonValue.put("retryNum", retryNum);
                            putMq(taskKey, jsonValue.encode());
                        }
                    }
                });
    }

    public static Mono<MetaData> getMetaData(String metaKey, String bucket) {
        int index = metaKey.indexOf(bucket);
        String key = metaKey.substring(index + bucket.length() + File.separator.length());
        index = key.indexOf(ZERO_STR);
        String[] object = new String[]{key.substring(0, index)};
        String versionId = key.substring(index + ZERO_STR.length());

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnodeId = storagePool.getBucketVnodeId(bucket);
        Map<String, String> bucketInfo = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket).block();
        String snapshotSwitch = bucketInfo.get(SNAPSHOT_SWITCH);
        String[] oldSnapshotMark = new String[]{null};
        if ("on".equals(snapshotSwitch)) {
            // 如果开启了桶快照，则此处object中包含有snapshotmark，重新获取真正的object
            String[] splits = object[0].split(File.separator, 2);
            oldSnapshotMark[0] = splits[0];
            object[0] = splits[1];
        }
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(bucketVnodeId).block();
//        log.info("{} {}",bucket, object[0], versionId, nodeList);
        return ErasureClient.getObjectMetaVersionRes(bucket, object[0], versionId, nodeList, null, oldSnapshotMark[0], null)
                .flatMap(res -> {
                    // fileMeta 中的metaKey对应的元数据不存在，可能是发生快照合并操作，数据被迁移了
                    if (StringUtils.isBlank(oldSnapshotMark[0]) || (!res.var1.deleteMark && !res.var1.equals(NOT_FOUND_META))) {
                        return Mono.just(res);
                    }
                    // 获取迁移映射
                    return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, DATA_MERGE_MAPPING)
                            .flatMap(dataMapping -> {
                                if (oldSnapshotMark[0].compareTo(dataMapping) >= 0) {
                                    return Mono.just(res);
                                }
                                // 查询迁移后的对象元数据
                                return ErasureClient.getObjectMetaVersionRes(bucket, object[0], versionId, nodeList, null, dataMapping, null);
                            }).switchIfEmpty(Mono.just(res));
                }).flatMap(res -> Mono.just(res.var1));
    }

    public static Tuple3<String, String, String> parseMetaKey(String metaKey) {
        String[] split = metaKey.split(File.separator);
        if (metaKey.startsWith(ROCKS_INODE_PREFIX) && split.length == 3) {
            return new Tuple3<>(split[0].substring(ROCKS_INODE_PREFIX.length()), split[1], split[2]);
        } else if (metaKey.startsWith(ROCKS_VERSION_PREFIX) && split.length >= 3) {
            return new Tuple3<>(split[0].substring(ROCKS_VERSION_PREFIX.length()), split[1], "-1");
        }
        throw new MsException(-1, "metaKey parse fail");
    }


    public void putMq(String key, String value) {
        try {
            mqDB.put(key.getBytes(), value.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
