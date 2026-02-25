package com.macrosan.storage.move;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.DedupMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.aggregator.CachePoolAggregateContainerWrapper;
import com.macrosan.storage.aggregation.manager.CachePoolContainerManager;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.coder.Limiter;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.quota.QuotaRecorder;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.Utils.ZERO_STR;
import static com.macrosan.ec.Utils.getDeduplicatMetaKey;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_OBJECT_FILE;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;
import static com.macrosan.storage.move.CacheMove.MQ_KEY_PREFIX_V1;
import static com.macrosan.storage.move.CacheMove.getKeyPrefix;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class TaskRunner {
    MonoProcessor<Boolean> res = MonoProcessor.create();
    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    final MSRocksIterator iterator;
    AtomicBoolean closed = new AtomicBoolean(false);
    AtomicBoolean scanEnd = new AtomicBoolean(false);
    StoragePool pool;
    MSRocksDB mqDB;
    AtomicLong queueSize = new AtomicLong();
    NameSpace ns;
    ConcurrentHashSet<CachePoolAggregateContainerWrapper> waitFlushContainer = new ConcurrentHashSet<>();

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong appendToAggregateSuccess = new AtomicLong();
    private final AtomicLong appendToAggregateFail = new AtomicLong();
    private final AtomicLong error = new AtomicLong();
    private final AtomicLong normal = new AtomicLong();
    private final AtomicLong delete = new AtomicLong();
    String taskId;

    TaskRunner(StoragePool pool, MSRocksDB mqDB, NameSpace ns) {
        this.pool = pool;
        this.mqDB = mqDB;
        this.ns = ns;
        this.iterator = mqDB.newIterator();
        taskId = UUID.randomUUID().toString();
        iterator.seek(getKeyPrefix(pool.getVnodePrefix()).getBytes());
        getSomeTask();
    }

    private synchronized void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {
                String key = new String(iterator.key());
                if (!key.startsWith(getKeyPrefix(pool.getVnodePrefix()))) {
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

    private synchronized boolean tryEnd() {
        synchronized (iterator) {
            if (closed.get()) {
                return true;
            }
            if (!closed.get() && scanEnd.get() && queue.isEmpty() && queueSize.get() == 0L) {
                Iterator<CachePoolAggregateContainerWrapper> cIterator = waitFlushContainer.iterator();
                while (cIterator.hasNext()) {
                    CachePoolAggregateContainerWrapper c = cIterator.next();
                    if (c.isEmpty() || !c.isEmpty() && !c.isImmutable()) {
                        c.clear();
                        cIterator.remove();
                    } else if (c.isImmutable() && !c.isFlushing()) {
                        c.flush(true).doOnError(e -> {
                            if (!(e instanceof IllegalStateException)) {
                                log.error("", e);
                                c.clear();
                            }
                        }).doOnSuccess(b -> c.clear()).subscribe();
                    }
                }
                if (waitFlushContainer.isEmpty()) {
                    if (appendToAggregateFail.get() != 0 || appendToAggregateSuccess.get() != 0) {
                        log.debug("【underbrush】task:{} runner end, total:{} error:{} normal:{} appendToAggregateSuccess:{}, appendToAggregateFail:{} deleted:{}",
                                taskId, total.get(), error.get(), normal.get(),
                                appendToAggregateSuccess.get(), appendToAggregateFail.get(),
                                delete.get());
                    }
                    iterator.close();
                    closed.set(true);
                    res.onNext(true);
                }
            }
            return false;
        }
    }

    void run() {
        AtomicBoolean decrement = new AtomicBoolean(false);
        try {
            Tuple2<String, String> task = queue.poll();
            if (null == task) {
                if (scanEnd.get()) {
                    if (!tryEnd()) {
                        DISK_SCHEDULER.schedule(this::run, 100, TimeUnit.MILLISECONDS);
                    }
                } else {
                    getSomeTask();
                    DISK_SCHEDULER.schedule(this::run);
                }
            } else {
                total.incrementAndGet();
                decrement.set(true);
                runTask(task.var1, task.var2)
                        .timeout(Duration.ofMinutes(15))
                        .doFinally(s -> {
                            normal.incrementAndGet();
                            queueSize.decrementAndGet();
                            decrement.set(false);
                            DISK_SCHEDULER.schedule(this::run);
                        })
                        .subscribe(t -> {
                        }, e -> {
                            error.incrementAndGet();
                            if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("syncStamp")) {
                                return;
                            }
                            if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("Append error")) {
                                return;
                            }
                            log.error("", e);
                        });
            }
        } catch (Exception e) {
            error.incrementAndGet();
            if (decrement.get()) {
                queueSize.decrementAndGet();
            }
            if (e instanceof MsException && ((MsException) e).getErrCode() == NO_SUCH_BUCKET) {

            } else {
                log.error("run task error", e);
            }
            DISK_SCHEDULER.schedule(this::run);
        }

    }

    private Mono<Boolean> runTask(String taskKey, String v) {
        int retryNum;
        String value;
        StoragePool[] dataPool = new StoragePool[]{null};
        boolean[] isMoveData = new boolean[]{true};
        String key = getTaskKey(taskKey);
        long[] fileOffset = new long[1];
        long[] fileSize = new long[1];
        v = handleTaskValue(v, fileOffset, fileSize);
        boolean newVersion = taskKey.startsWith(MQ_KEY_PREFIX_V1) ? true : false;
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

        if (v.startsWith(ROCKS_LATEST_KEY)) {
            retryNum = Integer.parseInt(v.substring(ROCKS_LATEST_KEY.length(), ROCKS_LATEST_KEY.length() + 1));
            value = v.substring(ROCKS_LATEST_KEY.length() + 1);
        } else if (v.startsWith(ROCKS_FILE_META_PREFIX)) {
            int end = v.substring(ROCKS_FILE_META_PREFIX.length()).indexOf(ROCKS_FILE_META_PREFIX)
                    + ROCKS_FILE_META_PREFIX.length();
            String pool = v.substring(ROCKS_FILE_META_PREFIX.length(), end);
            dataPool[0] = StoragePoolFactory.getStoragePool(pool, bucekt);
            value = v.substring(end + ROCKS_FILE_META_PREFIX.length());
            retryNum = 10;
        } else {
            retryNum = 10;
            value = v;
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


        if (dataPool[0] == null) {
            StorageOperate operate = new StorageOperate(DATA, "", Long.MAX_VALUE);
            dataPool[0] = StoragePoolFactory.getStoragePool(operate, bucekt);
        } else {
            isMoveData[0] = false;
        }

        String bucketVnodeId;
        String migrateVnodeId;
        String[] oldSnapshotMark = new String[]{null};
        Map<String, String> bucketInfo = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucekt).block();
        String snapshotSwitch = bucketInfo.get(SNAPSHOT_SWITCH);
        boolean isFS = bucketInfo.containsKey("fsid");
        if ("on".equals(snapshotSwitch)) {
            // 如果开启了桶快照，则此处object中包含有snapshotmark，重新获取真正的object
            String[] splits = object[0].split(File.separator, 2);
            oldSnapshotMark[0] = splits[0];
            object[0] = splits[1];
        }
        try {
            Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucekt, object[0]);
            bucketVnodeId = bucketVnodeIdTuple.var1;
            migrateVnodeId = bucketVnodeIdTuple.var2;
        } catch (MsException e) {
            // 只有确认桶不存在时才将缓存池中的数据块走删除流程
            Long exists = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).exists(bucekt);
            if (exists == 0) {
                putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                return Mono.just(true);
            } else {
                return Mono.just(false);
            }
        }
        // 判断小文件是否需要进行聚合操作
        AtomicBoolean aggregate = new AtomicBoolean(false);
        CachePoolAggregateContainerWrapper activeContainer;
        if (ns != null && !isFS) {
            aggregate.set(true);
            activeContainer = CachePoolContainerManager.getInstance().getActiveContainer(ns);
            if (activeContainer != null) {
                waitFlushContainer.add(activeContainer);
            }
        } else {
            activeContainer = null;
        }
        if (aggregate.get() && activeContainer == null) {
            appendToAggregateFail.incrementAndGet();
            return Mono.just(false);
        }

        return bucketPool.mapToNodeInfo(bucketVnodeId)
                .flatMap(nodeList -> {
                    Mono<reactor.util.function.Tuple2<Boolean, Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>>> tupleRes;
                    if (!isInodeKey) {
                        return ErasureClient.getObjectMetaVersionRes(bucekt, object[0], versionId[0], nodeList, null, oldSnapshotMark[0], null)
                                .flatMap(res -> {
                                    // fileMeta 中的metaKey对应的元数据不存在，可能是发生快照合并操作，数据被迁移了
                                    if (StringUtils.isBlank(oldSnapshotMark[0]) || (!res.var1.deleteMark && !res.var1.equals(NOT_FOUND_META))) {
                                        MetaData cur = res.var1;
                                        if (cur.isAvailable() && isFS) {
                                            // redis数据获取不建议优化成异步获取，可能导致第一次下刷失败，重试下刷影响性能，可参考SERVER-1881
                                            int nodeNum = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).keys("*").size();
                                            long now = System.currentTimeMillis();
                                            long metaStamp = Long.parseLong(cur.stamp);
                                            if (now - metaStamp < nodeNum * 5 * 1000L) {
                                                return Mono.delay(Duration.ofMillis(nodeNum * 5 * 1000L))
                                                        .then(Mono.just(res));
                                            }
                                        }
                                        return Mono.just(res);
                                    }
                                    // 获取迁移映射
                                    return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucekt, DATA_MERGE_MAPPING)
                                            .flatMap(dataMapping -> {
                                                if (oldSnapshotMark[0].compareTo(dataMapping) >= 0) {
                                                    return Mono.just(res);
                                                }
                                                // 查询迁移后的对象元数据
                                                return ErasureClient.getObjectMetaVersionRes(bucekt, object[0], versionId[0], nodeList, null, dataMapping, null);
                                            }).switchIfEmpty(Mono.just(res));
                                })
                                .flatMap(res0 -> {
                                    MetaData metaData = res0.var1;
                                    if (!metaData.isAvailable() || StringUtils.isNotEmpty(metaData.getDuplicateKey())) {
                                        return Mono.just(res0);
                                    }
                                    // 获取 duplicateKey
                                    return StoragePoolFactory.getDeduplicateByBucketName(bucekt)
                                            .filter(enableDedup -> enableDedup)
                                            .doOnNext(enableDedup -> {
                                                String md5 = new JsonObject(metaData.getSysMetaData()).getString(ETAG);
                                                String[] split = metaData.getFileName().split("_");
                                                String requestId = split[split.length - 1];
                                                StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                                                String dedupVnode = dedupPool.getBucketVnodeId(md5);
                                                String duplicate = Utils.getDeduplicatMetaKey(dedupVnode, md5, metaData.storage, requestId);
                                                metaData.setDuplicateKey(duplicate);
                                            }).thenReturn(res0);
                                })
                                .flatMap(res0 -> {
                                    MetaData metaData = res0.var1;
                                    // 加密、重删对象不走聚合
                                    if (StringUtils.isNotEmpty(metaData.getCrypto()) || StringUtils.isNotEmpty(metaData.duplicateKey)) {
                                        aggregate.set(false);
                                    }
                                    // cifs文件判断，meta中包含inode值
                                    if (metaData.getInode() != 0L) {
                                        aggregate.set(false);
                                        FileSystemRunner fileSystemRunner = new FileSystemRunner(pool, this);
                                        return fileSystemRunner.fileMove(taskKey, value, dataPool, isMoveData, bucekt, retryNum, vnode[0], metaData.getInode(), fileOffset[0], fileSize[0]);
                                    }

                                    return aws3FileMove(taskKey, retryNum, value, dataPool, isMoveData, bucekt, res0, metaData, vnode[0], aggregate.get(), activeContainer)
                                            .flatMap(tuple2 -> {
                                                if (aggregate.get()) {
                                                    return Mono.just(true);
                                                }

                                                if (tuple2.getT1()) {
                                                    MetaData meta = tuple2.getT2().var1;
                                                    tuple2.getT2().var1.setStorage(dataPool[0].getVnodePrefix());
                                                    if (StringUtils.isNotEmpty(meta.duplicateKey)) {
                                                        String suffix = meta.duplicateKey;
                                                        String dedupKey = suffix.replace(pool.getVnodePrefix(), dataPool[0].getVnodePrefix());
                                                        tuple2.getT2().var1.setDuplicateKey(dedupKey);
                                                    }
                                                    String metaKey = Utils.getVersionMetaDataKey(nodeList.get(0).var3, meta.bucket, meta.key, meta.versionId, meta.snapshotMark);
                                                    return Mono.just(StringUtils.isNotEmpty(migrateVnodeId))
                                                            .flatMap(b -> {
                                                                if (StringUtils.isNotEmpty(meta.duplicateKey)) {
                                                                    String md5 = meta.duplicateKey.split(File.separator)[1];
                                                                    StoragePool md5Pool = StoragePoolFactory.getMetaStoragePool(md5);
                                                                    String dedupVnode = md5Pool.getBucketVnodeId(md5);
                                                                    String firstKey = getDeduplicatMetaKey(dedupVnode, md5, dataPool[0].getVnodePrefix());
                                                                    return md5Pool.mapToNodeInfo(dedupVnode)
                                                                            .flatMap(getNodeList -> ErasureClient.getDeduplicateMeta(md5, meta.storage, firstKey, getNodeList, null))
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .flatMap(dedupMeta -> {
                                                                                if (StringUtils.isNotEmpty(dedupMeta.fileName)) {
                                                                                    tuple2.getT2().var1.setFileName(dedupMeta.fileName);
                                                                                } else if (DedupMeta.ERROR_DEDUP_META.equals(dedupMeta)) {
                                                                                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "get dedupKey failed");
//
                                                                                }

                                                                                return Mono.just(b);
                                                                            });
                                                                }
                                                                return Mono.just(b);
                                                            })
                                                            .flatMap(b -> {
                                                                if (b) {
                                                                    String migrateMetaKey = Utils.getVersionMetaDataKey(migrateVnodeId, meta.bucket, meta.key, meta.versionId, meta.snapshotMark);
                                                                    return bucketPool.mapToNodeInfo(migrateVnodeId)
                                                                            .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucekt, object[0], versionId[0], migrateVnodeList, null, meta.snapshotMark, null).zipWith(Mono.just(migrateVnodeList)))
                                                                            .flatMap(resTuple2 -> ErasureClient.updateMetaData(migrateMetaKey, tuple2.getT2().var1.clone(), resTuple2.getT2(), null, resTuple2.getT1().var2))
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .map(r -> r == 1)
                                                                            .onErrorReturn(false);
                                                                }
                                                                return Mono.just(true);
                                                            })
                                                            .flatMap(b -> {
                                                                if (b) {
                                                                    return ErasureClient.updateMetaData(metaKey, tuple2.getT2().var1, nodeList, null, tuple2.getT2().var2)
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .map(r -> r == 1)
                                                                            .onErrorReturn(false)
                                                                            .flatMap(c -> ErasureClient.getObjectMetaVersionRes(bucekt, object[0], versionId[0], nodeList, null, meta.snapshotMark, null)
                                                                                    .timeout(Duration.ofSeconds(30))
                                                                                    .flatMap(res -> {
                                                                                        List<String> needDeleteFile = new LinkedList<>();
                                                                                        MetaData aliveMeta = res.var1;

                                                                                        if (aliveMeta.equals(NOT_FOUND_META) || aliveMeta.deleteMark) {
                                                                                            if (meta.partUploadId == null) {
                                                                                                if (StringUtils.isNotEmpty(tuple2.getT2().var1.duplicateKey)) {
                                                                                                    needDeleteFile.add(tuple2.getT2().var1.duplicateKey);
                                                                                                }

                                                                                            } else {
                                                                                                for (PartInfo partInfo : tuple2.getT2().var1.partInfos) {
                                                                                                    if (StringUtils.isNotEmpty(tuple2.getT2().var1.duplicateKey)) {
                                                                                                        needDeleteFile.add(partInfo.deduplicateKey);
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                        if (needDeleteFile.size() > 0) {
                                                                                            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(meta.bucket);
                                                                                            return ErasureClient.deleteDedupObjectFile(metaStoragePool, needDeleteFile.toArray(new String[0]), null, false);
                                                                                        }

                                                                                        return Mono.just(c);
                                                                                    })
                                                                            )
                                                                            .doOnNext(c -> {
                                                                                if (c) {
                                                                                    putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                                                                                }
                                                                            });
                                                                }
                                                                return Mono.just(false);
                                                            });
                                                }

                                                return Mono.just(tuple2.getT1());
                                            });
                                });
                    } else {
                        aggregate.set(false);
                        FileSystemRunner fileSystemRunner = new FileSystemRunner(pool, this);
                        return fileSystemRunner.fileMove(taskKey, value, dataPool, isMoveData, bucekt, retryNum, vnode[0], nodeId[0], fileOffset[0], fileSize[0]);
                    }
                });

    }

    // 兼容旧版本taskKey的获取
    public static String getTaskKey(String taskKey) {
        String key;
        if (taskKey.startsWith(MQ_KEY_PREFIX_V1)) {
            key = taskKey.substring(0, taskKey.lastIndexOf("_"));
        } else {
            // 为了兼容版本升级
            String[] splite = taskKey.split("_");
            // 没有考虑s3文件名包含 '_' 的情况，最新版本直接走上面的逻辑，不会进入这里
            if (splite.length == 4) {
                key = taskKey.substring(0, taskKey.lastIndexOf("_"));
            } else {
                key = taskKey;
            }
        }
        return key;
    }

    private static String handleTaskValue(String v, long[] fileOffset, long[] fileSize) {
        if (v.contains(ROCKS_FILE_META_PREFIX) && !v.startsWith(ROCKS_FILE_META_PREFIX)) {
            String[] split = v.split(ROCKS_FILE_META_PREFIX);
            // 在s3复制还在缓存池中的文件端文件时，数据块的key会追加“#...”随机字符，兼容该情况时下刷任务value的处理
            if (split.length == 2) {
                // s3文件下刷重试
                v = split[0] + ROCKS_FILE_META_PREFIX + split[1];
            } else if (split.length == 3) {
                // 文件端文件首次下刷，下刷重试
                v = split[0];
                fileOffset[0] = Long.parseLong(split[1]);
                fileSize[0] = Long.parseLong(split[2]);
            } else {
                // s3文件首次下刷
                v = split[0] + ROCKS_FILE_META_PREFIX + split[1];
                fileOffset[0] = Long.parseLong(split[2]);
                fileSize[0] = Long.parseLong(split[3]);
            }
        }
        return v;
    }

    private Mono<reactor.util.function.Tuple2<Boolean, Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>>> aws3FileMove
            (String taskKey, int retryNum, String value, StoragePool[] dataPool, boolean[] isMoveData, String
                    bucekt, Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]> res, MetaData metaData, String vnode,
             boolean aggregate, CachePoolAggregateContainerWrapper activeContainer) {
        if (!metaData.equals(ERROR_META) && !metaData.deleteMark
                && value.equals(metaData.fileName) && pool.getVnodePrefix().equalsIgnoreCase(metaData.storage)) {
            if (isMoveData[0]) {
                if (!StoragePoolFactory.inStorageMAP(dataPool[0].getVnodePrefix()) && StoragePoolFactory.inNoUsedMap(dataPool[0].getVnodePrefix()) && StoragePoolFactory.inStorageMAP(dataPool[0].getUpdateECPrefix())) {
                    dataPool[0] = StoragePoolFactory.getStoragePool(dataPool[0].getUpdateECPrefix(), bucekt);
                }
                // 小文件聚合流程
                if (aggregate && metaData.getCrypto() == null && StringUtils.isEmpty(metaData.duplicateKey)) {
                    return activeContainer.append(taskKey, res).doOnNext(b -> {
                        if (b) {
                            appendToAggregateSuccess.incrementAndGet();
                        } else {
                            appendToAggregateFail.incrementAndGet();
                        }
                    }).zipWith(Mono.just(res));
                }
                // 正常下刷流程
                return move(metaData, dataPool[0], StringUtils.isNotEmpty(metaData.duplicateKey), vnode)
                        .doOnNext(b -> {
                            if (b) {
                                putMq(taskKey, ROCKS_FILE_META_PREFIX + dataPool[0].getVnodePrefix() + ROCKS_FILE_META_PREFIX + value);
                            }
                        }).zipWith(Mono.just(res));
            } else {
                return Mono.just(true).zipWith(Mono.just(res));
            }
        } else {
            Mono<Boolean> result;
            if (metaData.equals(ERROR_META)) {
                result = Mono.just(false);
            } else if (!isMoveData[0] && !value.equals(metaData.fileName)) {
                // 元数据中fileName和value不同，则说明该对象被覆盖或删除了
                // 同时如果isMoveData为false，表明该数据块之前从缓存池已经迁移到数据池了，则需要删除该数据块
                result = ErasureClient.deleteObjectFile(dataPool[0], new String[]{value}, null);
            } else {
                result = Mono.just(true);
            }
            return result.flatMap(b -> {
                if (b) {
                    delete.incrementAndGet();
                    if (retryNum <= 0) {
                        putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                    } else {
                        String prefix = ROCKS_LATEST_KEY + (retryNum - 1);
                        putMq(taskKey, prefix + value);
                    }
                }
                res.var1 = metaData;
                return Mono.just(false).zipWith(Mono.just(res));
            });
        }
    }

    public static class MoveMsRequest extends MsHttpRequest {
        MoveMsRequest() {
            super(null);
        }

        @Override
        public HttpServerRequest fetch(long amount) {
            return this;
        }
    }

    //缓存下刷时处理重删信息
    private Mono<Boolean> move(MetaData metaData, StoragePool targetPool, boolean deduplicate, String vnode) {

        String storage = targetPool.getVnodePrefix();
        String cacheName = pool.getVnodePrefix();

        if (deduplicate) {
            String md5 = metaData.duplicateKey.split(File.separator)[1];

            String dedupKey = metaData.duplicateKey.replace(cacheName, storage);

            StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
            String dedupVnode = dedupPool.getBucketVnodeId(md5);
            String firstKey = Utils.getDeduplicatMetaKey(dedupVnode, md5, storage);
            List<Tuple3<String, String, String>> getNodeList = dedupPool.mapToNodeInfo(dedupVnode).block();

            //获取新位置重删索引信息
            return ErasureClient.getDeduplicateMeta(md5, storage, firstKey, getNodeList, null)
                    .timeout(Duration.ofSeconds(30))
                    .flatMap(meta -> {
                        if (StringUtils.isNotEmpty(meta.fileName)) {
                            return ErasureClient.getAndUpdateDeduplicate(dedupPool, dedupKey, getNodeList, null, null, metaData, md5)
                                    .timeout(Duration.ofSeconds(30))
                                    .doOnError(b -> Mono.just(false))
                                    .flatMap(b -> {
                                        if (b) {
                                            metaData.setFileName(meta.fileName);
                                            metaData.duplicateKey = dedupKey;
                                        }
                                        return Mono.just(b);
                                    });

                        } else {
                            if (meta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                return Mono.just(false);
                            }
                            return move(metaData, targetPool, vnode)
                                    .flatMap(b -> {
                                        if (b) {
                                            metaData.setStorage(storage);
                                            return ErasureClient.getAndUpdateDeduplicate(dedupPool, dedupKey, getNodeList, null, null, metaData, md5)
                                                    .flatMap(b1 -> {
                                                        if (b1) {
                                                            metaData.duplicateKey = dedupKey;
                                                        } else {
                                                            metaData.setStorage(cacheName);
                                                        }
                                                        return Mono.just(b1);
                                                    });
                                        }
                                        return Mono.just(false);
                                    });
                        }
                    });

        } else {
            return move(metaData, targetPool, vnode);
        }
    }

    private Mono<Boolean> move(MetaData metaData, StoragePool targetPool, String vnode) {
        List<Tuple3<String, String, String>> getNodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(metaData)).block();

        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();

        ECUtils.getObject(pool, metaData.fileName, false, 0, metaData.endIndex, metaData.endIndex + 1,
                getNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    ecEncodeHandler.put(bytes);
                    streamController.onNext(1L);
                });

        List<Tuple3<String, String, String>> putNodeList = targetPool.mapToNodeInfo(targetPool.getObjectVnodeId(metaData)).block();
        String metaKey = Utils.getVersionMetaDataKey(vnode, metaData.getBucket(), metaData.getKey(), metaData.versionId, metaData.snapshotMark);
        AtomicReference<String> secretKey = new AtomicReference<>();
        if (CryptoUtils.checkCryptoEnable(metaData.getCrypto())) {
            secretKey.set(CryptoUtils.generateSecretKey(metaData.getCrypto()));
        }
        List<UnicastProcessor<Payload>> publisher = putNodeList.stream()
                .map(t -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", metaData.fileName)
                            .put("lun", t.var2)
                            .put("vnode", t.var3)
                            .put("compression", targetPool.getCompression())
                            .put("metaKey", metaKey);

                    CryptoUtils.putCryptoInfoToMsg(metaData.getCrypto(), secretKey.get(), msg);

                    return msg;
                })
                .map(msg0 -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create();
                    processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));
                    return processor;
                })
                .collect(Collectors.toList());

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            ecEncodeHandler.data()[index].subscribe(bytes -> {
                        publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        publisher.get(index).onComplete();
                    });
        }

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, putNodeList);

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>();
        Limiter limiter = new Limiter(new MoveMsRequest(), putNodeList.size(), targetPool.getK());

        responseInfo.responses.doOnNext(s -> {
            if (s.var2.equals(ERROR)) {
                errorChunksList.add(s.var1);
                limiter.request(s.var1, Long.MAX_VALUE);
            } else {
                limiter.request(s.var1, Long.MAX_VALUE);
            }

        }).doOnComplete(() -> {
            if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                QuotaRecorder.addCheckBucket(metaData.bucket);
                res.onNext(true);
            } else if (responseInfo.successNum >= targetPool.getK()) {
                QuotaRecorder.addCheckBucket(metaData.bucket);
                res.onNext(true);
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
                //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("errorChunksList", Json.encode(errorChunksList))
                        .put("storage", targetPool.getVnodePrefix())
                        .put("bucket", metaData.bucket)
                        .put("object", metaData.key)
                        .put("fileName", metaData.fileName)
                        .put("versionId", metaData.versionId)
                        .put("stamp", metaData.stamp)
                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                        .put("poolQueueTag", poolQueueTag)
                        .put("fileOffset", "");
                Optional.ofNullable(metaData.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                CryptoUtils.putCryptoInfoToMsg(metaData.crypto, secretKey.get(), errorMsg);

                for (int index : errorChunksList) {
                    if (targetPool.getK() + targetPool.getM() <= index) {
                        log.error("publish error {} {}", targetPool, errorChunksList);
                    }
                }

                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
            } else {
                res.onNext(false);
            }
        }).doOnError(e -> log.error("", e)).subscribe();

        return res;
    }

    public void putMq(String key, String value) {
        try {
            mqDB.put(key.getBytes(), value.getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
