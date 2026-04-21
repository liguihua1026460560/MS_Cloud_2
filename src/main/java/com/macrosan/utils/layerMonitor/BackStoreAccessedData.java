package com.macrosan.utils.layerMonitor;/**
 * @author niechengxing
 * @create 2026-02-28 10:16
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.FsTierUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.Utils.getVersionMetaDataKey;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.utils.FsTierUtils.FS_TIER_DEBUG;
import static com.macrosan.filesystem.utils.FsTierUtils.getBackStoreKey;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.storage.StorageOperate.PoolType.CACHE;
import static com.macrosan.storage.StoragePoolFactory.getStrategyNameByDataPrefix;
import static com.macrosan.storage.move.CacheMove.isEnableCacheAccessTimeFlush;
import static com.macrosan.storage.move.CacheMove.needDeleteDataInDataPool;
import static com.macrosan.storage.strategy.StorageStrategy.BUCKET_STRATEGY_NAME_MAP;

/**
 * @program: MS_Cloud
 * @description: 记录开启分层后数据池中被访问的对象并迁移回缓存池
 * @author: niechengxing
 * @create: 2026-02-28 10:16
 */
public class BackStoreAccessedData {
    private static final Logger log = LoggerFactory.getLogger(BackStoreAccessedData.class);
    //这里不处理过期

    public static Map<String, Long> needBackStoreData = new ConcurrentHashMap<>();//这里是否要存访问的时间戳呢
    public static MsExecutor executor = new MsExecutor(4, 1, new MsThreadFactory("BackStoreAccessedData"));
    public static Set<String> processing = new ConcurrentHashSet<>();//记录正在进行回迁处理的对象，避免重复处理


    /**
     * 需要等待某些组件初始化完成后再初始化
     */
    public static void init() {
        executor.schedule(BackStoreAccessedData::flush, 10, TimeUnit.SECONDS);
        executor.schedule(BackStoreAccessedData::queue, 30, TimeUnit.SECONDS);
    }

    /**
     * 记录需要回迁的对象
     *
     * @param bucket
     * @param objName
     * @param versionId
     */
    public static void addNeedBackStoreRecord(String bucket, String objName, String versionId) {
        needBackStoreData.computeIfAbsent(bucket + "/" + objName + "/" + versionId, k -> System.currentTimeMillis());//假设这里加上时间戳
    }

    /**
     * 将缓存中记录的要回迁的对象刷到rocksdb中
     */
    public static void flush() {
        try {
            Map<String, Long> tmp = new HashMap<>();
            for (Map.Entry<String, Long> entry : needBackStoreData.entrySet()) {
                tmp.put(entry.getKey(), entry.getValue());
                needBackStoreData.remove(entry.getKey());
            }

            BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, request) -> {
                for (Map.Entry<String, Long> entry : tmp.entrySet()) {

                    //将其记录到rocksdb
                    String key = "%" + entry.getKey();
                    //检查是否存在，存在的话比较记录的时间戳大小
                    byte[] stampBytes = db.get(key.getBytes());
                    if (stampBytes == null) {
                        w.put(key.getBytes(), String.valueOf(entry.getValue()).getBytes());
                    } else {
                        long stamp = Long.parseLong(new String(stampBytes));
                        if (entry.getValue() > stamp) {//存时间戳方便更新回迁时间
                            /**这里如果要存时间戳的话有可能会产生多次put，如果不存时间戳，那么只要有对象的key就会进行转存，转存完之后删除记录的key，
                             * 此时转存到了缓存池但是直到下次分层前对其进行的访问都不会增加该对象需要转存的记录key，应该不会出现频繁记录更新key的情况
                             * 而且只要有一个记录key使对象转存成功之后，后续的记录key在转存时查到对象数据存在缓存池之后也就不会再进行转存处理
                             */
                            w.put(key.getBytes(), String.valueOf(entry.getValue()).getBytes());

                        }
                        //时间戳比当前存的值小就不更新了
                    }
                }
            }).block();
        } finally {
            executor.schedule(BackStoreAccessedData::flush, 10, TimeUnit.SECONDS);
        }
    }

    public static void queue() {
        log.debug("backStore 30 second task begin!!!");
        Set<String> deleteKeys = new ConcurrentHashSet<>();
        UnicastProcessor<Tuple2<String, String>> processor = UnicastProcessor.create();
        MonoProcessor<Integer> res = MonoProcessor.create();

        //这里开始扫描mq中的key进行回迁处理
        //扫描当前节点cg_sp0中保存的待回迁记录，然后进行消息分发或者rocket分发到其他节点异步处理，或者直接使用DISK_SCHEDULER进行异步并发处理，处理完成就删除
        //在前端包中增加一个状态map，表明某些回迁记录正在开始处理中，无需重复发布任务进行处理
        try {
            processor.publishOn(DISK_SCHEDULER).flatMap(tuple2 -> {
                        String realKey = tuple2.var1.substring(1);
                        String stamp = tuple2.var2;

                        String bucket = realKey.substring(0, realKey.indexOf("/"));
                        String object = realKey.substring(realKey.indexOf("/") + 1, realKey.lastIndexOf("/"));
                        String versionId = realKey.substring(realKey.lastIndexOf("/") + 1);
                        return backStore(bucket, object, versionId, stamp).zipWith(Mono.just(tuple2.var1))
                                .doOnNext(tuple -> {
                                    if (tuple.getT1()) {
                                        log.debug("back store bucket:{}, object:{}, versionId:{} success", bucket, object, versionId);
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("back store bucket:{}, object:{}, versionId:{} error", bucket, object, versionId, e);
                                })
                                .doFinally(s -> {
                                    processing.remove(realKey);
                                });
                    })
                    .subscribe(tuple2 -> {
                        if (tuple2.getT1()) {
                            //删除cg_sp0中的记录
                            deleteKeys.add(tuple2.getT2());
                        }
//                    log.debug("back store bucket:{}, object:{}, versionId:{} success", bucket, object, versionId);
                    }, e -> {
                        log.error("", e);
                        res.onNext(1);
                    }, () -> {
                        res.onNext(1);
                    });
            BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, request) -> {
                try (RocksIterator iterator = w.getRocksIterator(db.newIterator())) {
                    iterator.seek("%".getBytes());
                    while (iterator.isValid() && new String(iterator.key()).startsWith("%")) {
                        String key = new String(iterator.key());
                        int sp = key.indexOf("_");
                        String realKey = key.substring(1);
                        String stamp = new String(iterator.value());

                        String bucket = realKey.substring(0, realKey.indexOf("/"));
                        //根据bucket获取存储策略，确认是否开启数据分层
                        if (!isEnableCacheAccessTimeFlush(BUCKET_STRATEGY_NAME_MAP.get(bucket))) {
                            w.delete(iterator.key());
                            if (processing.contains(realKey)) {
                                processing.remove(realKey);
                            }
                        } else if (!processing.contains(realKey)) {
                            //发布move任务，完成时清除processing中的记录
                            //这里能不能使用rockset将任务分发到其他节点进行处理
                            processing.add(realKey);
                            processor.onNext(new Tuple2<>(key, stamp));

                        }
                        iterator.next();
                    }
                    processor.onComplete();
                }
            }).flatMap(r -> res).flatMap(r -> {
                processing.clear();
                log.debug("backStore wait delete taskKey");
                return BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, request) -> {
                    for (String key : deleteKeys) {
                        log.debug("delete taskKey:{}", key);
                        w.delete(key.getBytes());
                    }
                });
            }).doOnError(e -> {
                processor.onComplete();
                log.error("", e);
            }).block();
            log.debug("backStore 30 second task end!!!");
        } catch (Exception e) {
            processor.onComplete();
            log.error("back store fail!", e);
        } finally {
            executor.schedule(BackStoreAccessedData::queue, 30, TimeUnit.SECONDS);
        }
    }


    public static Mono<Boolean> backStore(String bucket, String object, String versionId, String stamp) {
        //先获取对象元数据然后再重新存储
        //不在同一个盘，如果回迁过程保持fileName不变，后续删除数据池上的旧数据应该也不会出现因为fileName相同导致的将新数据误删的情况
        //所以这里可以使用缓存池下刷的逻辑去进行重新存储
        //1.获取当前策略下的缓存池，根据桶获取当前策略

        boolean[] isDup = new boolean[2];
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucket, object);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        List<String> oldFileName = new ArrayList<>();
//        List<String> oldDedupFileName = new ArrayList<>();//这里应该没用
        List<String> delFileName = new ArrayList<>();
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(infoList -> RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK)
                        .collectList()
                        .doOnNext(keyValues -> keyValues.forEach(kv -> {
                            if (kv.getKey().equals(CURRENT_SNAPSHOT_MARK)) {
                                currentSnapshotMark[0] = kv.getValueOrElse(null);
                                return;
                            }
                            if (kv.getKey().equals(SNAPSHOT_LINK)) {
                                snapshotLink[0] = kv.getValueOrElse(null);
                            }
                        }))
                        .map(b -> infoList))
                .flatMap(bucketNodeList -> ErasureClient.getObjectMetaVersionRes(bucket, object, versionId, bucketNodeList, null, currentSnapshotMark[0], snapshotLink[0]))
                .timeout(Duration.ofSeconds(60))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.var1;
                    if (metaData.equals(MetaData.ERROR_META)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                    }
                    if (metaData.inode > 0) {
                        return fsBackStore(bucket, metaData, stamp);
                    }

                    if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || metaData.deleteMarker || metaData.storage.startsWith("cache") || metaData.partInfos != null || (metaData.endIndex + 1) > 262144L) {//如果对象已经迁移到了另一个策略中，则该对象不再进行重新存储处理//分段对象不处理
                        return Mono.just(true);
                    } else {
                        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                        });
                        String md5 = sysMetaMap.get(ETAG);
                        StoragePool oldPool = StoragePoolFactory.getStoragePool(metaData);
                        StorageOperate dataOperate = new StorageOperate(CACHE, object, Long.MAX_VALUE);//直接获取与数据池同一个策略内的缓存池
                        StoragePool cachePool = StoragePoolFactory.getStoragePool(dataOperate, oldPool);
                        String newPoolType = cachePool.getVnodePrefix();
                        return StoragePoolFactory.getDeduplicateByBucketName(bucket)
                                .doOnNext(enableDedup -> isDup[0] = enableDedup)
//                                .doOnNext(enableDedup -> {
//                                    if (metaData.partUploadId != null) {
//                                        for (PartInfo partInfo : metaData.partInfos) {
//                                            oldDedupFileName.add(partInfo.fileName);
//                                        }
//                                    } else {
//                                        oldDedupFileName.add(metaData.fileName);//开启了重删记录要删除的重删记录
//                                    }
//                                })
                                .flatMap(enableDedup -> {
                                    String vnodeId = metaStoragePool.getBucketVnodeId(md5);
                                    return metaStoragePool.mapToNodeInfo(vnodeId);
                                })
                                .flatMap(nodeList -> {
                                    //这里如果对象经过了重删处理就需要找到其实际保存数据的fileName，然后再去进行get数据操作
                                    if (StringUtils.isNotEmpty(metaData.duplicateKey)) {
                                        return ErasureClient.getDeduplicateMeta(md5, metaData.storage, metaData.duplicateKey, nodeList, null)
                                                .timeout(Duration.ofSeconds(60))
                                                .flatMap(dupMeta -> {
                                                    if (StringUtils.isNotEmpty(dupMeta.fileName)) {
                                                        return Mono.just(dupMeta.fileName);
                                                    } else {
                                                        return Mono.just(metaData.fileName);
                                                    }
                                                });
                                    } else {
                                        return Mono.just(metaData.fileName);
                                    }
                                })
                                .flatMap(fileName -> {
                                    //回迁至缓存池无论是否开启重删都不进行重删处理
                                    return move(metaData, fileName, cachePool, bucketVnode, stamp)
                                            .flatMap(b -> {
                                                if (b) {
                                                    log.debug("begin update metadata!");
                                                    metaData.setStorage(cachePool.getVnodePrefix());
                                                    metaData.setLastAccessStamp(stamp);
                                                    if (StringUtils.isNotEmpty(metaData.duplicateKey)) {
                                                        delFileName.add(metaData.duplicateKey);
                                                        metaData.setDuplicateKey(null);
                                                    } else {
                                                        metaData.setDuplicateKey(null);
                                                        delFileName.add(fileName);
                                                    }
                                                    //更新对象元数据并且删除数据池上的旧数据
                                                    return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                                                            .flatMap(notEmpty -> {
                                                                if (notEmpty) {
                                                                    String migrateMetaKey = Utils.getVersionMetaDataKey(migrateVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
                                                                    return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                                            .flatMap(migrateNodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucket, object, versionId, migrateNodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(migrateNodeList)))
                                                                            .flatMap(resTuple -> ErasureClient.updateMetaData(migrateMetaKey, metaData.clone(), resTuple.getT2(), null, resTuple.getT1().var2))
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .map(r -> r == 1)
                                                                            .onErrorReturn(false);
                                                                }
                                                                return Mono.just(true);
                                                            })
                                                            .flatMap(b1 -> {
                                                                if (b1) {
                                                                    return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                                            .flatMap(nodeList -> ErasureClient.updateMetaData(getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark), metaData.clone(), nodeList, null, tuple2.var2))
                                                                            .timeout(Duration.ofSeconds(30))
                                                                            .map(r -> r == 1)
                                                                            .onErrorReturn(false);
                                                                }
                                                                return Mono.just(false);
                                                            })
                                                            //删除数据池上旧的数据
                                                            .flatMap(b2 -> {
                                                                if (b2) {
                                                                    //获取当前数据池所在策略名
                                                                    String strategy = getStrategyNameByDataPrefix(oldPool.getVnodePrefix());
                                                                    if (needDeleteDataInDataPool(strategy)) {//这里默认不判断是否删源，直接默认保留数据池上的旧数据, delData默认为false
                                                                        log.debug("delete file {} {}", Arrays.toString(delFileName.toArray(new String[0])), metaData.versionNum);
                                                                        boolean deDup = false;
                                                                        Set<String> allFile = new HashSet<>();
                                                                        for (String name : delFileName) {
                                                                            if (name.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                                                                deDup = true;
                                                                                break;
                                                                            }
                                                                        }
                                                                        for (String files : delFileName) {
                                                                            if (!files.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                                                                allFile.add(files);
                                                                            }
                                                                        }
                                                                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(oldPool.getVnodePrefix());
                                                                        if (deDup) {
                                                                            return ErasureClient.deleteDedupObjectFile(metaStoragePool, delFileName.toArray(new String[0]), null, false)
                                                                                    .flatMap(b1 -> {
                                                                                        if (b1 && !allFile.isEmpty()) {
                                                                                            return ErasureClient.restoreDeleteObjectFile(oldPool, allFile.toArray(new String[0]), null)
                                                                                                    .flatMap(r -> {
                                                                                                        if (!r) {//返false表示有文件在被读取无法进行删除
                                                                                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                                                                                    .put("fileName", Json.encode(allFile.toArray(new String[0])))
                                                                                                                    .put("storage", oldPool.getVnodePrefix())
                                                                                                                    .put("poolQueueTag", poolQueueTag);
                                                                                                            ObjectPublisher.basicPublish(CURRENT_IP, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
                                                                                                        }
                                                                                                        return Mono.just(true);
                                                                                                    });
                                                                                        }
                                                                                        return Mono.just(b1);
                                                                                    });
                                                                        }
                                                                        return ErasureClient.restoreDeleteObjectFile(oldPool, delFileName.toArray(new String[0]), null)
                                                                                .flatMap(r -> {
                                                                                    if (!r) {//返false表示有文件在被读取无法进行删除
                                                                                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                                                                .put("fileName", Json.encode(delFileName.toArray(new String[0])))
                                                                                                .put("storage", oldPool.getVnodePrefix())
                                                                                                .put("poolQueueTag",
                                                                                                        poolQueueTag);
                                                                                        ObjectPublisher.basicPublish(CURRENT_IP, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
                                                                                    }
                                                                                    return Mono.just(true);
                                                                                })
                                                                                .doOnNext(b1 -> {
                                                                                    if (b1) {
                                                                                        log.debug("back store object to {} success, bucket:{}, object:{}, versionId:{}", newPoolType, bucket, object, versionId);
                                                                                    }
                                                                                });
                                                                    } else {
                                                                        return Mono.just(true);
                                                                    }
                                                                } else {
                                                                    log.error("update meta storage error {} {} {} {}", metaData.bucket, metaData.key, metaData.versionId, metaData.storage);
                                                                    return Mono.just(false);
                                                                }
                                                            });
                                                } else {
                                                    log.error("back store object to {} error, bucket:{}, object:{}, versionId:{}", newPoolType, bucket, object, versionId);
                                                    return Mono.just(false);
                                                }
                                            });
                                });

                    }
                });

    }


    public static Mono<Boolean> fsBackStore(String bucket, MetaData metaData, String stamp) {

        //处理文件的多段数据情况
        Node node = Node.getInstance();
        if (node == null) {
            return Mono.just(false);
        }
        if (FS_TIER_DEBUG) {
            log.info("fsBackStore {} {} {} {}", bucket, metaData.key, metaData.inode, stamp);
        }
        return node.getInode(bucket, metaData.inode)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        return Mono.just(false);
                    }
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        return Mono.just(true);
                    }
                    return fsBackStore(bucket, inode.getInodeData(), node, stamp, metaData.inode, 0L);
                })
                .timeout(Duration.ofSeconds(59))
                .onErrorResume(e -> {
                    log.info("fsBackStore error {} {} {} {}", bucket, metaData.key, metaData.inode, stamp, e);
                    return Mono.just(false);
                });

    }

    public static Mono<Boolean> fsBackStore(String bucket, List<Inode.InodeData> inodeDataList, Node node, String stamp, long nodeId, long curOffset) {
        List<Mono<Boolean>> chunkRes = new LinkedList<>();
        for (Inode.InodeData inodeData : inodeDataList) {
            Mono<Boolean> recordRes = null;
            if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                long finalCurOffset = curOffset;
                if (FS_TIER_DEBUG) {
                    log.info("fsBackStore {} {} {} {}", bucket, inodeData.fileName, nodeId, stamp);
                }
                recordRes = Mono.just(true)
                        .flatMap(b -> node.getChunk(inodeData.getFileName()))
                        .flatMap(chunkFile -> {
                            if (chunkFile.getSize() == ERROR_CHUNK.getSize()) {
                                return Mono.just(false);
                            }

                            return fsBackStore(bucket, chunkFile.getChunkList(), node, stamp, nodeId, finalCurOffset);
                        });
            } else if (inodeData.storage.startsWith("data")) {
                //超过256k的数据块暂不进行回迁
                if (inodeData.getSize() > 262144L) {
                    recordRes = Mono.just(true);
                } else {
                    recordRes = recordTask(bucket, inodeData.getFileName().replace("/split/", ""), inodeData.storage, stamp, nodeId, curOffset, inodeData.getSize());
                }

            } else if (inodeData.storage.startsWith("cache")) {
                //如果数据已经在缓存池上了就不进行回迁处理了
                recordRes = Mono.just(true);
            }
            if (recordRes != null) {
                chunkRes.add(recordRes);
            }
            curOffset += inodeData.getSize();
        }
        if (chunkRes.isEmpty()) {
            return Mono.just(true);
        } else {
            return Flux.merge(Flux.fromStream(chunkRes.stream()), 1, 1)
                    .collectList()
                    .map(l -> l.stream().anyMatch(t -> t));
        }
    }


    public static Mono<Boolean> recordTask(String bucket, String fileName, String storage, String stamp, long nodeId, long fileOffSet, long fileSize) {
        String requestId = RandomStringUtils.randomAlphanumeric(32);
        String taskKey = getBackStoreKey(storage, nodeId, bucket, requestId);
        String value = FsTierUtils.buildTierRecordValue(fileName, fileOffSet, fileSize, stamp);
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, taskKey.getBytes());
            if (oldValue == null) {
                writeBatch.put(taskKey.getBytes(), value.getBytes());
            }
        };
        return BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), taskKey.hashCode(), consumer)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false);
    }


    /**
     * 回迁保持fileName不变
     *
     * @param metaData
     * @param fileName
     * @param cachePool
     * @param vnode
     * @return
     */
    public static Mono<Boolean> move(MetaData metaData, String fileName, StoragePool cachePool, String vnode, String stamp) {
        StoragePool oldPool = StoragePoolFactory.getStoragePool(metaData);
        List<Tuple3<String, String, String>> getNodeList = oldPool.mapToNodeInfo(oldPool.getObjectVnodeId(metaData)).block();

        Encoder ecEncodeHandler = cachePool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();

        int ontPutBytes = cachePool.getK() * cachePool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(oldPool.getK());
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(cachePool.getVnodePrefix());

        next.subscribe(t -> {
            if (t.var1 > 0) {
                exceptGetNum.decrementAndGet();
                waitEncodeBytes.addAndGet(t.var2);
                int n = waitEncodeBytes.get() / ontPutBytes; //原始数据均分至新盘存k个
                exceptPutNum.addAndGet(n);
                waitEncodeBytes.addAndGet(-n * ontPutBytes);
            } else {
                exceptPutNum.decrementAndGet();//一个块上传完成减1
            }

            while (exceptPutNum.get() == 0 && exceptGetNum.get() <= 0) {
                for (int j = 0; j < oldPool.getK(); j++) {
                    streamController.onNext(-1L);
                }
                exceptGetNum.addAndGet(oldPool.getK());//继续get数据
            }
        });

        ECUtils.getObject(oldPool, fileName, false, 0, metaData.endIndex, metaData.endIndex + 1,
                        getNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    ecEncodeHandler.put(bytes);
//                    streamController.onNext(1L);
                });

        List<Tuple3<String, String, String>> putNodeList = cachePool.mapToNodeInfo(cachePool.getObjectVnodeId(metaData)).block();
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
                            .put("compression", cachePool.getCompression())
                            .put("metaKey", metaKey)
                            .put("dataPool", oldPool.getVnodePrefix())
                            .put("bucket", metaData.bucket)
                            .put("object", metaData.key)
                            .put("versionId", metaData.versionId)
                            .put("storage", cachePool.getVnodePrefix())
                            .put("fileSize", String.valueOf(metaData.endIndex + 1));
                    Optional.ofNullable(metaData.snapshotMark).ifPresent(v -> msg.put("snapshotMark", v));

                    if (isEnableCacheAccessTimeFlush(cachePool)) {
                        msg.put("lastAccessStamp", stamp);//将对象的get访问时间作为缓存池中该数据的访问记录
                    }
                    //这里需要补充上传文件需要的属性参数，比如增加访问记录属性并在缓存盘上条件访问记录

                    CryptoUtils.putCryptoInfoToMsg(metaData.getCrypto(), secretKey.get(), msg);//这里使用重新生成的secretKey进行加密，生成的代码在上面

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

//        Limiter limiter = new Limiter(new TaskRunner.MoveMsRequest(), putNodeList.size(), targetPool.getK());
        AtomicInteger putNum = new AtomicInteger();

        responseInfo.responses.doOnNext(s -> {
            if (ERROR.equals(s.var2)) {
                errorChunksList.add(s.var1);
            }

            if (putNum.incrementAndGet() == putNodeList.size()) {
                next.onNext(new Tuple2<>(-1, 0));
                putNum.set(errorChunksList.size());
            }

        }).doOnComplete(() -> {
            if (responseInfo.successNum == cachePool.getK() + cachePool.getM()) {
//                QuotaRecorder.addCheckBucket(metaData.bucket);
                res.onNext(true);
            } else if (responseInfo.successNum >= cachePool.getK()) {
//                QuotaRecorder.addCheckBucket(metaData.bucket);
                res.onNext(true);

                //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("errorChunksList", Json.encode(errorChunksList))
                        .put("storage", cachePool.getVnodePrefix())
                        .put("bucket", metaData.bucket)
                        .put("object", metaData.key)
                        .put("fileName", metaData.fileName)
                        .put("versionId", metaData.versionId)
                        .put("stamp", metaData.stamp)
                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                        .put("lastAccessStamp", stamp)//写回缓存池的数据需要增加写入访问记录
                        .put("poolQueueTag", poolQueueTag)
                        .put("fileOffset", "");
                Optional.ofNullable(metaData.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                CryptoUtils.putCryptoInfoToMsg(metaData.crypto, secretKey.get(), errorMsg);

                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
            } else {
                res.onNext(false);
                //响应成功数量达不到k,发布回退消息，删掉成功的节点上的文件
                SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                errorMsg.put("bucket", metaData.bucket);
                errorMsg.put("object", metaData.key);
                errorMsg.put("fileName", metaData.fileName);
                errorMsg.put("storage", cachePool.getVnodePrefix());
                errorMsg.put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
            }
        }).doOnError(e -> log.error("", e)).subscribe();

        return res;
    }
}

