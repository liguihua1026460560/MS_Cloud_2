package com.macrosan.utils.layerMonitor;/**
 * @author niechengxing
 * @create 2026-02-25 14:45
 */

import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;
import static com.macrosan.ec.ErasureClient.getObjectMetaVersionRes;
import static com.macrosan.ec.Utils.getAccessTimeKey;
import static com.macrosan.ec.Utils.getVersionMetaDataKey;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;
import static com.macrosan.filesystem.utils.FsTierUtils.FS_TIER_DEBUG;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;
import static com.macrosan.storage.move.CacheMove.isEnableCacheAccessTimeFlush;
import static com.macrosan.storage.strategy.StorageStrategy.BUCKET_STRATEGY_NAME_MAP;

/**
 * @program: MS_Cloud
 * @description: 监测对象的访问
 * @author: niechengxing
 * @create: 2026-02-25 14:45
 */
public class ObjectAccessCache {
    private static final Logger log = LoggerFactory.getLogger(ObjectAccessCache.class);
    public static long cycleTime = 24L * 60 * 60 * 1000;//默认记录一天内最早的访问
    //    static Map<String, Long> cache = Collections.emptyMap();
    //缓存对象的访问记录
    public static Map<String, Long> accessCache = new ConcurrentHashMap<>();
    public static MsExecutor executor = new MsExecutor(4, 1, new MsThreadFactory("ObjectAccessCache"));
    public static Set<String> processing = new ConcurrentHashSet<>();//记录正在进行更新访问时间的对象，避免重复处理

    public static void init() {
        executor.schedule(ObjectAccessCache::flush, 10, TimeUnit.SECONDS);
        executor.schedule(ObjectAccessCache::queue, 30, TimeUnit.SECONDS);
        BackStoreAccessedData.init();
    }

    public static void addAccessRecord(String bucket, String objName, String versionId) {
        accessCache.computeIfAbsent(bucket + "/" + objName + "/" + versionId, k -> System.currentTimeMillis());
    }

    /**
     * 将缓存中的记录刷到rocksdb中,保存一段时间内所有的访问记录(去重)，10秒后再次触发
     */
    public static void flush() {
        try {
            long curTime = System.currentTimeMillis();
            Map<String, Long> tmp = new HashMap<>(accessCache.size());


            for (String key : new LinkedList<>(accessCache.keySet())) {
                if (curTime - accessCache.get(key) > cycleTime) {
                    accessCache.remove(key);
                } else {
                    tmp.put(key, accessCache.get(key));
                    accessCache.remove(key);
                }
            }

//            Iterator<Map.Entry<String, Long>> iterator = accessCache.entrySet().iterator();
//            while (iterator.hasNext()) {
//                Map.Entry<String, Long> next = iterator.next();
//                //筛除当前记录缓存时间超过当前时间一天的映射
//                if (curTime - next.getValue() > cycleTime) {
//                    iterator.remove();
//                }
//
//            }


            BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, request) -> {
                for (Map.Entry<String, Long> entry : tmp.entrySet()) {

                    //=========1==========
//                    String timeKey = "}" + entry.getValue() + "_" + entry.getKey();//记录10秒内最早的访问时间刷到db中
//                    w.put(timeKey.getBytes(), ZERO_BYTE);//mqDB中记录key中保存对象和最后访问时间，值为空

                    //===========2======
                    String key2 = "}" + entry.getKey();
                    byte[] stampBytes = db.get(key2.getBytes());//从底层db获取时间戳，然后判断是否需要更新时间戳
                    if (stampBytes == null) {
                        w.put(key2.getBytes(), String.valueOf(entry.getValue()).getBytes());//mqDB中记录key中保存对象名称，值为最后访问时间
                    } else {
                        long stamp = Long.parseLong(new String(stampBytes));
                        if (entry.getValue() < stamp || curTime - stamp > cycleTime) {
                            w.put(key2.getBytes(), String.valueOf(entry.getValue()).getBytes());
                        }
                    }
                }
            }).block();
        } finally {
            executor.schedule(ObjectAccessCache::flush, 10, TimeUnit.SECONDS);
        }
    }

    public static void queue() {
        log.debug("updateAccessRecord 30 second task begin!!!");
        Set<String> deleteKeys = new ConcurrentHashSet<>();
        long curStamp = System.currentTimeMillis();
        UnicastProcessor<Tuple2<String, String>> processor = UnicastProcessor.create();
        MonoProcessor<Integer> res = MonoProcessor.create();
        try {
            processor.publishOn(DISK_SCHEDULER).flatMap(tuple2 -> {
                        String realKey = tuple2.var1.substring(1);
                        String stamp = tuple2.var2;

                        String bucket = realKey.substring(0, realKey.indexOf("/"));
                        String object = realKey.substring(realKey.indexOf("/") + 1, realKey.lastIndexOf("/"));
                        String versionId = realKey.substring(realKey.lastIndexOf("/") + 1);
                        return update(bucket, object, versionId, stamp).zipWith(Mono.just(tuple2.var1))
                                .doOnNext(tuple -> {
                                    if (tuple.getT1()) {
                                        log.debug("update bucket:{}, object:{}, versionId:{} access record success", bucket, object, versionId);
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("update bucket:{}, object:{}, versionId:{} access record error", bucket, object, versionId, e);
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
                    }, e -> {
                        log.error("", e);
                        res.onNext(1);
                    }, () -> {
                        res.onNext(1);
                    });


            BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, request) -> {
                try (RocksIterator iterator = w.getRocksIterator(db.newIterator())) {
                    iterator.seek("}".getBytes());
                    while (iterator.isValid() && new String(iterator.key()).startsWith("}")) {
                        String key = new String(iterator.key());
                        int sp = key.indexOf("_");
                        String realKey = key.substring(1);
                        long stamp = Long.parseLong(new String(iterator.value()));

                        String bucket = realKey.substring(0, realKey.indexOf("/"));
//                        String object = realKey.substring(realKey.indexOf("/") + 1, realKey.lastIndexOf("/"));
//                        String versionId = realKey.substring(realKey.lastIndexOf("/") + 1);
                        //根据bucket获取存储策略，确认是否开启数据分层
                        if (!isEnableCacheAccessTimeFlush(BUCKET_STRATEGY_NAME_MAP.get(bucket))) {
                            w.delete(iterator.key());
                            if (!processing.contains(realKey)) {
                                processing.remove(realKey);
                            }
                        } else if (curStamp - stamp > cycleTime) {//当前时间据上次访问时间差一天，如果上次访问一直没更新记录，那么可能会出现记录一直没来的及更新直到超过1天直接不更新吗？一般都是先遍历老的数据，这样基本不会导致某个很早的访问记录一直未被遍历到
                            w.delete(iterator.key());
                            if (!processing.contains(realKey)) {
                                processing.remove(realKey);
                            }
                        } else if (!processing.contains(realKey)) {
                            //满足1天内的记录，直接处理更新
                            //                        map.computeIfAbsent(realKey, k -> stamp);
                            //直接更新记录
                            //如果是数据盘上数据的访问也更新，访问后数据会转存回缓存盘，未被访问的话就会一直在数据池中，也不会进行访问记录
                            //获取对象元数据，在缓存池中存储的对象生成访问记录然后去各自fileMeta所存节点盘上增加进行访问记录并更新fileMeta
                            //然后更新对象元数据中的lastAccessTime
                            processing.add(realKey);
                            processor.onNext(new Tuple2<>(key, String.valueOf(stamp)));
//                            Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> update(bucket, object, versionId, String.valueOf(stamp)))
//                                    .doFinally(s -> {
//                                        processing.remove(realKey);
//                                    })
//                                    .subscribe(b -> {
//                                        //更新完成删除cg_sp0中的记录
//                                        deleteKeys.add(key);
//                                        log.debug("update bucket:{}, object:{}, versionId:{} access record success",bucket, object, versionId);
//                                    }, e -> {
//                                        log.error("update bucket:{}, object:{}, versionId:{} access record error", bucket, object, versionId, e);
//                                    });
                        }
                        iterator.next();
                    }
                    processor.onComplete();
                }
            }).flatMap(r -> res).flatMap(r -> {
                processing.clear();
                log.debug("updateAccessRecord wait delete taskKey");
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
            log.debug("updateAccessRecord 30 second task end!!!");
            //        cache = map;
        } catch (Exception e) {
            processor.onComplete();
            log.error("", e);
        } finally {
            executor.schedule(ObjectAccessCache::queue, 30, TimeUnit.SECONDS);
        }

    }

    public static Mono<Boolean> update(String bucketName, String objName, String versionId, String stamp) {
        /**
         * 更新访问时间
         * 1.首先获取对象的元数据
         * 2.生成访问记录，并获取各fileMeta所在位置
         * 3.删除缓存盘上旧访问记录
         * 3.访问记录保存至各节点磁盘并更新fileMeta
         * 4.更新对象元数据
         */
        long compareStamp = System.currentTimeMillis() - cycleTime;

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucketName, objName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> getObjectMetaVersionRes(bucketName, objName, versionId, nodeList, null))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.var1;
                    if (metaData.equals(MetaData.ERROR_META)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                    }
                    if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || metaData.deleteMarker || StringUtils.isNotEmpty(metaData.lastAccessStamp) && metaData.lastAccessStamp.compareTo(String.valueOf(compareStamp)) > 0) {
                        //上次访问时间据当前时间超过1天则进行更新
                        return Mono.just(true);
                    } else {
                        //处理文件的更新
                        if (metaData.inode > 0) {
                            return updateInodeDataAccessTime(bucketName, bucketVnode, metaData, stamp, tuple2.var2);
                        }

                        if (metaData.storage.startsWith("cache")) {
                            //数据池数据被访问时与数据转存至缓存盘同时进行访问记录的更新，可以不在这里更新
                            //在缓存池中存储的对象
                            //生成访问记录，确认所有fileMeta的位置
                            String accessRecord = getAccessTimeKey(stamp, metaData.fileName);
                            StoragePool dataPool = StoragePoolFactory.getStoragePool(metaData.storage, bucketName);
                            //分段对象暂不考虑分层, 重删是不是只在数据池中才会执行
                            if (metaData.partInfos == null) {
                                //根据fileName获取nodeList
                                metaData.setLastAccessStamp(stamp);//对象元数据修改最近访问时间
                                return dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(metaData.fileName))
                                        .flatMap(nodeList -> {
                                            log.debug("update fileMeta AccessRecord, fileName:{}", metaData.fileName);
                                            //记录最新访问key,并更新对象所有fileMeta的最新访问时间
                                            return ErasureClient.updateFileAccessTime(dataPool, metaData.fileName, nodeList, stamp, metaData.storage);
                                        })
                                        .flatMap(b -> {
                                            if (b) {
                                                //更新对象元数据的最新访问时间
                                                log.debug("update metaData AccessRecord, metaData:{}", metaData);
                                                return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                                                        .flatMap(b0 -> {
                                                            if (b0) {
                                                                return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                                        .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(metaData.bucket, metaData.key, metaData.versionId, migrateVnodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(migrateVnodeList)))
                                                                        .flatMap(resTuple2 -> ErasureClient.updateMetaData(getVersionMetaDataKey(migrateVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark), metaData.clone(), resTuple2.getT2(), null, resTuple2.getT1().var2))
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
                                                        });
                                            } else {
                                                return Mono.just(false);
                                            }
                                        });
                            }
                        }
                        return Mono.just(true);
                    }
                });


    }


    public static Mono<Boolean> updateInodeDataAccessTime(String bucket, String vnode, MetaData metaData, String stamp, Tuple2<ErasureServer.PayloadMetaType, MetaData>[] getMetaRes) {
        Node nodeInstance = Node.getInstance();
        if (nodeInstance == null) {
            return Mono.just(false);
        }
        long nodeId = metaData.inode;
        if (FS_TIER_DEBUG) {
            log.info("updateInodeDataAccessTime bucket:{} vnode:{} nodeId:{}", bucket, vnode, nodeId);
        }
        long[] atime = new long[1];
//        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
//        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
//        String bucketVnode = bucketVnodeIdTuple.var1;
//        String migrateVnode = bucketVnodeIdTuple.var2;
        return nodeInstance.getInode(bucket, nodeId)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        return Mono.just(false);
                    }
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        return Mono.just(true);
                    }
                    atime[0] = inode.getAtime();
                    return updateInodeDataAccessTime(inode.getInodeData(), nodeInstance, bucket, stamp);
                })
                .flatMap(res -> {
                    if (res && InodeUtils.needUpdateAtime(atime[0])) {
                        int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                        return nodeInstance.updateInodeTime(nodeId, bucket, Long.parseLong(stamp), stampNano, true, false, false)
                                .map(i -> i.getLinkN() != ERROR_INODE.getLinkN());
                    }
                    return Mono.just(res);
                })
//                .flatMap(b -> {
//                    if (b) {
//                        //更新对象元数据的最新访问时间
//                        log.debug("update metaData AccessRecord, metaData:{}", metaData);
//                        metaData.setLastAccessStamp(stamp);
//                        String versionMetaDataKey = getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
//                        return Mono.just(StringUtils.isNotEmpty(migrateVnode))
//                                .flatMap(b0 -> {
//                                    if (b0) {
//                                        return metaStoragePool.mapToNodeInfo(migrateVnode)
//                                                .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(metaData.bucket, metaData.key, metaData.versionId, migrateVnodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(migrateVnodeList)))
//                                                .flatMap(resTuple2 -> ErasureClient.updateMetaData(versionMetaDataKey, metaData.clone(), resTuple2.getT2(), null, resTuple2.getT1().var2, true))
//                                                .timeout(Duration.ofSeconds(30))
//                                                .map(r -> r == 1)
//                                                .onErrorReturn(false);
//                                    }
//                                    return Mono.just(true);
//                                })
//                                .flatMap(b1 -> {
//                                    if (b1) {
//                                        return metaStoragePool.mapToNodeInfo(bucketVnode)
//                                                .flatMap(nodeList -> ErasureClient.updateMetaData(versionMetaDataKey, metaData.clone(), nodeList, null, getMetaRes, true))
//                                                .timeout(Duration.ofSeconds(30))
//                                                .map(r -> r == 1)
//                                                .onErrorReturn(false);
//                                    }
//                                    return Mono.just(false);
//                                });
//                    }
//                    return Mono.just(false);
//                })
                .timeout(Duration.ofSeconds(59))
                .onErrorResume(e -> {
                    log.info("updateInodeDataAccessTime error {} {} ", bucket, nodeId, e);
                    return Mono.just(false);
                });
    }

    public static Mono<Boolean> updateInodeDataAccessTime(List<Inode.InodeData> inodeDataList, Node node, String bucketName, String stamp) {
        return Flux.fromStream(inodeDataList.stream())
                .flatMap(inodeData -> {
                    if (inodeData.getFileName().startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        if (FS_TIER_DEBUG) {
                            log.info("updateInodeDataAccessTime chunk file, bucket:{} fileName:{}", bucketName, inodeData.getFileName());
                        }
                        return node.getChunk(inodeData.getFileName())
                                .flatMap(chunkFile -> {
                                    if (chunkFile.getSize() == ChunkFile.ERROR_CHUNK.getSize()) {
                                        log.info("updateInodeDataAccessTime chunk file fail, bucket:{} fileName:{}", bucketName, inodeData.getFileName());
                                        return Mono.just(false);
                                    }

                                    return updateInodeDataAccessTime(chunkFile.getChunkList(), node, bucketName, stamp);
                                });
                    }
                    if (inodeData.storage.startsWith("cache")) {
                        StoragePool dataPool = StoragePoolFactory.getStoragePool(inodeData.storage, bucketName);
                        return dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(inodeData.fileName))
                                .flatMap(nodeList -> {
                                    if (FS_TIER_DEBUG) {
                                        log.debug("update fileMeta AccessRecord, bucketName:{},fileName:{}", bucketName, inodeData.fileName);
                                    }
                                    //记录最新访问key,并更新对象所有fileMeta的最新访问时间
                                    return ErasureClient.updateFileAccessTime(dataPool, inodeData.fileName, nodeList, stamp, inodeData.storage);
                                });
                    }
                    return Mono.just(true);
                })
                .onErrorResume(e -> {
                    log.error("updateInodeDataAccessTime error bucket:{} inodeDataList:{}", bucketName, inodeDataList, e);
                    return Mono.just(false);
                })
                .collectList()
                .map(list -> list.stream().allMatch(b -> b));
    }

}

