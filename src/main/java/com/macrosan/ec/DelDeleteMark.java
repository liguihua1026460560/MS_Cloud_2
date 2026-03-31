package com.macrosan.ec;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.lifecycle.LifecycleService;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.ROCKS_OBJ_META_DELETE_MARKER;
import static com.macrosan.ec.ErasureClient.deleteObjectFileWithData;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

/**
 * TODO
 *
 * @Author chengyinfeng
 * @Date 2022/1/12 15:58
 */
@Log4j2
public class DelDeleteMark {

    public static void init() {
        deleteDelMark();
        DelFileRunner.init();
    }

    private static void deleteDelMark() {
        UnicastProcessor<Tuple2<byte[], byte[]>> res = UnicastProcessor.create();
        Flux.just(1).publishOn(DISK_SCHEDULER)
                .flatMap(b -> iterator(res))
                .flatMap(DelDeleteMark::deleteMeta)
                .doFinally(b -> {
                    res.cancel();
                    DISK_SCHEDULER.schedule(DelDeleteMark::deleteDelMark, 10L, TimeUnit.SECONDS);
                })
                .subscribe();
    }

    private static Flux<Tuple2<byte[], byte[]>> iterator(UnicastProcessor<Tuple2<byte[], byte[]>> res) {
        String iteratorKey = ROCKS_OBJ_META_DELETE_MARKER;
        next(iteratorKey, iteratorKey, res);
        return res;
    }

    private static Mono<Boolean> deleteMeta(Tuple2<byte[], byte[]> tuple) {
        String key = new String(tuple.var1);
        if (key.charAt(1) == '!') {
            return deletePartMeta(tuple);
        } else {
            return deleteObjMeta(tuple);
        }
    }

    private static Mono<Boolean> deleteObjMeta(Tuple2<byte[], byte[]> tuple) {
        BatchRocksDB.RequestConsumer consumer = (db, w, r) -> {
            try {
                w.delete(tuple.var1);
            } catch (Exception ignored) {
            }
        };
        MetaData metaData = Json.decodeValue(new String(tuple.var2), MetaData.class);
        long currStamp = System.currentTimeMillis();
        long versionStamp;
        if (DAVersionUtils.countHyphen(metaData.versionNum) == 2) {
            String versionStr = metaData.versionNum.split("-")[1];
            versionStamp = Long.parseLong(versionStr.substring(19, 32));
        } else {
            String versionStr = metaData.versionNum.split("-")[0];
            versionStamp = Long.parseLong(versionStr.substring(versionStr.length() - 13));
        }
        if (currStamp - versionStamp < 5 * 60 * 1000) {
            return Mono.just(true);
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(metaData.getBucket());
        String bucketVnode = Utils.getVnode(new String(tuple.var1));
        String migrateVnodeId;
        try {
            Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
            migrateVnodeId = bucketVnodeIdTuple.var2;
        } catch (MsException e) {
            migrateVnodeId = null;
        }
        if (bucketVnode.equals(migrateVnodeId)) {
            return Mono.just(true);
        }

        List<String> needDeleteFile = new ArrayList<>();
        if (metaData.partInfos != null) {
            for (PartInfo partInfo : metaData.getPartInfos()) {
                needDeleteFile.add(partInfo.fileName);
            }
        } else if (metaData.fileName != null) {
            needDeleteFile.add(metaData.fileName);
        }

        String key = Utils.getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
        String uploadIdOrFileName = metaData.fileName != null ? metaData.fileName : metaData.partUploadId;
        return Mono.just(true).flatMap(b -> {
                    if (metaData.isDeleteMark() && metaData.inode > 0) {
                        //处理带缓存池，文件重命名后，删除标记的清除
                        if (metaData.partInfos != null
                                && metaData.partInfos.length == 0
                                && "inode".equals(metaData.partUploadId)) {
                            return Mono.just(true);
                        }
                        Node instance = Node.getInstance();
                        if (instance == null) {
                            return Mono.empty();
                        }
                        return instance.getInodeNotRepair(metaData.bucket, metaData.inode)
                                .flatMap(inode -> {
                                    if ((inode.getLinkN() != NOT_FOUND_INODE.getLinkN() && !inode.isDeleteMark())
                                            || inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                        return Mono.empty();
                                    }
                                    return Mono.just(true);
                                });
                    }
                    return Mono.just(true);
                }).flatMap(c -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(vnodeList -> ErasureClient.deleteObjectMeta(key, metaData.bucket, metaData.key, metaData.versionId, metaData.versionNum, vnodeList, null, metaData.inode, uploadIdOrFileName, metaData.snapshotMark, metaData.cookie))
                .flatMap(b -> deleteVersionObj(needDeleteFile, new MetaData[]{metaData}))
                .doOnNext(b -> BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), consumer))
                .doOnError(log::error);
    }

    private static Mono<Boolean> deletePartMeta(Tuple2<byte[], byte[]> tuple) {
        BatchRocksDB.RequestConsumer consumer = (db, w, r) -> {
            try {
                w.delete(tuple.var1);
            } catch (Exception ignored) {
            }
        };
        InitPartInfo initPartInfo = Json.decodeValue(new String(tuple.var2), InitPartInfo.class);
        long currStamp = System.currentTimeMillis();
        long versionStamp;
        if (DAVersionUtils.countHyphen(initPartInfo.versionNum) == 2) {
            String versionStr = initPartInfo.versionNum.split("-")[1];
            versionStamp = Long.parseLong(versionStr.substring(19, 32));
        } else {
            String versionStr = initPartInfo.versionNum.split("-")[0];
            versionStamp = Long.parseLong(versionStr.substring(versionStr.length() - 13));
        }
        if (currStamp - versionStamp < 5 * 60 * 1000) {
            return Mono.just(true);
        }
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(initPartInfo.bucket);
        String bucketVnode = Utils.getVnode(new String(tuple.var1));
        String migrateVnodeId;
        try {
            Tuple2<String, String> bucketVnodeIdTuple = pool.getBucketVnodeIdTuple(initPartInfo.bucket, initPartInfo.object);
            migrateVnodeId = bucketVnodeIdTuple.var2;
        } catch (MsException e) {
            migrateVnodeId = null;
        }
        if (bucketVnode.equals(migrateVnodeId)) {
            return Mono.just(true);
        }
        String initPartSnapshotMark = initPartInfo.snapshotMark;
        StoragePool dataPool = StoragePoolFactory.getStoragePool(initPartInfo);
        String completePartSnapshotMark = initPartInfo.metaData == null ? initPartInfo.snapshotMark : initPartInfo.metaData.snapshotMark;
        return pool.mapToNodeInfo(bucketVnode)
                .flatMap(vnodeList -> {
                    if (dataPool != null && initPartInfo.metaData != null && initPartInfo.metaData.deleteMark) {
                        return PartUtils.deleteMultiPartUploadData(dataPool, initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, vnodeList, null, initPartSnapshotMark, completePartSnapshotMark)
                                .map(b -> vnodeList);
                    }
                    return Mono.just(vnodeList);
                })
                .flatMap(vnodeList -> PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, vnodeList, initPartInfo.metaData != null && initPartInfo.metaData.deleteMark, initPartSnapshotMark, completePartSnapshotMark))
                .doOnNext(b -> BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), consumer))
                .doOnError(log::error);
    }


    public static void putDeleteKey(String key, String value) {
        try {
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put((ROCKS_OBJ_META_DELETE_MARKER + key + ":" + RandomStringUtils.randomAlphanumeric(32)).getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            log.info("put error {}", key, e);
        }
    }

    public static void next(String prefix, String marker, UnicastProcessor<Tuple2<byte[], byte[]>> res) {
        if (res.isDisposed()) {
            return;
        }
        boolean end = false;
        String nextMarker = marker;

        int count = 0;

        if (res.size() < 100) {
            try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
                iterator.seek(marker.getBytes());
                while (iterator.isValid() && count < 1000) {
                    String key = new String(iterator.key());
                    if (key.startsWith(prefix)) {
                        if (!key.equalsIgnoreCase(marker)) {
                            res.onNext(new Tuple2<>(iterator.key(), iterator.value()));
                            count++;
                            nextMarker = key;
                        }
                    } else {
                        end = true;
                        break;
                    }

                    iterator.next();
                }

                if (count < 1000) {
                    end = true;
                }
            }

        }

        String finalNextMarker = nextMarker;

        if (!end) {
            DISK_SCHEDULER.schedule(() -> next(prefix, finalNextMarker, res), 1L, TimeUnit.SECONDS);
        } else {
            res.onComplete();
        }
    }

    public static Mono<Boolean> deleteVersionObj(List<String> needDeleteFile, MetaData[] metaData) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData[0]);
        if (storagePool == null) {
            return Mono.just(true);
        }
        List<String> dupFile = new LinkedList<>();
        List<String> allFile = new LinkedList<>();
        if (StringUtils.isNotEmpty(metaData[0].aggregationKey)) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData[0].bucket);
            String vnode = Utils.getVnode(metaData[0].aggregationKey);
            return metaStoragePool.mapToNodeInfo(vnode)
                    .flatMap(nodeList -> AggregateFileClient.freeAggregationSpace(metaData[0].aggregationKey, nodeList));
        }
        if (StringUtils.isEmpty(metaData[0].duplicateKey)) {
            if (metaData[0].partUploadId != null) {
                PartInfo[] partInfos = metaData[0].partInfos;
                for (PartInfo partInfo : partInfos) {
                    if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                        dupFile.add(partInfo.deduplicateKey);
                    } else {
                        allFile.add(partInfo.fileName);
                    }
                }
            }
        } else {
            dupFile.add(metaData[0].duplicateKey);
        }
        return Mono.just("")
                .flatMap(l -> {
                    if (dupFile.isEmpty()) {
                        try {
                            if (!needDeleteFile.isEmpty()) {
                                boolean needDelFileInData = storagePool.getVnodePrefix().startsWith("cache") && StringUtils.isNotEmpty(metaData[0].getLastAccessStamp());
                                return Mono.just(needDelFileInData)
                                        .flatMap(b0 -> {
                                            if (b0) {
                                                return deleteObjectFileWithData(metaData[0], needDeleteFile.toArray(new String[0]), null);
                                            } else {
                                                return Mono.just(b0);
                                            }
                                        })
                                        .flatMap(b1 -> ErasureClient.deleteObjectFile(storagePool, needDeleteFile.toArray(new String[0]), null));
                            }
                            return Mono.just(true);
                        } catch (Exception e) {
                            log.error("deleteObjectFile error !!! " + e);
                            return Mono.just(false);
                        }
                    } else {
                        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData[0].bucket);
                        return ErasureClient.deleteDedupObjectFile(metaStoragePool, dupFile.toArray(new String[0]), null, false)
                                .flatMap(b -> {
                                    if (b && allFile.size() > 0) {
                                        boolean needDelFileInData = storagePool.getVnodePrefix().startsWith("cache") && StringUtils.isNotEmpty(metaData[0].getLastAccessStamp());
                                        return Mono.just(needDelFileInData)
                                                .flatMap(b0 -> {
                                                    if (b0) {
                                                        return deleteObjectFileWithData(metaData[0], allFile.toArray(new String[0]), null);
                                                    } else {
                                                        return Mono.just(b0);
                                                    }
                                                })
                                                .flatMap(b1 -> ErasureClient.deleteObjectFile(storagePool, allFile.toArray(new String[0]), null));
                                    }
                                    return Mono.just(b);
                                });
                    }
                });
    }
}
