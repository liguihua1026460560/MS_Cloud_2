package com.macrosan.ec.migrate;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.rebuild.RebuildSpeed;
import com.macrosan.ec.server.RequestResponseServerHandler;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.*;
import static com.macrosan.ec.migrate.SstVnodeDataScannerUtils.getSeparator;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.storage.client.ListMetaVnode.SIGNAL_PREFIX;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class LocalMigrate {
    /**
     * 迁移过程中迁移对象数据。
     * 先尝试从源磁盘拷贝
     * 拷贝失败则使用ec进行恢复
     *
     * @param srcDisk 源磁盘
     * @param dstDisk 目标磁盘
     * @param next    扫描到的对象信息
     * @return 迁移结果
     */
    private static Mono<Boolean> recoverData(String srcDisk, String dstDisk, String errorVnode, String poolType,
                                             Tuple3<VnodeDataScanner.Type, byte[], byte[]> next) {
        FileMeta fileMeta = Json.decodeValue(new String(next.var3), FileMeta.class);
        return MigrateUtil.migrateFile(fileMeta, srcDisk, CURRENT_IP, dstDisk, poolType, false, 0);
    }


    /**
     * 从 srcDisk 拷贝 objVnode 对应的所有数据到 dstDisk
     * 增加磁盘时本地迁移
     *
     * @param objVnode 需要迁移的vnode
     * @param srcDisk  源磁盘
     * @param dstDisk  目标磁盘
     * @param vKey
     * @return 迁移结果
     */
    public static Mono<Boolean> copyVnodeData(String objVnode, String srcDisk, String dstDisk, String poolType, IndexDBEnum indexDBEnum, String vKey, AtomicBoolean hasMigrateData) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(poolType, null);
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
        Scanner scanner = new Scanner(ScannerConfig.builder()
                .pool(storagePool)
                .nodeList(nodeList)
                .indexDBEnum(indexDBEnum)
                .vnode(objVnode)
                .vKey(vKey)
                .dstDisk(dstDisk)
                .build());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        String runningKey = "running_" + poolQueueTag;
        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.res())
                .publishOn(DISK_SCHEDULER)
                .filter(next -> {
                    String signalKey = new String(next.var2);
                    if (!signalKey.startsWith(SIGNAL_PREFIX)) {
                        return true;
                    }
                    try {
                        // 保存marker
                        String value = new String(next.var3);
                        String marker = new JsonObject(value).getString("marker");
                        RebuildRabbitMq.getMaster().hset(vKey, signalKey.substring(SIGNAL_PREFIX.length()), marker);
                    } catch (Exception e) {
                        log.error("save migrate list marker error", e);
                    }
                    // 跳过信号
                    return false;
                })
                .flatMap(next -> {
                    try {
                        if (!hasMigrateData.get()) {
                            hasMigrateData.set(true);
                        }
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            String lun = dstDisk;
                            if (IndexDBEnum.UNSYNC_RECORD_DB.equals(indexDBEnum)) {
                                lun = getSyncRecordLun(dstDisk);
                            }
                            if (IndexDBEnum.COMPONENT_RECORD_DB.equals(indexDBEnum)) {
                                lun = getComponentRecordLun(dstDisk);
                            }
                            if (IndexDBEnum.STS_TOKEN_DB.equals(indexDBEnum)) {
                                lun = getSTSTokenLun(dstDisk);
                            }
                            if (IndexDBEnum.RABBITMQ_RECORD_DB.equals(indexDBEnum)) {
                                lun = getRabbitmqRecordLun(dstDisk);
                            }
                            if (IndexDBEnum.AGGREGATE_DB.equals(indexDBEnum)) {
                                lun = getAggregateLun(dstDisk);
                            }
                            String key = new String(next.var2);
                            boolean dedup = key.startsWith(ROCKS_DEDUPLICATE_KEY);
                            boolean sts = key.startsWith(ROCKS_STS_TOKEN_KEY);
                            boolean aggregate = key.startsWith(ROCKS_AGGREGATION_RATE_PREFIX);
                            String value = new String(next.var3);
                            JsonObject o = new JsonObject(value);
                            String versionNum = o.getString("versionNum");

                            BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
                                boolean needPut = true;
                                if ((null != versionNum && !key.startsWith(ROCKS_BUCKET_META_PREFIX)) || dedup || sts || aggregate) {
                                    byte[] oldValue = writeBatch.getFromBatchAndDB(db, next.var2);
                                    if (null != oldValue) {
                                        if (new String(next.var2).startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                                            needPut = false;
                                            BitSet current = BitSet.valueOf(oldValue);
                                            String oldBit = o.getString("value");
                                            BitSet bitSet = AggregationUtils.deserialize(oldBit);
                                            current.and(bitSet);
                                            writeBatch.put(next.var2, current.toByteArray());
                                        } else if (new String(next.var2).startsWith("+")) { //只记录了key
                                            needPut = false;
                                        } else if (StringUtils.isNotBlank(new String(oldValue))) {
                                            JsonObject o1 = new JsonObject(new String(oldValue));
                                            if (versionNum != null) {
                                                if (versionNum.compareTo(o1.getString("versionNum")) <= 0) {
                                                    needPut = false;
                                                }
                                            }
                                            if (dedup) {
                                                needPut = true;
                                            }
                                            if (sts) {
                                                needPut = true;
                                            }
                                        }
                                    }

                                    if (needPut) {
                                        if (new String(next.var2).startsWith("+")) {
                                            if (null != o.getBoolean("deleteMark") && o.getBoolean("deleteMark")) {
                                                writeBatch.put(next.var2, next.var3);
                                            }
                                            writeBatch.put(next.var2, new byte[]{0});
                                        } else if (new String(next.var2).startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                                            writeBatch.put(next.var2, AggregationUtils.deserialize(o.getString("value")).toByteArray());
                                        } else {
                                            writeBatch.put(next.var2, next.var3);
                                        }
                                    }
                                }

                            };

                            return BatchRocksDB.customizeOperateData(lun, consumer)
                                    .doOnNext(b -> {
                                        // NFS inode数与chunk数暂不计入进度；存入表12便于测试时统计
                                        if (key.startsWith(ROCKS_INODE_PREFIX)) {
                                            RebuildRabbitMq.getMaster().hincrby(runningKey, "inodeNum", 1L);
                                        } else if (key.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                                            RebuildRabbitMq.getMaster().hincrby(runningKey, "chunkNum", 1L);
                                        } else {
                                            RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1L);
                                        }
                                    });
                        } else {// 保持返回的数据是recoverData的结果
                            return recoverData(srcDisk, dstDisk, objVnode, poolType, next)
                                    .doOnNext(aBoolean -> {
                                        RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1L);
                                    })
                                    .onErrorReturn(false);// 如果不是元数据类型, 那就恢复数据，经查看这里的next只是一个文件的数值
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        return Mono.error(new RuntimeException("migrate vnode error!"));
                    }
                }, 1, 1)
                .doOnNext(b -> RebuildSpeed.add(1))
                .doOnComplete(() -> res.onNext(hasMigrateData.get()))
                .subscribe(b -> {
                }, res::onError);

        return res;
    }

    /**
     * 迁移完成后删除源磁盘剩余的vnode相关数据
     *
     * @param objVnode vnode
     * @param srcDisk  源磁盘
     * @param dstDisk  目的磁盘
     * @return 删除结果
     */
    public static Mono<Boolean> removeVnodeDate(String objVnode, String srcDisk, String dstDisk, IndexDBEnum indexDBEnum, String poolType) {
        StoragePool pool = StoragePoolFactory.getStoragePool(poolType, null);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(objVnode).block();
        for (Tuple3<String, String, String> t : nodeList) {
            if (t.var2.equalsIgnoreCase(dstDisk) && t.var1.equalsIgnoreCase(CURRENT_IP)) {
                t.var2 = srcDisk;
            }
        }

        Scanner scanner = new Scanner(ScannerConfig.builder()
                .pool(pool)
                .vnode(objVnode)
                .indexDBEnum(indexDBEnum)
                .nodeList(nodeList)
                .build());
        MonoProcessor<Boolean> res = MonoProcessor.create();

        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.res())
                .publishOn(DISK_SCHEDULER)
                .flatMap(next -> {
                    try {
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            String lun = srcDisk;
                            if (IndexDBEnum.UNSYNC_RECORD_DB.equals(indexDBEnum)) {
                                lun = getSyncRecordLun(srcDisk);
                            }
                            if (IndexDBEnum.COMPONENT_RECORD_DB.equals(indexDBEnum)) {
                                lun = getComponentRecordLun(srcDisk);
                            }
                            if (IndexDBEnum.STS_TOKEN_DB.equals(indexDBEnum)) {
                                lun = getSTSTokenLun(srcDisk);
                            }
                            if (IndexDBEnum.RABBITMQ_RECORD_DB.equals(indexDBEnum)) {
                                lun = getRabbitmqRecordLun(srcDisk);
                            }
                            if (IndexDBEnum.AGGREGATE_DB.equals(indexDBEnum)) {
                                lun = getAggregateLun(srcDisk);
                            }
                            MSRocksDB.getRocksDB(lun).delete(next.var2);
                            return Mono.just(true);
                        } else {
                            FileMeta fileMeta = Json.decodeValue(new String(next.var3), FileMeta.class);
                            log.debug("delete file: {}", fileMeta.getFileName());
                            BatchRocksDB.RequestConsumer consumer = RequestResponseServerHandler.getDeleteFileConsumer(srcDisk, fileMeta.getFileName());
                            return BatchRocksDB.customizeOperateData(srcDisk, consumer)
                                    .doOnError(e -> {
                                        log.error("", e);
                                    }).onErrorReturn(false);

                        }
                    } catch (Exception e) {
                        log.error("", e);
                        return Mono.just(false);
                    }
                }, 1, 1).doOnComplete(() -> res.onNext(true)).subscribe(b -> {
                }, res::onError);

        return res;
    }

    /**
     * 根据vnode确定数据范围并删除sst文件
     *
     * @param srcDisk     不带节点信息的源盘
     * @param objVnodes
     * @param indexDBEnum
     */
    public static void removeDataFileInRanges(String srcDisk, Set<String> objVnodes, IndexDBEnum indexDBEnum, boolean withMigrate) {
        MSRocksDB msRocksDB = null;
        String lun = srcDisk;
        switch (indexDBEnum) {
            case ROCKS_DB:
                msRocksDB = getRocksDB(srcDisk);
                lun = srcDisk;
                break;
            case UNSYNC_RECORD_DB:
                msRocksDB = getRocksDB(getSyncRecordLun(srcDisk));
                lun = getSyncRecordLun(srcDisk);
                break;
            case COMPONENT_RECORD_DB:
                msRocksDB = getRocksDB(getComponentRecordLun(srcDisk));
                lun = getComponentRecordLun(srcDisk);
                break;
            case STS_TOKEN_DB:
                msRocksDB = getRocksDB(getSTSTokenLun(srcDisk));
                lun = getSTSTokenLun(srcDisk);
                break;
            case RABBITMQ_RECORD_DB:
                msRocksDB = getRocksDB(getRabbitmqRecordLun(srcDisk));
                lun = getRabbitmqRecordLun(srcDisk);
                break;
            case AGGREGATE_DB:
                msRocksDB = getRocksDB(getAggregateLun(srcDisk));
                lun = getAggregateLun(srcDisk);
                break;
            default:
                log.error("indexDBEnum :{} is not support", indexDBEnum);
                break;
        }
        if (msRocksDB == null) {
            log.error("{} rocksDB is error", srcDisk);
            return;
        }

        //确定删除范围
        List<byte[]> ranges = new ArrayList<>();
        for (String vnode : objVnodes) {
            for (String prefix : SstVnodeDataScannerUtils.PREFIX_LIST) {
                String startRange = getStartRange(vnode, prefix);
                byte[] start = startRange.getBytes();//获取当前vnode的删除开始范围
                byte[] end = new byte[start.length];
                System.arraycopy(start, 0, end, 0, start.length);
                end[end.length - 1] += 1;//获取结束范围
                ranges.add(start);
                ranges.add(end);
            }
        }
        log.debug("vnode :{}, before delete range sst num in {} : {}", objVnodes, lun, msRocksDB.getLiveFilesMetaData().size());
        try {
            msRocksDB.deleteFilesInRangesDefaultColumnFamily(ranges, false);
            log.debug("vnode:{}, after delete default columnFamily range sst num in {} : {}", objVnodes, lun, msRocksDB.getLiveFilesMetaData().size());
            if (withMigrate) {
                msRocksDB.deleteFilesInRangesMigrateColumnFamily(getMigrateColumnFamilyHandle(lun), ranges, false);
                log.debug("vnode:{}, after delete migrate columnFamily range sst num in {} : {}", objVnodes, lun, msRocksDB.getLiveFilesMetaData().size());
            }
        } catch (RocksDBException e) {
            log.error(e);
        }

    }

    public static String getStartRange(String vnode, String prefix) {
        return prefix + vnode + getSeparator(prefix);
    }

    /**
     * 迁移完成后，将迁移过程中执行的删除元数据的操作在目标磁盘重放
     *
     * @param dstDisk 目标磁盘
     * @return 重放结果
     */
    public static Mono<Boolean> replayDeleteOperate(Tuple2<byte[], byte[]> tuple, String dstDisk) {
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, tuple.var1);
            if (null != oldValue && Arrays.equals(oldValue, tuple.var2)) {
                writeBatch.delete(tuple.var1);
            }
        };

        return BatchRocksDB.customizeOperateData(dstDisk, consumer);
    }


    /**
     * 迁移完成后，将迁移过程中执行的copy的操作在目标磁盘重放
     *
     * @param dstDisk 目标磁盘
     */
    public static Mono<Boolean> replayCopyOperate(Tuple2<byte[], byte[]> tuple, String dstDisk) {

        String keySuffix = new String(tuple.var1);
        MetaData metaData = Json.decodeValue(new String(tuple.var2), MetaData.class);

        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            if (metaData.getPartInfos() != null) {
                for (PartInfo info : metaData.getPartInfos()) {
                    String oldKey = FileMeta.getKey(info.fileName);
                    byte[] oldValue = writeBatch.getFromBatchAndDB(db, oldKey.getBytes());
                    if (oldValue == null) {
                        return;
                    }
                    FileMeta oldFileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                    oldFileMeta.setFileName(oldFileMeta.getFileName().split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
                    String oldFileName = info.fileName.split(ROCKS_FILE_META_PREFIX)[0];
                    byte[] keyBytes = (FileMeta.getKey(oldFileName) + keySuffix).getBytes();
                    // 迁移 重放copy操作时，如果filemeta已经存在，则不在进行重放操作，避免假copy的filemeta 覆盖迁移过程中真copy的filemeta
                    if (writeBatch.getFromBatchAndDB(db, keyBytes) == null) {
                        writeBatch.put(keyBytes, Json.encode(oldFileMeta).getBytes());
                    }
                }
            } else {
                String oldKey = FileMeta.getKey(metaData.fileName);
                String oldFileName = metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0];
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, oldKey.getBytes());
                if (oldValue == null) {
                    return;
                }
                FileMeta oldFileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                oldFileMeta.setFileName(oldFileMeta.getFileName().split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
                byte[] keyBytes = (FileMeta.getKey(oldFileName) + keySuffix).getBytes();
                if (writeBatch.getFromBatchAndDB(db, keyBytes) == null) {
                    // 迁移 重放copy操作时，如果filemeta已经存在，则不在进行重放操作，避免假copy的filemeta 覆盖迁移过程中真copy的filemeta
                    writeBatch.put(keyBytes, Json.encode(oldFileMeta).getBytes());
                }
            }
        };

        return BatchRocksDB.customizeOperateData(dstDisk, consumer);
    }
}
