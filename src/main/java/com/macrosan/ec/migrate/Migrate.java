package com.macrosan.ec.migrate;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.server.MigrateServer;
import com.macrosan.ec.server.RequestResponseServerHandler;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.netty.util.ReferenceCounted;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.LiveFileMetaData;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class Migrate {
    public static final Scheduler ADD_NODE_SCHEDULER;
    private final static ThreadFactory ADD_NODE_THREAD_FACTORY = new MsThreadFactory("add-node");

    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(PROC_NUM / 4, PROC_NUM / 4, ADD_NODE_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }

        ADD_NODE_SCHEDULER = scheduler;
    }

    /**
     * 迁移完成后删除源磁盘剩余的vnode相关数据
     *
     * @param objVnode vnode
     * @param disk     源磁盘
     * @return 删除结果
     */
    public static Mono<Boolean> removeVnodeData(String objVnode, String disk, String dstDisk, String dstNodeIp, String poolType) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        String splitter = File.separator.replace("\\","\\\\");
        StoragePool storagePool;
        Scanner scanner;
        if (disk.contains(File.separator)) {
            storagePool = StoragePoolFactory.getStoragePool(poolType, null);
            List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
            for (Tuple3<String, String, String> t : nodeList) {
                if (t.var2.equalsIgnoreCase(dstDisk) && t.var1.equalsIgnoreCase(dstNodeIp)) {
                    t.var1 = CURRENT_IP;
                    t.var2 = disk;
                }
            }
            //nodeList仅保留当前节点源盘， 避免扫到映射更新后续新写入到其他盘的数据
            nodeList = nodeList.stream().filter(t -> t.var1.equalsIgnoreCase(CURRENT_IP) && t.var2.equalsIgnoreCase(disk)).collect(Collectors.toList());
            MSRocksDB.IndexDBEnum indexDBEnum = MSRocksDB.IndexDBEnum.ROCKS_DB;
            for (MSRocksDB.IndexDBEnum dbEnum : MSRocksDB.IndexDBEnum.values()) {
                if (dbEnum.getDir().equals(disk.split(splitter)[1])) {
                    indexDBEnum = dbEnum;
                }
            }
            scanner = new Scanner(ScannerConfig.builder().pool(storagePool).vnode(objVnode).indexDBEnum(indexDBEnum).nodeList(nodeList).build());
        } else {
            storagePool = StoragePoolFactory.getStoragePool(poolType, null);
            List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
            for (Tuple3<String, String, String> t : nodeList) {
                if (t.var2.equalsIgnoreCase(dstDisk) && t.var1.equalsIgnoreCase(dstNodeIp)) {
                    t.var1 = CURRENT_IP;
                    t.var2 = disk;
                }
            }
            nodeList = nodeList.stream().filter(t -> t.var1.equalsIgnoreCase(CURRENT_IP) && t.var2.equalsIgnoreCase(disk)).collect(Collectors.toList());
            scanner = new Scanner(ScannerConfig.builder().pool(storagePool).vnode(objVnode).nodeList(nodeList).build());
        }

        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.res())
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(next -> {
                    try {
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            MSRocksDB.getRocksDB(disk, false).delete(next.var2);
                            return Mono.just(true);
                        } else {
                            FileMeta fileMeta = Json.decodeValue(new String(next.var3), FileMeta.class);
                            BatchRocksDB.RequestConsumer consumer = RequestResponseServerHandler.getDeleteFileConsumer(disk, fileMeta.getFileName());
                            return BatchRocksDB.customizeOperateData(disk, consumer)
                                    .doOnError(e -> {
                                        log.error("", e);
                                    }).onErrorReturn(false);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        return Mono.just(false);
                    }
                }, 1, 1)
                .doOnComplete(() -> res.onNext(true)).subscribe(b -> {}, res::onError);
        return res;
    }

    public static Mono<Boolean> replayDeleteOperate(Tuple2<byte[], byte[]> tuple, String dstDisk, String dstIp) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("lun", dstDisk)
                .put("key", new String(tuple.var1))
                .put("value", new String(tuple.var2));

        return RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_REPLAY_DELETE.name())))
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> {
                    log.error("", e);
                })
                .onErrorReturn(ERROR_PAYLOAD)
                .map(p -> {
                    try {
                        if (p.getMetadataUtf8().equalsIgnoreCase(SUCCESS.name())) {
                            return true;
                        } else {
                            replayDeleteOperateRetry(tuple, dstDisk, dstIp);
                            return false;
                        }
                    } finally {
                        p.release();
                    }
                });
    }

    public static void replayDeleteOperateRetry(Tuple2<byte[], byte[]> tuple, String dstDisk, String dstIp) {
        AtomicInteger retry = new AtomicInteger(0);
        Disposable[] disposables = new Disposable[1];
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("lun", dstDisk)
                .put("key", new String(tuple.var1))
                .put("value", new String(tuple.var2));
        disposables[0] = ADD_NODE_SCHEDULER.schedulePeriodically(() -> Mono.just(true)
                .flatMap(b -> {
                    return RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                            .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_REPLAY_DELETE.name())))
                            .timeout(Duration.ofSeconds(30))
                            .doOnError(e -> {
                                log.error("", e);
                            })
                            .onErrorReturn(ERROR_PAYLOAD)
                            .map(p -> {
                                try {
                                    if (p.getMetadataUtf8().equalsIgnoreCase(SUCCESS.name())) {
                                        return true;
                                    } else {
                                        return false;
                                    }
                                } finally {
                                    p.release();
                                }
                            });
                })
                .subscribe(b -> {
                    retry.incrementAndGet();
                    if (b || retry.get() > 9) {
                        disposables[0].dispose();
                    }
                }), 30, 30, TimeUnit.SECONDS);

    }

    public static Mono<Boolean> replayCopyOperate(Tuple2<byte[], byte[]> tuple, String dstDisk, String dstIp) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("lun", dstDisk)
                .put("key", new String(tuple.var1))
                .put("value", new String(tuple.var2))
                .put("replay", "true");

        return RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                .flatMap(rSocket ->
                        rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_FILE_META.name()))
                                .timeout(Duration.ofSeconds(30))
                                .doOnError(e -> log.error("replayCopyOperate error.", e))
                                .onErrorReturn(ERROR_PAYLOAD)
                                .map(p -> {
                                    try {
                                        return p.getMetadataUtf8().equalsIgnoreCase(SUCCESS.name());
                                    } finally {
                                        p.release();
                                    }
                                }));
    }

    public static void registerDeleteVnodeDataProcessor(String lun, UnicastProcessor<List<byte[]>> removeProcessor) {
        AtomicInteger index = new AtomicInteger();
        AtomicInteger used = new AtomicInteger();

        AtomicInteger listMapIndex = new AtomicInteger();
        ConcurrentSkipListMap<Integer, List<byte[]>> listMap = new ConcurrentSkipListMap<>();
        int count = 4;
        removeProcessor.subscribe(list -> {
//            if (used.get() >= count) {
//                listMap.put(listMapIndex.getAndIncrement(), list);
//                return;
//            }
//            processor.onNext(1L);
//            used.getAndIncrement();
            BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
                for (byte[] b : list) {
                    writeBatch.delete(b);
                }
            };
            BatchRocksDB.customizeOperateMeta(lun, index.getAndIncrement(), consumer)
                    .doOnError(e -> {
//                        log.error(e);
                        used.getAndDecrement();
                        Map.Entry<Integer, List<byte[]>> entry = listMap.pollFirstEntry();
                        if (entry != null) {
                            removeProcessor.onNext(entry.getValue());
                        }
                    })
                    .subscribe(b -> {
                        used.getAndDecrement();
                        Map.Entry<Integer, List<byte[]>> entry = listMap.pollFirstEntry();
                        if (entry != null) {
                            removeProcessor.onNext(entry.getValue());
                        }
                    });
        });
    }

    public static Mono<Boolean> copyVnodeData(String objVnode, String srcDisk, String dstDisk, String dstIp, String poolType) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool storagePool = StoragePoolFactory.getStoragePool(poolType, null);
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
        Scanner scanner = new Scanner(ScannerConfig.builder().pool(storagePool).vnode(objVnode).nodeList(nodeList).listLink(false).build());
        List<Tuple2<String, String>> list = new ArrayList<>();
        AtomicLong currSize = new AtomicLong();
        int maxNum = 1000;
        long maxSize = 1024 * 1024;
        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.res())
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(next -> {
                    try {
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            if (new String(next.var2).startsWith("$")) {
                                //桶容量信息不处理
                                return Mono.just(true);
                            }
                            long nextSize = next.var2.length + next.var3.length;
                            currSize.addAndGet(nextSize);
                            list.add(new Tuple2<>(new String(next.var2), new String(next.var3)));
                            if (list.size() < maxNum && currSize.get() < maxSize) {
                                return Mono.just(true);
                            }
                            if (!list.isEmpty()) {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", dstDisk)
                                        .put("resList", Json.encode(list))
                                        .put("dataType", "new");
                                list.clear();
                                currSize.set(0);
                                sendMigrateMsg(msg, dstIp);
                            }
                            return Mono.just(true);
                        } else {
                            FileMeta fileMeta = Json.decodeValue(new String(next.var3), FileMeta.class);
                            return MigrateUtil.migrateFile(fileMeta, srcDisk, dstIp, dstDisk, poolType, false, 0);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        return Mono.just(false);
                    }
                }, 1, 1).doOnComplete(() -> res.onNext(true)).subscribe(b -> {}, res::onError);

        return res;
    }

    public static void sendMigrateMsg(SocketReqMsg msg, String dstIp) {
        RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_PUT_ROCKS.name())))
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> {
                    log.error("", e);
//                    processor.onNext(l + 1);
                })
                .subscribe(p -> {
//                    processor.onNext(l + 1);
                    p.release();
                });
    }

    /**
     * 迁移完成后使用扫描CheckPoint下的sst文件的方式删除源磁盘剩余的vnode元数据
     *
     * @param objVnode vnode
     * @param disk     源磁盘
     * @return 删除结果
     */
    public static Mono<Boolean> removeVnodeMeta(String objVnode, String disk, String vnode, String dstIp, String dstDisk, boolean moveOtherMigrate) {
        boolean b = SstVnodeDataScannerUtils.vnodeExistMigrateKey(disk, objVnode);
        if (disk.contains(MSRocksDB.RabbitmqRecordDir) || moveOtherMigrate) {
            b = false;
        }
        SstVnodeDataScanner scanner = new SstVnodeDataScanner(objVnode, disk, vnode);
        UnicastProcessor<Long> processor = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        int maxNum = 1000;
        List<byte[]> keyList = new ArrayList<>(maxNum);
        UnicastProcessor<Tuple2<Boolean, List<byte[]>>> removeProcessor = UnicastProcessor.create(Queues.<Tuple2<Boolean, List<byte[]>>>unboundedMultiproducer().get());
        registerDeleteVnodeMetaProcessor(disk, removeProcessor, processor);
        AtomicBoolean isDeleteNew = new AtomicBoolean(false);
        Set<String> bucketMetaSet = new HashSet<>();

        if (b) {
            MigrateUtil.sstDelayStart(scanner, processor);
        } else {
            scanner.setOldScanEnd(true);
            isDeleteNew.set(true);
            scanner.refreshRocksIterator();
            processor.onNext(0L);
        }
        processor.publishOn(ADD_NODE_SCHEDULER)
                .doOnComplete(removeProcessor::onComplete)
                .doOnNext(l -> {
                    try {
                        Tuple3<VnodeDataScanner.Type, byte[], byte[]> next = scanner.next();
                        if (null == next) {
                            if (!keyList.isEmpty()) {
                                removeProcessor.onNext(new Tuple2<>(isDeleteNew.get(), new ArrayList<>(keyList)));
                                keyList.clear();
                            }
                            if (scanner.isScanEnd()) {
                                processor.onComplete();
                            } else {
                                if (scanner.isOldScanEnd()) {
                                    isDeleteNew.set(true);
                                    scanner.refreshRocksIterator();
                                }
                                processor.onNext(l + 1);
                            }
                            return;
                        }

                        if (next.var1 == VnodeDataScanner.Type.META) {
                            if (new String(next.var2).startsWith(SysConstants.ROCKS_BUCKET_META_PREFIX) && bucketMetaSet.add(new String(next.var2))) {
                                byte[] resultBytes = MSRocksDB.getRocksDB(disk).get(next.var2);
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", dstDisk)
                                        .put("key", new String(next.var2))
                                        .put("value", Json.encode(resultBytes));
                                RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                                        .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_MERGE_ROCKS.name())))
                                        .timeout(Duration.ofSeconds(30))
                                        .doOnError(e -> {
                                            log.error("", e);
                                        })
                                        .subscribe(ReferenceCounted::release);
                            }
                            keyList.add(next.var2);
                            if (keyList.size() >= maxNum) {
                                removeProcessor.onNext(new Tuple2<>(isDeleteNew.get(), new ArrayList<>(keyList)));
                                keyList.clear();
                                return;
                            }
                            processor.onNext(l + 1);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        processor.onComplete();
                        removeProcessor.onComplete();
                    }
                })
                .doOnComplete(() -> res.onNext(true)).subscribe();

        return res;
    }

    public static void registerDeleteVnodeMetaProcessor(String lun, UnicastProcessor<Tuple2<Boolean, List<byte[]>>> removeProcessor, UnicastProcessor<Long> processor) {
        AtomicInteger index = new AtomicInteger();
        AtomicInteger used = new AtomicInteger();

        AtomicInteger listMapIndex = new AtomicInteger();
        ConcurrentSkipListMap<Integer, Tuple2<Boolean, List<byte[]>>> listMap = new ConcurrentSkipListMap<>();
        int count = 16;
        removeProcessor.publishOn(ADD_NODE_SCHEDULER).subscribe(tuple2 -> {
            if (used.get() >= count) {
                listMap.put(listMapIndex.getAndIncrement(), tuple2);
                return;
            }
            processor.onNext(1L);
            used.getAndIncrement();
            BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
                for (byte[] b : tuple2.var2) {
                    if (tuple2.var1) {
                        writeBatch.delete(MSRocksDB.getMigrateColumnFamilyHandle(lun), b);
                    }
                    writeBatch.delete(b);
                }
            };
            BatchRocksDB.customizeOperateMeta(lun, index.getAndIncrement(), consumer)
                    .doOnError(x -> {
                        used.getAndDecrement();
                        Map.Entry<Integer, Tuple2<Boolean, List<byte[]>>> entry = listMap.pollFirstEntry();
                        if (entry != null) {
                            removeProcessor.onNext(entry.getValue());
                        }
                    })
                    .subscribe(b -> {
                        used.getAndDecrement();
                        Map.Entry<Integer, Tuple2<Boolean, List<byte[]>>> entry = listMap.pollFirstEntry();
                        if (entry != null) {
                            removeProcessor.onNext(entry.getValue());
                        }
                    });
        });
    }

    public static Mono<Boolean> copyVnodeMeta(String objVnode, String vnode, String srcDisk, String dstDisk, String dstIp, String poolQueueTag) {
        boolean b = SstVnodeDataScannerUtils.vnodeExistMigrateKey(srcDisk, objVnode);
        List<LiveFileMetaData> migrateList;
        if (b) {
            migrateList = SstVnodeDataScannerUtils.getSstFileList(objVnode, vnode, srcDisk);
        } else {
            migrateList = new ArrayList<>();
        }
        AtomicInteger sum = new AtomicInteger(migrateList.size());

        UnicastProcessor<Integer> oldProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        UnicastProcessor<Tuple3<Integer, SocketReqMsg, Boolean>> migrateOldProcessor = UnicastProcessor.create(Queues.<Tuple3<Integer, SocketReqMsg, Boolean>>unboundedMultiproducer().get());
        Map<Integer, Tuple3<Integer, SocketReqMsg, Boolean>> oldMap = new ConcurrentHashMap<>();

        int num = Math.min(migrateList.size(), 16);
        Map<Integer, List<Tuple2<byte[], byte[]>>> cacheMap = new ConcurrentHashMap<>();
        List<SstVnodeDataScanner> scanners = new ArrayList<>(num);
        Map<Integer, AtomicBoolean> statusMap = new ConcurrentHashMap<>();

        UnicastProcessor<Integer> newProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        UnicastProcessor<SocketReqMsg> migrateNewProcessor = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());

        SstVnodeDataScanner newMetaScanner = new SstVnodeDataScanner(objVnode, srcDisk, vnode);

        Set<String> migrateSstSet = ConcurrentHashMap.newKeySet();
        MonoProcessor<Boolean> res = MonoProcessor.create();

        if (migrateList.isEmpty()) {
            oldProcessor.onComplete();
        } else {
            //开始迁移时将待迁移的sst文件名发送到目标端，list按照文件名和level排序，目标端插入sst文件的顺序需和此保持一致,优先写入高level
            List<String> sstFileNameList = migrateList.stream().map(metaData -> metaData.fileName().substring(1)).collect(Collectors.toList());
            sendSstFileList(dstIp, objVnode, dstDisk, sstFileNameList)
                    .doOnError(res::onError)
                    .subscribe(x -> {
                        for (int i = 0; i < num; i++) {
                            LiveFileMetaData metaData = migrateList.remove(0);
                            scanners.add(new SstVnodeDataScanner(objVnode, metaData));
                            statusMap.put(i, new AtomicBoolean(true));
                            cacheMap.put(i, new ArrayList<>(1000));
                        }
                        for (int i = 0; i < num; i++) {
                            oldProcessor.onNext(i);
                        }
                    });
        }


        int maxNum = 1000;
        long maxSize = 1024 * 1024;
        AtomicLong currSize = new AtomicLong();

        UnicastProcessor<Tuple2<Boolean, Tuple3<Integer, SocketReqMsg, Boolean>>> dealDataProcessor = UnicastProcessor.create(Queues.<Tuple2<Boolean, Tuple3<Integer, SocketReqMsg, Boolean>>>unboundedMultiproducer().get());
        registerMigrateMetaProcessor(migrateOldProcessor, migrateNewProcessor, oldProcessor, newProcessor, dstIp, oldMap, statusMap, sum, dealDataProcessor, poolQueueTag);
        oldProcessor.publishOn(ADD_NODE_SCHEDULER)
                .doOnError(res::onError)
                .doOnNext(l -> {
                    try {
                        boolean publishNext = true;
                        SstVnodeDataScanner scanner = scanners.get(l);
                        List<Tuple2<byte[], byte[]>> list = cacheMap.get(l);
                        Tuple3<VnodeDataScanner.Type, byte[], byte[]> next = scanner.nextMigrateOld();
                        if (next == null) {
                            String oldSstFileName = scanner.sstFileName;
                            List<byte[]> deletionList = scanner.nextDeletion(1000);
                            SocketReqMsg msg = new SocketReqMsg("", 0)
                                    .put("lun", dstDisk)
                                    .put("metaList", Json.encode(list))
                                    .put("objVnode", objVnode)
                                    .put("dataType", "old")
                                    .put("sstFileName", oldSstFileName)
                                    .put("deletionList", Json.encode(deletionList));

                            if (!migrateSstSet.contains(objVnode + "-" + oldSstFileName)) {
                                msg.put("isFirst", "true");
                                migrateSstSet.add(objVnode + "-" + oldSstFileName);
                            }

                            //每个sst文件在目标端对应两个文件，一个用于写入元数据，一个用于写入删除标记，第一次迁移删除标记时，需要关闭目标端用于写入元数据的sst文件
                            if (scanner.firstScanDeletion.get()) {
                                msg.put("firstScanDeletion", "true");
                                scanner.firstScanDeletion.set(false);
                            }
                            if (deletionList.isEmpty()) {
                                msg.put("readEnd", "");
                                publishNext = scanner.refreshSstFile(migrateList);
                                if (publishNext) {
                                    sum.decrementAndGet();
                                }
                                migrateSstSet.remove(objVnode + "-" + oldSstFileName);
                            }

                            list.clear();
                            currSize.set(0);
                            dealDataProcessor.onNext(new Tuple2<>(true, new Tuple3<>(l, msg, publishNext)));
                            return;
                        }
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            if (new String(next.var2).startsWith(SysConstants.ROCKS_BUCKET_META_PREFIX)) {
                                //桶容量信息不处理
                                oldProcessor.onNext(l);
                                return;
                            }
                            list.add(new Tuple2<>(next.var2, next.var3));
                            long nextSize = next.var2.length + next.var3.length;
                            currSize.addAndGet(nextSize);
                            if (list.size() < maxNum && currSize.get() < maxSize) {
                                oldProcessor.onNext(l);
                            } else {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", dstDisk)
                                        .put("metaList", Json.encode(list))
                                        .put("objVnode", objVnode)
                                        .put("dataType", "old")
                                        .put("sstFileName", scanner.sstFileName);
                                if (!migrateSstSet.contains(objVnode + "-" + scanner.sstFileName)) {
                                    msg.put("isFirst", "true");
                                    migrateSstSet.add(objVnode + "-" + scanner.sstFileName);
                                }
                                list.clear();
                                currSize.set(0);
                                dealDataProcessor.onNext(new Tuple2<>(true, new Tuple3<>(l, msg, true)));
                            }

                        }
                    } catch (Exception e) {
                        log.error("", e);
                        oldProcessor.onComplete();
                    }
                })
                .doOnComplete(() -> {
                    MigrateServer.getInstance().startMigrate(srcDisk, objVnode, dstDisk, dstIp).subscribe();
                    MigrateServer.getInstance().deleteMigrateVnode(srcDisk, objVnode);
                    dealDataProcessor.onComplete();
                    newMetaScanner.setOldScanEnd(true);
                    ADD_NODE_SCHEDULER.schedule(() -> {
                        newMetaScanner.refreshRocksIterator();
                        newProcessor.onNext(1);
                    }, b ? 5 : 0, TimeUnit.SECONDS);
                }).subscribe();

        List<Tuple2<byte[], byte[]>> list = new ArrayList<>(maxNum);
        AtomicLong currNewSize = new AtomicLong();
        newProcessor.publishOn(ADD_NODE_SCHEDULER)
                .doOnError(res::onError)
                .doOnNext(l -> {
                    try {
                        Tuple3<VnodeDataScanner.Type, byte[], byte[]> next = newMetaScanner.next();
                        if (next == null) {
                            if (!list.isEmpty()) {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", dstDisk)
                                        .put("metaList", Json.encode(list))
                                        .put("dataType", "new");

                                list.clear();
                                currNewSize.set(0);
                                migrateNewProcessor.onNext(msg);
                            }
                            newProcessor.onComplete();
                            return;
                        }
                        if (next.var1 == VnodeDataScanner.Type.META) {
                            if (new String(next.var2).startsWith(SysConstants.ROCKS_BUCKET_META_PREFIX)) {
                                //桶容量信息不处理
                                newProcessor.onNext(l + 1);
                                return;
                            }
                            long nextSize = next.var2.length + next.var3.length;
                            currNewSize.addAndGet(nextSize);
                            list.add(new Tuple2<>(next.var2, next.var3));
                            if (list.size() < maxNum && currNewSize.get() < maxSize) {
                                newProcessor.onNext(l + 1);
                                return;
                            }
                            if (!list.isEmpty()) {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("lun", dstDisk)
                                        .put("metaList", Json.encode(list))
                                        .put("dataType", "new");
                                list.clear();
                                currNewSize.set(0);
                                migrateNewProcessor.onNext(msg);
                            }
                        }
                    } catch (Exception e) {
                        log.error(e);
                        newProcessor.onComplete();
                    }
                }).doOnComplete(() -> {
                    migrateNewProcessor.onComplete();
                    res.onNext(true);
                }).subscribe();
        return res;
    }


    public static void registerMigrateMetaProcessor(UnicastProcessor<Tuple3<Integer, SocketReqMsg, Boolean>> migrateOldProcessor, UnicastProcessor<SocketReqMsg> migrateNewProcessor, UnicastProcessor<Integer> oldProcessor, UnicastProcessor<Integer> newProcessor, String dstIp, Map<Integer, Tuple3<Integer, SocketReqMsg, Boolean>> oldMap, Map<Integer, AtomicBoolean> statusMap, AtomicInteger endSstSize, UnicastProcessor<Tuple2<Boolean, Tuple3<Integer, SocketReqMsg, Boolean>>> dealDataProcessor, String poolQueueTag) {
        dealDataProcessor.publishOn(ADD_NODE_SCHEDULER)
                .subscribe(tuple2 -> {
                    boolean type = tuple2.var1;
                    Tuple3<Integer, SocketReqMsg, Boolean> data = tuple2.var2;
                    if (type) {
                        if (statusMap.get(data.var1).get()) {
                            migrateOldProcessor.onNext(tuple2.var2);
                        } else {
                            oldMap.put(data.var1, data);
                        }
                    } else {
                        Tuple3<Integer, SocketReqMsg, Boolean> t2 = oldMap.remove(data.var1);
                        if (t2 != null) {
                            migrateOldProcessor.onNext(t2);
                        } else {
                            statusMap.get(data.var1).compareAndSet(false, true);
                            if (!data.var3 && endSstSize.decrementAndGet() == 0) {
                                oldProcessor.onComplete();
                            }
                        }
                    }
                });
        AtomicLong msgId = new AtomicLong();

        migrateOldProcessor.publishOn(ADD_NODE_SCHEDULER)
                .subscribe(tuple3 -> {
                    String retry = tuple3.var2.dataMap.getOrDefault("retry", "0");
                    if ("0".equals(retry)) {
                        statusMap.get(tuple3.var1).compareAndSet(true, false);
                        if (tuple3.var3) {
                            oldProcessor.onNext(tuple3.var1);
                        }
                        tuple3.var2.put("msgId", String.valueOf(msgId.incrementAndGet()));
                    }

                    tuple3.var2.put("retry", retry);
                    tuple3.var2.put("index", String.valueOf(tuple3.var1));

                    RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                            .publishOn(ADD_NODE_SCHEDULER)
                            .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(tuple3.var2), MIGRATE_PUT_ROCKS.name())))
                            .timeout(Duration.ofMinutes(2))
                            .doOnError(e -> {
                                log.error("migrate fail. cur retryNum {}. error message {}", retry, e.getMessage());

                                int retryNum = Integer.parseInt(retry);
                                if (retryNum >= 10 || (e.getMessage() != null && e.getMessage().contains("Request-Response not implemented"))) {
                                    if (!oldProcessor.isDisposed()) {
                                        oldProcessor.onError(new RuntimeException("Connection to " + dstIp + " failed"));
                                    }
                                    return;
                                } else {
                                    //retry
                                    tuple3.var2.put("retry", String.valueOf(retryNum + 1));
                                    migrateOldProcessor.onNext(tuple3);
                                }
                            })
                            .subscribe(p -> {
                                p.release();
                                // tuple3.var2为socketMsg，其中的key metaList为发送的list，其size为迁移的文件数
                                try {
                                    SocketReqMsg socketReqMsg = tuple3.var2;
                                    String metaList = socketReqMsg.get("metaList");
                                    long size = Json.decodeValue(metaList, List.class).size();
                                    RebuildRabbitMq.getMaster().hincrby("running_" + poolQueueTag, "migrateNum", -size);
                                    RebuildRabbitMq.getMaster().hget("running_" + poolQueueTag, "srcDiskObjNum");
                                } catch (Exception e) {
                                    log.error("calculate add node progress error", e);
                                }
                                dealDataProcessor.onNext(new Tuple2<>(false, tuple3));
                            });
                });

        ConcurrentSkipListMap<Long, SocketReqMsg> newMap = new ConcurrentSkipListMap<>();
        AtomicLong newId = new AtomicLong();
        AtomicInteger sum = new AtomicInteger();
        int maxRsocketNum = 16;
        migrateNewProcessor.publishOn(ADD_NODE_SCHEDULER)
                .subscribe(msg -> {
                    if (sum.get() >= maxRsocketNum) {
                        newMap.put(newId.incrementAndGet(), msg);
                        return;
                    }
                    if(msg.get("retry") == null) {
                        msg.put("retry", "0");
                    }

                    sum.incrementAndGet();
                    newProcessor.onNext(1);
                    RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                            .publishOn(ADD_NODE_SCHEDULER)
                            .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_PUT_ROCKS.name())))
                            .timeout(Duration.ofSeconds(30))
                            .doOnError(e -> {
                                log.error("migrateNew fail. retry num {}:{}", msg.get("retry"), e.getMessage());
                                int retry = Integer.parseInt(msg.dataMap.getOrDefault("retry","0"));

                                sum.decrementAndGet();

                                if(retry < 10) {
                                    msg.put("retry", String.valueOf(retry+1));
                                    migrateNewProcessor.onNext(msg);
                                } else {
                                    if (!newProcessor.isDisposed()) {
                                        newProcessor.onError(new RuntimeException("migrate new to" + dstIp + " failed"));
                                    }
                                }
                            })
                            .subscribe(p -> {
                                p.release();
                                sum.decrementAndGet();
                                Map.Entry<Long, SocketReqMsg> entry = newMap.pollFirstEntry();
                                if (entry != null) {
                                    migrateNewProcessor.onNext(entry.getValue());
                                }

                            });
                });
    }

    public static Mono<Boolean> sendSstFileList(String ip, String vnode, String lun, List<String> sstFileList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("sstFileList", Json.encode(sstFileList))
                .put("objVnode", vnode)
                .put("lun", lun)
                .put("dataType", "init");
        RSocketClient.getRSocket(ip, BACK_END_PORT)
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_PUT_ROCKS.name())))
                .timeout(Duration.ofSeconds(60))
                .doOnError(e -> {
                    log.error(e);
                    try {
                        res.onError(new RuntimeException("sendSstFileList fail"));
                    }catch (Exception e0) {

                    }
                })
                .subscribe(p -> {
                    res.onNext(true);
                    p.release();
                });
        return res;
    }

}
