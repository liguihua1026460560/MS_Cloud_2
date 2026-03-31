package com.macrosan.ec.migrate;

import com.macrosan.constants.ServerConstants;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.Utils;
import com.macrosan.ec.rebuild.*;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.GetServerHandler;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.GetFileStoragePool;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;
import static com.macrosan.ec.error.DiskErrorHandler.ADD_DISK_COMPLETED_SET_PREFIX;
import static com.macrosan.ec.error.DiskErrorHandler.ADD_DISK_COMPLETE_STATUS;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class MigrateUtil {
    static void delayStart(VnodeDataScanner scanner, UnicastProcessor<Long> processor) {
        Mono.delay(Duration.ofSeconds(0))
                .publishOn(ADD_NODE_SCHEDULER)
                .subscribe(l -> {
                    scanner.start();
                    processor.onNext(0L);
                });
    }

    static void sstDelayStart(SstVnodeDataScanner scanner, UnicastProcessor<Long> processor) {
        Mono.delay(Duration.ofSeconds(0))
                .publishOn(ADD_NODE_SCHEDULER)
                .subscribe(l -> {
                    scanner.start();
                    processor.onNext(0L);
                });
    }

    public static Mono<Boolean> tryRebuild(String srcDisk, FileMeta fileMeta, String poolType) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(poolType, null);
        String objVnode = storagePool.getObjectVnodeId(fileMeta.getFileName());
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
        int errorIndex = -1;
        int i = 0;
        for (Tuple3<String, String, String> tuple : nodeList) {
            if (tuple.var1.equalsIgnoreCase(CURRENT_IP) && tuple.var2.equalsIgnoreCase(srcDisk)) {
                errorIndex = i;
                break;
            }
            i++;
        }

        if (errorIndex == -1) {
            log.error("rebuild file error,lun:{},fileName:{}", srcDisk, fileMeta.getFileName());
            return Mono.just(false);
        }

        long fileSize = fileMeta.getSize();
        long endIndex = storagePool.getOriginalObjSize(fileSize) - 1;

        String sk = fileMeta.getSecretKey();
        if (CryptoUtils.checkCryptoEnable(fileMeta.getCrypto())) {
            sk = RootSecretKeyUtils.rootKeyDecrypt(fileMeta.getSecretKey(), fileMeta.getCryptoVersion());

        }

        return TaskHandler.rebuildObjFile(storagePool, fileMeta.getMetaKey(), srcDisk, String.valueOf(errorIndex), fileMeta.getFileName(), String.valueOf(endIndex),
                String.valueOf(fileMeta.getSize()), fileMeta.getCrypto(), sk, nodeList, fileMeta.getFlushStamp(), fileMeta.getLastAccessStamp(), String.valueOf(fileMeta.getFileOffset()));
    }

    public static Mono<Boolean> migrateFile(FileMeta fileMeta, String srcDisk, String dstIP, String dstDisk, String poolType, boolean remigrate, int retryNum) {
        if (fileMeta.getFileName().contains(ROCKS_FILE_META_PREFIX)) {//经过复制后的fileMeta
            //直接发消息到目标节点
            SocketReqMsg msg0 = new SocketReqMsg("", 0)
                    .put("migrateCopy", "1")
                    .put("lun", dstDisk)
                    .put("key", fileMeta.getKey());
            return sendMigrateMsg(msg0, dstIP)
                    .flatMap(s -> {
                        if ("1".equals(s)) {
                            return Mono.just(true);
                        } else if ("-1".equals(s)) {
                            return Mono.just(false);
                        }
                        return migrateFile0(fileMeta, srcDisk, dstIP, dstDisk, poolType, remigrate, retryNum);
                    });
        }
        return migrateFile0(fileMeta, srcDisk, dstIP, dstDisk, poolType, remigrate, retryNum);

    }

    public static Mono<Boolean> migrateFile0(FileMeta fileMeta, String srcDisk, String dstIP, String dstDisk, String poolType, boolean remigrate, int retryNum) {
        String uuid;
        if (CURRENT_IP.equals(dstIP)) {
            uuid = ServerConfig.getInstance().getHostUuid();
        } else {
            uuid = NodeCache.mapIPToNode(dstIP);
        }
        if (uuid == null) {
            uuid = ServerConfig.getInstance().getHostUuid();
        }
        UnicastProcessor<Long> streamController = UnicastProcessor.create();
        StoragePool pool = GetFileStoragePool.getInstance();
        List<Tuple3<String, String, String>> getNodeList = Collections
                .singletonList(new Tuple3<>(CURRENT_IP, srcDisk, pool.getObjectVnodeId(fileMeta.getFileName())));


        Flux<byte[]> dataFlux = ECUtils.getObject(pool, fileMeta.getFileName(), fileMeta.isSmallFile(), 0,
                fileMeta.getSize() - 1, fileMeta.getSize(), getNodeList, streamController, null, null);

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("metaKey", fileMeta.getMetaKey())
                .put("recover", "1")
                .put("fileName", fileMeta.getFileName())
                .put("lun", dstDisk)
                .put("compression", fileMeta.getCompression())
                .put("fileOffset", String.valueOf(fileMeta.getFileOffset()));

        if (fileMeta.getFlushStamp() != null) {
            msg.put("flushStamp", fileMeta.getFlushStamp());
        }

        if (StringUtils.isNotEmpty(fileMeta.getLastAccessStamp())) {//缓存池在盘间迁移数据时需要增加lastAccessStamp的记录
            msg.put("lastAccessStamp", fileMeta.getLastAccessStamp());
        }

        if (CryptoUtils.checkCryptoEnable(fileMeta.getCrypto())) {
            String sk = RootSecretKeyUtils.rootKeyDecrypt(fileMeta.getSecretKey(), fileMeta.getCryptoVersion());
            CryptoUtils.putCryptoInfoToMsg(fileMeta.getCrypto(), sk, msg);
        }

        UnicastProcessor<Payload> publisher = UnicastProcessor.create();

        publisher.onNext(DefaultPayload.create(Json.encode(msg), START_PUT_OBJECT.name()));


        dataFlux.subscribe(bytes -> {
                    publisher.onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                },
                e -> {
                    log.error("", e);
                    publisher.onNext(DefaultPayload.create("put file error", ERROR.name()));
                    publisher.onComplete();
                },
                () -> {
                    publisher.onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                    publisher.onComplete();
                });

        List<Tuple3<String, String, String>> putNodeList = Collections
                .singletonList(new Tuple3<>(dstIP, dstDisk, pool.getObjectVnodeId(fileMeta.getFileName())));

        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(Collections.singletonList(publisher), String.class, putNodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();

        responseInfo.responses
                .timeout(Duration.ofMinutes(10))
                .doOnNext(tuple3 -> {
                    streamController.onNext(-1L);
                }).doOnError(e -> {
                    if (!remigrate) {
                        log.error("migrate {} fail", fileMeta.getFileName(), e);
                    }
                    res.onNext(false);
                }).doOnComplete(() -> {
                    if (responseInfo.successNum == 1) {
                        res.onNext(true);
                    } else {
                        if (!remigrate) {
                            log.error("migrate {} fail", fileMeta.getFileName());
                        }
                        res.onNext(false);
                    }
                }).subscribe();

        int[] finalNum = {retryNum};
        String finalUuid = uuid;
        return res.flatMap(b -> {
            if (!b) {
                if (!remigrate) {
                    log.error("recover {} data from disk {} fail. try rebuild", fileMeta.getKey(), srcDisk);
                }
                if (RemovedDisk.getInstance().contains(finalUuid + "@" + dstDisk)) {
                    return Mono.just(true);
                } else if (finalNum[0] < 10) {
                    finalNum[0]++;
                    return tryRebuild(srcDisk, fileMeta, poolType)
                            .flatMap(b1 -> {
                                //这里的话统一把失败的重试放到队列中。是否需要区分重试的类型，从本届点获取数据还是从其他节点获取数据
                                //1.源盘rebuild成功，重试时直接跑migrate
                                //2.源盘rebuild失败，重试时尝试从其他节点获取数据
                                if (b1) {
                                    long delay = (finalNum[0] - 1) * 5L;
                                    return Mono.delay(Duration.ofSeconds(delay < 30 ? delay : 30))//至多每隔30秒重试迁移一次
                                            .flatMap(l -> {
                                                //这里开始发布消息到死信队列(区分消息类型，是从本地获取数据迁移还是从其他节点获取数据迁移)
                                                //保持原有延时处理
                                                SocketReqMsg msg0 = new SocketReqMsg("MIGRATE_FILE_LOCAL", 0)
//                                                        .put("migrateType", "MIGRATE_FILE_LOCAL")
                                                        .put("fileMeta", Json.encode(fileMeta))
                                                        .put("srcDisk", srcDisk)
                                                        .put("dstIP", dstIP)
                                                        .put("dstDisk", dstDisk)
                                                        .put("poolType", poolType)
                                                        .put("retryNum", String.valueOf(finalNum[0]));
                                                if (!remigrate) {
                                                    log.info("publish to dead letter queue MIGRATE_FILE_LOCAL about {}!", fileMeta.getFileName());
                                                }

                                                RebuildDeadLetter.getInstance().publishMigrateTask(msg0);//发布到死信队列中

//                                                migrateFile0(fileMeta, srcDisk, dstIP, dstDisk, poolType, true, finalNum[0]);
                                                return Mono.just(true);
                                            });
                                } else {
                                    if (srcDisk.contains("cache")) {
                                        return Mono.just(true);
                                    }
                                    log.error("rebuild at disk {} fail, try rebuild {} form other node", srcDisk, fileMeta.getKey());
                                    return Mono.just(1)
                                            .flatMap(l -> {
                                                SocketReqMsg msg0 = new SocketReqMsg("MIGRATE_FILE_REMOTE", 0)
//                                                        .put("migrateType", "MIGRATE_FILE_REMOTE")
                                                        .put("fileMeta", Json.encode(fileMeta))
                                                        .put("srcDisk", srcDisk)
                                                        .put("dstIP", dstIP)
                                                        .put("dstDisk", dstDisk)
                                                        .put("poolType", poolType)
                                                        .put("retryNum", String.valueOf(finalNum[0]));
                                                log.info("publish remote to dead letter queue MIGRATE_FILE_REMOTE about {}!", fileMeta.getFileName());
                                                RebuildDeadLetter.getInstance().publishMigrateTask(msg0);
                                                return Mono.just(true);
                                            });
//                                    return migrateFileFromOther(fileMeta, srcDisk, dstIP, dstDisk, poolType, 0);
                                }
                            });
                } else {
                    return Mono.just(true);
                }
            } else {
                return Mono.just(true);
            }
        });
    }

    public static Mono<Boolean> migrateFileFromOther(FileMeta fileMeta, String srcDisk, String dstIP, String dstDisk, String poolType, int retryNum) {
//        log.info("try rebuild form other node!");
        String uuid;
        if (CURRENT_IP.equals(dstIP)) {
            uuid = ServerConfig.getInstance().getHostUuid();
        } else {
            uuid = NodeCache.mapIPToNode(dstIP);
        }
        if (uuid == null) {
            uuid = ServerConfig.getInstance().getHostUuid();
        }
        StoragePool storagePool = StoragePoolFactory.getStoragePool(poolType, null);
        Encoder ecEncodeHandler = storagePool.getEncoder();
        String objVnode = storagePool.getObjectVnodeId(fileMeta.getFileName());
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(objVnode).block();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());

        long fileSize = fileMeta.getSize();
        long endIndex = storagePool.getOriginalObjSize(fileSize) - 1;
        int errorIndex = -1;
        int i = 0;
        for (Tuple3<String, String, String> tuple : nodeList) {
            if (tuple.var1.equalsIgnoreCase(CURRENT_IP) && tuple.var2.equalsIgnoreCase(srcDisk)) {
                errorIndex = i;
                break;
            }
            i++;
        }
        if (errorIndex == -1) {
            int n = 0;
            //如果源盘不在映射中，说明可能重平衡结束映射已经改到了目标盘，那么检查目标盘是否在映射中，在的话取目标盘应存的那份数据
            for (Tuple3<String, String, String> tuple : nodeList) {
                if (tuple.var1.equalsIgnoreCase(dstIP) && tuple.var2.equalsIgnoreCase(dstDisk)) {
                    errorIndex = n;
                    break;
                }
                n++;
            }
            if (errorIndex == -1) {
                return Mono.just(true);
            }
        }

        ECUtils.getObject(storagePool, fileMeta.getFileName(), false, 0, endIndex, endIndex + 1, nodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int j = 0; j < ecEncodeHandler.data().length; j++) {
                        ecEncodeHandler.data()[j].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(ecEncodeHandler::put);

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("metaKey", fileMeta.getMetaKey())
                .put("recover", "1")
                .put("fileName", fileMeta.getFileName())
                .put("lun", dstDisk)
                .put("compression", fileMeta.getCompression());

        if (CryptoUtils.checkCryptoEnable(fileMeta.getCrypto())) {
            String sk = RootSecretKeyUtils.rootKeyDecrypt(fileMeta.getSecretKey(), fileMeta.getCryptoVersion());
            CryptoUtils.putCryptoInfoToMsg(fileMeta.getCrypto(), sk, msg);
        }

        UnicastProcessor<Payload> publisher = UnicastProcessor.create();

        publisher.onNext(DefaultPayload.create(Json.encode(msg), START_PUT_OBJECT.name()));
        ecEncodeHandler.data()[errorIndex].subscribe(bytes -> {
                    publisher.onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                },
                e -> {
                    log.error("", e);
                    publisher.onNext(DefaultPayload.create("put file error", ERROR.name()));
                    publisher.onComplete();
                },
                () -> {
                    publisher.onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                    publisher.onComplete();
                });


        List<Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, String>>> noPutList = new LinkedList<>();
        for (int j = 0; j < ecEncodeHandler.data().length; j++) {
            if (errorIndex != j) {
                Tuple3<Integer, ErasureServer.PayloadMetaType, String> res = new Tuple3<>(j, CONTINUE, null);
                UnicastProcessor<Tuple3<Integer, ErasureServer.PayloadMetaType, String>> processor = UnicastProcessor.create();
                noPutList.add(processor);
                ecEncodeHandler.data()[j].doFinally(s -> {
                    processor.onNext(res);
                    processor.onComplete();
                }).doOnError(e -> {
                }).subscribe();
            }
        }
        for (int p = 0; p < nodeList.size(); p++) {
            if (p == errorIndex) {
                Tuple3<String, String, String> tuple3 = nodeList.get(p);
                tuple3.var1 = dstIP;
                tuple3.var2 = dstDisk;//向目标盘put
            }
        }

        List<Integer> toUploadList = Collections.singletonList(errorIndex);

//        List<Tuple3<String, String, String>> putNodeList = Collections
//                .singletonList(new Tuple3<>(dstIP, dstDisk, storagePool.getObjectVnodeId(fileMeta.getFileName())));

        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(Collections.singletonList(publisher), String.class, nodeList, Collections.singletonList(errorIndex), noPutList);
        MonoProcessor<Boolean> res = MonoProcessor.create();

        responseInfo.responses
                .timeout(Duration.ofMinutes(10))
                .doOnNext(tuple3 -> {
                    if (tuple3.var1.equals(toUploadList.get(0))) {
                        for (int j = 0; j < storagePool.getK(); j++) {
                            streamController.onNext(-1L);
                        }
                    }
                }).doOnError(e -> {
                    log.error("migrate {} fail", fileMeta.getFileName(), e);
                    res.onNext(false);
                }).doOnComplete(() -> {
                    if (responseInfo.successNum == 1) {
                        res.onNext(true);
                    } else {
                        log.error("migrate {} fail", fileMeta.getFileName());
                        res.onNext(false);
                    }
                }).subscribe();

        int[] finalNum = {retryNum};
        String finalUuid = uuid;
        return res.flatMap(b -> {
            if (!b) {
                if (RemovedDisk.getInstance().contains(finalUuid + "@" + dstDisk)) {
                    return Mono.just(true);
                } else if (finalNum[0] < 10) {
                    finalNum[0]++;
                    long delay = (finalNum[0] - 1) * 5L;
                    return Mono.delay(Duration.ofSeconds(delay < 30 ? delay : 30))//每隔30秒重试迁移一次
                            .flatMap(l -> {
                                SocketReqMsg msg0 = new SocketReqMsg("MIGRATE_FILE_REMOTE", 0)
//                                        .put("migrateType", "MIGRATE_FILE_REMOTE")
                                        .put("fileMeta", Json.encode(fileMeta))
                                        .put("srcDisk", srcDisk)
                                        .put("dstIP", dstIP)
                                        .put("dstDisk", dstDisk)
                                        .put("poolType", poolType)
                                        .put("retryNum", String.valueOf(finalNum[0]));

                                RebuildDeadLetter.getInstance().publishMigrateTask(msg0);
                                return Mono.just(true);
//                                migrateFileFromOther(fileMeta, srcDisk, dstIP, dstDisk, poolType, finalNum[0])
                            });
                } else {
                    return Mono.just(true);
                }
            } else {
                return Mono.just(true);
            }
        });

    }

    public static Mono<String> sendMigrateMsg(SocketReqMsg msg, String dstIp) {
        MonoProcessor<String> result = MonoProcessor.create();
        AtomicInteger retry = new AtomicInteger(0);
        Disposable[] disposables = new Disposable[1];
        disposables[0] = DISK_SCHEDULER.schedulePeriodically(() -> RSocketClient.getRSocket(dstIp, BACK_END_PORT)
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_PUT_ROCKS.name())))
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> {
                    log.error("", e);
                    retry.incrementAndGet();
                    if (retry.get() > 10) {
                        result.onNext("-1");
                        if (null != disposables[0] && !disposables[0].isDisposed()) {
                            disposables[0].dispose();
                        }
                    }
                })
                .subscribe(payload -> {
                    if (SUCCESS.name().equals(payload.getMetadataUtf8())) {
                        String res = (String) payload.getDataUtf8();
                        result.onNext(res);
                        if (null != disposables[0] && !disposables[0].isDisposed()) {
                            disposables[0].dispose();
                        }
                    } else {
                        //在这里尝试10次，然后返回最终结果
                        retry.incrementAndGet();
                        if (retry.get() > 10) {
                            result.onNext("-1");
                            if (null != disposables[0] && !disposables[0].isDisposed()) {
                                disposables[0].dispose();
                            }
                        }
                    }
                }), 0, 35, TimeUnit.SECONDS);

        return result;
    }

    public static void add(String queueKey, byte[] key, byte[] value) {
        String rocksKey = Utils.getMqRocksKey();
        int queueKeyLen = queueKey.getBytes().length;
        byte[] resKey = new byte[queueKeyLen + key.length + 1];

        System.arraycopy(queueKey.getBytes(), 0, resKey, 0, queueKeyLen);
        resKey[queueKeyLen] = '-';
        System.arraycopy(key, 0, resKey, queueKeyLen + 1, key.length);
        try {
            MSRocksDB.getRocksDB(rocksKey).put(resKey, value);
        } catch (Exception e) {

        }
    }

    private static void next(String prefix, int prefixLen, String marker, UnicastProcessor<Tuple2<byte[], byte[]>> res) {
        boolean end = false;
        String nextMarker = marker;

        int count = 0;
        List<byte[]> list = new LinkedList<>();

        if (res.size() < 100) {
            try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
                iterator.seek(marker.getBytes());
                while (iterator.isValid() && count < 1000) {
                    String key = new String(iterator.key());
                    if (key.startsWith(prefix)) {
                        if (!key.equalsIgnoreCase(marker)) {
                            byte[] realKey = iterator.key();
                            byte[] resKey = new byte[realKey.length - prefixLen];
                            System.arraycopy(realKey, prefixLen, resKey, 0, resKey.length);

                            res.onNext(new Tuple2<>(resKey, iterator.value()));
                            list.add(iterator.key());
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

            if (!list.isEmpty()) {
                BatchRocksDB.customizeOperateData(Utils.getMqRocksKey(), (db, w, r) -> {
                    for (byte[] key : list) {
                        w.delete(key);
                    }
                }).publishOn(DISK_SCHEDULER).subscribe();
            }
        }

        String finalNextMarker = nextMarker;

        if (!end) {
            DISK_SCHEDULER.schedule(() -> next(prefix, prefixLen, finalNextMarker, res), 1L, TimeUnit.SECONDS);
        } else {
            res.onComplete();
        }
    }

    public static Flux<Tuple2<byte[], byte[]>> iterator(String queueKey) {
        UnicastProcessor<Tuple2<byte[], byte[]>> res = UnicastProcessor.create();
        String iteratorKey = queueKey + "-";
        next(iteratorKey, iteratorKey.getBytes().length, iteratorKey, res);
        return res;
    }

    public static void dealCompressOrCrypto(boolean checkCompress, boolean checkCrypto, FileMeta fileMeta, String srcDisk, UnicastProcessor<byte[]> writeFlux) throws IOException {
        CompressorUtils.updateCompressStateIfNull(fileMeta);
        for (int k = 0; k < fileMeta.getOffset().length; k++) {
            long curOffset = fileMeta.getOffset()[k];
            long compressState = fileMeta.getCompressState()[k];
            long afterLen;
            if (checkCrypto) {
                afterLen = fileMeta.getCryptoAfterLen()[k];
            } else {
                afterLen = fileMeta.getCompressAfterLen()[k];
            }
            byte[] tmp = BlockDevice.get(srcDisk).read(curOffset, (int) afterLen);
            Tuple2<List<byte[]>, Integer> tuple2 = GetServerHandler.getAllPartBytes(srcDisk, k, fileMeta, tmp);

            if (tuple2.var1.size() > 1) {
                tmp = GetServerHandler.bytesPartMerge(tuple2);
                k += (tuple2.var1.size() - 1);
            }
            if (checkCrypto) {
                tmp = CryptoUtils.decrypt(fileMeta.getCrypto(), fileMeta.getSecretKey(), tmp);
            }
            if (checkCompress && compressState == 1) {
                tmp = CompressorUtils.uncompressData(tmp, fileMeta.getCompression());
            }
            writeFlux.onNext(tmp);
        }
    }

    /**
     * 检查并处理添加磁盘任务的完成状态
     *
     * @param poolQueueTag 存储池队列标签
     * @param vnode        处理完成的vnode
     * @param runningKey   重平衡任务key
     * @param operate      重平衡任务类型
     *
     */
    public static void checkAndHandleAddDiskCompletion(String poolQueueTag, String vnode, String runningKey, String operate) {
        RebuildRabbitMq.getMaster().hincrby(runningKey, "vnodeNum", -1);
        RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1);
        // 添加完成的vnode到set中
        RebuildRabbitMq.getMaster().sadd(ADD_DISK_COMPLETED_SET_PREFIX + poolQueueTag, vnode);
        // 判断是否所有vnode全部迁移完成
        String vnodeNumAll = RebuildRabbitMq.getMaster().hget(runningKey, "vnodeNumAll");
        Long addDiskTaskCompletedNum = RebuildRabbitMq.getMaster().scard(ADD_DISK_COMPLETED_SET_PREFIX + poolQueueTag);
        long addDiskTaskNum;
        if (ServerConstants.REMOVE_DISK.equals(operate)) {
            // 重构任务中 add_disk的任务数量等于 vnodeNumAll-remove_disk任务数量
            String removeDiskTaskNum = RebuildRabbitMq.getMaster().hget(runningKey, "removeDiskTaskNum");
            addDiskTaskNum = Long.parseLong(vnodeNumAll) - Long.parseLong(removeDiskTaskNum);
        } else {
            addDiskTaskNum = Long.parseLong(vnodeNumAll);
        }
        if (addDiskTaskCompletedNum >= addDiskTaskNum) {
            synchronized (runningKey.intern()) {
                String status = RebuildRabbitMq.getMaster().hget(runningKey, ADD_DISK_COMPLETE_STATUS);
                if (status == null) {
                    // add_disk任务全部完成,设置完成标志
                    RebuildRabbitMq.getMaster().hset(runningKey, ADD_DISK_COMPLETE_STATUS, RebuildCheckpointManager.RebuildCompletePhase.COMPLETION.name());
                    RebuildRabbitMq.getMaster().del(ADD_DISK_COMPLETED_SET_PREFIX + poolQueueTag);
                    log.info("migrate complete addDisk num:{} vnodeNumAll:{} operate:{} ", addDiskTaskCompletedNum, vnodeNumAll, operate);
                }
            }

        }
    }
}
