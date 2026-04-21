package com.macrosan.ec.server;

import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PART_UPLOAD_FILE;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_OBJECT_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.START_PUT_OBJECT;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * 加节点迁移过程中转发 写入当前节点vnode的所有数据到目标节点
 *
 * @author gaozhiyuan
 */
@Log4j2
public class MigrateServer {
    public volatile int start = 0;
    private Map<String, DstLunInfo> diskInMigrating = new ConcurrentHashMap<>();
    private static final AtomicIntegerFieldUpdater<MigrateServer> UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MigrateServer.class, "start");

    @AllArgsConstructor
    public static class DstLunInfo {
        public String lun;
        public String ip;
        public String key;

        public void addDelete(Tuple2<byte[], byte[]> tuple) {
            String queueKey = "node-" + key;
            MigrateUtil.add(queueKey, tuple.var1, tuple.var2);
        }

        public void addCopy(Tuple2<byte[], byte[]> tuple) {
            String queueKey = "node-copy-" + key;
            MigrateUtil.add(queueKey, tuple.var1, tuple.var2);
        }
    }

    private MigrateServer() {
    }

    public Mono<Boolean> startMigrate(String srcDisk, String vnode, String dstDisk, String ip) {
        DstLunInfo dstLunInfo = new DstLunInfo(dstDisk, ip, getKey(srcDisk, vnode));
        diskInMigrating.put(getKey(srcDisk, vnode), dstLunInfo);
        UPDATER.incrementAndGet(this);
        return ErasureServer.waitRunningStop();
    }

    public Flux<Tuple2<byte[], byte[]>> endMigrate(String srcDisk, String vnode) {
        DstLunInfo lunInfo = diskInMigrating.remove(getKey(srcDisk, vnode));
        UPDATER.decrementAndGet(this);
        String queueKey = "node-" + getKey(srcDisk, vnode);
        return ErasureServer.waitRunningStop().flatMapMany(b -> MigrateUtil.iterator(queueKey));
    }

    public Flux<Tuple2<byte[], byte[]>> endMigrateCopy(String srcDisk, String vnode) {
        String queueKey = "node-copy-" + getKey(srcDisk, vnode);
        return ErasureServer.waitRunningStop().flatMapMany(b -> MigrateUtil.iterator(queueKey));
    }

    private static MigrateServer instance = new MigrateServer();

    public String getKey(String lun, String vnode) {
        return lun + '$' + vnode;
    }

    public DstLunInfo getDstLunInfo(String key) {
        return diskInMigrating.get(key);
    }

    public DstLunInfo getDstLunInfo(String lun, String vnode) {
        return diskInMigrating.get(getKey(lun, vnode));
    }

    public static MigrateServer getInstance() {
        return instance;
    }

    public Flux<Payload> putChannel(String lun, String fileName, Flux<Payload> requestFlux, SocketReqMsg msg0) {
        int vnodeIndex = fileName.indexOf("_");
        int dirIndex = fileName.indexOf(File.separator);

        String vnode = fileName.substring(dirIndex + 1, vnodeIndex);
        DstLunInfo dstLunInfo = diskInMigrating.get(getKey(lun, vnode));

        if (null == dstLunInfo) {
            return requestFlux;
        }

        boolean[] first = new boolean[]{true};
        UnicastProcessor<Payload> publisher = UnicastProcessor.create();

        requestFlux = requestFlux.doOnNext(payload -> {
            if (first[0]) {
                SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
                msg.put("lun", dstLunInfo.lun);
                publisher.onNext(DefaultPayload.create(Json.encode(msg), START_PUT_OBJECT.name()));
                first[0] = false;
            } else {
                publisher.onNext(DefaultPayload.create(payload));
            }
        }).doOnComplete(publisher::onComplete);

        RSocketClient.getRSocket(dstLunInfo.ip, BACK_END_PORT)
                .flatMapMany(rSocket -> rSocket.requestChannel(publisher))
                .doOnError(e -> {
                    log.error("", e);
//                    log.info("msg0 != null :{}, storage:{}, lun:{}, fileName:{}", msg0 != null && msg0.get("storage") != null, msg0 == null ? null: msg0.get("storage"), lun, fileName);
                    //发布修复消息
                    if (msg0 != null && msg0.get("storage") != null) {
                        Set<Integer> errorChunksList = new ConcurrentHashSet<>();
                        StoragePool storagePool = StoragePoolFactory.getStoragePool(msg0.get("storage"), "");
                        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(vnode).block();
                        int errorIndex = -1;
                        for (int i = 0 ; i < nodeList.size(); i++) {
                            if ((nodeList.get(i).var1.equals(CURRENT_IP) && nodeList.get(i).var2.equals(lun)) || (nodeList.get(i).var1.equals(dstLunInfo.ip) && nodeList.get(i).var2.equals(dstLunInfo.lun))) {
                                errorIndex = i;
                                break;
                            }
                        }
                        if (errorIndex == -1) {
//                            log.error("srcDisk or dstDisk not in nodeList, currentIp:{}, lun:{}, dstIp:{}, dstLun:{}, fileName:{}, nodeList:{}", CURRENT_IP, lun, dstLunInfo.ip, dstLunInfo.lun, fileName, nodeList);
                            return;
                        }

                        errorChunksList.add(errorIndex);
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        if (StringUtils.isNotEmpty(msg0.get("uploadId"))) {
                            errorMsg.put("lun", dstLunInfo.lun)
                                    .put("errorChunksList", Json.encode(new ArrayList<>(errorChunksList)))
                                    .put("bucket", msg0.get("bucket"))
                                    .put("object", msg0.get("object"))
                                    .put("fileName", fileName)
                                    .put("versionId", msg0.get("versionId"))
                                    .put("storage", msg0.get("storage"))
                                    .put("poolQueueTag", poolQueueTag)
                                    .put("doubleWriteRecover", "1")
                                    .put("endIndex", msg0.get("endIndex"))
                                    .put("uploadId", msg0.get("uploadId"))
                                    .put("partNum", msg0.get("partNum"));
                            Optional.ofNullable(msg0.get("snapshotMark")).ifPresent(v -> errorMsg.put("snapshotMark", v));
                            Optional.ofNullable(msg0.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                            Optional.ofNullable(msg0.get("lastAccessStamp")).ifPresent(v -> errorMsg.put("lastAccessStamp", v));
                            CryptoUtils.putCryptoInfoToMsg(msg0.get("crypto"), msg0.get("secretKey"), errorMsg);
                            Mono.just(1).delayElement(Duration.ofSeconds(60)).doOnNext(i -> {
                                ObjectPublisher.publish(dstLunInfo.ip, errorMsg, ERROR_PART_UPLOAD_FILE);
                                log.info("publish addNode double writer error msg, dstIP:{}, dstLun:{}, fileName:{}", dstLunInfo.ip, dstLunInfo.lun, fileName);
                            }).subscribe();
                        } else {
                            errorMsg.put("lun", dstLunInfo.lun)
                                    .put("errorChunksList", Json.encode(new ArrayList<>(errorChunksList)))
                                    .put("bucket", msg0.get("bucket"))
                                    .put("object", msg0.get("object"))
                                    .put("fileName", fileName)
                                    .put("versionId", msg0.get("versionId"))
                                    .put("storage", msg0.get("storage"))
                                    .put("poolQueueTag", poolQueueTag)
                                    .put("doubleWriteRecover", "1")
                                    .put("fileOffset", "");
                            Optional.ofNullable(msg0.get("snapshotMark")).ifPresent(v -> errorMsg.put("snapshotMark", v));
                            Optional.ofNullable(msg0.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                            Optional.ofNullable(msg0.get("lastAccessStamp")).ifPresent(v -> errorMsg.put("lastAccessStamp", v));
                            CryptoUtils.putCryptoInfoToMsg(msg0.get("crypto"), msg0.get("secretKey"), errorMsg);
                            //延迟等待对象元数据写入完成
                            Mono.just(1).delayElement(Duration.ofSeconds(60)).doOnNext(i -> {
                                ObjectPublisher.publish(dstLunInfo.ip, errorMsg, ERROR_PUT_OBJECT_FILE);
                                log.info("publish addNode double writer error msg, dstIP:{}, dstLun:{}, fileName:{}", dstLunInfo.ip, dstLunInfo.lun, fileName);
                            }).subscribe();
                        }
                    }
                })
                .subscribe(ReferenceCounted::release);


        return requestFlux;
    }

    public Set<String> startMigrateVnode = new ConcurrentHashSet<>();

    public void addMigrateVnode(String lun, String vnode) {
        startMigrateVnode.add(getKey(lun, vnode));
    }

    public void deleteMigrateVnode(String lun, String vnode) {
        startMigrateVnode.remove(getKey(lun, vnode));
    }

    public boolean needWriteToMigrateColumnFamily(String lun, byte[] key) {
        String keyStr = new String(key);
        String vnode = LocalMigrateServer.getVnode(keyStr);
        return startMigrateVnode.contains(getKey(lun, vnode));
    }

}
