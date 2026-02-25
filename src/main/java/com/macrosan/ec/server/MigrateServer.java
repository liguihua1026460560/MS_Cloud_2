package com.macrosan.ec.server;

import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.utils.functional.Tuple2;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.START_PUT_OBJECT;
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

    public Flux<Payload> putChannel(String lun, String fileName, Flux<Payload> requestFlux) {
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
                .doOnError(e -> log.error("", e))
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
