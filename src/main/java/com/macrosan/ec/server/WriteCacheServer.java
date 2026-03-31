package com.macrosan.ec.server;

import com.macrosan.filesystem.cache.WriteCacheNode;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketDataMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class WriteCacheServer {
    public static boolean writeCacheDebug = false;

    public static Mono<Payload> writeCache(Payload payload) {
        boolean local = payload instanceof LocalPayload;
        SocketDataMsg msg = local ? ((LocalPayload<SocketDataMsg>) payload).data : SocketDataMsg.toSocketReqMsg(payload.data());

        Inode inode = Json.decodeValue(msg.get("inode"), Inode.class);
        long curOffset = Long.parseLong(msg.get("curOffset"));
        byte[] bytes = msg.data;

        if (writeCacheDebug) {
            log.info("writeHandle start, inode:{}, offset:{}", inode.getNodeId(), curOffset);
        }

        int curSize = bytes.length;

        if (curSize != 0) {
            return WriteCacheNode.getCache(inode.getBucket(), inode.getNodeId(), 0, inode.getStorage())
                    .flatMap(writeCacheNode -> writeCacheNode.writeCache(bytes, curOffset))
                    .map(res -> {
                        if (writeCacheDebug) {
                            log.info("writeHandle end {}, inode:{}, offset:{}, size:{}", res, inode.getNodeId(), curOffset, curSize);
                        }
                        if (res >= 0) {
                            return local ? new LocalPayload<>(ErasureServer.PayloadMetaType.SUCCESS, res) : DefaultPayload.create(String.valueOf(res), ErasureServer.PayloadMetaType.SUCCESS.name());
                        }  else {
                            return local ? new LocalPayload<>(ErasureServer.PayloadMetaType.ERROR, res) : DefaultPayload.create(String.valueOf(res), ErasureServer.PayloadMetaType.ERROR.name());
                        }
                    });
        } else {
            return Mono.just(local ? new LocalPayload<>(ErasureServer.PayloadMetaType.SUCCESS, 1) : DefaultPayload.create(String.valueOf(1), ErasureServer.PayloadMetaType.SUCCESS.name()));
        }
    }

    public static Mono<Payload> flushWriteCache(Payload payload) {
        boolean local = payload instanceof LocalPayload;
        SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());

        Inode inode = Json.decodeValue(msg.get("inode"), Inode.class);
        long offset = Long.parseLong(msg.get("offset"));
        int count = Integer.parseInt(msg.get("count"));
        int type = Integer.parseInt(msg.get("type"));
        boolean write = "1".equals(msg.get("write"));

        long end;
        if (msg.get("end") != null) {
            end = Long.parseLong(msg.get("end"));
        } else {
            end = Long.MAX_VALUE;
        }

        if (writeCacheDebug) {
            log.info("server flushWriteCache, ino: {}, offset: {}, count: {}, end: {}, write: {}", inode.getNodeId(), offset, count, end, write);
        }
        if (type <= 2) {
            // type 1(正常flush) 和 2(写满一块的flush)
            return WriteCacheNode.getCache(inode.getBucket(), inode.getNodeId(), 0, inode.getStorage())
                    .flatMap(writeCacheNode -> writeCacheNode.commit(inode, offset, count, end, type == 2, write))
                    .flatMap(b -> {
                        if (writeCacheDebug) {
                            log.info("server flushWriteCache end, ino: {}, offset: {}, count: {}, end: {}, write: {}, {}", inode.getNodeId(), offset, count, end, write, b);
                        }
                        if (b) {
                            return Mono.just(local ? LocalPayload.SUCCESS_PAYLOAD : ErasureServer.SUCCESS_PAYLOAD);
                        } else {
                            return Mono.just(local ? LocalPayload.ERROR_PAYLOAD : ErasureServer.ERROR_PAYLOAD);
                        }
                    });
        } else if (type == 3) {
            return WriteCacheNode.getCache(inode.getBucket(), inode.getNodeId(), 1, inode.getStorage())
                    .flatMap(writeCacheNode -> writeCacheNode.flushByteCache())
                    .flatMap(b -> {
                        if (writeCacheDebug) {
                            log.info("server flushWriteCache end, ino: {}, offset: {}, count: {}, end: {}, write: {}, {}", inode.getNodeId(), offset, count, end, write, b);
                        }
                        if (b) {
                            return Mono.just(local ? LocalPayload.SUCCESS_PAYLOAD : ErasureServer.SUCCESS_PAYLOAD);
                        } else {
                            return Mono.just(local ? LocalPayload.ERROR_PAYLOAD : ErasureServer.ERROR_PAYLOAD);
                        }
                    });
        } else {
            WriteCacheNode.removeCache(inode.getNodeId());
            return Mono.just(local ? LocalPayload.SUCCESS_PAYLOAD : ErasureServer.SUCCESS_PAYLOAD);
        }
    }
}
