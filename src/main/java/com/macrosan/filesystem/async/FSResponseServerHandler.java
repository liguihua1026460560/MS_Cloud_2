package com.macrosan.filesystem.async;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.RequestChannalHandler;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketDataMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.macrosan.ec.server.ErasureServer.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Log4j2
public class FSResponseServerHandler implements RequestChannalHandler {

    protected UnicastProcessor<Payload> responseFlux;

    public FSResponseServerHandler(UnicastProcessor<Payload> responseFlux) {
        this.responseFlux = responseFlux;
    }

    @Override
    public void timeOut() {
    }

    public static RequestChannalHandler fileAsyncChannel(Flux<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        FSResponseServerHandler handler = new FSResponseServerHandler(responseFlux);

        requestFlux.subscribe(payload -> {
            try {
                ErasureServer.PayloadMetaType metaType = ErasureServer.PayloadMetaType.valueOf(payload.getMetadataUtf8());
                switch (metaType) {
                    case START_FILE_ASYNC_CHANNEL:
                        handler.start(payload);
                        break;
                    case PUT_FILE_ASYNC_CHANNEL:
                        handler.put(payload);
                        break;
                    default:
                        log.error("no such metaType, {}", metaType);
                }
            } catch (Exception e) {
                log.error("{}", payload.getMetadataUtf8(), e);
                handler.timeOut();
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            } finally {
                payload.release();
            }
        });

        return handler;
    }

    Map<String, String> dataMap;
    byte[] data;
    int position = 0;


    private void start(Payload payload) {
        this.dataMap = Json.decodeValue(payload.getDataUtf8(), new TypeReference<Map<String, String>>() {
        });
        Inode.InodeData inodeData = Json.decodeValue(dataMap.get("inodeData"), Inode.InodeData.class);
        data = new byte[(int) inodeData.getSize()];
    }

    private void put(Payload payload) {
        ByteBuf byteBuf = payload.sliceData();
//        int read = byteBuf.readableBytes();
//        data = new byte[read];
//        byteBuf.readBytes(data);
        byte[] bytes = ByteBufUtil.getBytes(byteBuf);
        System.arraycopy(bytes, 0, data, position, bytes.length);
        position += bytes.length;

        if (DataSynChecker.isDebug) {
            log.info("FSR put, {} {} {} {}", dataMap.get("nodeId"), dataMap.get("fileOffset"), data.length, position);
        }
        if (position >= data.length) {
            Digest digest = new Md5Digest();
            digest.update(data);
            String md5 = Hex.encodeHexString(digest.digest());

            complete();
        } else {
            responseFlux.onNext(CONTINUE_PAYLOAD);
        }
    }

    private void complete() {
        long nodeId = Long.parseLong(dataMap.get("nodeId"));
        String bucket = dataMap.get("bucket");
        Inode.InodeData inodeData = Json.decodeValue(dataMap.get("inodeData"), Inode.InodeData.class);

        StorageOperate dataOperate = new StorageOperate(DATA, dataMap.get("obj"), data.length);
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucket);
//        StoragePool dataPool = StoragePoolFactory.getStoragePool(new StorageOperate(DATA, "", inodeData.size));
        String fileName = Utils.getObjFileName(dataPool, bucket, RandomStringUtils.randomAlphanumeric(4),
                RandomStringUtils.randomAlphanumeric(32)) + '/';
        long fileOffset = Long.parseLong(dataMap.get("fileOffset"));
        Inode.InodeData inodeData1 = new Inode.InodeData()
                .setSize(inodeData.getSize())
                .setStorage(dataPool.getVnodePrefix())
                .setOffset(0L)
                .setEtag(inodeData.getEtag())
                .setFileName(fileName);

        String vnode = fileName.split(File.separator)[1].split("_")[0];
        List<Tuple3<String, String, String>> nodeList = dataPool.mapToNodeInfo(vnode).block();
        Encoder encoder = dataPool.getEncoder(inodeData.getSize());
        encoder.put(data);
        encoder.complete();

        MonoProcessor<Boolean> rollBackProcessor = MonoProcessor.create();

        Inode tmpInode = new Inode()
                .setBucket(bucket)
                .setVersionId("null")
                .setObjName(dataMap.get("obj"))
                .setNodeId(nodeId);
        FsUtils.putObj(dataPool, encoder, fileName, nodeList, tmpInode, fileOffset, rollBackProcessor)
                .subscribe(b -> {
                    if (b) {
                        MonoProcessor<Inode> res = MonoProcessor.create();
                        SocketReqMsg socketReqMsg = new SocketReqMsg("", 0);
                        for (String k : dataMap.keySet()) {
                            socketReqMsg.dataMap.put(k, dataMap.get(k));
                        }

                        // opt 1，updateInode
                        socketReqMsg.put("inodeData", Json.encode(inodeData1));
                        Node.getInstance().exec(nodeId, socketReqMsg, 0, res);

                        res.subscribe(i -> {
                            if (i.getLinkN() >= 0) {
                                responseFlux.onNext(SUCCESS_PAYLOAD);
                                responseFlux.onComplete();
                            } else {
                                responseFlux.onNext(ERROR_PAYLOAD);
                                responseFlux.onComplete();
                            }
                        });
                    } else {
                        log.info("put file err, {}", Json.encode(dataMap));
                        responseFlux.onNext(ERROR_PAYLOAD);
                        responseFlux.onComplete();
                    }
                }, e -> {
                    log.error("channel put file err, {}, ", dataMap, e);
                    responseFlux.onNext(ERROR_PAYLOAD);
                    responseFlux.onComplete();
                });
    }

    public static Mono<Payload> fileAsyncReqRes(Payload payload) {
        try {
            boolean local = payload instanceof LocalPayload;

            SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());
            long nodeId = Long.parseLong(msg.get("nodeId"));

            int opt = Integer.parseInt(msg.get("opt"));
            if (opt == 1) {
                SocketDataMsg dataMsg = (SocketDataMsg) msg;
                String bucket = msg.get("bucket");
                Inode.InodeData inodeData = Json.decodeValue(msg.get("inodeData"), Inode.InodeData.class);

                StorageOperate dataOperate = new StorageOperate(DATA, msg.get("obj"), dataMsg.data.length);
                StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucket);
//                StoragePool dataPool = StoragePoolFactory.getStoragePool(new StorageOperate(DATA, "", inodeData.size));
                String fileName = Utils.getObjFileName(dataPool, bucket, RandomStringUtils.randomAlphanumeric(4),
                        RandomStringUtils.randomAlphanumeric(32)) + '/';
                long fileOffset = Long.parseLong(msg.get("fileOffset"));
                Inode.InodeData data = new Inode.InodeData()
                        .setSize(inodeData.getSize())
                        .setStorage(dataPool.getVnodePrefix())
                        .setOffset(0L)
                        .setEtag(inodeData.getEtag())
                        .setFileName(fileName);

                String vnode = fileName.split(File.separator)[1].split("_")[0];
                List<Tuple3<String, String, String>> nodeList = dataPool.mapToNodeInfo(vnode).block();
                Encoder encoder = dataPool.getEncoder(inodeData.getSize());
                encoder.put(dataMsg.data);
                encoder.complete();

                MonoProcessor<Boolean> rollBackProcessor = MonoProcessor.create();

                Inode tmpInode = new Inode()
                        .setBucket(bucket)
                        .setVersionId("null")
                        .setObjName(msg.get("obj"))
                        .setNodeId(nodeId);

                return FsUtils.putObj(dataPool, encoder, fileName, nodeList, tmpInode, fileOffset, rollBackProcessor)
                        .flatMap(b -> {
                            if (b) {
                                MonoProcessor<Inode> res = MonoProcessor.create();
                                SocketReqMsg socketReqMsg = new SocketReqMsg("", 0);
                                for (String k : dataMsg.getObjMap().keySet()) {
                                    socketReqMsg.dataMap.put(k, dataMsg.get(k));
                                }

                                socketReqMsg.put("inodeData", Json.encode(data));
                                if (DataSynChecker.isDebug) {
                                    log.info("start putObj, {}, {}, {}, {}, {}, {}", nodeId, dataPool.getVnodePrefix(), fileName, vnode, nodeList, socketReqMsg.dataMap);
                                }
                                Node.getInstance().exec(nodeId, socketReqMsg, 0, res);

                                return res.map(i -> {
                                    if (i.getLinkN() >= 0) {
                                        return DefaultPayload.create(Json.encode(i), SUCCESS.name());
                                    } else {
                                        return DefaultPayload.create(Json.encode(i), ERROR.name());
                                    }
                                });
                            } else {
                                log.info("put file err, {}", Json.encode(msg));
                                return Mono.just(ERROR_PAYLOAD);
                            }
                        });
            } else if (opt == 50) {
                Inode createInode = Json.decodeValue(msg.get("inode"), Inode.class);
                StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, "", Long.MAX_VALUE);
                String storage = StoragePoolFactory.getStoragePool(operate, createInode.getBucket()).getVnodePrefix();
                createInode.setStorage(storage);
                msg.put("inode", Json.encode(createInode));

                // create时使用本地的version，否则version仍使用对端站点的EC_VERSION
                String bucket = msg.get("bucket");
                Inode version = Node.getInstance().getVersion(nodeId, bucket, true, "").block();
                msg.put("version", version.getVersionNum());
            }

            MonoProcessor<Inode> res = MonoProcessor.create();


            Node.getInstance().exec(nodeId, msg, 0, res);

            return res.map(i -> {
                if (DataSynChecker.isDebug) {
                    log.info("fileAsyncReqRes done,{} {} {}", i.getLinkN(), opt, msg.dataMap);
                } else {
                    log.debug("fileAsyncReqRes done,{} {} {}", i.getLinkN(), opt, msg.dataMap);
                }

                if (Inode.NOT_FOUND_INODE.getLinkN() == i.getLinkN()) {
                    // 如同步deleteInode时,待同步端已经没有相关的inode。或者待同步端已经删除了该inode
                    return DefaultPayload.create(Json.encode(i), SUCCESS.name());
                }

                if (i.getLinkN() >= 0) {
                    return DefaultPayload.create(Json.encode(i), SUCCESS.name());
                } else {
                    return DefaultPayload.create(Json.encode(i), ERROR.name());
                }
            });
        } catch (Exception e) {
            log.error("fileAsync error, ", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

}
