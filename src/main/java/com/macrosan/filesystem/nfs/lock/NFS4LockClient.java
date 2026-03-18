package com.macrosan.filesystem.nfs.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.lock.CIFSLock;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.filesystem.lock.LockClient.addCurIpOrNot;
import static com.macrosan.filesystem.lock.LockClient.getNode;

@Log4j2
public class NFS4LockClient {
    public static boolean LOCK_DEBUG = false;

    private static Mono<NFS4Lock> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<NFS4Lock> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);
        NFS4Lock[] result = new NFS4Lock[1];
        responseInfo.responses.subscribe(tuple3 -> {
            if (tuple3.var2.equals(SUCCESS)) {
                String var3 = tuple3.var3;
                result[0] = Json.decodeValue(var3, NFS4Lock.class);
            }
        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(NFS4Lock.ERROR_LOCK);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(NFS4Lock.ERROR_LOCK);
            } else {
                try {
                } catch (Exception e) {

                    log.error("nfs v4 fail", e);
                }
                res.onNext(result[0]);
            }
        });

        return res;
    }

    private static Mono<NFS4Lock> tryUnLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<NFS4Lock> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.UNLOCK, String.class, nodeList);
        NFS4Lock[] result = new NFS4Lock[1];
        responseInfo.responses.subscribe(tuple3 -> {

        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(NFS4Lock.ERROR_LOCK);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(NFS4Lock.ERROR_LOCK);
            } else {
                res.onNext(NFS4Lock.DEFAULT_LOCK);
            }
        });

        return res;
    }


    public static Mono<NFS4Lock> lock(String bucket, String key, NFS4Lock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    String curIP = ServerConfig.getInstance().getHeartIp1();
                    boolean hadCurIP = false;

                    for (Tuple3<String, String, String> t : nodeList) {
                        if (t.var1.equals(curIP)) {
                            hadCurIP = true;
                            break;
                        }
                    }

                    if (!hadCurIP) {
                        nodeList.add(new Tuple3<>(curIP, "", ""));
                    }
                    int type = Lock.NFS4_LOCK_TYPE;

                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("bucket", bucket)
                            .put("key", key)
                            .put("type", String.valueOf(type))
                            .put("value", Json.encode(value));

                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    return tryLock(msgs, nodeList)
                            .flatMap(lock -> {
                                if (!NFS4Lock.ERROR_LOCK.equals(lock)) {
                                    return Mono.just(lock);
                                } else {
                                    return tryUnLock(msgs, nodeList);
                                }
                            });
                });
    }

    public static Mono<NFS4Lock> unLockOrRemoveWait(String bucket, String key, NFS4Lock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.NFS4_LOCK_TYPE;

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("value", Json.encode(value)))
                            .collect(Collectors.toList());
                    return tryUnLock(msgs, nodeList);
                });
    }


}
