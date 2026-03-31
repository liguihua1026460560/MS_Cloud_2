package com.macrosan.filesystem.nfs.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;

@Log4j2
public class NFS4LockClient {
    public static boolean LOCK_DEBUG = false;

    private static Mono<NFS4Lock> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<NFS4Lock> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);
        NFS4Lock[] results = new NFS4Lock[nodeList.size()];
        responseInfo.responses.subscribe(tuple3 -> {
            if (tuple3.var2.equals(SUCCESS)) {
                String var3 = tuple3.var3;
                results[tuple3.var1] = Json.decodeValue(var3, NFS4Lock.class);
            }else {
                results[tuple3.var1] = NFS4Lock.ERROR_LOCK;
            }
        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(NFS4Lock.ERROR_LOCK);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(NFS4Lock.ERROR_LOCK);
            } else {
                int sucNum = 0;
                try {
                    int hasConflict = 0;
                    NFS4Lock[] conflictLocks = new NFS4Lock[nodeList.size()];
                    NFS4Lock conflictLock = NFS4Lock.ERROR_LOCK;
                    for (int i = 0; i < results.length; i++) {
                        NFS4Lock nfs4Lock = results[i];
                        if (NFS4Lock.DEFAULT_LOCK.equals(nfs4Lock)) {
                            sucNum++;
                        } else if (!NFS4Lock.ERROR_LOCK.equals(nfs4Lock)) {
                            hasConflict++;
                            conflictLocks[i] = nfs4Lock;
                            conflictLock = nfs4Lock;
                        }
                    }
                    if (sucNum == nodeList.size()) {
                        res.onNext(NFS4Lock.DEFAULT_LOCK);
                    } else if (sucNum > nodeList.size() / 2) {
                        NFS4Lock clone = NFS4Lock.DEFAULT_LOCK;
                        if (hasConflict > 0) {
                            clone = NFS4Lock.DEFAULT_LOCK.clone();
                            for (int i = 0; i < conflictLocks.length; i++) {
                                NFS4Lock conflictLock0 = conflictLocks[i];
                                if (conflictLock0 != null && Objects.equals(nodeList.get(i).var1, ServerConfig.getInstance().getHeartIp1())){
                                    clone.clientLock = true;
                                    break;
                                }
                            }
                        }
                        res.onNext(clone);
                    } else {
                        if (sucNum > 0) {
                            conflictLock.optType = -1;
                        }
                        res.onNext(conflictLock);
                    }
                } catch (Exception e) {
                    log.error("nfs v4 fail", e);
                }
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
                                    if (!NFS4Lock.DEFAULT_LOCK.equals(lock) && lock.optType == -1) {
                                        return tryUnLock(msgs, nodeList).map(v -> lock);
                                    } else if (NFS4Lock.DEFAULT_LOCK.equals(lock) && lock.clientLock){
                                        return tryLock(msgs, nodeList).map(v -> NFS4Lock.DEFAULT_LOCK);
                                    }
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
