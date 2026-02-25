package com.macrosan.filesystem.nfs.delegate;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class DelegateClient {
    public static Mono<Boolean> tryDelegate(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("NFS V4 Delegate {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(false);
            } else {

                res.onNext(true);
            }
        });

        return res;
    }


    private static Mono<Boolean> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);
        DelegateLock[] result = new DelegateLock[1];
        responseInfo.responses.index().subscribe(tuple -> {
        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(false);
            } else {
                res.onNext(true);
            }
        });

        return res;
    }

    private static Mono<Boolean> tryUnLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.UNLOCK, String.class, nodeList);
        responseInfo.responses.subscribe(tuple3 -> {

        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(false);
            } else {
                res.onNext(true);
            }
        });

        return res;
    }


    public static Mono<Boolean> lock(String bucket, String key, DelegateLock value) {
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
                    int type = Lock.NFS4_DELEGATE_TYPE;

                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("bucket", bucket)
                            .put("key", key)
                            .put("type", String.valueOf(type))
                            .put("value", Json.encode(value));

                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    return tryLock(msgs, nodeList)
                            .flatMap(b -> {
                                if (b) {
                                    return Mono.just(true);
                                } else if (value.type == DelegateLock.ADD_DELEGATE_TYPE){
                                    return tryUnLock(msgs, nodeList).map(b0 -> false);
                                }else {
                                    return Mono.just(false);
                                }
                            });
                });
    }

    public static Mono<Boolean> unLock(String bucket, String key, DelegateLock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.NFS4_DELEGATE_TYPE;

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
