package com.macrosan.filesystem.nfs.shareAccess;

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

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;

@Log4j2
public class ShareAccessClient {
    private static Mono<ShareAccessLock> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<ShareAccessLock> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);
        ShareAccessLock[] result = new ShareAccessLock[1];
        responseInfo.responses.index().subscribe(tuple -> {
            Long index = tuple.getT1();
            Tuple3<Integer, ErasureServer.PayloadMetaType, String> tuple3 = tuple.getT2();
            if (tuple3.var2.equals(SUCCESS)) {
                String var3 = tuple3.var3;
                ShareAccessLock shareAccessLock = Json.decodeValue(var3, ShareAccessLock.class);
                if (result[0] == null) {
                    result[0] = shareAccessLock;
                } else if (result[0].versionNum.compareTo(shareAccessLock.versionNum) < 0) {
                    result[0] = shareAccessLock;
                }
            }
        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(ShareAccessLock.ERROR_SHARE);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(ShareAccessLock.ERROR_SHARE);
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

    private static Mono<ShareAccessLock> tryUnLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<ShareAccessLock> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.UNLOCK, String.class, nodeList);
        responseInfo.responses.subscribe(tuple3 -> {

        }, e -> {
            log.error("nfs v4 {} fail", msgs.get(0));
            res.onNext(ShareAccessLock.ERROR_SHARE);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(ShareAccessLock.ERROR_SHARE);
            } else {
                res.onNext(ShareAccessLock.NOT_FOUND_SHARE);
            }
        });

        return res;
    }


    public static Mono<ShareAccessLock> lock(String bucket, String key, ShareAccessLock value) {
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
                    int type = Lock.NFS4_SHARE_ACCESS_TYPE;

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
                                if (!ShareAccessLock.ERROR_SHARE.equals(lock)) {
                                    return Mono.just(lock);
                                } else {
                                    return tryUnLock(msgs, nodeList);
                                }
                            });
                });
    }

    public static Mono<ShareAccessLock> unLock(String bucket, String key, ShareAccessLock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.NFS4_SHARE_ACCESS_TYPE;

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
