package com.macrosan.filesystem.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.lease.LeaseLock;
import com.macrosan.filesystem.cifs.lock.CIFSLock;
import com.macrosan.filesystem.cifs.notify.NotifyLock;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessLock;
import com.macrosan.filesystem.lock.redlock.RedLock;
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

import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class LockClient {
    private static Mono<Boolean> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> lockRes = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("lock {} fail", msgs.get(0));
            lockRes.onNext(false);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                lockRes.onNext(false);
            } else {
                lockRes.onNext(true);
            }
        });

        return lockRes;
    }

    private static Mono<Boolean> tryUnLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.UNLOCK, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("unlock {} fail", msgs.get(0));
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

    public static <T extends Lock> Mono<Boolean> lock(String bucket, String key, T value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    String curIP = ServerConfig.getInstance().getHeartIp1();
                    addCurIpOrNot(nodeList, curIP);
                    int type = Lock.getType(value.getClass());

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
                                } else {
                                    return tryUnLock(msgs, nodeList)
                                            .map(b0 -> false);
                                }
                            });
                });
    }

    public static <T extends Lock> Mono<Boolean> unlock(String bucket, String key, T value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.getType(value.getClass());

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    String curIP = NodeCache.getIP(getNode(value, type));
                    addCurIpOrNot(nodeList, curIP);

                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("value", Json.encode(value)))
                            .collect(Collectors.toList());
                    return tryUnLock(msgs, nodeList);
                });
    }

    /**
     * ip 漂移时解掉原加锁节点的锁
     * @param node 原加锁节点
     */
    public static <T extends Lock> Mono<Boolean> unlock(String bucket, String key, T value, String node) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.getType(value.getClass());

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    try {
                        String oldIp = NodeCache.getIP(node);
                        if (oldIp != null) {
                            addCurIpOrNot(nodeList, oldIp);
                        }
                    } catch (Exception e) {
                        log.error(e);
                    }

                    String curIP = NodeCache.getIP(getNode(value, type));
                    addCurIpOrNot(nodeList, curIP);

                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("value", Json.encode(value)))
                            .collect(Collectors.toList());
                    return tryUnLock(msgs, nodeList);
                });
    }

    public static <T extends Lock> String getNode(T value, int type) {
        switch (type) {
            case Lock.RED_LOCK_TYPE:
                return ((RedLock) value).getNode();
            case Lock.NOTIFY_TYPE:
                return ((NotifyLock) value).getNode();
            case Lock.SHARE_ACCESS_TYPE:
                return ((ShareAccessLock) value).getNode();
            case Lock.LEASE_TYPE:
                return ((LeaseLock) value).getNode();
            case Lock.CIFS_LOCK_TYPE:
                return ((CIFSLock) value).getNode();
        }
        return "";
    }


    public static void addCurIpOrNot(List<Tuple3<String, String, String>> nodeList, String curIP) {
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
    }
}
