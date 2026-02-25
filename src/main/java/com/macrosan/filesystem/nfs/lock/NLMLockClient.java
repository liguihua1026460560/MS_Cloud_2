package com.macrosan.filesystem.nfs.lock;

import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.nfs.types.Owner;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class NLMLockClient {
    //Lock debug打印
    public static boolean LOCK_DEBUG = false;
    //是否开启Lock保持一致性
    public static boolean LOCK_ENABLE = true;


    private static Mono<Boolean> tryLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> lockRes = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, PayloadMetaType.NLM_LOCK, String.class, nodeList);

        if (LOCK_DEBUG) {
            log.info("NLM tryLock {}", msgs.get(0));
        }

        responseInfo.responses.subscribe(tuple3 -> {
            if (LOCK_DEBUG) {
                log.info("NLM lock {} return {}:{}", msgs.get(0), tuple3.var1, tuple3.var2);
            }
        }, e -> {
            log.error("NLM lock {} fail", msgs.get(0));
            lockRes.onNext(false);
        }, () -> {
            if (responseInfo.successNum > nodeList.size() / 2) {
                lockRes.onNext(true);
            } else {
                lockRes.onNext(false);
            }
        });

        return lockRes;
    }

    private static Mono<Boolean> tryUnLock(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, PayloadMetaType.NLM_UNLOCK, String.class, nodeList);

        if (LOCK_DEBUG) {
            log.info("NLM tryUnLock {}", msgs.get(0));
        }

        responseInfo.responses.subscribe(tuple3 -> {
            if (LOCK_DEBUG) {
                log.info("NLM unlock {} return {}:{}", msgs.get(0), tuple3.var1, tuple3.var2);
            }
        }, e -> {
            log.error("NLM unlock {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum > nodeList.size() / 2) {
                res.onNext(true);
            } else {
                res.onNext(false);
            }
        });

        return res;
    }

    private static Mono<Boolean> tryCancel(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, PayloadMetaType.NLM_CANCEL, String.class, nodeList);

        if (LOCK_DEBUG) {
            log.info("NLM tryCancel {}", msgs.get(0));
        }

        responseInfo.responses.subscribe(tuple3 -> {
            if (LOCK_DEBUG) {
                log.info("NLM cancel {} return {}:{}", msgs.get(0), tuple3.var1, tuple3.var2);
            }
        }, e -> {
            log.error("NLM cancel {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum > nodeList.size() / 2) {
                res.onNext(true);
            } else {
                res.onNext(false);
            }
        });

        return res;
    }

    private static Mono<Boolean> tryGranted(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, PayloadMetaType.NLM_GRANTED, String.class, nodeList);

        if (LOCK_DEBUG) {
            log.info("NLM tryGranted {}", msgs.get(0));
        }

        responseInfo.responses.subscribe(tuple3 -> {
            if (LOCK_DEBUG) {
                log.info("NLM granted {} return {}:{}", msgs.get(0), tuple3.var1, tuple3.var2);
            }
        }, e -> {
            log.error("NLM granted {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum > nodeList.size() / 2) {
                res.onNext(true);
            } else {
                res.onNext(false);
            }
        });

        return res;
    }

    public static Mono<Boolean> lock(String bucket, String key, Owner owner, boolean block) {
        if (!LOCK_ENABLE) {
            return Mono.just(true);
        }
        String value = owner.ip + "_" + owner.local + "_" + owner.svid; // 新版不使用
        int lockType = owner.exclusive ? 1 : 0;

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("bucket", bucket)
                            .put("key", key)
                            .put("lock", String.valueOf(lockType))
                            .put("value", value) // 新版不使用

                            .put("ip", owner.ip)
                            .put("clientName", owner.clientName)
                            .put("svid", String.valueOf(owner.svid))
                            .put("local", owner.local)
                            .put("offset", String.valueOf(owner.offset))
                            .put("len", String.valueOf(owner.len))

                            .put("block", String.valueOf(block))
                            .put("node", NLMLockWait.localNode)
                            .put("unlock", String.valueOf(false));

                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    return tryLock(msgs, nodeList)
                            .doOnNext(b -> {
                                if (b) {
                                    NLMLockServer.unlockMap.remove(owner.mergeKey());
                                    NLMLockServer.unlockMap.remove(owner.ownerKey());
                                    NLMLockKeeper.keep(bucket, key, owner);
                                }
                            })
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

    public static Mono<Boolean> unlock(String bucket, String key, Owner owner) {
        if (!LOCK_ENABLE) {
            return Mono.just(true);
        }
        String value = owner.ip + "_" + owner.local + "_" + owner.svid; // 新版不使用
        NLMLockKeeper.remove(bucket, key, owner);

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("value", value) // 新版不使用

                                    .put("ip", owner.ip)
                                    .put("clientName", owner.clientName)
                                    .put("svid", String.valueOf(owner.svid))
                                    .put("local", owner.local)
                                    .put("offset", String.valueOf(owner.offset))
                                    .put("len", String.valueOf(owner.len))

                                    .put("unlock", String.valueOf(true)))

                            .collect(Collectors.toList());

                    return tryUnLock(msgs, nodeList);
                });
    }

    public static Mono<Boolean> cancel(String bucket, String key, Owner owner) {
        if (!LOCK_ENABLE) {
            return Mono.just(true);
        }
        String value = owner.ip + "_" + owner.local + "_" + owner.svid; // 新版不使用

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("value", value) // 新版不使用

                                    .put("ip", owner.ip)
                                    .put("clientName", owner.clientName)
                                    .put("svid", String.valueOf(owner.svid))
                                    .put("local", owner.local)
                                    .put("offset", String.valueOf(owner.offset))
                                    .put("len", String.valueOf(owner.len)))

                            .collect(Collectors.toList());
                    return tryCancel(msgs, nodeList);
                });
    }

    public static Mono<Boolean> granted(String bucket, String key, Owner owner) {
        if (!LOCK_ENABLE) {
            return Mono.just(true);
        }
        String value = owner.ip + "_" + owner.local + "_" + owner.svid; // 新版不使用
        String curIP = NodeCache.getIP(owner.node);

        return Mono.just(new ArrayList<Tuple3<String, String, String>>())
                .flatMap(nodeList -> {
                    nodeList.add(new Tuple3<>(curIP, "", ""));
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("value", value) // 新版不使用
                                    .put("owner", Json.encode(owner)))
                            .collect(Collectors.toList());
                    return tryGranted(msgs, nodeList);
                });
    }
}
