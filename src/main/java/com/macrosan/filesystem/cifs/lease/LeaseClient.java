package com.macrosan.filesystem.cifs.lease;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;

@Log4j2
public class LeaseClient {
    public static boolean LOCK_DEBUG = false;

    private static Mono<Integer> tryLease(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Integer> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LOCK, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS Lease {} fail", msgs.get(0));
            res.onNext(-1);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(-1);
            } else {
                int state = 0;
                try {
                    for (int i = 0; i < 3; i++) {
                        final int bitPosition = i;
                        boolean allNodesHavePermission = Arrays.stream(responseInfo.res)
                                .filter(item -> SUCCESS.equals(item.var1))
                                .map(item -> Integer.parseInt(item.var2))
                                .allMatch(permission -> (permission & (1 << bitPosition)) != 0); // 如果所有成功节点的这一位权限都为 1，则将该位添加到 state
                        if (allNodesHavePermission) {
                            state |= (1 << bitPosition);
                        }
                    }
                } catch (Exception e) {
                    state = -1;
                    log.error("CIFS Lease fail", e);
                }
                res.onNext(state);
            }
        });

        return res;
    }

    private static Mono<Boolean> tryUnLease(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.UNLOCK, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS unLease {} fail", msgs.get(0));
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

    private static Mono<Boolean> tryBreak(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList, SMB2FileId smb2FileId) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.CIFS_BREAK_LEASE, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS breakLease {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(false);
            } else {
                AtomicBoolean block = new AtomicBoolean(false);
                try {
                    Set<String> leaseKeys = Arrays.stream(responseInfo.res)
                            .filter(item -> SUCCESS.equals(item.var1))
                            .filter(item -> !item.var2.isEmpty()) // 过滤掉空字符串
                            .flatMap(item -> {
                                try {
                                    return Json.decodeValue(item.var2, new TypeReference<Set<String>>() {}).stream();
                                } catch (Exception e) {
                                    log.error("CIFS breakLease json error {}", item.var2);
                                    return Stream.empty();
                                }
                            })
                            .collect(ConcurrentHashMap::newKeySet, Set::add, Set::addAll);
                    if (!leaseKeys.isEmpty()) {
                        LeaseCache.blockToBreakMap.compute(smb2FileId, (smb2FileId0, set) -> {
                            LeaseCache.blockToBreakSuccessMap.computeIfPresent(smb2FileId, (smb2FileId00, set0) -> {
                                leaseKeys.removeAll(set0);
                                return null;
                            });
                            if (!leaseKeys.isEmpty()) {
                                if (set == null) {
                                    set = leaseKeys;
                                } else {
                                    set.addAll(leaseKeys);
                                }
                                block.set(true);
                            } else {
                                block.set(false);
                            }
                            return set;
                        });
                    } else {
                        block.set(false);
                    }
                } catch (Exception e) {
                    log.error("CIFS breakLease fail", e);
                    block.set(false);
                }
                res.onNext(block.get());
            }
        });

        return res;
    }

    private static Mono<Boolean> tryBreakStart(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.CIFS_BREAK_LEASE_START, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS breakLeaseStart {} fail", msgs.get(0));
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

    private static Mono<Boolean> tryBreakEnd(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.CIFS_BREAK_LEASE_END, String.class, nodeList);

        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS breakLeaseEnd {} fail", msgs.get(0));
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

    public static Mono<Integer> lease(String bucket, String key, LeaseLock value) {
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
                    int type = Lock.LEASE_TYPE;

                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("bucket", bucket)
                            .put("key", key)
                            .put("type", String.valueOf(type))
                            .put("value", Json.encode(value));

                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    return tryLease(msgs, nodeList)
                            .flatMap(state -> {
                                if (state >= 0) {
                                    return Mono.just(state);
                                } else {
                                    return tryUnLease(msgs, nodeList)
                                            .map(b -> -1);
                                }
                            });
                });
    }

    public static Mono<Boolean> unLease(String bucket, String key, LeaseLock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.LEASE_TYPE;

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("value", Json.encode(value)))
                            .collect(Collectors.toList());
                    return tryUnLease(msgs, nodeList);
                });
    }

    public static Mono<Boolean> breakLease(String bucket, String key, String leaseKey, int state, SMB2FileId smb2FileId, String node) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.LEASE_TYPE;

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("fileId", Json.encode(smb2FileId))
                                    .put("leaseKey", leaseKey)
                                    .put("state", String.valueOf(state))
                                    .put("node", node))
                            .collect(Collectors.toList());
                    return tryBreak(msgs, nodeList, smb2FileId);
                });
    }

    public static Mono<Boolean> breakLeaseStart(String bucket, String key, Set<String> leases, int state, String curNode, boolean block, SMB2FileId smb2FileId, String node) {
        String curIP = NodeCache.getIP(curNode);
        int type = Lock.LEASE_TYPE;
        return Mono.just(new ArrayList<Tuple3<String, String, String>>())
                .flatMap(nodeList -> {
                    nodeList.add(new Tuple3<>(curIP, "", ""));
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("leases", Json.encode(leases))
                                    .put("state", String.valueOf(state))
                                    .put("block", String.valueOf(block))
                                    .put("fileId", Json.encode(smb2FileId))
                                    .put("node", node))
                            .collect(Collectors.toList());
                    return tryBreakStart(msgs, nodeList);
                });
    }

    public static Mono<Boolean> breakLeaseEnd(String bucket, String key, String leaseKey, SMB2FileId smb2FileId, String node) {
        String curIP = NodeCache.getIP(node);
        int type = Lock.LEASE_TYPE;
        return Mono.just(new ArrayList<Tuple3<String, String, String>>())
                .flatMap(nodeList -> {
                    nodeList.add(new Tuple3<>(curIP, "", ""));
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("leaseKey", leaseKey)
                                    .put("fileId", Json.encode(smb2FileId)))
                            .collect(Collectors.toList());
                    return tryBreakEnd(msgs, nodeList);
                });
    }
}
