package com.macrosan.filesystem.cifs.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockClient;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.macrosan.filesystem.lock.LockClient.addCurIpOrNot;
import static com.macrosan.filesystem.lock.LockClient.getNode;

@Log4j2
public class CIFSLockClient {
    private static Mono<Boolean> tryGranted(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.CIFS_LOCK_GRANTED, String.class, nodeList);
        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS Lock waitSuccess {} fail", msgs.get(0));
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

    private static Mono<Boolean> cancel(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.CIFS_LOCK_CANCEL, String.class, nodeList);
        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("CIFS Lock cancel {} fail", msgs.get(0));
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

    public static Mono<Boolean> lock(String bucket, String key, CIFSLock value) {
        if (value.isClientLockReq && !value.failImmediately) {
            CIFSLockServer.sessionTreeMap.computeIfAbsent(value.sessionId, sessionId -> new ConcurrentHashMap<>())
                    .computeIfAbsent(value.treeId, treeId -> new ConcurrentHashMap<>())
                    .computeIfAbsent(Tuples.of(value.smb2FileId, value.node), tuples -> ConcurrentHashMap.newKeySet())
                    .add(value);
        }
        return LockClient.lock(bucket, key, value)
                .flatMap(b -> {
                    if (b) {
                        if (value.isClientLockReq && value.failImmediately) {
                            CIFSLockServer.sessionTreeMap.computeIfAbsent(value.sessionId, sessionId -> new ConcurrentHashMap<>())
                                    .computeIfAbsent(value.treeId, treeId -> new ConcurrentHashMap<>())
                                    .computeIfAbsent(Tuples.of(value.smb2FileId, value.node), tuples -> ConcurrentHashMap.newKeySet())
                                    .add(value);
                        }
                    }
                    return Mono.just(b);
                });
    }

    public static Mono<Boolean> unlock(String bucket, String key, CIFSLock value, boolean unlock) {
        return LockClient.unlock(bucket, key, value)
                .flatMap(b -> {
                    if (unlock) {
                        CIFSLockServer.sessionTreeMap.computeIfPresent(value.sessionId, (sessionId, treeMap) -> {
                            treeMap.computeIfPresent(value.treeId, (treeId, fileIdMap) -> {
                                fileIdMap.computeIfPresent(Tuples.of(value.smb2FileId, value.node), (tuples, set) -> {
                                    set.remove(value);
                                    return set.isEmpty() ? null : set;
                                });
                                return fileIdMap.isEmpty() ? null : fileIdMap;
                            });
                            return treeMap.isEmpty() ? null : treeMap;
                        });
                    }
                    return Mono.just(true);
                });

    }

    public static Mono<Boolean> cancel(String bucket, String key, CIFSLock value) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        int type = Lock.CIFS_LOCK_TYPE;
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
                    return cancel(msgs, nodeList);
                });
    }

    public static Mono<Boolean> granted(String bucket, String key, CIFSLock value) {
        String curIP = NodeCache.getIP(value.node);
        int type = Lock.CIFS_LOCK_TYPE;
        return Mono.just(new ArrayList<Tuple3<String, String, String>>())
                .flatMap(nodeList -> {
                    nodeList.add(new Tuple3<>(curIP, "", ""));
                    List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("key", key)
                                    .put("type", String.valueOf(type))
                                    .put("value", Json.encode(value)))
                            .collect(Collectors.toList());
                    return tryGranted(msgs, nodeList)
                            .flatMap(b -> {
                                if (b) {
                                    return Mono.just(true);
                                } else {
                                    return unlock(bucket, key, value, false);
                                }
                            });
                });
    }

    public static boolean isLocked(CIFSLock value, SMB2Header header) {
        Map<Integer, Map<Tuple2<SMB2FileId, String>, Set<CIFSLock>>> treeMap = CIFSLockServer.sessionTreeMap.get(header.getSessionId());
        if (treeMap != null) {
            Map<Tuple2<SMB2FileId, String>, Set<CIFSLock>> fileIdMap = treeMap.get(header.getTid());
            if (fileIdMap != null) {
                Set<CIFSLock> locks = fileIdMap.get(Tuples.of(value.smb2FileId, value.node));
                if (locks != null) {
                    value.lockType = 2;
                    if (locks.contains(value)) {
                        return true;
                    }
                    value.lockType = 1;
                    if (locks.contains(value)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static void close(SMB2FileId smb2FileId, String node, SMB2Header header) {
        CIFSLockServer.sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, treeMap) -> {
            treeMap.computeIfPresent(header.getTid(), (treeId, fileIdMap) -> {
                fileIdMap.computeIfPresent(Tuples.of(smb2FileId, node), (tuple2, set) -> {
                    Flux.fromIterable(set)
                            .flatMap(value -> {
                                value.isClientLockReq = false;
                                value.isClientUnLockReq = true;
                                CIFSLockServer.pendingMap.remove(value);
                                CIFSLockServer.waitSuccessSet.remove(value);
                                return LockClient.unlock(value.bucket, String.valueOf(value.ino), value);
                            })
                            .subscribe();
                    return null;
                });
                return fileIdMap.isEmpty() ? null : fileIdMap;
            });
            return treeMap.isEmpty() ? null : treeMap;
        });
    }

    public static void treeDisconnect(SMB2Header header) {
        CIFSLockServer.sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, treeMap) -> {
            treeMap.computeIfPresent(header.getTid(), (treeId, fileIdMap) -> {
                Flux.fromIterable(fileIdMap.values())
                        .flatMap(set -> Flux.fromIterable(set)
                                .flatMap(value -> {
                                    value.isClientLockReq = false;
                                    value.isClientUnLockReq = true;
                                    CIFSLockServer.pendingMap.remove(value);
                                    CIFSLockServer.waitSuccessSet.remove(value);
                                    return LockClient.unlock(value.bucket, String.valueOf(value.ino), value);
                                }))
                        .subscribe();
                return null;
            });
            return treeMap.isEmpty() ? null : treeMap;
        });
    }

    public static void logOff(SMB2Header header) {
        CIFSLockServer.sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, treeMap) -> {
            Flux.fromIterable(treeMap.values())
                    .flatMap(fileIdMap -> Flux.fromIterable(fileIdMap.values())
                            .flatMap(set -> Flux.fromIterable(set)
                                    .flatMap(value -> {
                                        value.isClientLockReq = false;
                                        value.isClientUnLockReq = true;
                                        CIFSLockServer.pendingMap.remove(value);
                                        CIFSLockServer.waitSuccessSet.remove(value);
                                        return LockClient.unlock(value.bucket, String.valueOf(value.ino), value);
                                    })))
                    .subscribe();
            return null;
        });
    }
}
