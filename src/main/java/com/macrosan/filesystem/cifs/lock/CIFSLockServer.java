package com.macrosan.filesystem.cifs.lock;

import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.rsocket.Payload;
import io.vertx.core.json.Json;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;

@Log4j2
public class CIFSLockServer extends LockServer<CIFSLock> {

    public static void register() {
        CIFSLockServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final CIFSLockServer instance = new CIFSLockServer(Lock.CIFS_LOCK_TYPE, CIFSLock.class);

    public static CIFSLockServer getInstance() {
        return instance;
    }

    protected CIFSLockServer(int type, Class<CIFSLock> leaseLockClass) {
        super(type, leaseLockClass);
    }

    public static Map<String, Map<String, LockRange>> cifsLockMap = new ConcurrentHashMap<>();
    public static Map<Long, Map<Integer, Map<Tuple2<SMB2FileId, String>, Set<CIFSLock>>>> sessionTreeMap = new ConcurrentHashMap<>(); // 关闭 tree connect 和 session 时解锁，防止死锁
    public static Map<CIFSLock, Tuple2<SMBHandler, SMB2.SMB2Reply>> pendingMap = new ConcurrentHashMap<>();
    public static Set<CIFSLock> waitSuccessSet = ConcurrentHashMap.newKeySet();
    private static final Node nodeInstance = Node.getInstance();
    private static final String localNode = ServerConfig.getInstance().getHostUuid();

    static MsExecutor executorWait = new MsExecutor(1, 1, new MsThreadFactory("CIFSLock-wait"));

    static {
        executorWait.submit(CIFSLockServer::checkWait);
    }

    private static void checkWait() {
        long cur = System.nanoTime();
        Set<String> bucketSet = new HashSet<>(cifsLockMap.keySet());
        Flux.fromIterable(bucketSet)
                .flatMap(bucket -> {
                    Map<String, LockRange> keyMap = cifsLockMap.get(bucket);
                    if (keyMap == null) {
                        return Mono.just(false);
                    } else {
                        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
                        String vnode = pool.getBucketVnodeId(bucket);
                        return pool.mapToNodeInfo(vnode)
                                .flatMap(nodeList -> {
                                    String curIP = NodeCache.getIP(localNode);
                                    boolean hadCurIP = false;
                                    for (Tuple3<String, String, String> t : nodeList) {
                                        if (t.var1.equals(curIP)) {
                                            hadCurIP = true;
                                            break;
                                        }
                                    }
                                    return Mono.just(hadCurIP);
                                })
                                .onErrorResume(error -> Mono.just(false))
                                .flatMap(hadCurIP -> {
                                    if (hadCurIP) {
                                        Set<String> keySet = new HashSet<>(keyMap.keySet());
                                        return Flux.fromIterable(keySet)
                                                .flatMap(key -> {
                                                    LockRange lockRange = keyMap.get(key);
                                                    if (lockRange == null) {
                                                        return Mono.just(false);
                                                    } else {
                                                        if (nodeInstance.getInodeV(Long.parseLong(key)).isMaster()) {
                                                            List<CIFSLock> waitList = new ArrayList<>(lockRange.waitList);
                                                            return Flux.fromIterable(waitList)
                                                                    .concatMap(value -> {
                                                                        if (lockRange.lock(value)) {
                                                                            return CIFSLockClient.lock(bucket, key, value)
                                                                                    .flatMap(b -> {
                                                                                        if (b) {
                                                                                            return CIFSLockClient.cancel(bucket, key, value)
                                                                                                    .flatMap(cancel -> CIFSLockClient.granted(bucket, key, value));
                                                                                        } else {
                                                                                            return Mono.just(false);
                                                                                        }
                                                                                    });
                                                                        } else {
                                                                            return Mono.just(false);
                                                                        }
                                                                    })
                                                                    .collectList()
                                                                    .map(list -> true);
                                                        } else {
                                                            return Mono.just(false);
                                                        }
                                                    }
                                                })
                                                .collectList()
                                                .map(list -> true);
                                    } else {
                                        return Mono.just(false);
                                    }
                                });
                    }
                })
                .collectList()
                .subscribe(list -> {
                    long exec = cur + 10_000_000L - System.nanoTime();
                    if (exec < 0) {
                        exec = 0;
                    }
                    executorWait.schedule(CIFSLockServer::checkWait, exec, TimeUnit.NANOSECONDS);
                });
    }

    public static Mono<Payload> cancel(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        CIFSLock value = Json.decodeValue(msg.get("value"), CIFSLock.class);

        cifsLockMap.computeIfPresent(bucket, (bucket0, map) -> {
            map.computeIfPresent(key, (key0, lockRange) -> {
                lockRange.cancel(value);
                return lockRange.isEmpty() ? null : lockRange;
            });
            return map.isEmpty() ? null : map;
        });


        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> granted(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        CIFSLock value = Json.decodeValue(msg.get("value"), CIFSLock.class);

        AtomicBoolean flag = new AtomicBoolean(false);
        CIFSLockServer.pendingMap.compute(value, (cifsLock0, tuple2) -> {
            if (tuple2 == null) {

                CIFSLockServer.sessionTreeMap.computeIfPresent(value.sessionId, (sessionId, treeMap) -> {
                    treeMap.computeIfPresent(value.treeId, (treeId, fileIdMap) -> {
                        fileIdMap.computeIfPresent(Tuples.of(value.smb2FileId, value.node), (tuples, set) -> {
                            if (set.contains(value)) {
                                waitSuccessSet.add(value);
                                flag.set(true);
                            } else {
                                flag.set(false);
                            }
                            return set;
                        });
                        return fileIdMap;
                    });
                    return treeMap;
                });

            } else {
                SMBHandler smbHandler = tuple2.getT1();
                SMB2.SMB2Reply reply = tuple2.getT2();
                smbHandler.sendPending(reply);
                flag.set(true);
            }
            return null;
        });

        return Mono.just(flag.get() ? SUCCESS_PAYLOAD : ERROR_PAYLOAD);
    }

    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, CIFSLock value) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        cifsLockMap.compute(bucket, (bucket0, map) -> {
            if (map == null) {
                map = new ConcurrentHashMap<>();
            }
            map.compute(key, (key0, lockRange) -> {
                if (lockRange == null) {
                    lockRange = new LockRange();
                }
                res.onNext(lockRange.lock(value));
                return lockRange;
            });
            return map;
        });

        return res;
    }

    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, CIFSLock value) {
        cifsLockMap.computeIfPresent(bucket, (bucket0, map) -> {
            map.computeIfPresent(key, (key0, lockRange) -> {
                lockRange.unlock(value);
                return lockRange.isEmpty() ? null : lockRange;
            });
            return map.isEmpty() ? null : map;
        });
        return Mono.just(true);
    }

    @Override
    protected Mono<Boolean> keep(String bucket, String key, CIFSLock value) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        cifsLockMap.compute(bucket, (bucket0, map) -> {
            if (map == null) {
                map = new ConcurrentHashMap<>();
            }
            map.compute(key, (key0, lockRange) -> {
                if (lockRange == null) {
                    lockRange = new LockRange();
                }
                res.onNext(lockRange.lock(value));
                return lockRange;
            });
            return map;
        });

        return res;
    }

    @ToString
    private static class LockRange {
        Set<CIFSLock> shareSet = new HashSet<>();
        Set<CIFSLock> exclusiveSet = new HashSet<>();
        List<CIFSLock> waitList = new LinkedList<>();

        boolean lock(CIFSLock value) {
            synchronized (this) {
                boolean flag = true;
                if (value.lockType == 1) {
                    if (shareSet.contains(value)) {
                        return true;
                    }
                    for (CIFSLock cifsLock : exclusiveSet) {
                        if (check(value.offset, value.len, cifsLock.offset, cifsLock.len) && !value.smb2FileId.equals(cifsLock.smb2FileId) && !value.node.equals(cifsLock.node)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        shareSet.add(value);
                    }
                } else {
                    if (exclusiveSet.contains(value)) {
                        return true;
                    }
                    for (CIFSLock cifsLock : exclusiveSet) {
                        if (check(value.offset, value.len, cifsLock.offset, cifsLock.len)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        for (CIFSLock cifsLock : shareSet) {
                            if (check(value.offset, value.len, cifsLock.offset, cifsLock.len)) {
                                flag = false;
                                break;
                            }
                        }
                        if (flag) {
                            exclusiveSet.add(value);
                        }
                    }
                }

                if (value.isClientLockReq) {
                    value.isClientLockReq = false;
                    if (!flag && !value.failImmediately) {
                        wait(value);
                    }
                }
                return flag;
            }
        }

        boolean unlock(CIFSLock value) {
            synchronized (this) {
                if (value.isClientUnLockReq) {
                    cancel(value);
                } else if (value.isClientLockReq && !value.failImmediately) { // 处理当前节点加锁成功，其他节点失败时，收到锁回退，需要将其添加上
                    value.isClientLockReq = false;
                    wait(value);
                }
                value.lockType = 2;
                if (!exclusiveSet.remove(value)) {
                    value.lockType = 1;
                    return shareSet.remove(value);
                } else {
                    return true;
                }
            }
        }

        boolean wait(CIFSLock value) {
            synchronized (this) {
                if (!waitList.contains(value)) {
                    waitList.add(value);
                    return true;
                } else {
                    return false;
                }
            }
        }

        void cancel(CIFSLock value) {
            synchronized (this) {
                value.lockType = 2;
                if (!waitList.remove(value)) {
                    value.lockType = 1;
                    waitList.remove(value);
                }
            }
        }

        boolean isEmpty() {
            synchronized (this) {
                return shareSet.isEmpty() && exclusiveSet.isEmpty() && waitList.isEmpty();
            }
        }
    }

    public static boolean hasLock(String bucket, String key) {
        Map<String, LockRange> map = cifsLockMap.get(bucket);
        if (map != null) {
            LockRange lockRange = map.get(key);
            return lockRange != null;
        }
        return false;
    }

    public static boolean check(long offset1, long len1, long offset2, long len2) {
        boolean isPoint1 = (len1 == 0);
        boolean isPoint2 = (len2 == 0);

        if (isPoint1 && isPoint2) {
            return false;
        } else if (isPoint1) {
            return (offset1 > offset2) && (offset1 < offset2 + len2);
        } else if (isPoint2) {
            return (offset2 > offset1) && (offset2 < offset1 + len1);
        } else {
            return (offset1 < offset2 + len2) && (offset2 < offset1 + len1);
        }
    }

    public static String info() {
        String str = "";
        try {
            str = "\ncifsLockMap->" + cifsLockMap +
                    "\nbucketKeepMap->" + getInstance().bucketKeepMap +
                    "\nsessionTreeMap->" + sessionTreeMap +
                    "\npendingMap->" + pendingMap +
                    "\nwaitSuccessSet->" + waitSuccessSet;
        } catch (Exception e) {
            log.error("CIFS Lock info: ", e);
        }
        return str;
    }

}
