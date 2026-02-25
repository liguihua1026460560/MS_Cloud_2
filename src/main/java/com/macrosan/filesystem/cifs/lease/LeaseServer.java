package com.macrosan.filesystem.cifs.lease;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.lock.CIFSLockServer;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_ASYNC;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_REDIRECT;

@Log4j2
public class LeaseServer extends LockServer<LeaseLock> {

    public static void register() {
        LeaseServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final LeaseServer instance = new LeaseServer(Lock.LEASE_TYPE, LeaseLock.class);

    public static LeaseServer getInstance() {
        return instance;
    }

    protected LeaseServer(int type, Class<LeaseLock> leaseLockClass) {
        super(type, leaseLockClass);
    }

    public static Map<String, Lease> leaseMap = new ConcurrentHashMap<>();
    public static final String localNode = ServerConfig.getInstance().getHostUuid();

    public Mono<Integer> tryLease(String bucket, String key, LeaseLock value) {
        String k = bucket + "/" + key;

        if (CIFSLockServer.hasLock(bucket, key)) {
            value.leaseState &= ~0b001;
        }
        Lease lease = leaseMap.computeIfAbsent(k, k0 -> new Lease());
        int state = lease.lease(value.leaseKey, value.leaseState, value.node);

        return Mono.just(state);
    }

    public Mono<Boolean> tryUnLease(String bucket, String key, LeaseLock value) {
        String k = bucket + "/" + key;

        leaseMap.computeIfPresent(k, (k0, lease) -> {
            if (lease.unLease(value.leaseKey)) {
                lease = null;
            }
            return lease;
        });

        return Mono.just(true);
    }

    public Mono<Set<String>> tryBreak(String bucket, String key, SMB2FileId smb2FileId, String leaseKey, int state, String node) {
        String k = bucket + "/" + key;
        MonoProcessor<Set<String>> res = MonoProcessor.create();
        leaseMap.compute(k, (k0, lease) -> {
            if (lease == null) {
                res.onNext(new HashSet<>());
                return null;
            }
            Tuple3<Set<String>, Map<String, Set<String>>, Map<String, Set<String>>> tupleLeases = lease.breakLease(leaseKey, state);
            Set<String> blockLeases = tupleLeases.getT1();
            Map<String, Set<String>> breakMap = tupleLeases.getT2();
            Map<String, Set<String>> blockMap = tupleLeases.getT3();
            if (!breakMap.isEmpty()) {
                Flux.fromIterable(breakMap.entrySet())
                        .flatMap(entry -> LeaseClient.breakLeaseStart(bucket, key, entry.getValue(), state, entry.getKey(), false, smb2FileId, node))
                        .subscribe();
            }
            if (!blockMap.isEmpty()) {
                Flux.fromIterable(blockMap.entrySet())
                        .flatMap(entry -> LeaseClient.breakLeaseStart(bucket, key, entry.getValue(), state, entry.getKey(), true, smb2FileId, node))
                        .subscribe();
            }
            res.onNext(blockLeases);
            return lease;
        });

        return res;
    }

    public static Mono<Payload> breakLease(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        SMB2FileId smb2FileId = Json.decodeValue(msg.get("fileId"), SMB2FileId.class);
        String leaseKey = msg.get("leaseKey");
        int state = Integer.parseInt(msg.get("state"));
        String node = msg.get("node");

        return getInstance().tryBreak(bucket, key, smb2FileId, leaseKey, state, node)
                .map(blockLeases -> DefaultPayload.create(Json.encode(blockLeases), ErasureServer.PayloadMetaType.SUCCESS.name()));
    }

    public static Mono<Payload> breakLeaseStart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        Set<String> leases = Json.decodeValue(msg.get("leases"), new TypeReference<Set<String>>() {
        });
        int state = Integer.parseInt(msg.get("state"));
        boolean block = Boolean.parseBoolean(msg.get("block"));
        SMB2FileId smb2FileId = Json.decodeValue(msg.get("fileId"), SMB2FileId.class);
        String node = msg.get("node");
        return Mono.just(true)
                .doOnNext(ignore -> {
                    LeaseCache.breakCache(bucket, key, leases, state, block, smb2FileId, node);
                })
                .map(s -> SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> breakLeaseEnd(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        String leaseKey = msg.get("leaseKey");
        SMB2FileId blockFileId = Json.decodeValue(msg.get("fileId"), SMB2FileId.class);

        return Mono.just(leaseKey)
                .doOnNext(leaseKey0 -> {
                    LeaseCache.blockToBreakMap.compute(blockFileId, (blockFileId0, set) -> {
                        if (set == null) {
                            LeaseCache.blockToBreakSuccessMap.compute(blockFileId, (blockFileId00, set0) -> {
                                if (set0 == null) {
                                    set0 = ConcurrentHashMap.newKeySet();
                                }
                                set0.add(leaseKey);
                                return set0;
                            });
                            LeaseCache.pendingTimeoutMap.put(blockFileId, System.nanoTime());
                            return null;
                        }
                        set.remove(leaseKey);
                        if (set.isEmpty()) {
                            LeaseCache.pendingMap.computeIfPresent(blockFileId, (blockFileId00, tuple2) -> {
                                SMBHandler smbHandler = tuple2.getT1();
                                SMB2.SMB2Reply reply = tuple2.getT2();
                                reply.getHeader().setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_REDIRECT | SMB2_HDR_FLAG_ASYNC);
                                smbHandler.sendPending(reply);
                                return null;
                            });

                            LeaseCache.pendingTimeoutMap.remove(blockFileId);
                            return null;
                        } else {
                            return set;
                        }
                    });
                })
                .map(s -> SUCCESS_PAYLOAD);
    }

    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, LeaseLock value) {
        return null;
    }

    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, LeaseLock value) {
        return tryUnLease(bucket, key, value)
                .doOnNext(b -> {
                });
    }

    @Override
    public Mono<Boolean> keep(String bucket, String key, LeaseLock value) {
        return tryLease(bucket, key, value)
                .map(state -> value.leaseState == state);
    }

    @ToString
    private static class Lease {
        Set<String> readCacheLease = new HashSet<>();
        Set<String> handleCacheLease = new HashSet<>();
        Set<String> writeCacheLease = new HashSet<>();
        Map<String, Long> leaseTimeMap = new HashMap<>();
        Map<String, String> leaseNodeMap = new HashMap<>();

        int lease(String leaseKey, int leaseState, String node) {
            synchronized (this) {
                int state = 0;

                if ((leaseState & 0b100) != 0 && (leaseTimeMap.size() == 0 || (leaseTimeMap.size() == 1 && leaseTimeMap.containsKey(leaseKey)))) {
                    writeCacheLease.add(leaseKey);
                    state |= 0b100;
                } else {
                    writeCacheLease.remove(leaseKey);
                }

                if ((leaseState & 0b010) != 0) {
                    handleCacheLease.add(leaseKey);
                    state |= 0b010;
                } else {
                    handleCacheLease.remove(leaseKey);
                }

                if ((leaseState & 0b001) != 0) {
                    readCacheLease.add(leaseKey);
                    state |= 0b001;
                } else {
                    readCacheLease.remove(leaseKey);
                }

                leaseNodeMap.put(leaseKey, node);
                leaseTimeMap.put(leaseKey, System.nanoTime());

                return state;
            }
        }

        boolean unLease(String leaseKey) {
            synchronized (this) {
                writeCacheLease.remove(leaseKey);
                handleCacheLease.remove(leaseKey);
                readCacheLease.remove(leaseKey);
                leaseNodeMap.remove(leaseKey);
                leaseTimeMap.remove(leaseKey);
                return leaseTimeMap.isEmpty();
            }
        }

        Tuple3<Set<String>, Map<String, Set<String>>, Map<String, Set<String>>> breakLease(String leaseKey, int leaseState) {
            synchronized (this) {
                Set<String> breakLeases = new HashSet<>();
                Set<String> blockLeases = new HashSet<>();
                if ((leaseState & 0b100) != 0) {
                    Set<String> oldWrite = new HashSet<>(writeCacheLease);
                    if (writeCacheLease.contains(leaseKey)) {
                        writeCacheLease.clear();
                        writeCacheLease.add(leaseKey);
                    } else {
                        writeCacheLease.clear();
                    }
                    oldWrite.removeAll(writeCacheLease);
                    breakLeases.addAll(oldWrite);
                    blockLeases.addAll(oldWrite);
                }

                if ((leaseState & 0b010) != 0) {
                    Set<String> oldHandle = new HashSet<>(handleCacheLease);
                    if (handleCacheLease.contains(leaseKey)) {
                        handleCacheLease.clear();
                        handleCacheLease.add(leaseKey);
                    } else {
                        handleCacheLease.clear();
                    }
                    oldHandle.removeAll(handleCacheLease);
                    breakLeases.addAll(oldHandle);
                    if (leaseState == 0b010) {
                        blockLeases.addAll(oldHandle);
                    }
                }

                if ((leaseState & 0b001) != 0) {
                    Set<String> oldRead = new HashSet<>(readCacheLease);
                    if (readCacheLease.contains(leaseKey)) {
                        readCacheLease.clear();
                        readCacheLease.add(leaseKey);
                    } else {
                        readCacheLease.clear();
                    }
                    oldRead.removeAll(readCacheLease);
                    breakLeases.addAll(oldRead);
                }
                breakLeases.removeAll(blockLeases);
                Map<String, Set<String>> nodeBreakGrouped = breakLeases.stream()
                        .collect(Collectors.groupingBy(
                                leaseNodeMap::get,
                                Collectors.toSet()
                        ));
                Map<String, Set<String>> nodeBlockGrouped = blockLeases.stream()
                        .collect(Collectors.groupingBy(
                                leaseNodeMap::get,
                                Collectors.toSet()
                        ));
                return Tuples.of(blockLeases, nodeBreakGrouped, nodeBlockGrouped);
            }
        }
    }

    public static String info() {
        String str = "";
        try {
            str = "\nleaseMap->" + leaseMap +
                    "\nbucketKeepMap->" + getInstance().bucketKeepMap +
                    "\nsessionTreeMap->" + LeaseCache.sessionTreeMap +
                    "\nguidToLeaseMap->" + LeaseCache.guidToLeaseMap +
                    "\nguidToSMBHandler->" + LeaseCache.guidToSMBHandler +
                    "\nleaseKeyMap->" + LeaseCache.leaseKeyMap +
                    "\nsendBlockMap->" + LeaseCache.sendBlockMap +
                    "\nsendBreakMap->" + LeaseCache.sendBreakMap +
                    "\nblockToBreakMap->" + LeaseCache.blockToBreakMap +
                    "\nblockToBreakSuccessMap->" + LeaseCache.blockToBreakSuccessMap +
                    "\npendingMap->" + LeaseCache.pendingMap +
                    "\npendingTimeoutMap->" + LeaseCache.pendingTimeoutMap;
        } catch (Exception e) {
            log.error("CIFS Lease info: ", e);
        }
        return str;
    }

}