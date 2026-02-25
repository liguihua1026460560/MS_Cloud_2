package com.macrosan.filesystem.cifs.lease;

import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.reply.smb2.LeaseBreakNotificationReply;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_ASYNC;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_REDIRECT;

@Log4j2
public class LeaseCache {
    public static Map<Long, Map<Integer, Set<SMB2FileId>>> sessionTreeMap = new ConcurrentHashMap<>(); // 关闭 tree connect 和 session 时解除Lease
    public static Map<SMB2FileId, Tuple2<String, String>> guidToLeaseMap = new ConcurrentHashMap<>(); // (k, leaseKey) close
    public static Map<SMB2FileId, SMBHandler> guidToSMBHandler = new ConcurrentHashMap<>();
    public static Map<String, Map<String, LeaseLock>> leaseKeyMap = new ConcurrentHashMap<>(); // 当前节点 lease

    public static Map<String, LeaseLock> sendBreakMap = new ConcurrentHashMap<>(); // 通知列表
    public static Map<String, Tuple2<SMB2FileId, String>> sendBlockMap = new ConcurrentHashMap<>(); // 阻塞通知列表

    public static Map<SMB2FileId, Set<String>> blockToBreakMap = new ConcurrentHashMap<>();
    public static Map<SMB2FileId, Set<String>> blockToBreakSuccessMap = new ConcurrentHashMap<>();
    public static Map<SMB2FileId, Tuple2<SMBHandler, SMB2.SMB2Reply>> pendingMap = new ConcurrentHashMap<>();
    public static Map<SMB2FileId, Long> pendingTimeoutMap = new ConcurrentHashMap<>();

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("cifs-leaseBreak"));
    private static final long TIME_OUT_NAN = 35_000_000_000L;

    static {
        executor.submit(LeaseCache::checkLeaseBreakTimeout);
    }

    private static void checkLeaseBreakTimeout() {
        long cur = System.nanoTime() - TIME_OUT_NAN;

        Set<SMB2FileId> keySet = new HashSet<>(pendingTimeoutMap.keySet());
        for (SMB2FileId key : keySet) {
            pendingTimeoutMap.computeIfPresent(key, (k, oldV) -> {
                if (cur - oldV > 0) {
                    blockToBreakMap.remove(k);
                    blockToBreakSuccessMap.remove(k);
                    pendingMap.computeIfPresent(k, (k0, tuple2) -> {
                        SMBHandler smbHandler = tuple2.getT1();
                        SMB2.SMB2Reply reply = tuple2.getT2();
                        reply.getHeader().setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_REDIRECT | SMB2_HDR_FLAG_ASYNC);
                        smbHandler.sendPending(reply);
                        return null;
                    });
                    return null;
                } else {
                    return oldV;
                }
            });
        }

        long exec = cur + TIME_OUT_NAN + 1_000_000_000L - System.nanoTime();
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(LeaseCache::checkLeaseBreakTimeout, exec, TimeUnit.NANOSECONDS);
    }

    public static void addCache(SMB2FileId smb2FileId, LeaseLock leaseLock, SMB2Header header, SMBHandler smbHandler) {
        String k = leaseLock.bucket + "/" + leaseLock.ino;
        sessionTreeMap.computeIfAbsent(header.getSessionId(), sessionId -> new ConcurrentHashMap<>())
                .computeIfAbsent(header.getTid(), treeId -> ConcurrentHashMap.newKeySet())
                .add(smb2FileId);
        guidToLeaseMap.put(smb2FileId, Tuples.of(k, leaseLock.leaseKey));
        guidToSMBHandler.put(smb2FileId, smbHandler);

        leaseKeyMap.computeIfAbsent(k, k0 -> new ConcurrentHashMap<>())
                .compute(leaseLock.leaseKey, (leaseKey0, leaseLock0) -> {
                    if (leaseLock0 == null) {
                        leaseLock0 = leaseLock;
                    } else {
                        leaseLock0.leaseOpens.addAll(leaseLock.leaseOpens);
                        leaseLock0.leaseState = leaseLock.leaseState;
                    }
                    return leaseLock0;
                });
        // 更新保活state
        LeaseServer.getInstance().bucketKeepMap.computeIfAbsent(leaseLock.bucket, t -> new ConcurrentHashMap<>())
                .compute(String.valueOf(leaseLock.ino), (key0, set) -> {
                    if (set == null) {
                        set = ConcurrentHashMap.newKeySet();
                    }
                    set.remove(leaseLock);
                    set.add(leaseLock);
                    return set;
                });
    }

    public static void changeCache(LeaseLock leaseLock) {
        String k = leaseLock.bucket + "/" + leaseLock.ino;
        leaseKeyMap.computeIfAbsent(k, k0 -> new ConcurrentHashMap<>())
                .compute(leaseLock.leaseKey, (leaseKey0, leaseLock0) -> {
                    if (leaseLock0 == null) {
                        leaseLock0 = leaseLock;
                    } else {
                        leaseLock0.leaseOpens.addAll(leaseLock.leaseOpens);
                        leaseLock0.leaseState = leaseLock.leaseState;
                    }
                    return leaseLock0;
                });
        // 更新保活state
        LeaseServer.getInstance().bucketKeepMap.computeIfAbsent(leaseLock.bucket, t -> new ConcurrentHashMap<>())
                .compute(String.valueOf(leaseLock.ino), (key0, set) -> {
                    if (set == null) {
                        set = ConcurrentHashMap.newKeySet();
                    }
                    set.remove(leaseLock);
                    set.add(leaseLock);
                    return set;
                });
    }

    public static void breakCache(String bucket, String key, Set<String> leases, int state, boolean block, SMB2FileId smb2FileId, String node) {
        String k = bucket + "/" + key;

        Map<String, Tuple3<Integer, Integer, Short>> sendBreakMap0 = new HashMap<>(); // key, (old, new, epoch)
        leaseKeyMap.compute(k, (k0, map) -> {
            if (map != null) {
                for (String leaseKey : leases) {
                    LeaseLock leaseLock = map.get(leaseKey);
                    if (leaseLock != null) {
                        int oldState = leaseLock.leaseState;
                        leaseLock.leaseState &= ~state;
                        if (oldState != leaseLock.leaseState) {
                            leaseLock.epoch++;
                            sendBreakMap.put(leaseKey, leaseLock);
                            if (block) {
                                sendBlockMap.put(leaseKey, Tuples.of(smb2FileId, node));
                            }
                            // 更新保活state
                            LeaseServer.getInstance().bucketKeepMap.computeIfAbsent(leaseLock.bucket, t -> new ConcurrentHashMap<>())
                                    .compute(String.valueOf(leaseLock.ino), (key0, set) -> {
                                        if (set == null) {
                                            set = ConcurrentHashMap.newKeySet();
                                        }
                                        set.remove(leaseLock);
                                        set.add(leaseLock);
                                        return set;
                                    });
                            sendBreakMap0.put(leaseKey, Tuples.of(oldState, leaseLock.leaseState, leaseLock.epoch));
                        }
                    } else {
                        if (block && !sendBreakMap.containsKey(leaseKey)) {
                            // 发送成功回调
                            LeaseClient.breakLeaseEnd(bucket, key, leaseKey, smb2FileId, node).subscribe();
                        }
                    }
                }
            } else {
                if (block) {
                    // 发送成功回调
                    Flux.fromIterable(leases)
                            .flatMap(leaseKey -> {
                                if (!sendBreakMap.containsKey(leaseKey)) {
                                    return LeaseClient.breakLeaseEnd(bucket, key, leaseKey, smb2FileId, node);
                                } else {
                                    return Mono.just(false);
                                }
                            })
                            .subscribe();
                }
            }
            return map;
        });
        sendBreakLease(k, sendBreakMap0);
    }

    public static Mono<Boolean> closeCache(SMB2FileId smb2FileId, SMB2Header header) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        if (header != null) {
            sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, map) -> {
                map.computeIfPresent(header.getTid(), (treeId, set) -> {
                    set.remove(smb2FileId);
                    return set.isEmpty() ? null : set;
                });
                return map.isEmpty() ? null : map;
            });
        }
        Tuple2<String, String> tuple2 = guidToLeaseMap.remove(smb2FileId);
        guidToSMBHandler.remove(smb2FileId);
        if (tuple2 != null) {
            String k = tuple2.getT1();
            String leaseKey = tuple2.getT2();
            leaseKeyMap.compute(k, (k0, map) -> {
                if (map == null) {
                    res.onNext(true);
                    return null;
                }
                map.compute(leaseKey, (leaseKey0, leaseLock0) -> {
                    if (leaseLock0 == null) {
                        res.onNext(true);
                        return null;
                    }
                    leaseLock0.leaseOpens.remove(smb2FileId);
                    if (leaseLock0.leaseOpens.isEmpty()) {
                        LeaseClient.unLease(leaseLock0.bucket, String.valueOf(leaseLock0.ino), leaseLock0)
                                .subscribe(res::onNext);
                        return null;
                    } else {
                        res.onNext(true);
                        return leaseLock0;
                    }
                });
                return map.isEmpty() ? null : map;
            });

            // 删除breakMap
            sendBreakMap.computeIfPresent(leaseKey, (leaseKey0, leaseLock0) -> {
                leaseLock0.leaseOpens.remove(smb2FileId);
                if (leaseLock0.leaseOpens.isEmpty()) {
                    Tuple2<SMB2FileId, String> breakTuple2 = sendBlockMap.remove(leaseKey);
                    if (breakTuple2 != null) {
                        LeaseClient.breakLeaseEnd(leaseLock0.bucket, String.valueOf(leaseLock0.ino), leaseLock0.leaseKey, breakTuple2.getT1(), breakTuple2.getT2()).subscribe();
                    }
                    return null;
                } else {
                    return leaseLock0;
                }
            });
        } else {
            res.onNext(true);
        }
        return res;
    }

    public static void treeDisconnect(SMB2Header header) {
        sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, map) -> {
            map.computeIfPresent(header.getTid(), (treeId, set) -> {
                Flux.fromIterable(set)
                        .flatMap(value -> closeCache(value, null))
                        .subscribe();
                return null;
            });
            return map.isEmpty() ? null : map;
        });
    }

    public static void logOff(SMB2Header header) {
        sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, map) -> {
            Flux.fromIterable(map.values())
                    .flatMap(set -> Flux.fromIterable(set)
                            .flatMap(value -> closeCache(value, null)))
                    .subscribe();
            return null;
        });
    }

    public static void sendBreakLease(String k, Map<String, Tuple3<Integer, Integer, Short>> sendBreakMap) {
        Flux.fromIterable(sendBreakMap.entrySet())
                .subscribe(entry -> {
                    SMB2Header header = new SMB2Header()
                            .setMagic(SMB2Header.MAGIC)
                            .setHeaderLen((short) 64)
                            .setOpcode((short) 18)
                            .setFlags(1)
                            .setMessageId(0xFFFFFFFFFFFFFFFFL);
                    LeaseBreakNotificationReply leaseBreakNotificationReply = new LeaseBreakNotificationReply()
                            .setEpoch(entry.getValue().getT3())
                            .setLeaseKey(DatatypeConverter.parseHexBinary(entry.getKey()))
                            .setCurrentLeaseState(entry.getValue().getT1())
                            .setNewLeaseState(entry.getValue().getT2())
                            .setLeaseFlags(1);
                    SMB2.SMB2Reply reply = new SMB2.SMB2Reply();
                    reply.setHeader(header);
                    reply.setBody(leaseBreakNotificationReply);
                    leaseKeyMap.computeIfPresent(k, (k0, map) -> {
                        map.computeIfPresent(entry.getKey(), (leaseKey, leaseLock) -> {
                            guidToSMBHandler.computeIfPresent(leaseLock.leaseOpens.stream().iterator().next(), (guid, handle) -> {
                                handle.leaseBreakNotification(reply);
                                return handle;
                            });
                            return leaseLock;
                        });
                        return map;
                    });
                });
    }

    public static String getLeaseKey(SMB2FileId smb2FileId) {
        Tuple2<String, String> tuple2 = guidToLeaseMap.getOrDefault(smb2FileId, Tuples.of("", ""));
        return tuple2.getT2();
    }
}