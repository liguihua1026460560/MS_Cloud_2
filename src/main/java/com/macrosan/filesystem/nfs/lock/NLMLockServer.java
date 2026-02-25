package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.nfs.api.NLM4Proc;
import com.macrosan.filesystem.nfs.types.Owner;
import com.macrosan.filesystem.nfs.types.Sm;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.rsocket.Payload;
import io.vertx.core.json.Json;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.filesystem.nfs.lock.NLMLockClient.LOCK_DEBUG;

@Log4j2
public class NLMLockServer {
    //60s
    private static final long TIME_OUT_NAN = 60_000_000_000L;
    static ConcurrentHashMap<String, Lock> lockMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Long> unlockMap = new ConcurrentHashMap<>(); // 防止keep请求在unlock之后接收 记录两端ip和svid
    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("nlmlock-timeout"));

    private static void checkUnlockTimeout() {
        long cur = System.nanoTime() - TIME_OUT_NAN;
        try {
            for (Map.Entry<String, Long> entry : unlockMap.entrySet()) {
                if (cur - entry.getValue() > 0) {
                    unlockMap.remove(entry.getKey());
                }
            }
        } catch (Exception e) {
            log.error("NLM checkUnlockTimeout", e);
        }
        long exec = cur + TIME_OUT_NAN + 1000_000_000L - System.nanoTime();
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(NLMLockServer::checkUnlockTimeout, exec, TimeUnit.NANOSECONDS);
    }

    private static void checkTimeout() {
        long cur = System.nanoTime() - TIME_OUT_NAN;

        Set<String> keySet = new HashSet<>(lockMap.keySet());
        for (String key : keySet) {
            String bucket = key.split("/")[0];
            lockMap.computeIfPresent(key, (k, oldV) -> {
                if (oldV.checkTimeout(cur, bucket)) {
                    return null;
                } else {
                    return oldV;
                }
            });
        }

        long exec = cur + TIME_OUT_NAN + 1000_000_000L - System.nanoTime();
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(NLMLockServer::checkTimeout, exec, TimeUnit.NANOSECONDS);
    }

    static {
        executor.submit(NLMLockServer::checkTimeout);
        executor.submit(NLMLockServer::checkUnlockTimeout);
    }

    @ToString
    private static class Lock {
        Map<Owner, Long> map = new HashMap<>(); // 时间
        Set<Owner> readLock = new HashSet<>(); // 读锁
        Set<Owner> writeLock = new HashSet<>(); // 写锁
        Map<String, Set<Owner>> mergeMap = new HashMap<>();

        boolean checkTimeout(long time, String bucket) {
            synchronized (this) {
                List<Owner> list = map.entrySet().stream().filter(e -> time - e.getValue() > 0)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                if (list.isEmpty()) {
                    return false;
                }

                try {
                    for (Owner value : list) {
                        Sm sm = new Sm(bucket, value);
                        NLMLockKeeper.remove(sm.bucket, String.valueOf(sm.ino), value);
                        NSMServer.unMonLock(sm).block();
                        log.info("NLM timeout: {}", sm);
                        NSMServer.lockTimeout(sm);
                        unlock(value);
                    }
                } catch (Exception e) {
                    log.error("NLM checkTimeout", e);
                }

                return map.isEmpty();
            }
        }

        boolean lock(Owner value) {
            synchronized (this) {
                boolean flag = true;
                if (!value.exclusive) {
                    if (readLock.contains(value)) {
                        map.put(value, System.nanoTime());
                        return true;
                    }
                    for (Owner owner : writeLock) {
                        if (check(value.offset, value.len, owner.offset, owner.len)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        readLock.add(value);
                    }
                } else {
                    if (writeLock.contains(value)) {
                        map.put(value, System.nanoTime());
                        return true;
                    }
                    for (Owner owner : writeLock) {
                        if (check(value.offset, value.len, owner.offset, owner.len)) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        for (Owner owner : readLock) {
                            if (check(value.offset, value.len, owner.offset, owner.len)) {
                                flag = false;
                                break;
                            }
                        }
                        if (flag) {
                            writeLock.add(value);
                        }
                    }
                }
                mergeMap.computeIfAbsent(value.mergeKey(), k -> ConcurrentHashMap.newKeySet())
                        .add(value);
                map.put(value, System.nanoTime());
                return flag;
            }
        }

        boolean unlock(Owner value) {
            synchronized (this) {
                if (value.offset == 0 && value.len == 0) {
                    Set<Owner> set = mergeMap.remove(value.mergeKey());
                    if (set != null) {
                        for (Owner owner : set) {
                            map.remove(owner);
                            readLock.remove(owner);
                            writeLock.remove(owner);
                        }
                    }
                } else {
                    mergeMap.computeIfPresent(value.mergeKey(), (k, set) -> {
                        set.remove(value);
                        return set.isEmpty() ? null : set;
                    });
                    map.remove(value);
                    readLock.remove(value);
                    writeLock.remove(value);
                }
                return map.isEmpty();
            }
        }
    }

    public static boolean waitTryLock(String bucket, String key, Owner value) {
        AtomicBoolean b = new AtomicBoolean(false);
        lockMap.compute(bucket + "/" + key, (k, lock) -> {
            if (lock == null) {
                lock = new Lock();
            }
            b.set(lock.lock(value));
            return lock;
        });
        return b.get();
    }

    public static Mono<Boolean> tryLock(String bucket, String key, Owner value) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        lockMap.compute(bucket + "/" + key, (k, lock) -> {
            if (lock == null) {
                lock = new Lock();
            }
            if (lock.lock(value)) {
                if (LOCK_DEBUG) {
                    log.info("NLM lock server lock {} {}: {} success", bucket, key, value);
                }
                res.onNext(true);
            } else {
                if (LOCK_DEBUG) {
                    log.info("NLM lock server lock {} {}: {} fail", bucket, key, value);
                }
                res.onNext(false);
            }
            return lock;
        });
        return res;
    }

    public static Mono<Boolean> tryUnLock(String bucket, String key, Owner value) {
        lockMap.computeIfPresent(bucket + "/" + key, (k, lock) -> {
            if (LOCK_DEBUG) {
                log.info("NLM lock server unlock {} {}: {} success", bucket, key, value);
            }
            return lock.unlock(value) ? null : lock;
        });
        return Mono.just(true);
    }

    public static Mono<Boolean> keep(String bucket, String key, Owner value) {
        return tryLock(bucket, key, value);
    }

    public static Mono<Payload> lock(Payload payload) {
        // nsm启动时未读取完rocksdb信息
        if (!NSMServer.readSmBak.get()) {
            return Mono.just(ERROR_PAYLOAD);
        }

        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        boolean lockType = "1".equals(msg.get("lock")) || "WRITE".equals(msg.get("lock"));

        String ip = msg.get("ip");
        String clientName = msg.get("clientName");
        int svid = Integer.parseInt(msg.get("svid"));
        String local = msg.get("local");
        long ino = Long.parseLong(key);

        long offset;
        long len;
        if (msg.get("offset") != null) {
            offset = Long.parseLong(msg.get("offset"));
        } else {
            offset = 0;
        }
        if (msg.get("len") != null) {
            len = Long.parseLong(msg.get("len"));
        } else {
            len = 0;
        }

        Owner owner = new Owner(ip, clientName, svid, ino, local, offset, len);
        owner.exclusive = lockType;
        return tryLock(bucket, key, owner)
                .flatMap(b -> {
                    if (LOCK_DEBUG) {
                        log.info("NLM server lock {} res {}", msg, b);
                    }

                    if (b) {
                        Sm sm = new Sm(bucket, owner);
                        NLMLockKeeper.keep(bucket, key, owner); // 加锁添加保活
                        return NSMServer.monLock(sm)
                                .map(bb -> SUCCESS_PAYLOAD);
                    } else {
                        return Mono.just(ERROR_PAYLOAD);
                    }
                });
    }

    public static Mono<Payload> unlock(Payload payload) {
        if (!NSMServer.readSmBak.get()) {
            return Mono.just(ERROR_PAYLOAD);
        }

        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");

        if (LOCK_DEBUG) {
            log.info("NLM server unlock {}", msg);
        }

        String ip = msg.get("ip");
        String clientName = msg.get("clientName");
        int svid = Integer.parseInt(msg.get("svid"));
        String local = msg.get("local");
        long ino = Long.parseLong(key);

        boolean unlock = Boolean.parseBoolean(msg.get("unlock"));

        long offset;
        long len;
        if (msg.get("offset") != null) {
            offset = Long.parseLong(msg.get("offset"));
        } else {
            offset = 0;
        }
        if (msg.get("len") != null) {
            len = Long.parseLong(msg.get("len"));
        } else {
            len = 0;
        }

        Owner owner = new Owner(ip, clientName, svid, ino, local, offset, len);
        NLMLockKeeper.remove(bucket, key, owner); // 取消保活
        if (unlock) {
            NLMLockWait.cancel(bucket, key, owner, 1);
            unlockMap.put(owner.ownerKey(), System.nanoTime()); // 防止keep请求在unlock之后到，导致锁无法移除
        }

        return tryUnLock(bucket, key, owner).flatMap(b -> {
            Sm sm = new Sm(bucket, owner);
            return NSMServer.unMonLock(sm)
                    .map(bb -> {
                        NLMLockKeeper.remove(bucket, key, owner); // 取消保活
                        if (!unlock && Boolean.parseBoolean(msg.get("block"))) {
                            owner.node = msg.get("node");
                            NLMLockWait.addWait(bucket, key, owner);
                        }
                        return SUCCESS_PAYLOAD;
                    });

        });
    }

    public static Mono<Payload> keep(Payload payload) {
        if (!NSMServer.readSmBak.get()) {
            return Mono.just(ERROR_PAYLOAD);
        }

        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");

        if (LOCK_DEBUG) {
            log.info("NLM server keep {}", msg);
        }

        String[] keys = Json.decodeValue(msg.get("key"), String[].class);
        int[] lockType = Json.decodeValue(msg.get("lock"), int[].class);

        String[] ips = Json.decodeValue(msg.get("ip"), String[].class);
        String[] clientNames = Json.decodeValue(msg.get("clientName"), String[].class);
        int[] svids = Json.decodeValue(msg.get("svid"), int[].class);
        String[] locals = Json.decodeValue(msg.get("local"), String[].class);

        long[] offsets;
        long[] lens;
        if (msg.get("offset") != null) {
            offsets = Json.decodeValue(msg.get("offset"), long[].class);
        } else {
            offsets = new long[keys.length];
        }
        if (msg.get("len") != null) {
            lens = Json.decodeValue(msg.get("len"), long[].class);
        } else {
            lens = new long[keys.length];
            for (int i = 0; i < keys.length; i++) {
                lens[i] = 0;
            }
        }

        return Flux.range(0, keys.length)
                .flatMap(index -> {
                    Owner owner = new Owner(ips[index], clientNames[index], svids[index], Long.parseLong(keys[index]), locals[index], offsets[index], lens[index]);
                    owner.exclusive = lockType[index] == 1;
                    if (unlockMap.containsKey(owner.mergeKey()) || unlockMap.containsKey(owner.ownerKey())) {
                        return Mono.empty();
                    }
                    return keep(bucket, keys[index], owner)
                            .flatMap(b -> {
                                if (unlockMap.containsKey(owner.mergeKey()) || unlockMap.containsKey(owner.ownerKey())) {
                                    return Mono.just(b);
                                }
                                NLMLockKeeper.keep(bucket, keys[index], owner); // 恢复锁添加保活
                                Sm sm = new Sm(bucket, owner);
                                return NSMServer.monLock(sm)
                                        .flatMap(bb -> {
                                            if (unlockMap.containsKey(owner.mergeKey()) || unlockMap.containsKey(owner.ownerKey())) {
                                                NLMLockKeeper.remove(bucket, keys[index], owner);
                                                return NSMServer.unMonLock(sm);
                                            } else {
                                                return Mono.just(bb);
                                            }
                                        });
                            });
                })
                .collectList()
                .map(b -> SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> cancel(Payload payload) {
        if (!NSMServer.readSmBak.get()) {
            return Mono.just(ERROR_PAYLOAD);
        }

        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");

        if (LOCK_DEBUG) {
            log.info("NLM server cancel {}", msg);
        }

        String ip = msg.get("ip");
        String clientName = msg.get("clientName");
        int svid = Integer.parseInt(msg.get("svid"));
        String local = msg.get("local");
        long ino = Long.parseLong(key);

        long offset;
        long len;
        if (msg.get("offset") != null) {
            offset = Long.parseLong(msg.get("offset"));
        } else {
            offset = 0;
        }
        if (msg.get("len") != null) {
            len = Long.parseLong(msg.get("len"));
        } else {
            len = 0;
        }

        Owner owner = new Owner(ip, clientName, svid, ino, local, offset, len);

        NLMLockWait.cancel(bucket, key, owner, 2);
        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> granted(Payload payload) {
        if (!NSMServer.readSmBak.get()) {
            return Mono.just(ERROR_PAYLOAD);
        }

        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");

        if (LOCK_DEBUG) {
            log.info("NLM server granted {}", msg);
        }

        String key = msg.get("key");
        if (msg.get("owner") != null) {
            Owner owner = Json.decodeValue(msg.get("owner"), Owner.class);
            NLMLockWait.grantedMap.computeIfPresent(bucket, (bucket0, keyMap) -> {
                keyMap.computeIfPresent(key, (key0, ownerMap) -> {
                    ownerMap.computeIfPresent(owner, (owner0, tuple2) -> {
                        NLM4Proc.sendGranted(owner, tuple2, 1);
                        return null;
                    });
                    return ownerMap.isEmpty() ? null : ownerMap;
                });
                return keyMap.isEmpty() ? null : keyMap;
            });
        }

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static String lockInfo() {
        String str = "";
        try {
            str = "\nlockMap->" + lockMap
                    + "\nkeepMap->" + NLMLockKeeper.keepMap
                    + "\nwaitMap->" + NLMLockWait.waitMap
                    + "\ngrantedMap->" + NLMLockWait.grantedMap
                    + "\nunlockMap->" + unlockMap
                    + "\nsmMap->" + NSMServer.smMap
                    + "\nsmMergeMap->" + NSMServer.smMergeMap
                    + "\nsmBakMap->" + NSMServer.smBakMap;
        } catch (Exception e) {
            log.error("NLM lock info: ", e);
        }
        return str;
    }

    /**
     * @return 有冲突返回true
     */
    public static boolean check(long offset1, long len1, long offset2, long len2) {
        boolean isEnd1 = (len1 == 0);
        boolean isEnd2 = (len2 == 0);

        if (isEnd1 && isEnd2) {
            return true;
        } else if (isEnd1) {
            return offset1 < offset2 + len2;
        } else if (isEnd2) {
            return offset2 < offset1 + len1;
        } else {
            return (offset1 < offset2 + len2) && (offset2 < offset1 + len1);
        }
    }
}
