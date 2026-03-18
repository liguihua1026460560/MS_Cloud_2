package com.macrosan.filesystem.nfs.shareAccess;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.filesystem.nfs.types.StateIdOps;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.filesystem.lock.LockKeeper.KEEP_NAN;
import static com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock.*;

@Log4j2
public class ShareAccessServer extends LockServer<ShareAccessLock> {
    public static void register() {
        ShareAccessServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final ShareAccessServer instance = new ShareAccessServer(Lock.NFS4_SHARE_ACCESS_TYPE, ShareAccessLock.class);
    public static ConcurrentHashMap<ShareAccessLock, Long> unLockMap = new ConcurrentHashMap<>();
    public static Map<ShareAccessLock, Object> unLocker = new ConcurrentHashMap<>();
    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("NFS4Share-timeout"));

    public static ShareAccessServer getInstance() {
        return instance;
    }


    protected ShareAccessServer(int type, Class<ShareAccessLock> shareAccessLockClass) {
        super(type, shareAccessLockClass);
    }

    public static Map<String, Set<ShareAccessLock>> shareAccessMap = new ConcurrentHashMap<>();

    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, ShareAccessLock value) {
        return tryShare(bucket, key, value).map(f -> true);
    }

    static {
        executor.submit(ShareAccessServer::checkUnlock);
    }

    private static void checkUnlock() {
        long start = System.nanoTime();
        try {
            Iterator<Map.Entry<ShareAccessLock, Long>> iterator = unLockMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ShareAccessLock, Long> entry = iterator.next();
                if (System.nanoTime() - entry.getValue() > 5 * KEEP_NAN) {
                    iterator.remove();
                    unLocker.remove(entry.getKey());
                }
            }
        } catch (Exception e) {
            log.error(e);
        }

        long exec = KEEP_NAN - (System.nanoTime() - start);
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(ShareAccessServer::checkUnlock, exec, TimeUnit.NANOSECONDS);
    }


    public Mono<ShareAccessLock> tryShare(String bucket, String key, ShareAccessLock value) {
        Object o = unLocker.computeIfAbsent(value, k -> new Object());
        synchronized (o) {
            MonoProcessor<ShareAccessLock> res = MonoProcessor.create();
            tryShare(bucket, key, value, res);
            return res.doOnNext(lock -> {
                if (lock.getShareAccess() >= 0){
                    unLockMap.remove(lock);
                    if (!ShareAccessLock.CONFLICT_SHARE.equals(lock) && !ShareAccessLock.NOT_FOUND_SHARE.equals(lock)) {
                        ShareAccessLock clone = lock.clone();
                        clone.setType(ADD_SHARE_TYPE);
                        addKeep(bucket, key, clone);
                    }
                }
            });
        }
    }

    public void tryShare(String bucket, String key, ShareAccessLock value, MonoProcessor<ShareAccessLock> res) {
        shareAccessMap.compute(key, (k0, set) -> {
            if (set == null) {
                if (value.type == ADD_SHARE_TYPE || value.type == EXIST_SHARE_TYPE){
                    set = ConcurrentHashMap.newKeySet();
                }else {
                    res.onNext(NOT_FOUND_SHARE);
                    return null;
                }
            }
            switch (value.type) {
                case ADD_SHARE_TYPE:
                    //addOpen or upgradeOpen
                    Optional<ShareAccessLock> conflictLock = set.stream().filter(l -> (value.shareAccess & l.shareDeny) != 0 || (value.shareDeny & l.shareAccess) != 0).findAny();
                    if (!conflictLock.isPresent()) {
                        ShareAccessLock existShare = null;
                        for (ShareAccessLock lock : set) {
                            if (lock.equals(value) && lock.stateId.equals(value.stateId)) {
                                if (lock.stateId.seqId <= value.stateId.seqId){
                                    lock.shareAccess |= value.shareAccess;
                                    lock.shareDeny |= value.shareDeny;
                                    lock.stateId = value.stateId;
                                    lock.versionNum = value.versionNum;
                                    lock.type = EXIST_SHARE_TYPE;
                                }
                                res.onNext(lock);
                                return set;
                            }
                        }
                        set.add(value);
                        res.onNext(value);
                    } else {
                        res.onNext(ShareAccessLock.CONFLICT_SHARE);
                    }
                    return set;
                case EXIST_SHARE_TYPE:
                    ShareAccessLock existShare0 = set.stream()
                            .filter(s -> value.clientId == s.clientId)
                            .filter(s -> s.stateId.equals(value.stateId))
                            .filter(s -> Objects.equals(s.node, value.node))
                            .findFirst()
                            .orElse(null);
                    if (existShare0 == null) {
                        res.onNext(NOT_FOUND_SHARE);
                        return set;
                    }
                    if ((existShare0.shareAccess & value.shareAccess) != value.shareAccess || (existShare0.shareDeny & value.shareDeny) != value.shareDeny) {
                        res.onNext(CONFLICT_SHARE);
                        return set;
                    }
                    if (existShare0.stateId.seqId < value.stateId.seqId) {
                        existShare0.shareAccess = value.shareAccess;
                        existShare0.shareDeny = value.shareDeny;
                        existShare0.stateId = value.stateId;
                        existShare0.versionNum = value.versionNum;
                    }
                    res.onNext(existShare0);
                    return set;
//                case GET_SHARE_TYPE:
//                    ShareAccessLock existShare = set.stream()
//                            .filter(s -> value.clientId == s.clientId)
//                            .filter(s -> s.stateId.equals(value.stateId))
//                            .findFirst()
//                            .orElse(null);
//                    if (existShare == null) {
//                        res.onNext(NOT_FOUND_SHARE);
//                        return set;
//                    }
//                    res.onNext(existShare);
//                    return set;
                case OPEN_SHARE_CONFLICT_TYPE:
                    Optional<ShareAccessLock> shareConflictLock = set.stream()
                            .filter(l -> StateIdOps.openConflict(value.shareAccess, l.shareAccess, value.shareDeny, l.shareDeny)
                                    && !l.stateId.equals(value.stateId)).findAny();
                    if (shareConflictLock.isPresent()) {
                        res.onNext(CONFLICT_SHARE);
                        return set;
                    }
                    res.onNext(NOT_FOUND_SHARE);
                    return set;
                case OPEN_DENY_CONFLICT_TYPE:
                    Optional<ShareAccessLock> denyConflictLock = set.stream()
                            .filter(l -> (l.shareDeny & value.shareDeny) != 0).findAny();
                    if (denyConflictLock.isPresent()) {
                        res.onNext(CONFLICT_SHARE);
                        return set;
                    }
                    res.onNext(NOT_FOUND_SHARE);
                    return set;
                default:
                    res.onNext(NOT_FOUND_SHARE);
                    return set;
            }

        });
    }


    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, ShareAccessLock value) {
        Object o = unLocker.computeIfAbsent(value, k -> new Object());
        synchronized (o) {
            String k = String.valueOf(value.nodeId);
            shareAccessMap.computeIfPresent(k, (k0, set) -> {
                boolean b = set.removeIf(v -> v.stateId != null && value.stateId != null && v.stateId.equals(value.stateId));
                if (b) {
                    unLockMap.put(value, System.nanoTime());
                }
                return set.isEmpty() ? null : set;
            });
            removeKeep(bucket, key, value);
            return Mono.just(true);
        }
    }

    @Override
    protected Mono<Boolean> keep(String bucket, String key, ShareAccessLock value) {
        Object o = unLocker.computeIfAbsent(value, k -> new Object());
        synchronized (o) {
            if (unLockMap.containsKey(value)) {
                return Mono.just(true);
            }
            MonoProcessor<ShareAccessLock> res = MonoProcessor.create();
            tryShare(bucket, key, value, res);
            return res.map(f -> true);
        }
    }
}
