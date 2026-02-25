package com.macrosan.filesystem.nfs.shareAccess;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.filesystem.nfs.types.StateIdOps;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock.*;

@Log4j2
public class ShareAccessServer extends LockServer<ShareAccessLock> {
    public static void register() {
        ShareAccessServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final ShareAccessServer instance = new ShareAccessServer(Lock.NFS4_SHARE_ACCESS_TYPE, ShareAccessLock.class);

    public static ShareAccessServer getInstance() {
        return instance;
    }

    protected ShareAccessServer(int type, Class<ShareAccessLock> shareAccessLockClass) {
        super(type, shareAccessLockClass);
    }

    public static Map<String, Set<ShareAccessLock>> shareAccessMap = new ConcurrentHashMap<>();

    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, ShareAccessLock value) {
        return Mono.just(true);
    }


    public Mono<ShareAccessLock> tryShare(String bucket, String key, ShareAccessLock value) {
        MonoProcessor<ShareAccessLock> res = MonoProcessor.create();
        shareAccessMap.compute(String.valueOf(value.nodeId), (k0, set) -> {
            if (set == null) {
                set = ConcurrentHashMap.newKeySet();
            }
            switch (value.type) {
                case ADD_SHARE_TYPE:
                    //addOpen or upgradeOpen
                    if (set.stream().noneMatch(o -> (value.shareAccess & o.shareDeny) != 0 || (value.shareDeny & o.shareAccess) != 0)) {
                        ShareAccessLock existShare = null;
                        for (ShareAccessLock lock : set) {
                            if (lock.equals(value)) {
                                //本地缓存中没有这个缓存去除shareAccess缓存，添加新缓存
                                lock.shareAccess |= value.shareAccess;
                                lock.shareDeny |= value.shareDeny;
                                lock.stateId = value.stateId;
                                lock.versionNum = value.versionNum;
                                lock.type = EXIST_SHARE_TYPE;
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
                    if (existShare0.stateId.seqId <= value.stateId.seqId) {
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
                    if (set.stream()
                            .anyMatch(o -> StateIdOps.openConflict(value.shareAccess, o.shareAccess, value.shareDeny, o.shareDeny) && !o.stateId.equals(value.stateId))) {
                        res.onNext(CONFLICT_SHARE);
                        return set;
                    }
                    res.onNext(NOT_FOUND_SHARE);
                    return set;
                case OPEN_DENY_CONFLICT_TYPE:
                    if (set.stream()
                            .anyMatch(o -> (o.shareDeny & value.shareDeny) != 0)) {
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

        return res;
    }


    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, ShareAccessLock value) {
        String k = String.valueOf(value.nodeId);
        shareAccessMap.computeIfPresent(k, (k0, set) -> {
            set.removeIf(v -> v.stateId != null && value.stateId != null && v.stateId.equals(value.stateId));
            return set.isEmpty() ? null : set;
        });
        return Mono.just(true);
    }

    @Override
    protected Mono<Boolean> keep(String bucket, String key, ShareAccessLock value) {
        return tryLock(bucket, key, value);
    }
}
