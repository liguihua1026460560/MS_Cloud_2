package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.lock.NFS4Lock.DEFAULT_LOCK;

@Log4j2
public class NFS4LockServer extends LockServer<NFS4Lock> {

    public static void register() {
        NFS4LockServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final NFS4LockServer instance = new NFS4LockServer(Lock.NFS4_LOCK_TYPE, NFS4Lock.class);

    public static NFS4LockServer getInstance() {
        return instance;
    }

    protected NFS4LockServer(int type, Class<NFS4Lock> nfs4LockClass) {
        super(type, nfs4LockClass);
    }

    public static Map<String, DealLock> lockMap = new ConcurrentHashMap<>();

    public Mono<NFS4Lock> tryKeep(String bucket, String key, NFS4Lock value) {
        String k = bucket + "/" + key;

        DealLock nfs4Lock = lockMap.computeIfAbsent(k, k0 -> new DealLock());
        NFS4Lock conflictLock = nfs4Lock.lock(value);
        return Mono.just(conflictLock);
    }

    @Override
    public Mono<Boolean> tryLock(String bucket, String key, NFS4Lock value) {
        return tryKeep(bucket, key, value)
                .map(b -> true);
    }

    public Mono<NFS4Lock> tryLock0(String bucket, String key, NFS4Lock value) {
        return tryKeep(bucket, key, value);
    }

    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, NFS4Lock value) {
        String k = bucket + "/" + key;
        DealLock nfs4Lock = lockMap.computeIfAbsent(k, k0 -> new DealLock());
        return Mono.just(nfs4Lock.unLock(value)).map(b -> true);
    }

    public Mono<NFS4Lock> tryUnLock0(String bucket, String key, NFS4Lock value) {
        String k = bucket + "/" + key;
        DealLock nfs4Lock = lockMap.computeIfAbsent(k, k0 -> new DealLock());
        NFS4Lock b = nfs4Lock.unLock(value);
        return Mono.just(b);
    }

    @Override
    public Mono<Boolean> keep(String bucket, String key, NFS4Lock value) {
        return tryKeep(bucket,key,value).map(DEFAULT_LOCK::equals);
    }

    @ToString
    private static class DealLock {
        Set<NFS4Lock> lockSet = new HashSet<>();
//        private final Map<StateOwner, TreeMap<Long, Lock>> lockMap = new HashMap<>();
//        //
//        // 添加锁并检测冲突
//        public boolean addLock(Nfs4Lock newLock) {
//            TreeMap<Long, Lock> locks = lockMap.computeIfAbsent(newLock.stateOwner, k -> new TreeMap<>());
//            for (Map.Entry<StateOwner, TreeMap<Long, Lock>> entry : lockMap.entrySet()) {
//                if (entry.getKey().equals(newLock.stateOwner)) {
//                    continue;
//                }
//                TreeMap<Long, Lock> otherLocks = entry.getValue();
//                NavigableMap<Long, Lock> candidates = otherLocks.subMap(newLock.offset, false, newLock.offset + newLock.length, false);
//                if (!candidates.isEmpty()) {
//                    return false;
//                }
//            }
//            locks.put(newLock.offset, newLock);
//            return true;
//        }

        public NFS4Lock lock(NFS4Lock addLock) {
            synchronized (this) {
                Optional<NFS4Lock> conflictLock = lockSet.stream().filter(l -> l.conflict(addLock)).findAny();
                //有冲突锁
                if (conflictLock.isPresent()) {
                    return conflictLock.get();
                } else {
//                    Set<Nfs4Lock> mergeLocks = lockSet.stream()
//                            .filter(l -> l.existOverRange(addLock) && l.sameOwner(addLock) && l.getLockType() == addLock.getLockType())
//                            .collect(Collectors.toSet());
//                    //同一个owner有多段锁
//                    if (!mergeLocks.isEmpty()) {
//                        //合并成一个锁
//                        long lockBegin = addLock.getOffset();
//                        long lockEnd = addLock.getLength() == UINT64_MAX ? UINT64_MAX : (lockBegin + addLock.getLength());
//                        for (Nfs4Lock l : mergeLocks) {
//                            lockBegin = Math.min(lockBegin, l.getOffset());
//                            lockEnd = lockEnd == UINT64_MAX || l.getLength() == UINT64_MAX ? UINT64_MAX : Math.max(lockEnd, l.getOffset() + l.getLength() - 1);
//                        }
//                        lockEnd = lockEnd == UINT64_MAX ? lockEnd : lockEnd - lockBegin;
//                        Nfs4Lock mergeLock = new Nfs4Lock(addLock.getLockType(), addLock.getStateOwner(), lockBegin, lockEnd, addLock.node, addLock.clientId);
//                        lockSet.removeAll(mergeLocks);
//                        lockSet.add(mergeLock);
//                    } else {
                    if (addLock.lockType == NFS4Lock.GET_LOCK_TYPE ){
                        return  NFS4Lock.GET_LOCK;
                    }
                    lockSet.add(addLock);

//                    }
                    return NFS4Lock.DEFAULT_LOCK;
                }
            }
        }

        public NFS4Lock unLock(NFS4Lock delLock) {
            synchronized (this) {
                if (!lockSet.isEmpty()) {
                    boolean remove = lockSet.remove(delLock);
                    if (remove) {
                        //存在同段锁
                        return NFS4Lock.DEFAULT_LOCK;
                    } else {
                        return NFS4Lock.NOMATCH_LOCK;
//                        Set<Nfs4Lock> removeLocks = new HashSet<>();
//                        Set<Nfs4Lock> addLocks = new HashSet<>();
//                        lockSet.stream().filter(l -> l.sameOwner(delLock) && l.existOverRange(delLock)).forEach(lock -> {
//                            removeLocks.add(lock);
//                            long firstLen = delLock.getOffset() - lock.getOffset();
//                            if (firstLen > 0) {
//                                Nfs4Lock firstPart = new Nfs4Lock(lock.getLockType(), lock.getStateOwner(), lock.getOffset(), firstLen, lock.getNode(), lock.getClientId());
//                                addLocks.add(firstPart);
//                            }
//                            if (delLock.getLength() != UINT64_MAX) {
//                                long unlockEnd = delLock.getOffset() + delLock.getLength();
//                                long originalEnd = lock.getOffset() + lock.getLength();
//                                long secondLen = originalEnd - unlockEnd;
//                                if (secondLen > 0) {
//                                    Nfs4Lock secondPart = new Nfs4Lock(lock.getLockType(), lock.getStateOwner(), delLock.getOffset() + delLock.getLength(), secondLen, lock.getNode(), lock.getClientId());
//                                    addLocks.add(secondPart);
//                                }
//                            }
//                        });
//                        if (removeLocks.isEmpty()) {
//
//                        }
//                        lockSet.removeAll(removeLocks);
//                        lockSet.addAll(addLocks);
                    }
                }
                return NFS4Lock.NOMATCH_LOCK;
            }
        }
    }
}