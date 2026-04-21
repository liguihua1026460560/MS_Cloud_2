package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.filesystem.nfs.types.StateOwner;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.macrosan.filesystem.lock.LockKeeper.KEEP_NAN;
import static com.macrosan.filesystem.nfs.lock.NFS4Lock.*;

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

    public static Map<String, Map<String, DealLock>> lockMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<NFS4Lock, Long> unLockMap = new ConcurrentHashMap<>();
    public static Map<NFS4Lock, AtomicInteger> unLocker = new ConcurrentHashMap<>();
    private static final String localNode = ServerConfig.getInstance().getHostUuid();

    public Mono<NFS4Lock> tryKeep(String bucket, String key, NFS4Lock value) {
        AtomicInteger o = getLock(value);
        synchronized (o) {
            MonoProcessor<NFS4Lock> res = MonoProcessor.create();
            lockMap.compute(bucket, (bucket0, map) -> {
                if (map == null) {
                    map = new ConcurrentHashMap<>();
                }
                map.compute(key, (key0, dealLock) -> {
                    if (dealLock == null) {
                        dealLock = new DealLock();
                    }
                    if (unLockMap.containsKey(value)) {
                        res.onNext(DEFAULT_LOCK);
                        return dealLock;
                    }
                    NFS4Lock lock = dealLock.lock(value);
                    o.decrementAndGet();
                    res.onNext(lock);
                    return dealLock;
                });
                return map;
            });
            return res;
        }

    }

    @Override
    public Mono<Boolean> tryLock(String bucket, String key, NFS4Lock value) {
        return tryLock0(bucket, key, value)
                .map(DEFAULT_LOCK::equals);
    }

    public Mono<NFS4Lock> tryLock0(String bucket, String key, NFS4Lock value) {
        AtomicInteger o = getLock(value);
        synchronized (o) {
            MonoProcessor<NFS4Lock> res = MonoProcessor.create();
            lockMap.compute(bucket, (bucket0, map) -> {
                if (map == null) {
                    map = new ConcurrentHashMap<>();
                }
                map.compute(key, (key0, dealLock) -> {
                    if (dealLock == null) {
                        dealLock = new DealLock();
                    }
                    NFS4Lock lock = dealLock.lock(value);
                    int i = o.decrementAndGet();
                    if (DEFAULT_LOCK.equals(lock)) {
                        unLockMap.remove(lock);
                        addKeep(bucket, key, value);
                    }else if (i == 0){
                        unLocker.remove(value, o);
                    }
                    res.onNext(lock);
                    return dealLock;
                });
                return map;
            });
            return res;
        }
    }

    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, NFS4Lock value) {
        AtomicInteger o = getLock(value);
        synchronized (o) {
            lockMap.computeIfPresent(bucket, (bucket0, map) -> {
                map.computeIfPresent(key, (key0, dealLock) -> {
                    switch (value.optType) {
                        case UNLOCK_TYPE:
                            dealLock.unLock(value);
                            unLockMap.put(value, System.nanoTime());
                            break;
                        case REMOVE_WAIT_TYPE:
                            dealLock.removeWait(value);
                            break;
                        case RECALL_TYPE:
                            recall(value);
                            break;
                    }
                    int i = o.decrementAndGet();
                    if (value.optType != UNLOCK_TYPE && i == 0){
                        unLocker.remove(value, o);
                    }
                    return dealLock.isEmpty() ? null : dealLock;
                });
                return map.isEmpty() ? null : map;
            });
            return Mono.just(true);
        }
    }

    public Mono<NFS4Lock> tryUnLock0(String bucket, String key, NFS4Lock value) {
        AtomicInteger o = getLock(value);
        synchronized (o) {
            MonoProcessor<NFS4Lock> res = MonoProcessor.create();
            NFS4LockServer.lockMap.compute(bucket, (bucket0, map) -> {
                if (map == null) {
                    res.onNext(DEFAULT_LOCK);
                    return null;
                }
                map.compute(key, (key0, dealLock) -> {
                    if (dealLock == null) {
                        res.onNext(DEFAULT_LOCK);
                        return null;
                    }
                    switch (value.optType) {
                        case UNLOCK_TYPE:
                            List<NFS4Lock> nfs4Locks = dealLock.unLock(value);
                            if (nfs4Locks.contains(DEFAULT_LOCK)) {
                                nfs4Locks.remove(DEFAULT_LOCK);
                                if (value.removeOwner()) {
                                    for (NFS4Lock nfs4Lock : nfs4Locks) {
                                        removeKeep(bucket, key, nfs4Lock);
                                        unLockMap.put(nfs4Lock, System.nanoTime());
                                    }
                                }
                                removeKeep(bucket, key, value);
                                unLockMap.put(value, System.nanoTime());
                                res.onNext(DEFAULT_LOCK);
                            } else {
                                unLockMap.put(value, System.nanoTime());
                                res.onNext(nfs4Locks.get(nfs4Locks.size() - 1));
                            }
                            break;
                        case REMOVE_WAIT_TYPE:
                            dealLock.removeWait(value);
                            res.onNext(DEFAULT_LOCK);
                            break;
                        case RECALL_TYPE:
                            res.onNext(recall(value) ? DEFAULT_LOCK : ERROR_LOCK);
                            break;
                    }
                    res.onNext(ERROR_LOCK);
                    int i = o.decrementAndGet();
                    if (value.optType != UNLOCK_TYPE && i == 0){
                        unLocker.remove(value, o);
                    }
                    return dealLock.isEmpty() ? null : dealLock;
                });
                return map.isEmpty() ? null : map;
            });
            return res;
        }
    }

    static MsExecutor executorWait = new MsExecutor(1, 1, new MsThreadFactory("NFS4Lock-wait"));
    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("NFS4Lock-timeout"));

    static {
        executorWait.submit(NFS4LockServer::checkWait);
        executor.submit(NFS4LockServer::checkUnlock);
    }


    private static void checkUnlock() {
        long start = System.nanoTime();
        try {
            Iterator<Map.Entry<NFS4Lock, Long>> iterator = unLockMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<NFS4Lock, Long> entry = iterator.next();
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
        executor.schedule(NFS4LockServer::checkUnlock, exec, TimeUnit.NANOSECONDS);
    }

    private static void checkWait() {
        long cur = System.nanoTime();
        Set<String> bucketSet = new HashSet<>(lockMap.keySet());
        Flux.fromIterable(bucketSet)
                .flatMap(bucket -> {
                    Map<String, DealLock> keyMap = lockMap.get(bucket);
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
                                                    DealLock dealLock = keyMap.get(key);
                                                    if (dealLock == null) {
                                                        return Mono.just(false);
                                                    } else {
                                                        List<NFS4Lock> waitList = new ArrayList<>(dealLock.waitLocks);
                                                        return Flux.fromIterable(waitList)
                                                                .filter(lock0 -> ServerConfig.getInstance().getHostUuid().equals(lock0.node))
                                                                .concatMap(value -> {
                                                                    if (!value.needKeep()) {
                                                                        return NFS4LockClient.unLockOrRemoveWait(bucket, key, value.setOptType(REMOVE_WAIT_TYPE));
                                                                    }
                                                                    NFS4Lock lock1 = dealLock.lock(value);
                                                                    if (DEFAULT_LOCK.equals(lock1)) {
                                                                        return NFS4LockClient.lock(bucket, key, value)
                                                                                .flatMap(lock -> {
                                                                                    if (NFS4Lock.DEFAULT_LOCK.equals(lock)) {
                                                                                        return NFS4LockClient.unLockOrRemoveWait(bucket, key, value.setOptType(REMOVE_WAIT_TYPE))
                                                                                                .flatMap(cancel -> NFS4LockClient.unLockOrRemoveWait(bucket, key, value.setOptType(RECALL_TYPE)));
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
                    executorWait.schedule(NFS4LockServer::checkWait, exec, TimeUnit.NANOSECONDS);
                }, e -> {
                    long exec = cur + 10_000_000L - System.nanoTime();
                    if (exec < 0) {
                        exec = 0;
                    }
                    executorWait.schedule(NFS4LockServer::checkWait, exec, TimeUnit.NANOSECONDS);
                    log.error("NFS4LOCK checkWait error ", e);
                });
    }

    @Override
    public Mono<Boolean> keep(String bucket, String key, NFS4Lock value) {
        return tryKeep(bucket, key, value).map(DEFAULT_LOCK::equals);
    }

    @ToString
    private static class DealLock {
        public Set<NFS4Lock> writeLocks = new HashSet<>();
        public Set<NFS4Lock> readLocks = new HashSet<>();
        public Set<NFS4Lock> waitLocks = new HashSet<>();
        public Map<StateOwner, Set<NFS4Lock>> ownerLockMap = new HashMap<>();


        public NFS4Lock lock(NFS4Lock addLock) {
            synchronized (this) {
                Optional<NFS4Lock> conflictLock;
                boolean getLock = addLock.lockType == NFS4Lock.GET_LOCK_TYPE;
                if (getLock) {
                    conflictLock = Stream.concat(writeLocks.stream(), readLocks.stream())
                            .filter(l -> l.conflict(addLock))
                            .findFirst();
                    return conflictLock.orElse(NFS4Lock.GET_LOCK);
                }
                Set<NFS4Lock> ownerLocks = ownerLockMap.computeIfAbsent(addLock.stateOwner, k -> new HashSet<>());
                if (addLock.readType()) {
                    if (readLocks.contains(addLock)) {
                        return NFS4Lock.DEFAULT_LOCK;
                    }
                    conflictLock = writeLocks.stream().filter(l -> l.conflict(addLock)).findAny();
                    if (!conflictLock.isPresent()) {
                        readLocks.add(addLock);
                        ownerLocks.add(addLock);
                        return NFS4Lock.DEFAULT_LOCK;
                    } else if (addLock.blockType()) {
                        addWait(addLock);
                        ownerLocks.add(addLock);
                    }
                    return conflictLock.get();
                } else if (addLock.writeType()) {
                    if (writeLocks.contains(addLock)) {
                        return DEFAULT_LOCK;
                    }
                    conflictLock = Stream.concat(writeLocks.stream(), readLocks.stream())
                            .filter(l -> l.conflict(addLock))
                            .findFirst();
                    if (!conflictLock.isPresent()) {
                        writeLocks.add(addLock);
                        ownerLocks.add(addLock);
                        return NFS4Lock.DEFAULT_LOCK;
                    } else if (addLock.blockType() && addLock.clientLock) {
                        addLock.clientLock = false;
                        addWait(addLock);
                        ownerLocks.add(addLock);
                    }
                    return conflictLock.get();
                }
                return ERROR_LOCK;
            }
        }

        public List<NFS4Lock> unLock(NFS4Lock delLock) {
            synchronized (this) {
                if (delLock.clientUnLock) {
                    removeWait(delLock);
                } else if (delLock.clientLock && delLock.blockType()) {
                    delLock.clientLock = false;
                    addWait(delLock);
                }
                boolean exist;
                boolean remove = readLocks.remove(delLock);
                boolean remove1 = writeLocks.remove(delLock);
                exist = remove || remove1;
                Set<NFS4Lock> ownerLocks = ownerLockMap.getOrDefault(delLock.stateOwner, new HashSet<>());
                Set<NFS4Lock> ownerLocks0 = new HashSet<>();
                if (delLock.removeOwner()) {
                    if (!ownerLocks.isEmpty()) {
                        for (NFS4Lock ownerLock : ownerLocks) {
                            remove = readLocks.remove(ownerLock);
                            remove1 = writeLocks.remove(ownerLock);
                            removeWait(ownerLock);
                            exist = exist || remove || remove1;
                            ownerLocks0.add(ownerLock);
                        }
                        ownerLocks.clear();
                    }
                    ownerLockMap.remove(delLock.stateOwner);
                } else {
                    ownerLocks.remove(delLock);
                    removeWait(delLock);
                    ownerLocks0.add(delLock);
                }
                if (exist) {
                    ownerLocks0.add(DEFAULT_LOCK);
                    return new ArrayList<>(ownerLocks0);
                }
                return Collections.singletonList(NFS4Lock.NOMATCH_LOCK);
            }
        }

        public boolean addWait(NFS4Lock waitLock) {
            synchronized (this) {
                if (!waitLocks.contains(waitLock)) {
                    waitLocks.add(waitLock);
                    return true;
                } else {
                    return false;
                }
            }
        }

        public void removeWait(NFS4Lock waitLock) {
            synchronized (this) {
                waitLocks.remove(waitLock);
            }
        }

        public boolean isEmpty() {
            synchronized (this) {
                return writeLocks.isEmpty() && readLocks.isEmpty() && waitLocks.isEmpty();
            }
        }


    }

    public boolean recall(NFS4Lock lock) {
        return lock.notifyLock();
    }

    public AtomicInteger getLock(NFS4Lock value){
        return unLocker.compute(value, (k, v) ->{
            if (v == null){
                return new AtomicInteger(1);
            }
            v.incrementAndGet();
            return v;
        });
    }
}