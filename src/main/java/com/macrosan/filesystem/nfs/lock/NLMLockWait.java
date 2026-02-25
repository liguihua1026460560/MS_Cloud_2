package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.types.NLM4Lock;
import com.macrosan.filesystem.nfs.types.Owner;
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
import reactor.util.function.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
public class NLMLockWait {
    private static final long TIME_OUT_NAN = 40_000_000_000L;
    public static Map<String, Map<String, WaitLock>> waitMap = new ConcurrentHashMap<>(); // (bucket, key, waitLock)
    public static Map<String, Map<String, Map<Owner, Tuple2<Boolean, NLM4Lock>>>> grantedMap = new ConcurrentHashMap<>(); // (bucket, key, owner, <isUdp, NLM4Lock>)
    public static final Node nodeInstance = Node.getInstance();
    public static final String localNode = ServerConfig.getInstance().getHostUuid();
    static MsExecutor executorTime = new MsExecutor(1, 1, new MsThreadFactory("NLMLock-timeout"));
    static MsExecutor executorWait = new MsExecutor(1, 1, new MsThreadFactory("NLMLock-wait"));

    static {
        executorTime.submit(NLMLockWait::checkTimeout);
        executorWait.submit(NLMLockWait::checkWait);
    }

    private static void checkTimeout() {
        long cur = System.nanoTime() - TIME_OUT_NAN;
        try {
            Set<String> bucketSet = new HashSet<>(waitMap.keySet());
            for (String bucket : bucketSet) {
                Map<String, WaitLock> keyMap = waitMap.get(bucket);
                if (keyMap == null) {
                    continue;
                }
                Set<String> keySet = new HashSet<>(keyMap.keySet());
                for (String key : keySet) {
                    WaitLock waitLock = keyMap.get(key);
                    if (waitLock == null) {
                        continue;
                    }
                    waitLock.checkTimeout(cur, bucket, key);
                }
            }
        } catch (Exception e) {
            log.error("NLMLockWait checkTimeout error", e);
        }
        long exec = cur + TIME_OUT_NAN + 2_000_000_000L - System.nanoTime();
        if (exec < 0) {
            exec = 0;
        }
        executorTime.schedule(NLMLockWait::checkTimeout, exec, TimeUnit.NANOSECONDS);

    }

    private static void checkWait() {
        long cur = System.nanoTime();
        Set<String> bucketSet = new HashSet<>(waitMap.keySet());
        Flux.fromIterable(bucketSet)
                .flatMap(bucket -> {
                    Map<String, WaitLock> keyMap = waitMap.get(bucket);
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
                                        Set<String> keySet = keyMap.keySet();
                                        return Flux.fromIterable(keySet)
                                                .flatMap(key -> {
                                                    WaitLock waitLock = keyMap.get(key);
                                                    if (waitLock == null) {
                                                        return Mono.just(false);
                                                    } else {
                                                        if (nodeInstance.getInodeV(Long.parseLong(key)).isMaster()) {
                                                            LinkedList<Owner> waitList = waitLock.waitList;
                                                            return Flux.fromIterable(waitList)
                                                                    .concatMap(value -> {
                                                                        if (Boolean.TRUE.equals(NLMLockServer.waitTryLock(bucket, key, value))) {
                                                                            return NLMLockClient.lock(bucket, key, value, false)
                                                                                    .flatMap(b -> {
                                                                                        if (b) {
                                                                                            return NLMLockClient.cancel(bucket, key, value)
                                                                                                    .flatMap(cancel -> NLMLockClient.granted(bucket, key, value));
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
                    executorWait.schedule(NLMLockWait::checkWait, exec, TimeUnit.NANOSECONDS);
                });
    }

    public static void addWait(String bucket, String key, Owner value) {
        waitMap.computeIfAbsent(bucket, bucket0 -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, key0 -> new WaitLock())
                .addWait(value);
    }

    public static void cancel(String bucket, String key, Owner value, int type) {
        waitMap.computeIfPresent(bucket, (bucket0, keyMap) -> {
            keyMap.computeIfPresent(key, (key0, waitLock) -> waitLock.cancel(value) ? null : waitLock);
            return keyMap.isEmpty() ? null : keyMap;
        });
    }

    @ToString
    public static class WaitLock {
        Map<Owner, Long> timeMap = new HashMap<>(); // 排队时间
        LinkedList<Owner> waitList = new LinkedList<>();
        Map<String, Set<Owner>> mergeMap = new HashMap<>();

        void checkTimeout(long time, String bucket, String key) {
            synchronized (this) {
                List<Owner> list = timeMap.entrySet().stream().filter(e -> time - e.getValue() > 0)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                for (Owner owner : list) {
                    log.info("NLM waitList timeout, owner:{}, time:{}", owner, timeMap.get(owner));
                    NLMLockWait.cancel(bucket, key, owner, 3);
                }
            }
        }

        void addWait(Owner value) {
            synchronized (this) {
                if (!waitList.contains(value)) {
                    waitList.add(value);
                    mergeMap.computeIfAbsent(value.mergeKey(), k -> ConcurrentHashMap.newKeySet())
                            .add(value);
                }
                timeMap.put(value, System.nanoTime());
            }
        }

        boolean cancel(Owner value) {
            synchronized (this) {
                if (value.offset == 0 && value.len == 0) {
                    Set<Owner> set = mergeMap.remove(value.mergeKey());
                    if (set != null) {
                        for (Owner owner : set) {
                            waitList.remove(owner);
                            timeMap.remove(owner);
                        }
                    }
                } else {
                    mergeMap.computeIfPresent(value.mergeKey(), (k, set) -> {
                        set.remove(value);
                        return set.isEmpty() ? null : set;
                    });
                    waitList.remove(value);
                    timeMap.remove(value);
                }

                return timeMap.isEmpty();
            }
        }
    }
}
