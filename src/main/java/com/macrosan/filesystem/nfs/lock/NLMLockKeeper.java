package com.macrosan.filesystem.nfs.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.nfs.api.NSMProc;
import com.macrosan.filesystem.nfs.types.Owner;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//加锁成功后保活
@Log4j2
public class NLMLockKeeper {
    static Map<String, Map<String, Map<String, Set<Owner>>>> keepMap = new ConcurrentHashMap<>();// (bucket, key, mergeKey, owner)

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("nlmlock-keep"));

    private static Mono<Boolean> send(String bucket, List<String> keyList, List<String> valueList, List<Integer> lock,
                                      List<String> ipList, List<String> clientNameList, List<Integer> svidList, List<String> localList, List<Long> offsetList, List<Long> lenList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("key", Json.encode(keyList.toArray()))
                .put("value", Json.encode(valueList.toArray())) // 新版不使用
                .put("lock", Json.encode(lock.toArray()))
                .put("ip", Json.encode(ipList.toArray()))
                .put("clientName", Json.encode(clientNameList.toArray()))
                .put("svid", Json.encode(svidList.toArray()))
                .put("local", Json.encode(localList.toArray()))
                .put("offset", Json.encode(offsetList.toArray()))
                .put("len", Json.encode(lenList.toArray()));


        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.NLM_KEEP_LOCK, String.class, nodeList);
                    responseInfo.responses.doFinally(s -> {
                        res.onNext(true);
                    }).subscribe();

                    //保活不处理响应结果
                    return res;
                });
    }

    private static void lock() {
        long start = System.nanoTime();
        try {
            for (String bucket : keepMap.keySet()) {
                int max = 10000;
                List<String> keyList = new LinkedList<>();
                List<String> valueList = new LinkedList<>();
                List<Integer> lockList = new LinkedList<>();
                List<String> ipList = new LinkedList<>();
                List<String> clientNameList = new LinkedList<>();
                List<Integer> svidList = new LinkedList<>();
                List<String> localList = new LinkedList<>();
                List<Long> offsetList = new LinkedList<>();
                List<Long> lenList = new LinkedList<>();

                Map<String, Map<String, Set<Owner>>> map = keepMap.get(bucket);
                if (map != null) {
                    for (String key : map.keySet()) {
                        Map<String, Set<Owner>> ownerMap = map.get(key);

                        if (ownerMap != null) {
                            for (String mergeKey : ownerMap.keySet()) {
                                Set<Owner> ownerSet = ownerMap.get(mergeKey);
                                if (ownerSet != null) {
                                    for (Owner owner : ownerSet) {
                                        if (NSMProc.isLocalIp(owner.local)) { // 判断是否为收到请求ip
                                            keyList.add(key);

                                            String value = owner.ip + "_" + owner.local + "_" + owner.svid;
                                            valueList.add(value); // 新版不使用

                                            lockList.add(owner.exclusive ? 1 : 0);
                                            ipList.add(owner.ip);
                                            clientNameList.add(owner.clientName);
                                            svidList.add(owner.getSvid());
                                            localList.add(owner.getLocal());
                                            offsetList.add(owner.getOffset());
                                            lenList.add(owner.getLen());
                                            if (keyList.size() >= max) {
                                                send(bucket, keyList, valueList, lockList, ipList, clientNameList, svidList, localList, offsetList, lenList).block();
                                                keyList.clear();
                                                valueList.clear();
                                                lockList.clear();
                                                ipList.clear();
                                                clientNameList.clear();
                                                svidList.clear();
                                                localList.clear();
                                                offsetList.clear();
                                                lenList.clear();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (keyList.size() > 0) {
                    send(bucket, keyList, valueList, lockList, ipList, clientNameList, svidList, localList, offsetList, lenList).block();
                    keyList.clear();
                    valueList.clear();
                    lockList.clear();
                    ipList.clear();
                    clientNameList.clear();
                    svidList.clear();
                    localList.clear();
                    offsetList.clear();
                    lenList.clear();
                }
            }
        } catch (Exception e) {
            log.error("NLM LockKeep", e);
        }


        long exec = 1000_000_000L - System.nanoTime() + start;
        if (exec < 0) {
            exec = 0;
        }

        executor.schedule(NLMLockKeeper::lock, exec, TimeUnit.NANOSECONDS);
    }

    static {
        executor.submit(NLMLockKeeper::lock);
    }

    public static void keep(String bucket, String key, Owner value) {
        keepMap.computeIfAbsent(bucket, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(value.mergeKey(), k -> ConcurrentHashMap.newKeySet())
                .add(value);
    }

    public static void remove(String bucket, String key, Owner value) {
        keepMap.computeIfPresent(bucket, (bucket0, map) -> {
                    map.computeIfPresent(key, (key0, ownerMap) -> {
                        if (value.offset == 0 && value.len == 0) {
                            ownerMap.remove(value.mergeKey());
                        } else {
                            ownerMap.computeIfPresent(value.mergeKey(), (mergeKey0, ownerSet) -> {
                                ownerSet.remove(value);
                                return ownerSet.isEmpty() ? null : ownerSet;
                            });
                        }
                        return ownerMap.isEmpty() ? null : ownerMap;
                    });
                    return map.isEmpty() ? null : map;
                });

    }
}
