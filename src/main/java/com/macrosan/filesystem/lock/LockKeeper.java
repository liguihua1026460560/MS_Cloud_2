package com.macrosan.filesystem.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
public class LockKeeper<T extends Lock> {
    protected static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-lock-keep"));

    public static final long KEEP_NAN = 1000_000_000L; //1s
    private static final long TIME_OUT_NUM = 15; //15 * KEEP_NAN

    private final LockServer<T> server;

    LockKeeper(LockServer<T> server) {
        this.server = server;
        executor.submit(this::keepLock);
    }

    private Mono<Boolean> send(String bucket, List<String> keys, List<String> values, int type) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("key", Json.encode(keys))
                .put("value", Json.encode(values))
                .put("type", String.valueOf(type));

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);

        return pool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(t -> msg.copy())
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.KEEP_LOCK, String.class, nodeList);
                    responseInfo.responses.doFinally(s -> {
                        res.onNext(true);
                    }).subscribe();

                    //保活不处理响应结果
                    return res;
                });
    }

    private void keepLock() {
        long start = System.nanoTime();

        List<Tuple3<String, String, T>> needUnlock = new LinkedList<>();

        try {
        for (String bucket : server.bucketKeepMap.keySet()) {
            Map<String, Set<T>> keepQueue = server.bucketKeepMap.get(bucket);
            if (keepQueue == null) {
                continue;
            }
            int max = 16384;
            List<String> keyList = new ArrayList<>(16384);
            List<String> valueList = new ArrayList<>(16384);

            HashSet<Map.Entry<String, Set<T>>> keys = new HashSet<>(keepQueue.entrySet());
            for (Map.Entry<String, Set<T>> key : keys) {
                Set<T> set = new HashSet<>(key.getValue());
                for (T keepLock : set) {
                    if (keepLock.needKeep()) {
                        keyList.add(key.getKey());
                        String value = Json.encode(keepLock);
                        valueList.add(value);

                        if (keyList.size() >= max) {
                            send(bucket, keyList, valueList, server.type).block();
                            keyList.clear();
                            valueList.clear();
                        }
                    } else {
                        if (keepLock.keepNum++ >= TIME_OUT_NUM) {
                            needUnlock.add(new Tuple3<>(bucket, key.getKey(), keepLock));
                        }
                    }
                }
            }

            if (keyList.size() > 0) {
                send(bucket, keyList, valueList, server.type).block();
                keyList.clear();
                valueList.clear();
            }
        }

        for (Tuple3<String, String, T> tuple : needUnlock) {
            server.removeKeep(tuple.var1, tuple.var2, tuple.var3);
            server.tryUnLock(tuple.var1, tuple.var2, tuple.var3).block();
        }
        } catch (Exception e) {
                log.error("LockKeeper error", e);
        }
        long exec = KEEP_NAN - (System.nanoTime() - start);
        if (exec < 0) {
            exec = 0;
        }

        executor.schedule(this::keepLock, exec, TimeUnit.NANOSECONDS);
    }

}
