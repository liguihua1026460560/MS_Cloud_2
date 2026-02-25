package com.macrosan.filesystem.nfs.delegate;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.call.v4.OpenV4Call.*;
import static com.macrosan.filesystem.nfs.delegate.DelegateLock.*;

@Log4j2
public class DelegateServer extends LockServer<DelegateLock> {
    private static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("nfs4-unDelegate"));

    public static void register() {
        DelegateServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final DelegateServer instance = new DelegateServer(Lock.NFS4_DELEGATE_TYPE, DelegateLock.class);
    public static Map<String, Delegate> delegateMap = new ConcurrentHashMap<>();


    public static DelegateServer getInstance() {
        return instance;
    }

    private DelegateServer(int type, Class<DelegateLock> delegateLockClass) {
        super(type, delegateLockClass);
//        executor.submit(this::tryClearTimeOutLock);
    }


    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, DelegateLock value) {
        return null;
    }

    public Mono<Boolean> tryDelegate(String bucket, String key, DelegateLock value) {
        String k = String.valueOf(value.nodeId);
        Delegate delegate = delegateMap.computeIfAbsent(k, k1 -> new Delegate());
        Tuple2<Boolean, Set<DelegateLock>> res = delegate.delegate(value);
        if (res.var1 && !res.var2.isEmpty()) {
            sendRecall(k, res.var2);
        }
        return Mono.just(res.var1);
    }

    public Mono<DelegateLock> getDelegate(String bucket, String key, DelegateLock value) {
        String k = String.valueOf(value.nodeId);
        Delegate delegate = delegateMap.computeIfAbsent(k, k1 -> new Delegate());
        DelegateLock res = delegate.getDelegate(value);
        return Mono.just(res);
    }


    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, DelegateLock value) {
        String k = String.valueOf(value.nodeId);
        Delegate delegate = delegateMap.computeIfAbsent(k, k1 -> new Delegate());
        return Mono.just(delegate.releaseDelegate(value));
    }

    @Override
    protected Mono<Boolean> keep(String bucket, String key, DelegateLock value) {
        return tryDelegate(bucket, key, value);
    }

    @Data
    public static class Delegate {
        Set<DelegateLock> writeDelegate = new HashSet<>();
        Set<DelegateLock> readDelegate = new HashSet<>();
        Set<DelegateLock> needRecallDelegate = new HashSet<>();
        Set<DelegateLock> waitRecallDelegate = new HashSet<>();

        public Tuple2<Boolean, Set<DelegateLock>> delegate(DelegateLock value) {
            synchronized (this) {
                Set<DelegateLock> delegateLocks = new HashSet<>();
                boolean write = value.shareAccess == OPEN_SHARE_ACCESS_WRITE || value.shareAccess == OPEN_SHARE_ACCESS_BOTH;
                boolean read = value.shareAccess == OPEN_SHARE_ACCESS_READ;
                boolean res = false;
                switch (value.type) {
                    case CHECK_DELEGATE_TYPE:
                        //存在不是该客户端的写委托
                        if (!writeDelegate.isEmpty() && writeDelegate.stream()
                                .filter(o -> o.getClientId() != value.getClientId())
                                .findFirst().orElse(null) != null) {
                            res = true;
                            delegateLocks.addAll(writeDelegate);
                        }
                        if (!readDelegate.isEmpty() && write) {
                            delegateLocks.addAll(readDelegate);
                        }
                        return new Tuple2<>(res && !delegateLocks.isEmpty(), delegateLocks);
                    case ADD_DELEGATE_TYPE:
                        //查询是否已经存在委托
                        if (!writeDelegate.isEmpty() || (!readDelegate.isEmpty() && write)) {
                            return new Tuple2<>(false, delegateLocks);
                        }
                        //是否已经授予该客户端delegate
                        if (read && readDelegate.stream()
                                .filter(o -> o.getClientId() == value.getClientId())
                                .findFirst().orElse(null) == null) {
                            res = true;
                            readDelegate.add(value);

                        } else if (write) {
                            res = true;
                            writeDelegate.add(value);
                        }
                        return new Tuple2<>(res, delegateLocks);
                }
                return new Tuple2<>(false, delegateLocks);
            }

        }


        public boolean releaseDelegate(DelegateLock value) {
            synchronized (this) {
                writeDelegate.removeIf(v -> v.stateId != null && value.stateId != null && v.stateId.equals(value.stateId));
                readDelegate.removeIf(v -> v.stateId != null && value.stateId != null && v.stateId.equals(value.stateId));
                if (value.node.equals(ServerConfig.getInstance().getHostUuid())) {
                    unDelegateMap.remove(value);
                }
                return true;
            }
        }

        public DelegateLock getDelegate(DelegateLock value) {
            synchronized (this) {
                Set<DelegateLock> all = new HashSet<>();
                all.addAll(writeDelegate);
                all.addAll(readDelegate);
                return all.stream()
                        .filter(s -> value.clientId == s.clientId)
                        .filter(s -> s.stateId.equals(value.stateId))
                        .findFirst()
                        .orElse(NOT_FOUND_DELEGATE);
            }
        }

    }


    public void sendRecall(String key, Set<DelegateLock> delegateList) {
        if (!delegateList.isEmpty()) {
            for (DelegateLock delegateLock : delegateList) {
                if (delegateLock.node.equals(ServerConfig.getInstance().getHostUuid())) {
                    //防止重复recall
                    unDelegateMap.compute(delegateLock, (k, v) -> {
                        if (v == null) {
                            k.recall();
                            return System.currentTimeMillis();
                        }
                        return v;
                    });
                }
            }
        }
    }
    //recall中已实现超时自动释放
//    public void tryClearTimeOutLock() {
//        try {
//            for (DelegateLock delegateLock : unDelegateLockMap.keySet()) {
//                if (delegateLock.node.equals(ServerConfig.getInstance().getHostUuid())) {
//                    unDelegateLockMap.compute(delegateLock, (k, v) -> {
//                        if (v == null) {
//                            return null;
//                        }
//                        if (System.currentTimeMillis() - v > 10 * 1000L) {
//                            DelegateClient.unLock(delegateLock.bucket, delegateLock.objName, k).subscribe(b -> {
//                                if (delegateLock.node.equals(ServerConfig.getInstance().getHostUuid())) {
//                                    long clientId = delegateLock.getClientId();
//                                    NFS4Client client0 = clientControl.getClient0(clientId);
//                                    if (client0 != null) {
//                                        client0.removeState(delegateLock.stateId);
//                                    }
//                                }
//                            });
//                        }
//                        return v;
//                    });
//                }
//            }
//        } finally {
//            executor.schedule(this::tryClearTimeOutLock, 5, TimeUnit.SECONDS);
//        }
//    }
}
