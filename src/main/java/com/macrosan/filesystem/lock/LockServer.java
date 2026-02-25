package com.macrosan.filesystem.lock;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cifs.lease.LeaseLock;
import com.macrosan.filesystem.cifs.lease.LeaseServer;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.delegate.DelegateServer;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessServer;
import com.macrosan.filesystem.nfs.lock.NFS4LockServer;
import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import com.macrosan.message.socketmsg.SocketReqMsg;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.filesystem.nfs.lock.NFS4Lock.DEFAULT_LOCK;

@Log4j2
public abstract class LockServer<T extends Lock> {
    public Class<T> tClass;
    public int type;
    protected LockKeeper<T> keeper;

    public interface KeeperConstructor<T extends Lock> {
        LockKeeper<T> newInstance(LockServer<T> server);
    }

    protected LockServer(int type, Class<T> tClass) {
        this.type = type;
        this.tClass = tClass;
        this.keeper = new LockKeeper<>(this);
    }

    public Map<String, Map<String, Set<T>>> bucketKeepMap = new ConcurrentHashMap<>();

    public void addKeep(String bucket, String key, T value) {
        bucketKeepMap.computeIfAbsent(bucket, t -> new ConcurrentHashMap<>())
                .compute(key, (key0, set) -> {
                    if (set == null) {
                        set = ConcurrentHashMap.newKeySet();
                    }
                    set.remove(value);
                    set.add(value);
                    return set;
                });
    }

    public void removeKeep(String bucket, String key, T value) {
        bucketKeepMap.computeIfPresent(bucket, (t, qMap) -> {
            qMap.computeIfPresent(key, (t1, q) -> {
                q.remove(value);
                return q.isEmpty() ? null : q;
            });

            return qMap.isEmpty() ? null : qMap;
        });
    }

    protected abstract Mono<Boolean> tryLock(String bucket, String key, T value);

    protected abstract Mono<Boolean> tryUnLock(String bucket, String key, T value);

    protected abstract Mono<Boolean> keep(String bucket, String key, T value);

    public static <T extends Lock> Mono<Payload> lock(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");

        int type = Integer.parseInt(msg.dataMap.getOrDefault("type", "-1"));
        LockServer<T> server = (LockServer<T>) Lock.getServer(type);
        if (server == null) {
            log.error("no such lock server type {}", type);
            return Mono.just(ERROR_PAYLOAD);
        }

        T value = Json.decodeValue(msg.get("value"), server.tClass);

        if (type == Lock.LEASE_TYPE) {
            try {
                LeaseServer leaseServer = (LeaseServer) server;
                LeaseLock leaseLock = (LeaseLock) value;
                return leaseServer.tryLease(bucket, key, leaseLock)
                        .doOnNext(state -> {
                            if (state >= 0) {
                                leaseLock.leaseState = state;
                                server.addKeep(bucket, key, value);
                            }
                        })
                        .map(state -> DefaultPayload.create(state.toString(), ErasureServer.PayloadMetaType.SUCCESS.name())
                        );
            } catch (Exception e) {
                log.info("CIFS Lease server type error {}", type);
                return Mono.just(ERROR_PAYLOAD);
            }
        }else if (type == Lock.NFS4_LOCK_TYPE){
            try {
                NFS4LockServer v4Server = (NFS4LockServer) server;
                NFS4Lock nfs4Lock = (NFS4Lock) value;
                return v4Server.tryLock0(bucket, key, nfs4Lock)
                        .flatMap(resLock -> {
                            if (DEFAULT_LOCK.equals(resLock)) {
                                server.addKeep(bucket, key, value);
                            }
                            String res = Json.encode(resLock);
                            return Mono.just(res);
                        })
                        .map(res -> DefaultPayload.create(res, ErasureServer.PayloadMetaType.SUCCESS.name())
                        );
            } catch (Exception e) {
                log.info("NSF V4 lock server type error {}", type);
                return Mono.just(ERROR_PAYLOAD);
            }
        }else if (type == Lock.NFS4_SHARE_ACCESS_TYPE){
            try {
                ShareAccessServer shareAccessServer = (ShareAccessServer) server;
                ShareAccessLock shareAccessLock = (ShareAccessLock) value;
                return shareAccessServer.tryShare(bucket, key, shareAccessLock)
                        .flatMap(resLock -> {
                            if (!ShareAccessLock.CONFLICT_SHARE.equals(resLock) && !ShareAccessLock.NOT_FOUND_SHARE.equals(resLock)) {
                                shareAccessServer.addKeep(bucket, key, resLock);
                            }
                            String res = Json.encode(resLock);
                            return Mono.just(res);
                        })
                        .map(res -> DefaultPayload.create(res, ErasureServer.PayloadMetaType.SUCCESS.name())
                        );
            } catch (Exception e) {
                log.info("NFS V4 share type error {}", type);
                return Mono.just(ERROR_PAYLOAD);
            }
        }else if (type == Lock.NFS4_DELEGATE_TYPE){
            try {
                DelegateServer delegateServer = (DelegateServer) server;
                DelegateLock delegateLock = (DelegateLock) value;
                return delegateServer.tryDelegate(bucket, key, delegateLock)
                        .doOnNext(b -> {
                            if (b && delegateLock.type == DelegateLock.ADD_DELEGATE_TYPE) {
                                server.addKeep(bucket, key, value);
                            }
                        })
                        .map(b -> b ? SUCCESS_PAYLOAD : ERROR_PAYLOAD);
            } catch (Exception e) {
                log.info("NFS V4 delegate type error {}", type);
                return Mono.just(ERROR_PAYLOAD);
            }
        } else {
            return server.tryLock(bucket, key, value)
                    .map(b -> b ? SUCCESS_PAYLOAD : ERROR_PAYLOAD);
        }
    }

    public static <T extends Lock> Mono<Payload> unlock(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String key = msg.get("key");
        int type = Integer.parseInt(msg.dataMap.getOrDefault("type", "-1"));
        LockServer<T> server = (LockServer<T>) Lock.getServer(type);
        if (server == null) {
            log.error("no such lock server type {}", type);
            return Mono.just(ERROR_PAYLOAD);
        }

        T value = Json.decodeValue(msg.get("value"), server.tClass);
        if (type == Lock.NFS4_LOCK_TYPE){
            NFS4LockServer v4Server = (NFS4LockServer) server;
            NFS4Lock nfs4Lock = (NFS4Lock) value;
            return v4Server.tryUnLock0(bucket, key, nfs4Lock)
                    .flatMap(resLock -> {
                        if (DEFAULT_LOCK.equals(resLock)) {
                            server.removeKeep(bucket, key, value);
                        }
                        String res = Json.encode(resLock);
                        return Mono.just(res);
                    })
                    .map(res -> DefaultPayload.create(res, ErasureServer.PayloadMetaType.SUCCESS.name())
                    );
        }
        return server.tryUnLock(bucket, key, value)
                .map(b -> SUCCESS_PAYLOAD);
    }

    public static <T extends Lock> Mono<Payload> keep(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String[] keys = Json.decodeValue(msg.get("key"), String[].class);
        String[] values = Json.decodeValue(msg.get("value"), String[].class);
        int type = Integer.parseInt(msg.dataMap.getOrDefault("type", "-1"));
        LockServer<T> server = (LockServer<T>) Lock.getServer(type);

        if (server == null) {
            log.error("no such lock server type {}", type);
            return Mono.just(ERROR_PAYLOAD);
        }

        LinkedList<Integer> errors = new LinkedList<>();
        return Flux.range(0, keys.length)
                .flatMap(index -> Mono.just(index)
                        .flatMap(i -> {
                            T o = Json.decodeValue(values[index], server.tClass);
                            return server.keep(bucket, keys[index], o)
                                    .doOnNext(b -> {
                                        if (!b) {
                                            errors.add(index);
                                        }
                                    });
                        }))
                .collectList()
                .map(b -> DefaultPayload.create(Json.encode(errors), ErasureServer.PayloadMetaType.SUCCESS.name()));
    }
}
