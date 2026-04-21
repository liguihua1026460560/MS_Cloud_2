package com.macrosan.filesystem.cifs.shareAccess;

import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.LockClient;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public class ShareAccessClient {
    public static boolean shareAccessSwitch = true;

    public static Mono<Boolean> lock(String bucket, String key, ShareAccessLock value, SMB2Header header) {
        if (!shareAccessSwitch) {
            return Mono.just(true);
        }
        return LockClient.lock(bucket, key, value)
                .flatMap(b -> {
                    if (b) {
                        ShareAccessServer.sessionTreeMap.computeIfAbsent(header.getSessionId(), sessionId -> new ConcurrentHashMap<>())
                                .computeIfAbsent(header.getTid(), treeId -> new ConcurrentHashMap<>())
                                .put(value.smb2FileId, value);
                    }
                    return Mono.just(b);
                });
    }

    public static Mono<Boolean> unlock(SMB2FileId smb2FileId, SMB2Header header) {
        if (!shareAccessSwitch) {
            return Mono.just(true);
        }
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ShareAccessServer.sessionTreeMap.compute(header.getSessionId(), (sessionId, map) -> {
            if (map == null) {
                res.onNext(true);
                return null;
            }
            map.compute(header.getTid(), (treeId, map0) -> {
                if (map0 == null) {
                    res.onNext(true);
                    return null;
                }
                ShareAccessLock remove = map0.remove(smb2FileId);
                if (remove != null) {
                    LockClient.unlock(remove.bucket, String.valueOf(remove.ino), remove)
                            .subscribe(res::onNext);
                } else {
                    res.onNext(true);
                }
                return map0.isEmpty() ? null : map0;
            });
            return map.isEmpty() ? null : map;
        });
        return res;
    }

    public static Mono<Boolean> reconnectLock(String bucket, String key, ShareAccessLock value, ShareAccessLock reconnect, SMB2Header header) {
        if (!shareAccessSwitch) {
            return Mono.just(true);
        }
        ShareAccessServer.sessionTreeMap.computeIfPresent(header.getSessionId(), (sessionId, map) -> {
            map.computeIfPresent(header.getTid(), (treeId, map0) -> {
                map0.remove(reconnect.smb2FileId);
                return map0.isEmpty() ? null : map0;
            });
            return map.isEmpty() ? null : map;
        });

        String node = SMB2FileId.getNode(reconnect.smb2FileId.getPersistent());
        return LockClient.unlock(reconnect.bucket, String.valueOf(reconnect.ino), reconnect, node)
                .flatMap(ignore -> LockClient.lock(bucket, key, value)
                        .flatMap(b -> {
                            if (b) {
                                ShareAccessServer.sessionTreeMap.computeIfAbsent(header.getSessionId(), sessionId -> new ConcurrentHashMap<>())
                                        .computeIfAbsent(header.getTid(), treeId -> new ConcurrentHashMap<>())
                                        .put(value.smb2FileId, value);
                            }
                            return Mono.just(b);
                        }));
    }

    public static void treeDisconnect(long sid, int tid) {
        if (!shareAccessSwitch) {
            return;
        }
        ShareAccessServer.sessionTreeMap.computeIfPresent(sid, (sessionId, map) -> {
            map.computeIfPresent(tid, (treeId, map0) -> {
                Flux.fromIterable(map0.values())
                        .flatMap(value -> LockClient.unlock(value.bucket, String.valueOf(value.ino), value))
                        .subscribe();
                return null;
            });
            return map.isEmpty() ? null : map;
        });
    }

    public static void logOff(long sid) {
        if (!shareAccessSwitch) {
            return;
        }
        ShareAccessServer.sessionTreeMap.computeIfPresent(sid, (sessionId, map) -> {
            Flux.fromIterable(map.values())
                    .flatMap(map0 -> Flux.fromIterable(map0.values())
                            .flatMap(value -> LockClient.unlock(value.bucket, String.valueOf(value.ino), value)))
                    .subscribe();
            return null;
        });
    }
}
