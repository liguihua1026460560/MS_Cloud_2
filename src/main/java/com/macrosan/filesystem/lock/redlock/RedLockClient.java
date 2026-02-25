package com.macrosan.filesystem.lock.redlock;

import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.lock.LockClient;
import com.macrosan.httpserver.ServerConfig;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.concurrent.TimeUnit;

import static com.macrosan.filesystem.lock.redlock.LockType.UNKNOWN;

@Log4j2
public class RedLockClient {
    public static boolean LOCK_DEBUG = false;
    public static boolean LOCK_ENABLE = false;

    public static Mono<Boolean> lockDir(ReqInfo header, long inode, LockType lockType, boolean block) {
        if (inode == 1L) {
            return Mono.just(true);
        }

        return lock(header, ".inode." + inode, lockType, block, false);
    }

    private static void tryTask(MonoProcessor<Boolean> res, ReqInfo header, String key, LockType lockType) {
        lock(header, key, lockType, false, true)
                .subscribe(b -> {
                    if (b) {
                        res.onNext(true);
                    } else {
                        FsUtils.fsExecutor.schedule(() -> tryTask(res, header, key, lockType), 10, TimeUnit.MILLISECONDS);
                    }
                });
    }

    public static Mono<Boolean> lock(ReqInfo header, String key, LockType lockType, boolean block, boolean isLock) {
        String value = VersionUtil.getVersionNum();
        String node = ServerConfig.getInstance().getHostUuid();
        return lock(header, key, value, node, lockType, block, isLock);
    }

    public static Mono<Boolean> lock(ReqInfo header, String key, String value, String node, LockType lockType, boolean block, boolean isLock) {
        if (!LOCK_ENABLE && !isLock) {
            return Mono.just(true);
        }

        if (header.timeout) {
            log.info("opt timeout stop tryLock");
            return Mono.just(true);
        }

        if (LOCK_DEBUG) {
            log.info("redlock client lock {} {}:{} {}", header.bucket, key, lockType, block);
        }

        String bucket = header.bucket;
        RedLock lock = new RedLock(lockType, value, node);
        return LockClient.lock(bucket, key, lock)
                .flatMap(b -> {
                    if (!b) {
                        if (block) {
                            MonoProcessor<Boolean> res = MonoProcessor.create();
                            FsUtils.fsExecutor.schedule(() -> tryTask(res, header, key, lockType), 10, TimeUnit.MILLISECONDS);
                            return res;
                        } else {
                            return Mono.just(false);
                        }
                    } else {
                        if (header.timeout) {
                            log.info("opt timeout stop tryLock");
                            return unlock(bucket, key, value, true).map(bb -> true);
                        }
                        header.lock.put(key, value);
                        return Mono.just(true);
                    }
                });

    }

    public static Mono<Boolean> unlock(String bucket, String key, String value, boolean isLock) {
        if (!LOCK_ENABLE && !isLock) {
            return Mono.just(true);
        }

        if (LOCK_DEBUG) {
            log.info("redlock client unlock {} {}: {}", bucket, key, value);
        }

        RedLock lock = new RedLock(UNKNOWN, value, ServerConfig.getInstance().getHostUuid());
        return LockClient.unlock(bucket, key, lock);
    }

    public static Mono<Boolean> unlock(String bucket, String key, String value, boolean isLock, String node) {
        if (!LOCK_ENABLE && !isLock) {
            return Mono.just(true);
        }

        if (LOCK_DEBUG) {
            log.info("redlock client unlock {} {}: {}", bucket, key, value);
        }

        RedLock lock = new RedLock(UNKNOWN, value, node);
        return LockClient.unlock(bucket, key, lock);
    }
}
