package com.macrosan.filesystem.lock.redlock;

import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.filesystem.lock.LockKeeper.KEEP_NAN;
import static com.macrosan.filesystem.lock.redlock.RedLockClient.LOCK_DEBUG;

@Log4j2
public class RedLockServer extends LockServer<RedLock> {
    public static Map<String, Long> unLock = new ConcurrentHashMap<>();
    public static Map<String, Object> unLockMap = new ConcurrentHashMap<>();
    protected static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-clear-old-lock-recode"));
    private static RedLockServer instance = null;
    public static void register() {
        RedLockServer server = new RedLockServer(com.macrosan.filesystem.lock.Lock.RED_LOCK_TYPE, RedLock.class);
        instance = server;
        com.macrosan.filesystem.lock.Lock.register(server.type, server, server.tClass);
    }

    private static final ConcurrentHashMap<String, Lock> lockMap = new ConcurrentHashMap<>();

    private RedLockServer(int type, Class<RedLock> tClass) {
        super(type, tClass);
        executor.submit(this::clearOldLockRecode);
    }

    @Override
    public Mono<Boolean> tryLock(String bucket, String key, RedLock redLock) {
        String realPath = bucket + "/" + key;
        String[] split = realPath.split("/");
        StringBuilder dirs = new StringBuilder(1024);

        String lockKey = realPath + redLock.value;
        Object o = unLockMap.computeIfAbsent(lockKey, k -> new Object());
        synchronized (o) {
            if (unLock.containsKey(lockKey)) {
                return Mono.just(false);
            }

            for (int i = 0; i < split.length - 1; i++) {
                dirs.append(split[i]);
                dirs.append('/');

                Lock lock = lockMap.computeIfAbsent(dirs.toString(), k -> new Lock());
                //对所有dir上读锁
                if (!lock.lock(LockType.READ, redLock.value)) {
                    return Mono.just(false);
                }
            }

            dirs.append(split[split.length - 1]);
            dirs.append('/');
            Lock lock = lockMap.computeIfAbsent(dirs.toString(), k -> new Lock());

            if (!lock.lock(redLock.lockType, redLock.value)) {
                if (LOCK_DEBUG) {
                    log.info("redlock server lock {} {}: {} {} fail", bucket, key, dirs.toString(), redLock);
                }
                return Mono.just(false);
            }

            if (LOCK_DEBUG) {
                log.info("redlock server lock {} {}: {} {} success", bucket, key, dirs.toString(), redLock);
            }
            addKeep(bucket, key, redLock);
            return Mono.just(true);
        }
    }

    @Override
    public Mono<Boolean> tryUnLock(String bucket, String key, RedLock redLock) {
        String realPath = bucket + "/" + key;
        String[] split = realPath.split("/");
        StringBuilder dirs = new StringBuilder(1024);

        String lockKey = realPath + redLock.value;
        Object o = unLockMap.computeIfAbsent(lockKey, k -> new Object());
        synchronized (o) {
            unLock.put(lockKey, System.nanoTime());

            for (int i = 0; i < split.length - 1; i++) {
                dirs.append(split[i]);
                dirs.append('/');

                Lock lock = lockMap.computeIfAbsent(dirs.toString(), k -> new Lock());

                if (lock.unlock(redLock.value)) {
                    lockMap.computeIfPresent(dirs.toString(), (k, oldLock) -> {
                        if (oldLock.readLock.isEmpty() && oldLock.writeLock.isEmpty()) {
                            return null;
                        } else {
                            return oldLock;
                        }
                    });
                }
            }

            dirs.append(split[split.length - 1]);
            dirs.append('/');
            Lock lock = lockMap.computeIfAbsent(dirs.toString(), k -> new Lock());

            if (lock.unlock(redLock.value)) {
                lockMap.computeIfPresent(dirs.toString(), (k, oldLock) -> {
                    if (oldLock.readLock.isEmpty() && oldLock.writeLock.isEmpty()) {
                        return null;
                    } else {
                        return oldLock;
                    }
                });
            }

            if (LOCK_DEBUG) {
                log.info("redlock server unlock {} {}: {} {} success", bucket, key, dirs.toString(), redLock);
            }
            removeKeep(bucket, key, redLock);
            return Mono.just(true);
        }
    }

    @Override
    public Mono<Boolean> keep(String bucket, String key, RedLock value) {
        return tryLock(bucket, key, value);
    }

    @ToString
    private static class Lock {
        Set<String> readLock = new HashSet<>();
        Set<String> writeLock = new HashSet<>();

        boolean lock(LockType lockType, String value) {
            synchronized (this) {
                switch (lockType) {
                    case READ:
                        if (!readLock.contains(value)) {
                            if (!writeLock.isEmpty()) {
                                return false;
                            }
                        }

                        readLock.add(value);
                        break;
                    case WRITE:
                        if (!writeLock.contains(value)) {
                            if (!readLock.isEmpty() || !writeLock.isEmpty()) {
                                return false;
                            }
                        }

                        writeLock.add(value);
                        break;
                }

                return true;
            }
        }

        boolean unlock(String value) {
            synchronized (this) {
                readLock.remove(value);
                writeLock.remove(value);

                return readLock.isEmpty() && writeLock.isEmpty();
            }
        }
    }

    public static String info() {
        String str = "";
        try {
            str = "\nlockMap->" + lockMap +
                   "\nbucketKeepMap->" + instance.bucketKeepMap;
        } catch (Exception e) {
            log.error("RedLock info: ", e);
        }
        return str;
    }

    private void clearOldLockRecode() {
        long start = System.nanoTime();
        try {
            Iterator<Map.Entry<String, Long>> iterator = unLock.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                if (System.nanoTime() - entry.getValue() > 5 * KEEP_NAN) {
                    iterator.remove();
                    unLockMap.remove(entry.getKey());
                }
            }
        }catch (Exception e) {
            log.error(e);
        }

        long exec = KEEP_NAN - (System.nanoTime() - start);
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(this::clearOldLockRecode, exec, TimeUnit.NANOSECONDS);
    }
}
