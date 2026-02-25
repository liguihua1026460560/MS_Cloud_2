package com.macrosan.filesystem.cifs.shareAccess;

import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.filesystem.cifs.shareAccess.ShareAccessLock.*;
import static com.macrosan.filesystem.lock.LockKeeper.KEEP_NAN;

@Log4j2
public class ShareAccessServer extends LockServer<ShareAccessLock> {
    public static Map<ShareAccessLock, Long> unLock = new ConcurrentHashMap<>();
    public static Map<ShareAccessLock, Object> unLockMap = new ConcurrentHashMap<>();
    protected static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-unlock-shareAccess-clear"));
    public static void register() {
        ShareAccessServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final ShareAccessServer instance = new ShareAccessServer(Lock.SHARE_ACCESS_TYPE, ShareAccessLock.class);

    public static ShareAccessServer getInstance() {
        return instance;
    }

    protected ShareAccessServer(int type, Class<ShareAccessLock> shareAccessLockClass) {
        super(type, shareAccessLockClass);
        executor.submit(this::checkUnlock);
    }

    public static Map<String, Set<ShareAccessLock>> shareAccessMap = new ConcurrentHashMap<>();
    public static Map<Long, Map<Integer, Map<SMB2FileId, ShareAccessLock>>> sessionTreeMap = new ConcurrentHashMap<>(); // 关闭 tree connect 和 session 时解除共享模式，防止死锁

    @Override
    protected Mono<Boolean> tryLock(String bucket, String key, ShareAccessLock value) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        String k = bucket + '/' + key;

        Object o = unLockMap.computeIfAbsent(value, v0 -> new Object());
        synchronized (o) {
            if (unLock.containsKey(value)) {
                return Mono.just(false);
            }

            shareAccessMap.compute(k, (k0, set) -> {
                if (set == null) {
                    set = ConcurrentHashMap.newKeySet();
                }
                boolean b;
                if (set.contains(value)) {
                    b = false;
                } else {
                    b = set.stream().anyMatch(shareAccessLock ->
                            !checkAccessAllowed(value.accessMask, shareAccessLock.shareAccess) ||
                            !checkAccessAllowed(shareAccessLock.accessMask, value.shareAccess)
                    );
                }
                if (b) {
                    res.onNext(false);
                } else {
                    set.add(value);
                    res.onNext(true);
                }
                return set;
            });
            addKeep(bucket, key, value);
        }

        return res;
    }

    @Override
    protected Mono<Boolean> tryUnLock(String bucket, String key, ShareAccessLock value) {
        String k = bucket + '/' + key;

        Object o = unLockMap.computeIfAbsent(value, v0 -> new Object());

        synchronized (o) {
            unLock.put(value, System.nanoTime());
            shareAccessMap.computeIfPresent(k, (k0, set) -> {
                set.remove(value);
                return set.isEmpty() ? null : set;
            });
            removeKeep(bucket, key, value);
        }
        return Mono.just(true);
    }

    @Override
    protected Mono<Boolean> keep(String bucket, String key, ShareAccessLock value) {
        return tryLock(bucket, key, value);
    }

    /**
     * 检查共享模式是否允许请求的访问权限
     * @param accessMask  请求访问权限位掩码
     * @param shareAccess 共享模式位掩码
     * @return true通过
     */
    private static boolean checkAccessAllowed(long accessMask, int shareAccess) {
        // 分解共享模式的三类权限
        boolean allowRead = (shareAccess & FILE_SHARE_READ) != 0;
        boolean allowWrite = (shareAccess & FILE_SHARE_WRITE) != 0;
        boolean allowDelete = (shareAccess & FILE_SHARE_DELETE) != 0;
        // 检查读权限
        if (!allowRead && (accessMask & FILE_CHECK_READ) != 0) {
            return false;
        }
        // 检查写权限
        if (!allowWrite && (accessMask & FILE_CHECK_WRITE) != 0) {
            return false;
        }
        // 检查删除权限
        if (!allowDelete && (accessMask & FILE_CHECK_DELETE) != 0) {
            return false;
        }
        return true;
    }

    private void checkUnlock() {
        long start = System.nanoTime();
        try {
            Iterator<Map.Entry<ShareAccessLock, Long>> iterator = unLock.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ShareAccessLock, Long> entry = iterator.next();
                if (System.nanoTime() - entry.getValue() > 5 * KEEP_NAN) {
                    iterator.remove();
                    unLockMap.remove(entry.getKey());
                }
            }
        } catch (Exception e) {
            log.error(e);
        }

        long exec = KEEP_NAN - (System.nanoTime() - start);
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(this::checkUnlock, exec, TimeUnit.NANOSECONDS);
    }

    public static String info() {
        String str = "";
        try {
            str = "\nshareAccessMap->" + shareAccessMap +
                    "\nbucketKeepMap->" + getInstance().bucketKeepMap +
                    "\nsessionTreeMap->" + sessionTreeMap +
                    "\nunLockMap->" + unLockMap +
                    "\nunLock->" + unLock +
                    "\nlocalIpMap->" + SMBHandler.localIpMap +
                    "\nnewIpMap->" + SMBHandler.newIpMap;
        } catch (Exception e) {
            log.error("CIFS ShareAccess info: ", e);
        }
        return str;
    }
}
