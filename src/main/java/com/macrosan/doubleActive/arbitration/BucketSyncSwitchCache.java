package com.macrosan.doubleActive.arbitration;


import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.async.AsyncUtils;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * 缓存桶的同步开关状态。
 */
@Log4j2
public class BucketSyncSwitchCache {
    final static Map<String, String> switchMap = new ConcurrentHashMap<>();
    final static Map<String, Map<String, Set<Integer>>> syncIndexMap = new ConcurrentHashMap<>();
    final static Map<String, Map<String, Set<Integer>>> archiveIndexMap = new ConcurrentHashMap<>();
    final static Map<String, Boolean> fsAsyncMap = new ConcurrentHashMap<>();

    /**
     * 在默认需要获取主站点syncStamp的场合，使用该key。
     */
    public static final String DEFAULT_ON = "DEFAULT_ON";
    public static final String SWITCH_ON = "on";
    public static final String SWITCH_OFF = "off";
    public static final String SWITCH_SUSPEND = "suspend";
    public static final String SWITCH_CLOSED = "closed";

    private static BucketSyncSwitchCache instance;

    public static BucketSyncSwitchCache getInstance() {
        if (instance == null) {
            instance = new BucketSyncSwitchCache();
        }
        return instance;
    }


    public void init() {
        if (!switchMap.isEmpty()) {
            return;
        }

        switchMap.put(DEFAULT_ON, SWITCH_ON);
        ScanArgs scanArgs = new ScanArgs().match("*").limit(10);
        ScanIterator<String> iterator = ScanIterator.scan(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX), scanArgs);
        while (iterator.hasNext()) {
            String bucket = iterator.next();
            if (BUCKET_NAME_PATTERN.matcher(bucket).matches()) {
                final Map<String, String> info = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
                updateBucketSyncCache(bucket, info);
            }
        }
    }

    public static final Scheduler SCHEDULER;

    static {

        Scheduler scheduler = null;
        try {
            ThreadFactory DISK_THREAD_FACTORY = new MsThreadFactory("sync-switch");
            MsExecutor executor = new MsExecutor(PROC_NUM * 2, 8, DISK_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        SCHEDULER = scheduler;
    }

    Map<String, AtomicBoolean> bucketLockMap = new ConcurrentHashMap<>();

    public static Mono<Boolean> isSyncSwitchOffMono(String bucket) {
        if (bucket == null) {
            return Mono.just(true);
        }

        String curStatus = switchMap.get(bucket);
        if (curStatus != null) {
            return Mono.just(SWITCH_OFF.equals(curStatus));
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();
        RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                .publishOn(SCHEDULER)
                .switchIfEmpty(Mono.just(new HashMap<>()))
                .timeout(Duration.ofSeconds(3))
                .subscribe(info -> {
                    updateBucketSyncCache(bucket, info);
                    res.onNext(SWITCH_OFF.equals(info.getOrDefault(DATA_SYNC_SWITCH, SWITCH_OFF)));
                }, e -> {
                    log.error("isSyncSwitchOn error, {}", bucket, e);
                    res.onError(e);
                });
        return res;
    }

    private static void updateBucketSyncCache(String bucket, Map<String, String> info) {
        final String status = info.getOrDefault(DATA_SYNC_SWITCH, SWITCH_OFF);
        final String archiveIndex = info.getOrDefault(ARCHIVE_INDEX, "");
        final String syncIndex = info.getOrDefault(SYNC_INDEX, "");
        Map<String, Set<Integer>> archiveMap = new ConcurrentHashMap<>();
        Map<String, Set<Integer>> syncMap = new ConcurrentHashMap<>();

        if (!archiveIndex.isEmpty()) {
            try {
                archiveMap = Json.decodeValue(archiveIndex, new TypeReference<Map<String, Set<Integer>>>() {});
            } catch (Exception e) {
                log.error("get archiveIndex error: {}", archiveIndex, e);
            }
        }

        if (!syncIndex.isEmpty()) {
            try {
                syncMap = Json.decodeValue(syncIndex, new TypeReference<Map<String, Set<Integer>>>() {});
            } catch (Exception e) {
                log.error("get syncIndex error: {}", syncIndex, e);
            }
        }

        switchMap.put(bucket, status);
        archiveIndexMap.put(bucket, archiveMap);
        syncIndexMap.put(bucket, syncMap);
        fsAsyncMap.put(bucket, AsyncUtils.checkFSProtocol(info));
    }

    public static Set<Integer> getSyncIndexMap(String bucket, Integer localIndex) {
        return syncIndexMap.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>()).computeIfAbsent(localIndex + "", s -> new ConcurrentHashSet<>());
    }


    public static Set<Integer> getArchiveIndexMap(String bucket, Integer localIndex) {
        return archiveIndexMap.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>()).computeIfAbsent(localIndex + "", s -> new ConcurrentHashSet<>());
    }

    public static boolean isBucketNeedFsAsync(String bucket) {
        return fsAsyncMap.getOrDefault(bucket, false);
    }

    public boolean isSyncSwitchOn(String bucket) {
        if (bucket == null) {
            return false;
        }

        String curStatus = switchMap.get(bucket);
        if (curStatus != null) {
            return SWITCH_ON.equals(curStatus);
        }

        AtomicBoolean locked = bucketLockMap.computeIfAbsent(bucket, k -> new AtomicBoolean());
        if (locked.compareAndSet(false, true)) {
            MonoProcessor<Boolean> res = MonoProcessor.create();
            RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, DATA_SYNC_SWITCH)
                    .publishOn(SCHEDULER)
                    .switchIfEmpty(Mono.just("off"))
                    .doFinally(s -> locked.compareAndSet(true, false))
                    .timeout(Duration.ofSeconds(3))
                    .subscribe(hget -> {
                        switchMap.put(bucket, hget);
                        res.onNext(SWITCH_ON.equals(hget));
                    }, e -> {
                        log.error("isSyncSwitchOn error, {}", bucket, e);
                        res.onError(e);
                    });
            return res.publishOn(SCHEDULER).block();
        } else {
            long start = DateChecker.getCurrentTime();
            while (locked.get() && DateChecker.getCurrentTime() - start < 3_000) {
                String status = switchMap.get(bucket);
                if (status != null) {
                    return SWITCH_ON.equals(status);
                }
            }

            if (switchMap.get(bucket) == null) {
                throw new RuntimeException("isSyncSwitchOn error, cannot get status from redis");
            } else {
                return SWITCH_ON.equals(switchMap.get(bucket));
            }

        }
    }

    /**
     * 更新缓存。若该桶的缓存不存在则不更新，防止remove后又被立刻添加。remove后只能通过isSyncSwitchOn重新加回来。
     */
    public void update(String bucket, String status) {
        synchronized (switchMap) {
            String curSwitch = switchMap.get(bucket);
            if (curSwitch != null && !status.equals(curSwitch)) {
                log.info("update {} to {}", bucket, status);
                switchMap.put(bucket, status);
                if (SWITCH_CLOSED.equals(status)) {
                    switchMap.put(bucket, status);
                    log.info("bucket {} data sync switch is closed.", bucket);
                }
            }
        }
    }

    public void check(String bucket, Map<String, String> info) {
        if (info.isEmpty()) {
            return;
        }
        updateBucketSyncCache(bucket, info);
    }

    public void remove(String bucket) {
        synchronized (switchMap) {
            log.info("remove {}", bucket);
            switchMap.remove(bucket);
        }
    }

    public String get(String bucket) {
        synchronized (switchMap) {
            return switchMap.getOrDefault(bucket, "off");
        }
    }

    /**
     * 如果差异记录扫描到不存在的桶，isSyncSwitchOn依然会添加桶状态为off，导致后续建立同名桶时开开关，isSyncSwitchOn无法更新为true。
     * 所以添加桶时要调用该方法
     */
    public void add(String bucket, String s) {
        synchronized (switchMap) {
            log.info("add {}, sync switch {}", bucket, s);
            switchMap.put(bucket, s);
        }
    }

    public static boolean isSwitchOn(String status) {
        return SWITCH_ON.equals(status) || SWITCH_SUSPEND.equals(status);
    }

    public static boolean isSwitchOn(Map<String, String> bucketInfo) {
        String status = getBucketSyncStatus(bucketInfo);
        return isSwitchOn(status);
    }

    public static String getBucketSyncStatus(Map<String, String> bucketInfo) {
        return bucketInfo.getOrDefault(DATA_SYNC_SWITCH, "off");
    }

    public static String getBucketArchiveStatus(Map<String, String> bucketInfo) {
        return bucketInfo.getOrDefault(ARCHIVE_SWITCH, "off");
    }

    public static boolean isSwitchSuspend(Map<String, String> bucketInfo) {
        String status = getBucketSyncStatus(bucketInfo);
        if (SWITCH_SUSPEND.equals(status)) {
            return true;
        }
        return false;
    }

    public static boolean isSwitchClose(Map<String, String> bucketInfo) {
        String status = getBucketSyncStatus(bucketInfo);
        if (SWITCH_CLOSED.equals(status)) {
            return true;
        }
        return false;
    }
}
