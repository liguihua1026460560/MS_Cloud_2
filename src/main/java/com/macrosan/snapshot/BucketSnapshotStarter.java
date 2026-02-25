package com.macrosan.snapshot;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.snapshot.pojo.SnapshotMergeTask;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.json.Json;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_SNAPSHOT_INDEX;
import static com.macrosan.snapshot.SnapshotMergeHandler.mergeSnapshotObject;

/**
 * @author zhaoyang
 * @date 2024/06/26
 **/
public class BucketSnapshotStarter {

    public static final ScheduledThreadPoolExecutor SNAP_TIMER;

    public final static Scheduler SNAP_SCHEDULER;
    private static final RedisConnPool POOL = RedisConnPool.getInstance();
    public static final Map<String, String> RUNNING_MERGE_TASKS = new ConcurrentHashMap<>();
    public static final int MAX_MERGE_SNAPSHOT_COUNT = 3;

    static {
        SNAP_TIMER = new ScheduledThreadPoolExecutor(PROC_NUM, runnable -> {
            AtomicInteger atomicInteger = new AtomicInteger();
            return new Thread(runnable, "snap-scheduler-" + atomicInteger.incrementAndGet());
        });
        SNAP_SCHEDULER = Schedulers.fromExecutor(SNAP_TIMER);
    }

    public static void start() {
        scheduleDiscardSnapshotsScan();
        scheduleSnapshotDeletionScan();
    }

    /**
     * 扫描因为回滚而被跳过的快照
     * 将这些快照中的对象数据进行删除
     */
    private static void scheduleDiscardSnapshotsScan() {
        SNAP_TIMER.scheduleAtFixedRate(() -> {
            //主站点扫描
            if (!"master".equals(Utils.getRoleState())) {
                return;
            }
            ScanArgs scanArgs = new ScanArgs().match(SNAPSHOT_DISCARD_LIST_PREFIX + "*");
            ScanStream.scan(POOL.getReactive(REDIS_SNAPSHOT_INDEX), scanArgs)
                    .publishOn(SNAP_SCHEDULER)
                    .subscribe(key -> {
                        ScanStream.sscan(POOL.getReactive(REDIS_SNAPSHOT_INDEX), key)
                                .publishOn(SNAP_SCHEDULER)
                                .subscribe(mark -> {
                                    SnapshotCleanUpHandler.cleanUpDiscardObject(key.substring(SNAPSHOT_DISCARD_LIST_PREFIX.length()), mark);
                                });
                    });
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 扫描需要进行异步删除的快照
     */
    private static void scheduleSnapshotDeletionScan() {
        SNAP_TIMER.scheduleAtFixedRate(() -> {
            //主站点扫描
            if (!"master".equals(Utils.getRoleState())) {
                return;
            }
            ScanArgs scanArgs = new ScanArgs().match(SNAPSHOT_MERGE_TASK_PREFIX + "*");
            ScanStream.scan(POOL.getReactive(REDIS_SNAPSHOT_INDEX), scanArgs)
                    .publishOn(SNAP_SCHEDULER)
                    .subscribe(key -> {
                        POOL.getReactive(REDIS_SNAPSHOT_INDEX).hgetall(key)
                                .publishOn(SNAP_SCHEDULER)
                                .subscribe(kv -> {
                                    if (kv.isEmpty()) {
                                        return;
                                    }
                                    String minMergeMark = kv.keySet().stream().sorted().limit(1).collect(Collectors.toList()).get(0);
                                    SnapshotMergeTask snapshotMergeTask = Json.decodeValue(kv.get(minMergeMark), SnapshotMergeTask.class);
                                    mergeSnapshotObject(snapshotMergeTask);
                                });
                    });
        }, 10, 10, TimeUnit.SECONDS);
    }
}
