package com.macrosan.storage.metaserver;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.storage.metaserver.ShardingScheduler.*;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.ARCHIVE_SUFFIX;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.recovery;

/**
 * @author zhangjiliang
 */
@Log4j2
public class ShardingWorker {

    private static final Map<String, AbstractShardingTaskRunner> SHARDING_RUNNING_MAP = new ConcurrentHashMap<>();

    private static final UnicastProcessor<AbstractShardingTaskRunner>[] SHADING_TASK_PROCESSOR = new UnicastProcessor[PROC_NUM];

    public static final Set<String> SHARDING_LOCK_SET = new ConcurrentSkipListSet<>();

    public static final Scheduler SHARDING_SCHEDULER;

    static {
        Scheduler scheduler = null;
        try {
            ThreadFactory shardingTaskSchedule = new MsThreadFactory("sharding");
            MsExecutor executor = new MsExecutor(PROC_NUM, 1, shardingTaskSchedule);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        SHARDING_SCHEDULER = scheduler;
    }

    private static final int MAX_RUNNING_NUM = 3;

    public static final String SHARDING_LOCK_PREFIX = "sharding_lock_";

    public static final String INDEX_KEY = "bucket_index_map";

    public static final String SHARDING_CONF_REDIS_KEY = "sharding_config";

    private static final AtomicBoolean initialized = new AtomicBoolean(false);


    private static void initShardingProcessor() {
        for (int i = 0; i < SHADING_TASK_PROCESSOR.length; i++) {
            SHADING_TASK_PROCESSOR[i] = UnicastProcessor.create(Queues.<AbstractShardingTaskRunner>unboundedMultiproducer().get());
            SHADING_TASK_PROCESSOR[i].publishOn(SHARDING_SCHEDULER).subscribe(AbstractShardingTaskRunner::run);
        }
    }

    private static final ScheduledThreadPoolExecutor SCHEDULE = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "sharding-checker"));

    public static void init() {
        if (initialized.compareAndSet(false, true)) {
            ShardHealthRecorder.init();
            initShardingProcessor();
            SCHEDULE.schedule(ShardingWorker::startChecker, 180, TimeUnit.SECONDS);
        }
    }

    private static void startChecker() {
        try {
            // 索引池重构迁移程中，不进行桶的元数据散列
            if (indexMigrateIsRunning()) {
                log.debug("The current environment is undergoing reconstruction and migration, and the bucket hash function will be disabled.");
                DISABLED_BUCKET_HASH.set(true);
                return;
            }

            DISABLED_BUCKET_HASH.set(false);
            ShardingScheduler.processWaitSet();
            RedisCommands<String, String> command = RedisConnPool.getInstance().getCommand(REDIS_TASKINFO_INDEX);
            ScanArgs scanArgs = new ScanArgs().match("*" + ARCHIVE_SUFFIX).limit(10);
            ScanIterator<String> iterator = ScanIterator.scan(command, scanArgs);
            Set<String> list = new HashSet<>();
            while (iterator.hasNext()) {
                String key = iterator.next();
                String bucket = key.substring(0, key.indexOf("_"));
                list.add(bucket);
                boolean lock = false;
                try {
                    lock = tryLock(bucket);
                    if (!lock) {
                        continue;
                    }
                    if (CheckUtils.bucketFsCheck(bucket) || SnapshotUtil.checkBucketSnapshotEnable(bucket)) {
                        continue;
                    }
                    if (contains(bucket) || SHARDING_RUNNING_MAP.size() >= MAX_RUNNING_NUM) {
                        continue;
                    }
                    // 如果任务中断前的运行节点与当前节点一致，则直接在当前节点恢复任务的执行
                    Map<String, String> map = RedisConnPool.getInstance().getCommand(REDIS_TASKINFO_INDEX).hgetall(key);
                    String uuid = map.get("runningNode");
                    String sourceNode = map.get("sourceVnode");
                    if (ServerConfig.getInstance().getHostUuid().equals(uuid) && !SHARDING_RUNNING_MAP.containsKey(bucket)) {
                        AbstractShardingTaskRunner recovery = recovery(key);
                        addWorker(bucket, recovery);
                        continue;
                    }

                    // 若任务的旧的执行节点服务正常则不处理
                    if (nodeMsCloudIsUp(uuid)) {
                        continue;
                    }

                    // 为中断的任务挑选新的执行节点
                    Tuple2<String, String> runningNode = selectRunningNode(bucket, sourceNode);
                    if (runningNode == null || !ServerConfig.getInstance().getHostUuid().equals(runningNode.var1)) {
                        continue;
                    }
                    // 在新选择的节点上运行
                    AbstractShardingTaskRunner recovery = recovery(key);
//                    recovery.archive();
                    addWorker(bucket, recovery);
                } finally {
                    if (lock) {
                        unLock(bucket);
                    }
                }
            }
            SHARDING_LOCK_SET.removeIf(v -> !list.contains(v));
        } finally {
            SCHEDULE.schedule(ShardingWorker::startChecker, 180, TimeUnit.SECONDS);
        }
    }

    public synchronized static void addWorker(String bucketName, AbstractShardingTaskRunner runner) {
        if (!SHARDING_RUNNING_MAP.containsKey(bucketName)) {
            if (SHARDING_RUNNING_MAP.size() < MAX_RUNNING_NUM) {
                if (!runner.tryGetLock()) {
                    log.info("Try get {} sharding task runner lock Error!", runner.bucketName);
                    return;
                }
                runner.archive();
                SHARDING_RUNNING_MAP.put(bucketName, runner);
                int index = Math.abs(bucketName.hashCode()) % SHADING_TASK_PROCESSOR.length;
                SHADING_TASK_PROCESSOR[index].onNext(runner);
                runner.res()
                        .subscribe(b -> {
                            if (b) {
                                SHARDING_RUNNING_MAP.remove(runner.bucketName);
                                runner.deleteArchive();
                                log.info("bucket {} sharding successfully!", bucketName);
                            } else {
                                SHARDING_RUNNING_MAP.remove(runner.bucketName);
                                log.error("bucket {} sharding error!", bucketName);
                            }
                        }, e -> {
                            SHARDING_RUNNING_MAP.remove(runner.bucketName);
                            log.error("bucket {} sharding error!", bucketName);
                        });
            } else {
                log.info("The current node has reached or exceeded the maximum allowable number of shard tasks that can be executed. {}", SHARDING_RUNNING_MAP.size());
            }
        } else {
            log.info("The bucket {} should have a shard task that is currently being executed", bucketName);
        }
    }

    public static boolean contains(String bucket) {
        return SHARDING_RUNNING_MAP.containsKey(bucket);
    }

    static boolean tryLock(String bucketName) {
        SetArgs args = new SetArgs().nx().ex(60);
        String res = RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX)
                .set(SHARDING_LOCK_PREFIX + bucketName, ServerConfig.getInstance().getHostUuid(), args);
        return "OK".equals(res);
    }

    static void unLock(String bucketName) {
        RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).del(SHARDING_LOCK_PREFIX + bucketName);
    }

    public static boolean bucketHasShardingTask(String bucketName) {
        return RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX) != 0;
    }

    public static boolean indexMigrateIsRunning() {
        Long running = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).exists("running");
        if (running != 0) {
            String poolName = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).hget("running", "poolName");
            String poolType = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(poolName, "role");
            return "meta".equals(poolType);
        }
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool("ShardingWorker");
        String vnodePrefix = metaStoragePool.getVnodePrefix();
        String strategyName = "storage_" + vnodePrefix;
        String pool = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
        String runningKey = "running_" + pool;
        Long exists = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).exists(runningKey);
        return exists != 0;
    }

    /**
     * 删除桶的分片记录信息
     */
    public static void deleteBucketShardInfo(String bucketName) {
        RedisConnPool.getInstance().getShortMasterCommand(REDIS_ACTION_INDEX).hdel(INDEX_KEY, bucketName);
        RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + ARCHIVE_SUFFIX);
    }

}
