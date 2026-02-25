package com.macrosan.storage.metaserver;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.VnodeCache;
import com.macrosan.storage.metaserver.move.*;
import com.macrosan.storage.metaserver.move.copy.AbstractCopyTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.rsocket.util.DefaultPayload;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.metaserver.ShardHealthRecorder.shardingIsHealth;
import static com.macrosan.storage.metaserver.ShardingWorker.*;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

@Log4j2
public class ShardingScheduler {

    /**
     * 是否启用桶散列功能
     */
    public static volatile boolean BUCKET_HASH_SWITCH;
    public static final AtomicBoolean DISABLED_BUCKET_HASH = new AtomicBoolean(false);
    static {
        boolean enabled;
        String bucketHashSwitch = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("bucket_hash_switch")).orElse("1");
        if ("1".equals(bucketHashSwitch)) {
            enabled = true;
            log.info("The bucket hash feature is currently enabled in the environment.");
        } else {
            enabled = false;
            log.info("The bucket hash feature is currently not enabled in the environment.");
        }
        BUCKET_HASH_SWITCH = enabled;
        DISABLED_BUCKET_HASH.set(!enabled);
    }

    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    private static final ScheduledThreadPoolExecutor SCHEDULE = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "sharding-job"));
    private static final Semaphore semaphore = new Semaphore(1);

    private static void scheduleShardingTask() {
        try {
            execute();
        } finally {
            SCHEDULE.schedule(ShardingScheduler::scheduleShardingTask, 60, TimeUnit.MINUTES);
        }
    }

    public static void start() {
        if (!BUCKET_HASH_SWITCH) {
            return;
        }
        if (initialized.compareAndSet(false, true)) {
            StoragePoolFactory.getMetaStoragePool("Sharding").getShardingMapping().update();
            ShardingWorker.init();
            String state = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("bucket_hash_switch");
            if (StringUtils.isEmpty(state)) {
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).set("bucket_hash_switch", "1");
            }
            SCHEDULE.schedule(ShardingScheduler::scheduleShardingTask, 60, TimeUnit.MINUTES);
            log.info("Sharding timing task start!");
        }
    }

    public static void execute(){
        if (!ShardingScheduler.BUCKET_HASH_SWITCH) {
            log.debug("bucket sharding has stopped!");
            return;
        }
        // 加载分片配置
        String maxCopyKeys = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("sharding_config", "max_copy_keys")).orElse(String.valueOf(MAX_COPY_KEYS));
        setMaxCopyKeys(Long.parseLong(maxCopyKeys));
        String threshold = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("sharding_config", "max_expansion_threshold")).orElse(String.valueOf(MAX_EXPANSION_THRESHOLD));
        setMaxExpansionThreshold(Double.parseDouble(threshold));
        String maxShardAmount = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("sharding_config", "max_shard_amount")).orElse(String.valueOf(MAX_SHARD_AMOUNT));
        setMaxShardAmount(Integer.parseInt(maxShardAmount));
        String factor = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("sharding_config", "balance_load_factor")).orElse(String.valueOf(LOAD_FACTOR));
        setLoadFactor(Double.parseDouble(factor));
        log.debug("-------------------------------- begin bucket sharding operator -------------------------------");
        log.debug("max-copy-keys={}, threshold={}, shard-amount={} ", maxCopyKeys, threshold, maxShardAmount);
        startBuildShardingRunner();
    }


    private static final Queue<String> WAIT_QUEUE = new ConcurrentLinkedQueue<>();

    private static void startBuildShardingRunner() {
        try {
            if (indexMigrateIsRunning()) {
                log.info("meta pool have migrate task running!");
                return;
            }

            // 散列前更新ShardingMapping
            ShardingMapping shardingMapping = StoragePoolFactory.getMetaStoragePool("Sharding").getShardingMapping();
            shardingMapping.update();

            if (!semaphore.tryAcquire( 60, TimeUnit.SECONDS)) {
                log.info("The last sharding check task is running!");
                return;
            }
            RedisReactiveCommands<String, String> reactive = RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX);
            ScanArgs scanArgs = new ScanArgs().match("*").limit(10);
            Flux<String> bucketFlux = ScanStream.scan(reactive, scanArgs);
            bucketFlux
                    .publishOn(SHARDING_SCHEDULER)
                    .filter(key -> BUCKET_NAME_PATTERN.matcher(key).matches() && "hash".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).type(key)))
                    .filter(key -> !CheckUtils.bucketFsCheck(key) && !SnapshotUtil.checkBucketSnapshotEnable(key))
                    .filter(bucket -> ServerConfig.getInstance().getHostUuid().equals(selectRunningNode(bucket, StoragePoolFactory.getMetaStoragePool(bucket).getBucketVnodeId(bucket)).var1))
                    .doOnNext(bucket -> {
                        boolean lock = false;
                        try {
                            lock = tryLock(bucket + "_sharding_schedule_lock");
                            if (lock) {
                                scheduleBucketSharding(bucket);
                            } else {
                                WAIT_QUEUE.offer(bucket);
                                log.info("bucket {} try get lock fail!", bucket);
                            }
                        } finally {
                            if (lock) {
                                unLock(bucket + "_sharding_schedule_lock");
                            }
                        }
                    })
                    .doFinally(l -> semaphore.release())
                    .subscribe();
        } catch (Exception e) {
            log.error("", e);
            semaphore.release();
        }
    }

    public static void processWaitSet() {
        Queue<String> tmp = new ArrayDeque<>();
        while (!WAIT_QUEUE.isEmpty()) {
            tmp.offer(WAIT_QUEUE.poll());
        }
        while (!tmp.isEmpty()) {
            String bucket = tmp.poll();
            boolean lock = false;
            try {
                lock = tryLock(bucket + "_sharding_schedule_lock");
                if (lock) {
                    scheduleBucketSharding(bucket);
                } else {
                    WAIT_QUEUE.offer(bucket);
                    log.info("bucket {} try get lock fail!", bucket);
                }
            } finally {
                if (lock) {
                    unLock(bucket + "_sharding_schedule_lock");
                }
            }
        }
    }

    private static void scheduleBucketSharding(String bucket) {
        if (ShardingWorker.contains(bucket) || bucketHasShardingTask(bucket)) {
            log.info("shard task is currently being executed in the bucket {}.", bucket);
            return;
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucket);
        Map<String, List<String>> lunConflictGroup = storagePool.getShardingMapping().getLunConflictGroup(bucketVnodeList);
        if (!lunConflictGroup.isEmpty()) {
            log.info("Disk conflicts exist in bucket {} fragments.", bucket);
            // 进行磁盘冲突处理
            resolveConflict(bucket, bucketVnodeList, lunConflictGroup);
            return;
        }
        String[] bucketVnodeArray = bucketVnodeList.toArray(new String[0]);
        for (String bucketVnode : bucketVnodeArray) {
            // 如果桶已存在分片任务则不继续构建分片任务
            if (ShardingWorker.contains(bucket) || bucketHasShardingTask(bucket)) {
                log.info("shard {} task is currently being executed in the bucket {}.", bucketVnode, bucket);
                break;
            }
            // 查询该分片上的对象数，若分片上的对象数超过阈值，则进行扩容或者平衡
            Long objNum = queryIdxObjects(bucket, bucketVnode).block(Duration.ofSeconds(60));
            Boolean hadObject = Optional.ofNullable(ECUtils.hadObject(bucket, bucketVnode).block(Duration.ofSeconds(60))).orElse(false);
            if (objNum == null || objNum < MAX_EXPANSION_THRESHOLD || !hadObject) {
                log.debug("bucket {} shard {} objNum {} not meeting sharding criteria. hadObject:{}", bucket, bucketVnode, objNum, hadObject);
                continue;
            }
            // 当桶的分片数目大于桶的最大分片数目时， 不进行分片拓展，只进行分片间的数据平衡
            if (bucketVnodeList.size() >= MAX_SHARD_AMOUNT) {
                balance(bucket, bucketVnode);
                continue;
            }
            // 在桶的已有分片基础上分片新的分片
            String[] allocate = storagePool.getShardingMapping().allocate(bucket, bucketVnodeList, 1);
            if (allocate == null || !shardingIsHealth(bucket, allocate[0])) {
                balance(bucket, bucketVnode);
                continue;
            }
            log.info("allocate:" + Arrays.toString(allocate));
            // 构建桶分片拓展任务, 拓展任务，每次最小迁移一定数目的对象
            long copyMaxKeys = Math.min(objNum / 2, MAX_COPY_KEYS);
            ShardingExpansionTaskRunner shardingExpansionTaskRunner = new ShardingExpansionTaskRunner(bucket, bucketVnode, allocate[0], copyMaxKeys);
            // 为散列任务选择运行的节点
            String runningNode = selectRunningNode(bucket, bucketVnode).var1();
            if (runningNode.equals(ServerConfig.getInstance().getHostUuid())) {
                addWorker(bucket, shardingExpansionTaskRunner);
            } else {
                shardingExpansionTaskRunner.archive(runningNode);
            }
        }
    }

    private static void resolveConflict(String bucket, List<String> vnodeList, Map<String, List<String>> map) {
        // 打印桶的分片冲突信息
        log.info("** Bucket {} shard lun conflict information. ** ", bucket);
        log.info("** Bucket shard list : {} **", vnodeList);
        log.info("----------------------------------------------------\n");
        map.forEach((lun, list) -> log.info("** {} ** = {}", lun, list));
        log.info("----------------------------------------------------\n");
        // 每次从所有冲突的分片中选择冲突最多并且所含对象数最小的分片进行处理
        Set<String> conflictList = new HashSet<>();
        Map<String, Integer> count = new HashMap<>(8);
        for (List<String> value : map.values()) {
            for (String vnode : value) {
                conflictList.add(vnode);
                Integer i = count.getOrDefault(vnode, 0);
                count.put(vnode, ++i);
            }
        }

        List<String> collect = conflictList.stream().sorted((v1, v2) -> {
            int compare = Integer.compare(count.getOrDefault(v1, 0), count.getOrDefault(v2, 0));
            // 如果冲突数目相等，则选择对象数目最小的进行处理
            if (compare == 0) {
                Long objNum1 = Optional.ofNullable(queryIdxObjects(bucket, v1).block(Duration.ofSeconds(30))).orElse(0L);
                Long objNum2 = Optional.ofNullable(queryIdxObjects(bucket, v2).block(Duration.ofSeconds(30))).orElse(0L);
                compare = Long.compare(objNum1, objNum2);
            }
            return compare;
        }).collect(Collectors.toList());

        collect.stream().findFirst().ifPresent(v -> {
            log.info("The best option for resolving conflicts is [{}]", v);
//            vnodeList.remove(v);
            String[] allocate = StoragePoolFactory.getMetaStoragePool(bucket).getShardingMapping().allocate(bucket, vnodeList, 1);
            if (allocate == null) {
                vnodeList.remove(v);
                allocate = StoragePoolFactory.getMetaStoragePool(bucket).getShardingMapping().allocate(bucket, vnodeList, 1);
                if (allocate == null) {
                    log.info("Bucket {} has no allocatable shards.", bucket);
                    // 进行合并操作
                    vnodeList.add(v);
                    merge(bucket, collect);
                    return;
                }
            }
            if (v.equals(allocate[0])) {
                log.info("The value of v=[{}] is the same as that of allocate=[{}]", v, allocate[0]);
                return;
            }
            log.info("bucket {} allocate=[{}]", bucket, allocate[0]);
            // 将v中的数据向allocate中迁移
            ShardingRebuildTaskRunner runner = new ShardingRebuildTaskRunner(bucket, v, allocate[0]);
            // 挑选运行节点
            String runningNode = selectRunningNode(bucket, v).var1();
            if (runningNode.equals(ServerConfig.getInstance().getHostUuid())) {
                addWorker(bucket, runner);
            } else {
                runner.archive(runningNode);
            }
        });
    }

    private static void merge(String bucketName, List<String> sortConflictList) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        ObjectSplitTree tree = pool.getBucketShardCache().get(bucketName);
        for (String v : sortConflictList)
        {
            if (ShardingWorker.contains(bucketName) || bucketHasShardingTask(bucketName)) {
                return;
            }
            ObjectSplitTree.Node left;
            ObjectSplitTree.Node right;
            // 判断当前节点是左子节点还是右子节点
            ObjectSplitTree.Node current = tree.findNodeByValue(v);
            if (ObjectSplitTree.isLeft(current)) {
                left = current;
                right = tree.rightNeighbor(current);
            } else if (ObjectSplitTree.isRight(current)) {
                right = current;
                left = tree.leftNeighbor(current);
            } else {
                log.debug("bucket {} v {} merge failed!", bucketName, current);
                return;
            }
            if (left == null || right == null) {
                continue;
            }
            ObjectSplitTree.Node top = ObjectSplitTree.intersection(left, right);
            if (top == null || left.parent != top || right.parent != top) {
                continue;
            }
            boolean hasPublicDisk = pool.getShardingMapping().hasPublicDisk(left.value, right.value);
            if (!hasPublicDisk) {
                continue;
            }
            Long leftObjNum = queryIdxObjects(bucketName, left.value).block(Duration.ofSeconds(30));
            Long rightObjNum = queryIdxObjects(bucketName, right.value).block(Duration.ofSeconds(30));
            if (leftObjNum == null || rightObjNum == null) {
                continue;
            }
            if (leftObjNum + rightObjNum > MAX_EXPANSION_THRESHOLD) {
                continue;
            }
            AbstractShardingTaskRunner runner;
            String srcv;
            if (leftObjNum < rightObjNum) {
                srcv = left.value;
                runner = new ShardingMergeTaskRunner(bucketName, left.value, right.value, AbstractCopyTaskRunner.Direction.RIGHT);
            } else {
                srcv = right.value;
                runner = new ShardingMergeTaskRunner(bucketName, right.value, left.value, AbstractCopyTaskRunner.Direction.LEFT);
            }
            // 挑选运行节点
            String runningNode = selectRunningNode(bucketName, srcv).var1();
            if (runningNode.equals(ServerConfig.getInstance().getHostUuid())) {
                addWorker(bucketName, runner);
            } else {
                runner.archive(runningNode);
            }
            return;
        }
    }

    private static void balance(String bucket, String vnode) {
        String runningNode = selectRunningNode(bucket, vnode).var1();
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
        ObjectSplitTree.Node node = metaStoragePool.getBucketShardCache().get(bucket).findNodeByValue(vnode);
        ObjectSplitTree.Node leftNeighbor = metaStoragePool.getBucketShardCache().get(bucket).leftNeighbor(node);
        // 向左邻节点平衡数据
        if (leftNeighbor != null && shardingIsHealth(bucket, leftNeighbor.value)) {
            if (metaStoragePool.getShardingMapping().hasPublicDisk(vnode, leftNeighbor.value)) {
                return;
            }
            Long objNum1 = queryIdxObjects(bucket, vnode).block(Duration.ofSeconds(60));
            Long objNum2 = queryIdxObjects(bucket, leftNeighbor.value).block(Duration.ofSeconds(60));
            if (objNum1 != null && objNum2 != null && (objNum1 - objNum2 >= objNum1 * LOAD_FACTOR)) {
                // 只在固定的时间内执行数据平衡任务
                long limit = Math.min(objNum1 - (objNum1 + objNum2) / 2, MAX_COPY_KEYS);
                log.info("bucket {} source vnode {} left balance to {} limit {} is start......runningNode:{}", bucket, vnode, leftNeighbor.value, limit, runningNode);
                // 构建桶分片平衡任务
                ShardingLoadBalanceTaskRunner shardingLoadBalanceTaskRunner = new ShardingLoadBalanceTaskRunner(bucket, node.value, leftNeighbor.value, limit, AbstractCopyTaskRunner.Direction.LEFT);
                if (runningNode.equals(ServerConfig.getInstance().getHostUuid())) {
                    addWorker(bucket, shardingLoadBalanceTaskRunner);
                } else {
                    shardingLoadBalanceTaskRunner.archive(runningNode);
                }
                return;
            } else {
                log.debug("Shard {} in the bucket {} does not meet the conditions for left {} balancing. - objNum1:{}, objNum2:{}", vnode, bucket, leftNeighbor.value, objNum1, objNum2);
            }
        }
        // 向右邻节点平衡数据
        ObjectSplitTree.Node rightNeighbor = metaStoragePool.getBucketShardCache().get(bucket).rightNeighbor(node);
        if (rightNeighbor != null && shardingIsHealth(bucket, rightNeighbor.value)) {
            if (metaStoragePool.getShardingMapping().hasPublicDisk(vnode, rightNeighbor.value)) {
                return;
            }
            Long objNum1 = queryIdxObjects(bucket, vnode).block(Duration.ofSeconds(60));
            Long objNum2 = queryIdxObjects(bucket, rightNeighbor.value).block(Duration.ofSeconds(60));
            if (objNum1 != null && objNum2 != null && (objNum1 - objNum2 >= objNum1 * LOAD_FACTOR)) {
                long limit = Math.min(objNum1 - (objNum1 + objNum2) / 2, MAX_COPY_KEYS);
                log.info("bucket {} source vnode {} right balance to {} limit {} is start......runningNode:{}", bucket, vnode, rightNeighbor.value, limit, runningNode);
                // 构建桶分片平衡任务
                ShardingLoadBalanceTaskRunner shardingLoadBalanceTaskRunner = new ShardingLoadBalanceTaskRunner(bucket, node.value, rightNeighbor.value, limit, AbstractCopyTaskRunner.Direction.RIGHT);
                if (runningNode.equals(ServerConfig.getInstance().getHostUuid())) {
                    addWorker(bucket, shardingLoadBalanceTaskRunner);
                } else {
                    shardingLoadBalanceTaskRunner.archive(runningNode);
                }
            } else {
                log.debug("Shard {} in the bucket {} does not meet the conditions for right {} balancing. - objNum1:{}, objNum2:{}", vnode, bucket, rightNeighbor.value, objNum1, objNum2);
            }
        }
    }

    public static Tuple2<String, String> selectRunningNode(String bucketName, String bucketVnode) {
        Tuple2<String, String> res = null;
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        VnodeCache cache = pool.getCache();
        String[] link = Arrays.stream(pool.getLink(pool.getVnodePrefix() + bucketVnode))
                .filter(vnode -> RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).exists(pool.getVnodePrefix() + vnode) != 0)
                .toArray(String[]::new);
        int executeIndex = Math.abs((bucketName + File.separator + bucketVnode).hashCode() % link.length);
        String executeVnode;
        boolean serverState;
        int tryCount = 0;
        do {
            String newVnode = pool.getVnodePrefix() + link[executeIndex];
            String newNode = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hget(newVnode, VNODE_S_UUID);
            serverState = nodeMsCloudIsUp(newNode);
            executeVnode = newVnode;
            executeIndex = executeIndex + 1 < link.length ? executeIndex + 1 : 0;
        } while (++tryCount < link.length && !serverState);

        if (tryCount <= link.length) {
            String runningNode = cache.hget(executeVnode, "s_uuid");
            String runningLun = cache.hgetVnodeInfo(executeVnode, "lun_name").block();
            res = new Tuple2<>(runningNode, runningLun);
        }
        log.debug("the bucket {} bucketVnode {} runningNode:{}", bucketName, bucketVnode, res);
        return res;
    }

    /**
     * 查询桶的单个分片上存储的对象数
     * @param bucketName 桶名
     * @param idxVnode 分片所映射的vnode
     * @return 分片上的对象数
     */
    private static Mono<Long> queryIdxObjects(String bucketName, String idxVnode) {
        return ErasureClient.getBucketInfo(bucketName, idxVnode)
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket storage info error!");
                    }
                })
                .filter(BucketInfo::isAvailable)
                .map(bucketInfo -> {
                    String objectNum = bucketInfo.getObjectNum();
                    if (objectNum == null) {
                        return 0L;
                    }
                    return Long.parseLong(objectNum);
                })
                .defaultIfEmpty(0L)
                .doOnError(e -> log.error("get bucket {} storage info error! {}", bucketName, e));
    }

    public static boolean nodeMsCloudIsUp(String uuid) {
        String ip = NodeCache.getIP(uuid);
        String serverState = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(uuid, NODE_SERVER_STATE);
        if ("1".equals(serverState)) {
            return true;
        }
        return Boolean.TRUE.equals(RSocketClient.getRSocket(ip, BACK_END_PORT)
                .flatMap(socket -> socket.requestResponse(DefaultPayload.create("", ErasureServer.PayloadMetaType.PING.name())))
                .flatMap(payload -> Mono.just(payload.getMetadataUtf8().equals(SUCCESS.name())))
                .onErrorReturn(false)
                .block(Duration.ofSeconds(60)));
    }

    /**
     *  分片扩容阈值，当分片中的对象数达到此数目是，分片进行分裂
     */
    public static double MAX_EXPANSION_THRESHOLD = 1.5625E8;

    /**
     * 元数据每次最大迁移数目，默认最大为5kw
     */
    public static long MAX_COPY_KEYS = 50_000_000L;

    /**
     * 单个桶允许最大分片数据
     */
    public static int MAX_SHARD_AMOUNT = 64;

    /**
     * 分片平衡因子
     */
    public static double LOAD_FACTOR = 0.25;

    public static void setMaxExpansionThreshold(double threshold) {
        MAX_EXPANSION_THRESHOLD = threshold;
    }

    public static void setMaxCopyKeys(long copyKeys) {
        MAX_COPY_KEYS = copyKeys;
    }

    public static void setMaxShardAmount(int amount) {
        MAX_SHARD_AMOUNT = amount;
    }

    public static void setLoadFactor(double factor) {
        LOAD_FACTOR = factor;
    }
}
