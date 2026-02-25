package com.macrosan.inventory;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.xmlmsg.inventory.InventoryConfiguration;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.VnodeCache;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.macrosan.action.managestream.BucketInventoryService.SUFFIX;
import static com.macrosan.action.managestream.BucketInventoryService.SUFFIX_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.isSwitchOn;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@Log4j2
public class InventoryService {

    private static final RedisConnPool REDIS_CONN_POOL = RedisConnPool.getInstance();

    /**
     * 当前节点的uuid
     */
    public static final String LOCAL_VM_UUID = ServerConfig.getInstance().getHostUuid();

    /**
     * 每天零点开始调度Inventory任务
     */
    private static final String DAILY_CYCLE = "0 0 0 1/1 * ? ";

    /**
     * 每周一零点开始调度Inventory任务
     */
    private static final String WEEKLY_CYCLE = "0 0 0 ? * 2 *";

    /**
     * 为执行Inventory任务提供线程
     */
    public static final Scheduler INVENTORY_SCHEDULER;

    /**
     * 按天频率执行的临时任务队列 mapKey: bucketName/InventoryId value:InventoryTask实例
     */
    private static final Map<String, InventoryTask> DAILY_INVENTORY_TASK = new ConcurrentHashMap<>();

    /**
     * 按每周频率执行的临时任务队列 mapKey: bucketName/InventoryId value:InventoryTask实例
     */
    private static final Map<String, InventoryTask> WEEKLY_INVENTORY_TASK = new ConcurrentHashMap<>();

    /**
     * 正在运行的Inventory任务 mapKey: bucketName/InventoryId value:InventoryTask实例
     */
    private static final Map<String, InventoryTask> INVENTORY_RUNNING_TASK = new ConcurrentHashMap<>();

    /**
     * 用于执行任务, 当前支持10个任务同时运行
     */
    private static final int PROC_NUM = 10;
    private static final UnicastProcessor<Tuple2<String, InventoryTask>>[] INVENTORY_PROCESSORS = new UnicastProcessor[PROC_NUM];
    private static final String JOIN = File.separator;

    /**
     * 用于检测任务的异常中断与恢复任务的执行
     */
    private static final ScheduledThreadPoolExecutor SENTINEL = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "inventory-sentinel"));

    /**
     * 允许同时运行的任务数量，超过此数量，则交给SENTINEL调度执行
     */
    private static final int MAX_RUNNING_TASK = 5;

    static {
        Scheduler scheduler = null;
        try {
            ThreadFactory inventoryTaskSchedule = new MsThreadFactory("inventory");
            MsExecutor executor = new MsExecutor(2 * PROC_NUM, 1, inventoryTaskSchedule);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        INVENTORY_SCHEDULER = scheduler;
    }

    /**
     * 启动Inventory API服务
     */
    public static void start() {
        initInventoryProcessors();
        scheduleInventoryTask("Daily");
        scheduleInventoryTask("Weekly");
        SENTINEL.schedule(InventoryService::startSentinel, 300, TimeUnit.SECONDS);
        log.info("Inventory server is started.");
    }

    /**
     * 初始化Inventory任务处理器
     */
    private static void initInventoryProcessors() {
        for (int i = 0; i < INVENTORY_PROCESSORS.length; i++) {
            INVENTORY_PROCESSORS[i] = UnicastProcessor.create(Queues.<Tuple2<String, com.macrosan.inventory.InventoryTask>>unboundedMultiproducer().get());
            INVENTORY_PROCESSORS[i].publishOn(INVENTORY_SCHEDULER)
                    .doOnNext(tuple2 -> {
                        String bucketName = tuple2.var1.substring(0, tuple2.var1.indexOf(JOIN));
                        String inventoryId = tuple2.var1.substring(tuple2.var1.indexOf(JOIN) + 1);
                        boolean hasArchive = hasArchive(bucketName, inventoryId);
                        if (!hasArchive) {
                            // 非异常任务执行，初始化存档
                            JSONObject archive = new JSONObject();
                            archive.put("uuid", LOCAL_VM_UUID);
                            archive.put("disk", getLocalDiskByBucket(bucketName));
                            archive.put("type", "0");
                            REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX)
                                    .hset(bucketName + "_archive", inventoryId, archive.toString());
                        }
                        // 执行任务
                        if (INVENTORY_RUNNING_TASK.size() < MAX_RUNNING_TASK) {
                            // 当到了调度的时间点，若当前InventoryId有存档但是当前不在运行，不开辟新的任务
                            if (hasArchive && ("Daily".equals(tuple2.var2.type) || "Weekly".equals(tuple2.var2.type))) {
                                return;
                            }
                            if (DAILY_INVENTORY_TASK.containsKey(tuple2.var1) && !INVENTORY_RUNNING_TASK.containsKey(tuple2.var1)) {
                                InventoryTask remove = DAILY_INVENTORY_TASK.remove(tuple2.var1);
                                INVENTORY_RUNNING_TASK.put(tuple2.var1, remove);
                                remove.run();
                            } else if (WEEKLY_INVENTORY_TASK.containsKey(tuple2.var1) && !INVENTORY_RUNNING_TASK.containsKey(tuple2.var1)) {
                                InventoryTask remove = WEEKLY_INVENTORY_TASK.remove(tuple2.var1);
                                INVENTORY_RUNNING_TASK.put(tuple2.var1, remove);
                                remove.run();
                            } else {
                                // 若之前有Inventory任务执行，则等待下个周期执行
                                DAILY_INVENTORY_TASK.remove(tuple2.var1);
                                WEEKLY_INVENTORY_TASK.remove(tuple2.var1);
                            }
                        } else {
                            // 当前最大可执行任务数已满，则将已初始化的task移除
                            DAILY_INVENTORY_TASK.remove(tuple2.var1);
                            WEEKLY_INVENTORY_TASK.remove(tuple2.var1);
                        }
                    }).subscribe();
        }
    }

    public static class InventoryDailyJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Inventory daily server is started.");
            initInventoryTask("Daily");
            DAILY_INVENTORY_TASK.forEach((key, value) -> onNextInventoryTask(new Tuple2<>(key, value)));
        }
    }

    public static class InventoryWeeklyJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Inventory weekly server is started.");
            initInventoryTask("Weekly");
            WEEKLY_INVENTORY_TASK.forEach((key, value) -> onNextInventoryTask(new Tuple2<>(key, value)));
        }
    }

    private static void scheduleInventoryTask(String periodic) {
        try {
            String inventoryJobGroup = String.format("Inventory%sJobGroup", periodic);
            String inventoryTriggerGroup = String.format("inventory%sTriggerGroup", periodic);
            String inventoryJob = String.format("inventory%sJob", periodic);
            String inventoryTrigger = String.format("inventory%sTrigger", periodic);
            JobDetail jobDetail = JobBuilder
                    .newJob("Daily".equals(periodic) ? InventoryDailyJob.class : InventoryWeeklyJob.class)
                    .withIdentity(inventoryJob, inventoryJobGroup)
                    .build();

            Trigger trigger = newTrigger()
                    .withIdentity(inventoryTrigger, inventoryTriggerGroup)
                    .withSchedule(cronSchedule("Daily".equals(periodic) ? DAILY_CYCLE : WEEKLY_CYCLE).withMisfireHandlingInstructionDoNothing())
                    .forJob(inventoryJob, inventoryJobGroup)
                    .startNow()
                    .build();
            org.quartz.Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);

        } catch (SchedulerException e) {
            log.error(String.format("schedule%sInventoryTask error!", periodic), e);
        }
    }

    public static void onNextInventoryTask(Tuple2<String, InventoryTask> taskTuple2) {
        int index = Math.abs(taskTuple2.var1.hashCode() % PROC_NUM);
        String bucketName = taskTuple2.var1.substring(0, taskTuple2.var1.indexOf(JOIN));
        String inventoryId = taskTuple2.var1.substring(taskTuple2.var1.indexOf(JOIN) + 1);
        INVENTORY_PROCESSORS[index].onNext(taskTuple2);
        taskTuple2.var2.res()
                .subscribe(b -> {
                    if (b) {
                        resetTask(bucketName, inventoryId);
                        taskTuple2.var2.deleteArchive();
                        log.info("The bucket {} inventoryId {} export data successfully, will remove it from task queue!", bucketName, inventoryId);
                    } else {
                        resetTask(bucketName, inventoryId);
                        log.error("The bucket {} inventoryId {} is running error!", bucketName, inventoryId);
                    }
                }, e -> {
                    resetTask(bucketName, inventoryId);
                    log.error("The bucket {} inventoryId {} execute encounter error! {}", bucketName, inventoryId, e.getMessage());
                });
    }

    public static void resetTask(String bucketName, String inventoryId) {
        String key = bucketName + JOIN + inventoryId;
        DAILY_INVENTORY_TASK.remove(key);
        WEEKLY_INVENTORY_TASK.remove(key);
        INVENTORY_RUNNING_TASK.remove(key);
    }

    /**
     * 判断当前任务是否有存档
     *
     * @param bucketName  桶名
     * @param inventoryId 任务Id
     * @return true|false
     */
    public static boolean hasArchive(String bucketName, String inventoryId) {
        return REDIS_CONN_POOL.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hexists(bucketName + "_archive", inventoryId);
    }

    /**
     * 初始化Inventory任务，将任务添加到任务队列中
     *
     * @param frequency 调度频率
     */
    private static void initInventoryTask(String frequency) {
        ScanArgs scanArgs = new ScanArgs().match("*").limit(10);
        KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
        keyScanCursor.setCursor("0");
        KeyScanCursor<String> res;
        do {
            res = REDIS_CONN_POOL.getCommand(REDIS_BUCKETINFO_INDEX).scan(keyScanCursor, scanArgs);
            List<String> bucketSet = res.getKeys();
            for (String bucket : bucketSet) {
                String type = REDIS_CONN_POOL.getCommand(REDIS_BUCKETINFO_INDEX).type(bucket);
                if (!"hash".equals(type) || !BUCKET_NAME_PATTERN.matcher(bucket).matches()) {
                    continue;
                }

                Long exists = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).exists(bucket + SUFFIX_ID);
                if (exists == 0) {
                    continue;
                }

                String localRegion = ServerConfig.getInstance().getRegion();
                String bucketRegion = REDIS_CONN_POOL.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, REGION_NAME);
                if (StringUtils.isNotEmpty(bucketRegion) && !localRegion.equals(bucketRegion)) {
                    log.info("The bucket {} region {} inventory tasks can not running on the current region {}.", bucket, bucketRegion, localRegion);
                    continue;
                }

                String localSite = ServerConfig.getInstance().getSite();
                Map<String, String> bucketMap = REDIS_CONN_POOL.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
                String bucketSite = bucketMap.get(CLUSTER_NAME);

                // 源桶未开启数据同步，只在源桶的所属站点执行
                if (!isSwitchOn(bucketMap) && StringUtils.isNotEmpty(bucketSite) && !localSite.equals(bucketSite)) {
                    log.info("The bucket {} site {} inventory task can not running on the current site {}", bucket, bucketSite, localSite);
                    continue;
                }

                List<String> inventoryList = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).zrange(bucket + SUFFIX_ID, 0, -1);
                for (String inventory : inventoryList) {
                    buildTaskQueueByFrequency(bucket, inventory, frequency);
                }
            }
            keyScanCursor.setCursor(res.getCursor());
        } while (!res.isFinished());
    }

    /**
     * 为桶与其inventory构建一个任务放置任务队列中
     *
     * @param bucketName  桶名
     * @param inventoryId id
     */
    public static void buildTaskQueueByFrequency(String bucketName, String inventoryId, String freq) {
        try {
            String config = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).hget(bucketName + SUFFIX, inventoryId);
            InventoryConfiguration inventoryConfiguration = (InventoryConfiguration) JaxbUtils.toObject(config);
            if (inventoryConfiguration == null || !"true".equalsIgnoreCase(inventoryConfiguration.getIsEnabled())) {
                return;
            }
            String frequency = inventoryConfiguration.getSchedule().getFrequency();
            if (!frequency.equals(freq)) {
                return;
            }
            Tuple2<String, String> runningNode = selectRunningNode(bucketName, inventoryId);
            if (runningNode != null && LOCAL_VM_UUID.equals(runningNode.var1)) {
                if ("Weekly".equals(frequency)) {
                    InventoryTask inventoryTask = InventoryTaskFactory.buildTask(bucketName, runningNode.var2, inventoryConfiguration);
                    inventoryTask.type = frequency;
                    WEEKLY_INVENTORY_TASK.put(bucketName + JOIN + inventoryId, inventoryTask);
                } else if ("Daily".equals(frequency)) {
                    InventoryTask inventoryTask = InventoryTaskFactory.buildTask(bucketName, runningNode.var2, inventoryConfiguration);
                    inventoryTask.type = frequency;
                    DAILY_INVENTORY_TASK.put(bucketName + JOIN + inventoryId, inventoryTask);
                }
                log.info("The bucket {} inventoryId {} will be execute at the current node {}!", bucketName, inventoryId, frequency);
            }
        } catch (NullPointerException e) {
            log.error("start bucket {} inventory {} task fail, because inventoryConfiguration doesn't exist!", bucketName, inventoryId);
        } catch (Exception e) {
            log.error("start bucket {} inventory {} task fail {}", bucketName, inventoryId, e.getMessage());
        }
    }

    /**
     * 根据桶名与InventoryId散列计算，清单任务运行节点
     *
     * @param bucketName 桶名
     * @return (节点uuid, lun)
     */
    private static Tuple2<String, String> selectRunningNode(String bucketName, String inventoryId) {
        Tuple2<String, String> res = null;
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        VnodeCache cache = pool.getCache();
        String[] link = pool.getLink(pool.getVnodePrefix() + pool.getBucketVnodeId(bucketName));
        int executeIndex = Math.abs((bucketName + File.separator + inventoryId).hashCode() % link.length);
        String executeVnode;
        String serverState;
        int tryCount = 0;
        boolean diskIsNormal;
        do {
            String newVnode = pool.getVnodePrefix() + link[executeIndex];
            String newNode = REDIS_CONN_POOL.getCommand(REDIS_MAPINFO_INDEX).hget(newVnode, VNODE_S_UUID);
            serverState = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(newNode, NODE_SERVER_STATE);
            executeVnode = newVnode;
            executeIndex = executeIndex + 1 < link.length ? executeIndex + 1 : 0;
            String runningLun = cache.hgetVnodeInfo(executeVnode, "lun_name").block();
            String runningNode = cache.hget(executeVnode, "s_uuid");
            diskIsNormal = diskIsNormal(runningNode, runningLun);
        } while (++tryCount < link.length && !("1".equals(serverState) && diskIsNormal));

        if (tryCount <= link.length) {
            String runningNode = cache.hget(executeVnode, "s_uuid");
            String runningLun = cache.hgetVnodeInfo(executeVnode, "lun_name").block();
            res = new Tuple2<>(runningNode, runningLun);
        }
        log.info("the bucket {} inventory {} runningNode:{}", bucketName, inventoryId, res);
        return res;
    }

    /**
     * 启用异常监控,每5min检测一次
     * 暂不考虑前端包进程被kill的情况
     * 异常: 执行节点关机(切换其他节点执行)，拔掉桶所在的索引盘、加盘(拔盘/加盘节点不执行，切换至其他节点执行)，加节点(迁移过程中，任务不执行)
     */
    public static void startSentinel() {
        try {
            ScanArgs scanArgs = new ScanArgs().match("*_archive").limit(10);
            KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
            keyScanCursor.setCursor("0");
            KeyScanCursor<String> res;
            do {
                res = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).scan(keyScanCursor, scanArgs);
                List<String> archives = res.getKeys();
                for (String archive : archives) {
                    String bucket = archive.substring(0, archive.lastIndexOf("_"));
                    List<String> hvals = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).hkeys(archive);
                    hvals.forEach((key) -> {
                        try {
                            // 尝试获取锁,同一时刻只允许一个节点进行处理
                            if (!tryLock(bucket, key)) {
                                return;
                            }
                            String value = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).hget(archive, key);
                            JSONObject arc = JSONObject.parseObject(value);
                            String uuid = arc.getString("uuid");
                            String disk = arc.getString("disk");
                            String mapKey = bucket + JOIN + key;
                            boolean waitTask = arc.containsKey("type") && "0".equals(arc.getString("type"));
                            // 已有该任务实例在当前节点运行或者达到最大运行数量，不做处理
                            if (INVENTORY_RUNNING_TASK.containsKey(mapKey) || INVENTORY_RUNNING_TASK.size() >= MAX_RUNNING_TASK) {
                                return;
                            }

                            // 该任务实例上次执行位置在当前节点，但是当前节点无该任务的实例运行，可能已运行在其他节点
                            if (LOCAL_VM_UUID.equals(uuid) && !INVENTORY_RUNNING_TASK.containsKey(mapKey)) {
                                if (StringUtils.isEmpty(disk) || !diskIsNormal(uuid, disk)) {
                                    return;
                                }
                                recoverInventoryTask(bucket, key, disk, !waitTask);
                                return;
                            }

                            // 若任务的旧的执行节点服务正常则不处理
                            String serverState = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, NODE_SERVER_STATE);
                            if ("1".equals(serverState) && diskIsNormal(uuid, disk)) {
                                return;
                            }

                            // 为中断的任务挑选新的执行节点
                            Tuple2<String, String> runningNode = selectRunningNode(bucket, key);
                            if (runningNode == null || runningNode.var2 == null || !LOCAL_VM_UUID.equals(runningNode.var1)) {
                                return;
                            }

                            // 更新uuid以及disk
                            arc.put("uuid", runningNode.var1);
                            arc.put("disk", runningNode.var2);
                            REDIS_CONN_POOL.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).hset(bucket + "_archive", key, arc.toString());
                            recoverInventoryTask(bucket, key, runningNode.var2, true);
                        } finally {
                            unLock(bucket, key);
                        }
                    });
                }
                keyScanCursor.setCursor(res.getCursor());
            } while (!res.isFinished());
        } finally {
            SENTINEL.schedule(InventoryService::startSentinel, 300, TimeUnit.SECONDS);
        }
    }

    /**
     * 恢复Inventory任务，继续执行
     *
     * @param bucket       桶名
     * @param key          InventoryId
     * @param lun          磁盘名
     * @param needRollBack 是否需要回滚
     */
    private static void recoverInventoryTask(String bucket, String key, String lun, boolean needRollBack) {
        String config = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX).hget(bucket + SUFFIX, key);
        InventoryConfiguration inventoryConfiguration = (InventoryConfiguration) JaxbUtils.toObject(config);
        InventoryTask task = InventoryTaskFactory.buildTask(bucket, lun, inventoryConfiguration);
        InventoryTask inventoryTask = needRollBack ? task.rollback() : task;
        inventoryTask.type = "ERROR";
        if ("Daily".equals(inventoryConfiguration.getSchedule().getFrequency())) {
            DAILY_INVENTORY_TASK.put(bucket + JOIN + key, inventoryTask);
        } else if ("Weekly".equals(inventoryConfiguration.getSchedule().getFrequency())) {
            WEEKLY_INVENTORY_TASK.put(bucket + JOIN + key, inventoryTask);
        }
        log.info("The bucket {} inventoryId {} was Interrupted. It will continue run on the current node!", bucket, key);
        onNextInventoryTask(new Tuple2<>(bucket + JOIN + key, inventoryTask));
    }


    private static boolean tryLock(String bucket, String inventoryId) {
        SetArgs setArgs = new SetArgs().nx().ex(30);
        String key = bucket + JOIN + inventoryId + "_lock";
        String res = REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX).set(key, "1", setArgs);
        return "OK".equals(res);
    }

    private static void unLock(String bucket, String inventoryId) {
        String key = bucket + JOIN + inventoryId + "_lock";
        REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(key);
    }

    /**
     * 获取桶在本地的磁盘信息
     *
     * @param bucketName 桶名
     * @return 磁盘名
     */
    public static String getLocalDiskByBucket(String bucketName) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnodeId = pool.getBucketVnodeId(bucketName);
        VnodeCache cache = pool.getCache();
        String[] link = pool.getLink(pool.getVnodePrefix() + bucketVnodeId);
        String res = null;
        for (String vnode : link) {
            String runningNode = cache.hget(pool.getVnodePrefix() + vnode, "s_uuid");
            if (LOCAL_VM_UUID.equals(runningNode)) {
                return cache.hgetVnodeInfo(pool.getVnodePrefix() + vnode, "lun_name").block();
            }
        }
        return res;
    }

    /**
     * 磁盘状态是否正常
     *
     * @param disk 磁盘名
     * @return
     */
    private static boolean diskIsNormal(String uuid, String disk) {
        return true;
//        String key = uuid + "@" + disk;
//        // 磁盘被拔除
//        Long exists = REDIS_CONN_POOL.getCommand(REDIS_LUNINFO_INDEX).exists(key);
//        if (exists == 0) {
//            return false;
//        }
//
//        // 磁盘在重构迁移
//        Long running = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).exists("running");
//        if (running != 0) {
//            Map<String, String> runningMap = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).hgetall("running");
//            String node = runningMap.get("node");
//            String diskName = runningMap.get("diskName");
//            String lun = node + "@" + diskName;
//            String state = runningMap.get("state");
//            if (lun.equals(key) && "running".equals(state)) {
//                return false;
//            }
//        }
//
//        String state = REDIS_CONN_POOL.getCommand(REDIS_LUNINFO_INDEX).hget(key, "state");
//        // state为0表示正常，2表示已被拔除
//        return "0".equals(state);
    }

    /**
     * 索引池迁移过程中Inventory任务暂不执行,迁移结束后执行
     *
     * @return
     */
    private static boolean indexMigrateIsRunning() {
        Long running = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).exists("running");
        if (running != 0) {
            String state = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).hget("running", "state");
            String operate = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).hget("running", "operate");
            String poolName = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).hget("running", "poolName");
            if ("running".equals(state) && ("add_disk".equals(operate) || "add_node".equals(operate)) && poolName.contains("index")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 删除桶相关的inventory信息
     *
     * @param bucketName 桶名
     */
    public static void deleteBucketInventoryConfiguration(String bucketName) {
        REDIS_CONN_POOL.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).del(bucketName + SUFFIX);
        REDIS_CONN_POOL.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).del(bucketName + SUFFIX_ID);
        REDIS_CONN_POOL.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).del(bucketName + "_archive");
        REDIS_CONN_POOL.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).del(bucketName + "_incrementalStamp");
    }
}