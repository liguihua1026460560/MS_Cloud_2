package com.macrosan.ec.rebuild;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.error.StatisticErrorHandler;
import com.macrosan.ec.server.ListServerHandler;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ListMetaVnode;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.functional.exception.Sleep;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.ErrorFunctionCache.NODE_LIST_TYPE_REFERENCE;
import static com.macrosan.ec.rebuild.ReBuildTask.Type.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.HEART_IP_LIST;

@Slf4j
public class RebuildCache {
    public static final Map<String, UnicastProcessor<RunningTask>> cacheQueues = new ConcurrentHashMap<>();
    private static final Map<String, RebuildCache> diskMap = new ConcurrentHashMap<>();
    private static final int MAX_CACHE_NUMS = 10_000;
    private static final MsExecutor executor = new MsExecutor(10, 2, new MsThreadFactory("recoverCache-"));
    public static final Scheduler REBUILD_CACHE_SCHEDULER = Schedulers.fromExecutor(executor);
    private static final AtomicInteger id = new AtomicInteger(0);
    private static final Integer request = 2;
    public static UnicastProcessor<Integer> qosProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
    private static Integer newRequest = 2;
    Map<String, CopyObjectFileRebuilder> copyObjectFileRebuilderMap = new ConcurrentHashMap<>();

    static {
        try {
            qosProcessor.subscribe(qos -> {
                String request = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).get("request");
                if (null != request) {
                    newRequest = Integer.valueOf(request);
                } else {
                    newRequest = qos;
                }
                log.debug("qos:{}", newRequest);
            });
            RebuildRateLimiter.getInstance();

        } catch (Exception e) {
            newRequest = 2;
        }
    }
    @AllArgsConstructor
    public static class RunningTask {
        MonoProcessor<Boolean> res;
        ReBuildTask task;
        boolean reQueue;
        AtomicBoolean result;
        long retryTime;
    }

    public static RebuildCache getCache(String key) {
        return diskMap.compute(key, (k1, v1) -> {
            if (v1 == null) {
                v1 = new RebuildCache(key);
            }
            return v1;
        });
    }

    public RebuildCache(String key) {
        UnicastProcessor<RunningTask> processor = cacheQueues.compute(key, (k1, v1) -> {
            if (v1 == null) {
                return UnicastProcessor.create(Queues.<RunningTask>unboundedMultiproducer().get());
            }
            return v1;
        });
        processor.publishOn(REBUILD_CACHE_SCHEDULER).subscribe(subscriber);
    }

    private UnicastProcessor<RunningTask> errorQueues(String key) {
        if (!cacheQueues.containsKey(key)) {
            UnicastProcessor<RunningTask> errorProcessor = UnicastProcessor.create(Queues.<RunningTask>unboundedMultiproducer().get());
            errorProcessor.publishOn(REBUILD_CACHE_SCHEDULER).subscribe(runningTask -> {
                try {
                    dealMsg(runningTask).subscribe(b -> {
                        if (!b || runningTask.reQueue) {
                            Sleep.sleep(1000);
                            if (runningTask.reQueue) {
                                runningTask.reQueue = false;
                            }
                            errorProcessor.onNext(runningTask);
                        }
                    }, e -> {
                        if (e instanceof MsException && e.getMessage() != null
                                && (e.getMessage().contains("version time is not init"))) {
                            String prefix = "error" + ListServerHandler.getVnode(runningTask.task.getMap().get("fileKey")).var1;
                            Mono.delay(Duration.ofSeconds(30)).doOnNext(l -> {
                                errorQueues("error" + runningTask.task.disk).onNext(runningTask);
                            }).subscribe();//30秒后重试
                            log.error("deal error1", e);
                        }
                    });
                } catch (Exception e) {
                    log.error("deal error 3", e);
                }
            });
            cacheQueues.put(key, errorProcessor);
        }
        return cacheQueues.get(key);
    }

    private Subscription[] subscriptions = new Subscription[1];

    private Subscriber<RunningTask> subscriber = new BaseSubscriber<RunningTask>() {
        final AtomicInteger count = new AtomicInteger();

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(request);
            count.set(request);
            subscriptions[0] = subscription;
        }

        private void doFinally(RunningTask runningTask) {
            if (!runningTask.reQueue && runningTask.task.copyObjectFileRebuilder != null) {
                runningTask.task.copyObjectFileRebuilder.cancelTrackCopySourceFile(runningTask.task.map.get("fileName"));
            }
            if (!runningTask.res.isTerminated()) {
                runningTask.res.onNext(true);
            }
            String vKey = runningTask.task.map.get("vKey");
            ListMetaVnode.vKeyMap.computeIfPresent(vKey, (k, v) -> {
                if (!runningTask.reQueue && runningTask.result.get()) {
                    if (v.decrementAndGet() == 0) {
                        runningTask.task.res.onNext(1);
                    }
                } else {
                    if (runningTask.reQueue) {
                        runningTask.reQueue = false;
                        log.debug("rebuild task requeue {} about {}!", runningTask.task.map.get("metaKey"), runningTask.task.map.get("fileName"));
                        cacheQueues.get(runningTask.task.disk).onNext(runningTask);//在当前任务处理流结束后才重试或者返回队列
                    } else if (!runningTask.result.get()) {
                        //这里考虑将消息发布到其他队列中进行消费
                        //然后这里直接消费完成
                        //消息发布到rebuildDeadLetter中
                        ReBuildTask reBuildTask = new ReBuildTask(runningTask.task.type, runningTask.task.disk, runningTask.task.vnode, runningTask.task.diskLink, runningTask.task.pool, null, runningTask.task.map);//避免json解析失败
                        RebuildDeadLetter.getInstance().publishRebuildTask(reBuildTask);
                        log.info("publish to dead letter queue {} about {}!", runningTask.task.map.get("metaKey"), runningTask.task.map.get("fileName"));
                        if (v.decrementAndGet() == 0) {
                            runningTask.task.res.onNext(1);
                        }
                        try {
                            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(runningTask.task.pool);
                            String runningKey = "running_" + poolQueueTag;
                            RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1L);
                        } catch (Exception e) {
                            log.error("do rebuild Finally error. {} about {}", runningTask.task.map.get("metaKey"), runningTask.task.map.get("fileName"), e);
                        }

                    }

                }
                return v;
            });

            int cur;
            while ((cur = count.get()) < request) {
                if (count.compareAndSet(cur, cur + 1)) {
                    subscriptions[0].request(newRequest - 1);
                }
            }
        }

        @Override
        protected void hookOnNext(RunningTask runningTask) {
            try {
                count.decrementAndGet();
                if (runningTask.task.type == OBJECT_FILE && runningTask.task.map.get("fileName").contains("#")) {
                    processCopyTask(runningTask);
                } else {
                    processNormalTask(runningTask);
                }
            } catch (Exception e) {
                doFinally(runningTask);
                if (e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                    return;
                }
                String prefix = "error" + ListServerHandler.getVnode(runningTask.task.getMap().get("fileKey")).var1;
                errorQueues("error" + runningTask.task.disk).onNext(runningTask);
                log.error("deal error2", e);
            }
        }

        private void processCopyTask(RunningTask runningTask) throws IOException {
            String currentFileName = runningTask.task.map.get("fileName");
            String copySourceFileName = currentFileName.split("#")[0];
            if (runningTask.task.copyObjectFileRebuilder.isRebuildingInProgress(copySourceFileName)) {
                // copy源对象正在执行重构，则等待copy源对象处理完毕
                log.debug("Waiting for copy source file to be processed: {}", currentFileName);
                Mono.delay(Duration.ofMillis(100))
                        .subscribe(b -> cacheQueues.get(runningTask.task.disk).onNext(runningTask));
            } else {
                // copy源文件已经处理完毕，则继续处理当前文件
                processNormalTask(runningTask);
            }
        }

        private void processNormalTask(RunningTask runningTask) throws IOException {
            dealMsg(runningTask)
                    .doFinally(s -> {
                        doFinally(runningTask);
                    })
                    .doOnNext(b -> {
                        if (!b) {
                            String prefix = "error" + ListServerHandler.getVnode(runningTask.task.getMap().get("fileKey")).var1;
                            errorQueues("error" + runningTask.task.disk).onNext(runningTask);
                        }
                    })
                    .subscribe(s -> {
                        if (runningTask.result.get()) {
                            id.decrementAndGet();
                        }
                    }, e -> {
                        if (e != null && e.getMessage() != null) {
                            if (e.getMessage().contains("no such bucket name")) {
                                return;
                            }
                            if (e instanceof MsException && e.getMessage() != null
                                    && (e.getMessage().contains("version time is not init"))) {
                                String prefix = "error" + ListServerHandler.getVnode(runningTask.task.getMap().get("fileKey")).var1;
                                Mono.delay(Duration.ofSeconds(30)).doOnNext(l -> {
                                    errorQueues("error" + runningTask.task.disk).onNext(runningTask);
                                }).subscribe();//30秒后重试
                                log.error("deal error1", e);
                                return;
                            }
                        }
                        String prefix = "error" + ListServerHandler.getVnode(runningTask.task.getMap().get("fileKey")).var1;
                        errorQueues("error" + runningTask.task.disk).onNext(runningTask);
                        log.error("deal error1", e);
                    });
        }

    };

    private Mono<Boolean> dealMsg(RunningTask runningTask) throws IOException {
        ReBuildTask task = runningTask.task;
        if (RemovedDisk.getInstance().contains(task.disk)) {
            runningTask.reQueue = false;
            runningTask.result.compareAndSet(false, true);
            return Mono.just(true);
        }

        if (task.type.equals(OBJECT_FILE)) {
            //如果目标数据磁盘离线超6分钟则不再进行重构直接ack
            if (DiskStatusChecker.isRebuildWaiter(task.disk.split("@")[1])) {
                runningTask.reQueue = false;
                runningTask.result.compareAndSet(false, true);
                return Mono.just(true);
            }
        }
        String[] linkKeys = new String[runningTask.task.getDiskLink().length];
        try {
            if (!acquireRateLimitPermit(runningTask, linkKeys)) {
                // 被限流，重新返回 processor 队列
                runningTask.reQueue = true;
                runningTask.result.compareAndSet(true, false);
                return Mono.just(true)
                        .publishOn(REBUILD_CACHE_SCHEDULER)
                        .delayElement(Duration.ofMillis(ThreadLocalRandom.current().nextInt(10, 50)));
            }
        } catch (Exception e) {
            log.error("rebuild {} fail, fileName:{}", runningTask.task.map.get("metaKey"), runningTask.task.map.get("fileName"), e);
            runningTask.result.compareAndSet(false, true);//异常导致的false不使用普通QUEUE处理
            return Mono.just(false);
        }

        List<Tuple3<String, String, String>> nodeList;
        if (task.map.containsKey("nodeList")) {
            nodeList = Json.decodeValue(task.map.get("nodeList"),
                    NODE_LIST_TYPE_REFERENCE);
        } else {
            nodeList = new ArrayList<>();
        }

        StoragePool pool = StoragePoolFactory.getStoragePool(task.pool, null);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(task.pool);
        String runningKey = "running_" + poolQueueTag;
        Mono<Boolean> taskRes;

        switch (task.type) {
            case OBJECT_FILE:
                String errorIndex = task.map.get("errorIndex");
                String fileName = task.map.get("fileName");
                String endIndex = task.map.get("endIndex");
                String fileSize = task.map.get("fileSize");
                String metaKey = task.map.get("metaKey");
                String vnodeNum = String.valueOf(task.vnode);
                String lun = task.map.get("lun");

                String crypto = task.map.get("crypto");
                String secretKey = task.map.get("secretKey");
                String flushStamp = task.map.get("flushStamp");
                taskRes = Mono.just(fileName.contains("#"))
                        .flatMap(isCopyObjFileMeta -> {
                            if (isCopyObjFileMeta) {
                                return runningTask.task.copyObjectFileRebuilder.rebuildCopyObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, flushStamp, 0);
                            } else {
                                return TaskHandler.rebuildObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, flushStamp);
                            }
                        })
                        .doOnNext(b -> {
                            if (b) {
                                RebuildSpeed.add(Long.parseLong(fileSize));
                            }
                        });
                break;
            case OBJECT_META:
                taskRes = TaskHandler.rebuildObjMeta(task.map.get("bucket"), task.map.get("object"), task.map.get("versionId"), nodeList, task.map.get("snapshotMark"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case INIT_PART_UPLOAD:
                taskRes = TaskHandler.rebuildInitPartUpload(task.map.get("bucket"), task.map.get("object"),
                                task.map.get("uploadId"), nodeList, task.map.get("snapshotMark"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case PART_UPLOAD:
                PartInfo partInfo = Json.decodeValue(task.map.get("value"), PartInfo.class);
                taskRes = TaskHandler.rebuildPartUpload(partInfo, nodeList)
                        .timeout(Duration.ofMinutes(1));
                break;
            case STATISTIC:
                taskRes = StatisticErrorHandler.putMinuteRecord(task.map.get("value"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case BUCKET_STORAGE:
                taskRes = Mono.just(true);
                break;
            case SYNC_RECORD:
                taskRes = TaskHandler.rebuildSyncRecord(task.map.get("value"));
                break;
            case DEDUP_INFO:
                taskRes = TaskHandler.rebuildDeduplicateInfo(task.map.get("realKey"), task.map.get("value"));
                break;
            case COMPONENT_RECORD:
                taskRes = TaskHandler.rebuildComponentRecord(task.map.get("value"));
                break;
            case NFS_INODE:
                taskRes = TaskHandler.rebuildInode(task.map.get("inodeKey"), task.map.get("inodeValue"));
                break;
            case NFS_CHUNK:
                taskRes = TaskHandler.rebuildChunkFile(task.map.get("chunkKey"), task.map.get("chunkValue"));
                break;
            case STS_TOKEN:
                taskRes = TaskHandler.rebuildSTSToken(task.map.get("value"));
                break;
            case AGGREGATE_META:
                taskRes = TaskHandler.rebuildAggregateMeta(task.map.get("key"));
                break;
            default:
                log.error("no such rebuild task type {}", task.type);
                taskRes = Mono.just(false);
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();

        taskRes.doFinally(b -> {
            try {
                RebuildCounter.getCounter(runningKey).decrementTaskNum(1L);

                // NFS inode数与chunk数暂不计入进度；存入表12便于测试时统计
                if (task.type.equals(NFS_INODE)) {
                    RebuildCounter.getCounter(runningKey).incrementInodeNum(1L);
                } else if (task.type.equals(NFS_CHUNK)) {
                    RebuildCounter.getCounter(runningKey).incrementChunkNum(1L);
                } else {
                    if (task.disk.contains("index") && StringUtils.isBlank(ListServerHandler.getVnode(task.map.get("fileKey")).var1)) {
                        RebuildCounter.getCounter(runningKey).decrementMigrateNum(4L);
                    } else {
                        if (!OBJECT_FILE.equals(task.type) || runningTask.result.get()) {
                            RebuildCounter.getCounter(runningKey).decrementMigrateNum(1L);
                        }
                    }
                }
                if (task.type.equals(ReBuildTask.Type.OBJECT_FILE)) {
                    for (String linkKey : linkKeys) {
                        RebuildRabbitMq.getMaster().del(linkKey);
                    }
                }
                res.onNext(true);
            } catch (Exception e) {
                log.error("", e);
                res.onNext(false);
            }
        }).subscribe(b -> {
            if (!b) {
                //数据块重构未成功时如果目标盘离线未触发移除，那么进行重试，磁盘触发移除后不再重试
                if (OBJECT_FILE.equals(task.type)) {
                    if (runningTask.retryTime == 0L) {//首次返回ERROR打印日志
                        log.info("rebuild {} fail", task);
                        log.info("Retry rebuild {} about {}!", runningTask.task.map.get("metaKey"), runningTask.task.map.get("fileName"));
                    }
                    runningTask.result.compareAndSet(true, false);
                } else {
                    log.info("rebuild {} fail", task);
                    runningTask.result.compareAndSet(false, true);
                }
            } else {
                runningTask.result.compareAndSet(false, true);
            }
        }, e -> {
            log.error("rebuild {} fail ", task, e);
            runningTask.result.compareAndSet(false, true);
            if (e instanceof MsException && e.getMessage() != null
                    && (e.getMessage().contains("version time is not init"))) {
                res.onError(e);
            }
            res.onNext(true);
        });

        return res;
    }

    public Mono<Boolean> cacheWrite(ReBuildTask task) {
        if (task.type == OBJECT_FILE) {
            task.copyObjectFileRebuilder = copyObjectFileRebuilderMap.computeIfAbsent(task.getMap().get("vKey"), k -> new CopyObjectFileRebuilder());
            task.copyObjectFileRebuilder.trackCopySourceFile(task.map.get("fileName"));
        }
        MonoProcessor<Boolean> res = MonoProcessor.create();
        id.incrementAndGet();
        if (id.get() < MAX_CACHE_NUMS) {
            res.onNext(true);
        }
        cacheQueues.compute(task.disk, (k1, v1) -> {
            RunningTask runningTask = new RunningTask(res, task, false, new AtomicBoolean(false), 0L);
            if (v1 == null) {
                v1 = UnicastProcessor.create(Queues.<RunningTask>unboundedMultiproducer().get());
            }
            v1.onNext(runningTask);
            return v1;
        });
        return res;
    }

    /**
     * 判断是否被限流
     * 索引盘采用 RateLimiter 限制速率
     * 数据盘、缓存盘使用 redis限流 限制并发
     *
     * @param runningTask task
     * @return 限流结果
     */
    private boolean acquireRateLimitPermit(RunningTask runningTask, String[] linkKeys) {
        if (runningTask.task.type.equals(OBJECT_FILE)) {
            String[] diskLink = runningTask.task.getDiskLink();
            for (String disk : diskLink) {
                Map<String, String> map = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hgetall(runningTask.task.pool + disk);
                String disk0 = map.get("s_uuid") + "@" + map.get("lun_name");
                int num = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX)
                        .keys("rebuild_running_" + disk0 + "_*").size();
                int size;
                if (newRequest == 10) {
                    size = diskLink.length;
                } else {
                    size = HEART_IP_LIST.size();
                }
                if (num > newRequest * size + newRequest) {
                    return false;
                }
            }

            for (int i = 0; i < diskLink.length; i++) {
                String disk = diskLink[i];
                Map<String, String> map = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hgetall(runningTask.task.pool + disk);
                String disk0 = map.get("s_uuid") + "@" + map.get("lun_name");
                linkKeys[i] = "rebuild_running_" + disk0 + "_" + runningTask.task.vnode + "_" + System.currentTimeMillis();
                RebuildRabbitMq.getMaster().setex(linkKeys[i], 15 * 60, runningTask.task.map.toString());
            }
            return true;
        } else {
            return RebuildRateLimiter.getInstance().tryAcquire(runningTask.task.disk);
        }
    }
}
