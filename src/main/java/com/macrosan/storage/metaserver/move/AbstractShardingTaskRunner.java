package com.macrosan.storage.metaserver.move;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.move.copy.AbstractCopyTaskRunner;
import com.macrosan.storage.metaserver.move.copy.CopyInitPartMetaTaskRunner;
import com.macrosan.storage.metaserver.move.copy.CopyObjectMetaDataTaskRunner;
import com.macrosan.storage.metaserver.move.copy.CopyPartMetaDataTaskRunner;
import com.macrosan.storage.metaserver.move.remove.AbstractRemoveTaskRunner;
import com.macrosan.storage.metaserver.move.remove.RemoveInitPartMetaTaskRunner;
import com.macrosan.storage.metaserver.move.remove.RemoveObjectMetaTaskRunner;
import com.macrosan.storage.metaserver.move.remove.RemovePartMetaTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.quota.QuotaRecorder;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.metaserver.ShardingWorker.INDEX_KEY;
import static com.macrosan.storage.metaserver.ShardingWorker.SHARDING_SCHEDULER;

@Log4j2
public abstract class AbstractShardingTaskRunner {

    public final String bucketName;
    protected final String sourceVnode;
    protected final String targetVnode;
    protected final long copyMaxKeys;
    /**
     * 起始迁移位置
     */
    protected String startPosition = "";

    protected final AbstractCopyTaskRunner.Direction direction;
    protected final MonoProcessor<Boolean> res = MonoProcessor.create();
    protected final StoragePool storagePool;
    protected final AtomicInteger runningIndex = new AtomicInteger(0);
    private static final PHASE[] PHASES = PHASE.values();
    protected PHASE phase = PHASE.START;
    private static final RedisConnPool POOL = RedisConnPool.getInstance();
    protected final AtomicBoolean discard = new AtomicBoolean(false);

    protected final AtomicBoolean end = new AtomicBoolean(false);

    protected final String node = ServerConfig.getInstance().getHostUuid();
    AtomicBoolean locked = new AtomicBoolean(false);
    protected final String lockKey;

    protected ShardingContext context;

    protected AbstractShardingTaskRunner(String bucketName, String sourceVnode, String targetVnode, long copyMaxKeys, AbstractCopyTaskRunner.Direction direction, Type type) {
        this.bucketName = bucketName;
        this.sourceVnode = sourceVnode;
        this.targetVnode = targetVnode;
        this.copyMaxKeys = copyMaxKeys;
        this.direction = direction;
        this.type = type;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        this.lockKey = bucketName + "_sharding";
        context = new ShardingContext();
        context.put("type", type.name());
    }

    public boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(30);
            String setKey = RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).set(lockKey, this.node, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
            }
            return res;
        } catch (Exception e) {
            return false;
        }
    }

    private void keepLock() {
        log.debug("bucket {} keep lock", bucketName);
        if (!end.get() && locked.get()) {
            SetArgs setArgs = SetArgs.Builder.xx().ex(30);
            try (StatefulRedisConnection<String, String> tmpConnection =
                         RedisConnPool.getInstance().getSharedConnection(4).newMaster()) {
                RedisCommands<String, String> target = tmpConnection.sync();
                target.watch(lockKey);
                String lockNode = target.get(lockKey);
                if (node.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set(lockKey, this.node, setArgs);
                    target.exec();
                    SHARDING_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                SHARDING_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
                log.error("bucket {} keep lock errir", bucketName, e);
            }
        } else {
            log.info("bucket {} sharding task unlock!", bucketName);
        }
    }

    public void run() {
        log.info("sharding task runner will start. bucket:{} sourceVnode:{} targetVnode:{} copyMaxKeys:{}", bucketName, sourceVnode, targetVnode, copyMaxKeys);
        if (!locked.get()) {
            log.info("Try get {} sharding task runner lock Error!", bucketName);
            res.onNext(false);
            return;
        }
        UnicastProcessor<PHASE> phaseUnicastProcessor = UnicastProcessor.create(Queues.<PHASE>unboundedMultiproducer().get());
        phaseUnicastProcessor
                .doFinally((v) -> this.end.set(true))
                .subscribe(phase -> {
                    log.info("Phase:" + phase.name());
                    try {
                        switch (phase) {
                            case START:
                                this.phase = PHASE.START;
                                archive();
                                log.info("bucket {} sourceVnode {} targetVnode {} will start double write.", bucketName, sourceVnode, targetVnode);
                                start().subscribe(b -> {
                                    try {
                                        if (b) {
                                            String serialize = storagePool.getBucketShardCache().get(bucketName).serialize();
                                            ;
                                            log.info("start mapping serialize:" + serialize);
                                            POOL.getShortMasterCommand(REDIS_ACTION_INDEX).hset(INDEX_KEY, bucketName, serialize);
                                            while (!queue.isEmpty()) {
                                                Tuple2<String, String> value = queue.poll();
                                                POOL.getShortMasterCommand(REDIS_ACTION_INDEX).lpush(value.var1 + "_reload_bucket_shard_cache_queue", value.var2);
                                            }
                                            log.info("bucket {} sourceVnode {} targetVnode {} start double write successfully!.", bucketName, sourceVnode, targetVnode);
                                            int i = runningIndex.incrementAndGet();
                                            if (i < PHASES.length) {
                                                phaseUnicastProcessor.onNext(PHASES[i]);
                                            } else {
                                                res.onNext(true);
                                                phaseUnicastProcessor.onComplete();
                                            }
                                        } else {
                                            queue.clear();
                                            log.info("bucket {} sourceVnode {} targetVnode {} start failed.", bucketName, sourceVnode, targetVnode);
                                            res.onNext(false);
                                            phaseUnicastProcessor.onComplete();
                                        }
                                    } catch (Exception e) {
                                        queue.clear();
                                        log.info("bucket {} sourceVnode {} targetVnode {} start encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                        res.onNext(false);
                                        phaseUnicastProcessor.onComplete();
                                    }
                                }, e -> {
                                    queue.clear();
                                    log.info("bucket {} sourceVnode {} targetVnode {} start encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                    res.onNext(false);
                                    phaseUnicastProcessor.onComplete();
                                });
                                break;

                            case COPY:
                                this.phase = PHASE.COPY;
                                archive();
                                log.info("bucket {} sourceVnode {} targetVnode {} will start copy phase. copyMaxKeys:{}", bucketName, sourceVnode, targetVnode, copyMaxKeys);
                                delay().flatMap(b -> copy()).subscribe(b -> {
                                    if (b) {
                                        log.info("bucket {} sourceVnode {} targetVnode {} COPY successfully!.", bucketName, sourceVnode, targetVnode);
                                        int i = runningIndex.incrementAndGet();
                                        if (i < PHASES.length) {
                                            phaseUnicastProcessor.onNext(PHASES[i]);
                                        } else {
                                            res.onNext(true);
                                            phaseUnicastProcessor.onComplete();
                                        }
                                    } else {
                                        if (discard.get()) {
                                            log.info("the bucket {} sourceNode:{} targetNode:{} task will be discard!", bucketName, sourceVnode, targetVnode);
                                            phaseUnicastProcessor.onNext(PHASE.DISCARD);
                                        } else {
                                            log.info("bucket {} sourceVnode {} targetVnode {} copy failed.", bucketName, sourceVnode, targetVnode);
                                            res.onNext(false);
                                            phaseUnicastProcessor.onComplete();
                                        }
                                    }
                                }, e -> {
                                    if (discard.get()) {
                                        log.info("the bucket {} sourceNode:{} targetNode:{} task will be discard!", bucketName, sourceVnode, targetVnode);
                                        phaseUnicastProcessor.onNext(PHASE.DISCARD);
                                    } else {
                                        log.info("bucket {} sourceVnode {} targetVnode {} copy encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                        res.onNext(false);
                                        phaseUnicastProcessor.onComplete();
                                    }
                                });
                                break;

                            case UPDATE:
                                this.phase = PHASE.UPDATE;
                                archive();
                                log.info("bucket {} sourceVnode {} targetVnode {} will start update phase, divider:{}", bucketName, sourceVnode, targetVnode, divider.get());
                                update().subscribe(b -> {
                                    if (b) {
                                        try {
                                            String serialize = storagePool.getBucketShardCache().get(bucketName).serialize();
                                            ;
                                            log.info("update mapping serialize:" + serialize);
                                            POOL.getShortMasterCommand(REDIS_ACTION_INDEX).hset(INDEX_KEY, bucketName, serialize);
                                            while (!queue.isEmpty()) {
                                                Tuple2<String, String> value = queue.poll();
                                                POOL.getShortMasterCommand(REDIS_ACTION_INDEX).lpush(value.var1 + "_reload_bucket_shard_cache_queue", value.var2);
                                            }
                                            int i = runningIndex.incrementAndGet();
                                            if (i < PHASES.length) {
                                                log.info("bucket {} sourceVnode {} targetVnode {} UPDATE successfully!.", bucketName, sourceVnode, targetVnode);
                                                phaseUnicastProcessor.onNext(PHASES[i]);
                                            } else {
                                                res.onNext(true);
                                                phaseUnicastProcessor.onComplete();
                                            }
                                        } catch (Exception e) {
                                            queue.clear();
                                            res.onNext(false);
                                            log.error("", e);
                                            phaseUnicastProcessor.onComplete();
                                        }
                                    } else {
                                        log.info("bucket {} sourceVnode {} targetVnode {} update failed.", bucketName, sourceVnode, targetVnode);
                                        res.onNext(false);
                                        queue.clear();
                                        phaseUnicastProcessor.onComplete();
                                    }
                                }, e -> {
                                    log.info("bucket {} sourceVnode {} targetVnode {} update encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                    queue.clear();
                                    res.onNext(false);
                                    phaseUnicastProcessor.onComplete();
                                });
                                break;

                            case REMOVE:
                                this.phase = PHASE.REMOVE;
                                archive();
                                log.info("bucket {} sourceVnode {} targetVnode {} will start remove phase, divider:{}", bucketName, sourceVnode, targetVnode, divider.get());
                                fillWaitIpList();
                                delay().flatMap(b -> remove()).subscribe(b -> {
                                    if (b) {
                                        log.info("bucket {} sourceVnode {} targetVnode {} REMOVE successfully.", bucketName, sourceVnode, targetVnode);
                                        int i = runningIndex.incrementAndGet();
                                        if (i < PHASES.length) {
                                            phaseUnicastProcessor.onNext(PHASES[i]);
                                        } else {
                                            res.onNext(true);
                                            phaseUnicastProcessor.onComplete();
                                        }
                                    } else {
                                        log.info("bucket {} sourceVnode {} targetVnode {} remove failed.", bucketName, sourceVnode, targetVnode);
                                        res.onNext(false);
                                        phaseUnicastProcessor.onComplete();
                                    }
                                }, e -> {
                                    log.info("bucket {} sourceVnode {} targetVnode {} remove encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                    res.onNext(false);
                                    phaseUnicastProcessor.onComplete();
                                });
                                break;

                            case FINISH:
                                this.phase = PHASE.FINISH;
                                QuotaRecorder.addCheckBucket(bucketName);
                                archive();
                                ErasureClient.mergeTempBucketInfo(sourceVnode, bucketName)
                                        .flatMap(b -> {
                                            if (b) {
                                                return ErasureClient.mergeTempBucketInfo(targetVnode, bucketName);
                                            }
                                            return Mono.just(false);
                                        })
                                        .subscribe(b -> {
                                            if (b) {
                                                log.info("bucket {} sourceVnode {} targetVnode {} sharding successfully!", bucketName, sourceVnode, targetVnode);
                                                res.onNext(true);
                                                phaseUnicastProcessor.onComplete();
                                            } else {
                                                log.info("bucket {} sourceVnode {} targetVnode {} sharding failed.", bucketName, sourceVnode, targetVnode);
                                                res.onNext(false);
                                                phaseUnicastProcessor.onComplete();
                                            }
                                        }, e -> {
                                            log.info("bucket {} sourceVnode {} targetVnode {} remove encounter exception:{}", bucketName, sourceVnode, targetVnode, e);
                                            res.onNext(false);
                                            phaseUnicastProcessor.onComplete();
                                        });
                                break;

                            case DISCARD:
                                this.phase = PHASE.DISCARD;
                                archive();
                                log.info("The bucket {} sourceVnode {} targetVnode {} sharding task running conditions are not met, and the task has been abandoned.", bucketName, sourceVnode, targetVnode);
                                recoverSourceNodeState()
                                        .subscribe(b -> {
                                            if (b) {
                                                discard.set(true);
                                                setStartIndex(PHASE.REMOVE.value);
                                                phaseUnicastProcessor.onNext(PHASE.REMOVE);
                                            } else {
                                                log.error("roll back node:{} state error!", sourceVnode);
                                                res.onNext(false);
                                                phaseUnicastProcessor.onComplete();
                                            }
                                        }, e -> {
                                            res.onNext(false);
                                            phaseUnicastProcessor.onComplete();
                                            log.error("", e);
                                        });
                                break;

                            default:
                        }
                    } catch (Exception e) {
                        log.error("runner processor encounter exception. ", e);
                        res.onNext(false);
                        phaseUnicastProcessor.onComplete();
                    }
                });

        if (runningIndex.get() < 0) {
            phaseUnicastProcessor.onNext(PHASE.DISCARD);
        } else {
            log.info("startPhase:" + PHASES[runningIndex.get()]);
            phaseUnicastProcessor.onNext(PHASES[runningIndex.get()]);
        }
    }

    /**
     * 开启双写后，只有等待ip列表所有节点在双写开启过程中上传中的流量停止后才开始copy或者remove
     */
    protected final List<Tuple2<String, Long>> waitIpList = new CopyOnWriteArrayList<>();

    protected final Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();

    /**
     * 开启双写，更新节点状态
     */
    private Mono<Boolean> start() {
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Flux<Tuple3<String, String, Long>> res = Flux.empty();
        AtomicInteger errorNum = new AtomicInteger(0);
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("sourceNode", sourceVnode)
                .put("goalNode", targetVnode);
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            res = res.mergeWith(Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), START_DOUBLE_WRITE.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> SUCCESS.name().equals(r.getMetadataUtf8())
                            ? new Tuple3<>(ip, SUCCESS.name(), Long.parseLong(r.getDataUtf8()))
                            : new Tuple3<>(ip, ERROR.name(), 0L))
                    .doOnError(log::error)
                    .onErrorReturn(new Tuple3<>(ip, ERROR.name(), 0L)));
        }

        res.publishOn(SHARDING_SCHEDULER)
                .doOnNext(t -> {
                    if (t.var2.equals(ERROR.name())) {
                        queue.offer(new Tuple2<>(t.var1, bucketName));
                        errorNum.incrementAndGet();
                        if (t.var1.equals(ServerConfig.getInstance().getHeartIp1())) {
                            errorNum.set(Integer.MAX_VALUE);
                        }
                    } else if (t.var3 != 0) {
                        waitIpList.add(new Tuple2<>(t.var1, null));
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    result.onNext(false);
                })
                .doOnComplete(() -> {
                    if (errorNum.get() >= RabbitMqUtils.HEART_IP_LIST.size()) {
                        result.onNext(false);
                    } else {
                        result.onNext(true);
                    }
                })
                .subscribe();

        return result.timeout(Duration.ofSeconds(60)).onErrorReturn(false);
    }

    /**
     * 将节点从双写状态恢复成正常状态
     *
     * @return
     */
    private Mono<Boolean> recoverSourceNodeState() {
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Flux<Tuple2<String, Boolean>> res = Flux.empty();
        Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("type", "4")
                .put("bucket", bucketName)
                .put("sourceNode", sourceVnode);
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            res = res.mergeWith(Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_BUCKET_INDEX.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> new Tuple2<>(ip, ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8())))
                    .onErrorReturn(new Tuple2<>(ip, false)));
        }

        res.publishOn(DISK_SCHEDULER)
                .doOnNext(t -> {
                    if (!t.var2) {
                        queue.offer(new Tuple2<>(t.var1, bucketName));
                        if (t.var1.equals(ServerConfig.getInstance().getHeartIp1())) {
                            throw new RuntimeException("update local mapping error!");
                        }
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    result.onNext(false);
                })
                .doOnComplete(() -> {
                    try {
                        ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache().get(bucketName);
                        String serialize = objectSplitTree.serialize();
                        log.info("bucket {} serialize:{}", bucketName, serialize);
                        POOL.getShortMasterCommand(REDIS_ACTION_INDEX).hset("bucket_index_map", bucketName, serialize);
                        while (!queue.isEmpty()) {
                            Tuple2<String, String> value = queue.poll();
                            POOL.getShortMasterCommand(REDIS_ACTION_INDEX).lpush(value.var1 + "_reload_bucket_shard_cache_queue", value.var2);
                        }
                        result.onNext(true);
                    } catch (Exception e) {
                        log.error("", e);
                        result.onNext(false);
                    }
                })
                .subscribe();

        return result.timeout(Duration.ofSeconds(60)).onErrorReturn(false);
    }

    private Mono<Boolean> waitAllNodeRunningStop() {
        if (waitIpList.isEmpty()) {
            log.info("node running is stopped!");
            return Mono.just(true);
        }
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Flux<Tuple3<String, String, Long>> res = Flux.empty();
        Tuple2<String, Long>[] waitIpArray = waitIpList.toArray(new Tuple2[0]);
        waitIpList.clear();
        for (Tuple2<String, Long> tuple : waitIpArray) {
            SocketReqMsg msg = new SocketReqMsg("", 0);
            log.info("bucket {} send Ip :{} curId:{}", bucketName, tuple.var1, tuple.var2);
            if (tuple.var2 != null) {
                msg.put("curId", String.valueOf(tuple.var2));
            }
            res = res.mergeWith(Mono.just(true).flatMap(b -> RSocketClient.getRSocket(tuple.var1, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), NODE_IS_RUNNING_STOP.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> new Tuple3<>(tuple.var1, r.getMetadataUtf8(), Long.parseLong(r.getDataUtf8())))
                    .onErrorReturn(new Tuple3<>(tuple.var1, ERROR.name(), 0L)));
        }

        res.publishOn(SHARDING_SCHEDULER)
                .doOnNext(t -> {
                    if (t.var3 != 0) {
                        waitIpList.add(new Tuple2<>(t.var1, t.var3));
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    result.onNext(false);
                })
                .doOnComplete(() -> {
                    if (waitIpList.isEmpty()) {
                        result.onNext(true);
                    } else {
                        result.onNext(false);
                    }
                })
                .subscribe();

        return result.timeout(Duration.ofSeconds(60)).onErrorReturn(true);
    }

    private Mono<Boolean> waitRunningStop() {
        MonoProcessor<Boolean> processor = MonoProcessor.create();
        Disposable[] disposable = new Disposable[1];
        boolean[] stop = new boolean[]{false};
        disposable[0] = Flux.interval(Duration.ofSeconds(3L))
                .publishOn(ErasureServer.DISK_SCHEDULER)
                .flatMap(l -> waitAllNodeRunningStop())
                .subscribe(b -> {
                    if (b) {
                        if (stop[0]) {
                            processor.onNext(true);
                            log.info("node running is stopped!");
                            disposable[0].dispose();
                        } else {
                            stop[0] = true;
                        }
                    } else {
                        stop[0] = false;
                    }
                });
        return processor;
    }

    private void fillWaitIpList() {
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            waitIpList.add(new Tuple2<>(ip, null));
        }
    }

    /**
     * 开启双写后延迟一段时间运行
     */
    private Mono<Boolean> delay() {
        log.info("wait running stop...");
        return Mono.delay(Duration.ofSeconds(60))
                .flatMap(l -> waitRunningStop())
                .flatMap(b -> Mono.delay(Duration.ofSeconds(60)))
                .map(l -> true);
    }

    /**
     * 迁移任务的复制操作
     */
    protected abstract Mono<Boolean> copy();

    /**
     * 迁移任务更新映射
     */
    protected abstract Mono<Boolean> update();

    /**
     * 迁移任务，移除分片上重复的数据
     */
    protected abstract Mono<Boolean> remove();

    /**
     * @return 返回任务执行的结果
     */
    public Mono<Boolean> res() {
        return res;
    }

    /**
     * 由copy操作产生的分割线,删除阶段根据此分割线去删除分片上重复的元数据
     */
    protected final AtomicReference<String> divider = new AtomicReference<>();

    /**
     * 问题单SERVER-1323
     * 由于@ partKey不是严格的按照字典排序，所以需要依赖与initPart copy产生的part divider来确定partKey的copy顺序，以防止@ partKey出现缺失的情况
     */
    protected final AtomicReference<String> partDivider = new AtomicReference<>();

    /**
     * 当前任务是否需要延续
     */
    protected final AtomicBoolean continuation = new AtomicBoolean(true);

    protected Mono<Boolean> realCopy() {
        MonoProcessor<Boolean> copyResult = MonoProcessor.create();
        AtomicInteger startIndex = new AtomicInteger(0);
        UnicastProcessor<AbstractCopyTaskRunner<Tuple2<byte[], byte[]>>> copyProcessor = UnicastProcessor.create();
        copyProcessor.subscribe(runner -> {
            if (runner == null) {
                return;
            }
            runner.run();
            runner.res().subscribe(b -> {
                if (b) {
                    try {
                        // 如果是第一次copy操作
                        if (startIndex.get() == 0) {
                            if (!runner.scanner.hasRemaining()) {
                                continuation.set(false);
                                context.put("continuation", "false");
                            }
                            // 以copy元数据的结果作为分割线
                            divider.set(runner.divider.get());
                            log.info("copy result divider:{}", runner.divider.get());
                            // 如果出现的分割线为空，则放弃当前任务
                            if (StringUtils.isEmpty(divider.get())) {
                                if (type.equals(Type.REBUILD) || type.equals(Type.MERGE)) {
                                    continuation.set(false);
                                } else {
                                    discard.set(true);
                                    copyResult.onNext(false);
                                    copyProcessor.onComplete();
                                    return;
                                }
                            }
                        } else if (startIndex.get() == 1) {
                            partDivider.set(runner.partDivider.get());
                            log.info("copy part result divider:{}", runner.partDivider.get());
                        }
                        AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> nextCopyRunner = nextCopyRunner(startIndex.incrementAndGet());
                        if (nextCopyRunner != null) {
                            copyProcessor.onNext(nextCopyRunner);
                        } else {
                            copyResult.onNext(true);
                            copyProcessor.onComplete();
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        copyResult.onNext(false);
                        copyProcessor.onComplete();
                    }
                } else {
                    copyResult.onNext(false);
                    copyProcessor.onComplete();
                }
            }, e -> {
                copyResult.onNext(false);
                copyProcessor.onComplete();
            });
        });
        copyProcessor.onNext(Objects.requireNonNull(nextCopyRunner(startIndex.get())));
        return copyResult;
    }

    private AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> nextCopyRunner(int index) {
        String[] copy = new String[]{ROCKS_VERSION_PREFIX, ROCKS_PART_PREFIX, ROCKS_PART_META_PREFIX};
        if (index >= copy.length) {
            return null;
        }
        AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> runner;
        switch (copy[index]) {
            case ROCKS_VERSION_PREFIX:
                runner = new CopyObjectMetaDataTaskRunner(bucketName, sourceVnode, startPosition, targetVnode, copyMaxKeys, direction);
                break;
            case ROCKS_PART_PREFIX:
                runner = new CopyInitPartMetaTaskRunner(bucketName, sourceVnode, startPosition, targetVnode, direction, divider.get());
                break;
            case ROCKS_PART_META_PREFIX:
                runner = new CopyPartMetaDataTaskRunner(bucketName, sourceVnode, startPosition, targetVnode, direction, divider.get(), partDivider.get());
                break;
            default:
                return null;
        }
        runner.setContext(context);
        return runner;
    }

    protected Mono<Boolean> realRemove(String vnode, AbstractRemoveTaskRunner.POSITION position) {
        MonoProcessor<Boolean> removeResult = MonoProcessor.create();
        AtomicInteger startIndex = new AtomicInteger(0);
        UnicastProcessor<AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>>> removeTaskRunners = UnicastProcessor.create();
        removeTaskRunners.subscribe(runner -> {
            if (runner == null) {
                return;
            }
            runner.run();
            runner.res().subscribe(b -> {
                if (b) {
                    AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>> nextRemoveRunner = nextRemoveRunner(startIndex.incrementAndGet(), vnode, position);
                    if (nextRemoveRunner != null) {
                        removeTaskRunners.onNext(nextRemoveRunner);
                    } else {
                        removeResult.onNext(true);
                        removeTaskRunners.onComplete();
                    }
                } else {
                    removeResult.onNext(false);
                    removeTaskRunners.onComplete();
                }
            }, e -> {
                removeResult.onNext(false);
                removeTaskRunners.onComplete();
            });
        }, e -> {
            removeResult.onNext(false);
            removeTaskRunners.onComplete();
        });
        removeTaskRunners.onNext(Objects.requireNonNull(nextRemoveRunner(startIndex.get(), vnode, position)));
        return removeResult;
    }

    private AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>> nextRemoveRunner(int index, String vnode, AbstractRemoveTaskRunner.POSITION position) {
        String[] remove = new String[]{ROCKS_VERSION_PREFIX, ROCKS_PART_PREFIX, ROCKS_PART_META_PREFIX};
        if (index >= remove.length) {
            return null;
        }
        switch (remove[index]) {
            case ROCKS_VERSION_PREFIX:
                return new RemoveObjectMetaTaskRunner(bucketName, vnode, divider.get(), position);
            case ROCKS_PART_PREFIX:
                return new RemoveInitPartMetaTaskRunner(bucketName, vnode, divider.get(), position);
            case ROCKS_PART_META_PREFIX:
                return new RemovePartMetaTaskRunner(bucketName, vnode, divider.get(), position, partDivider.get());
        }
        return null;
    }

    private void setStartIndex(int index) {
        this.runningIndex.set(index);
    }

    private void setDivider(String divider) {
        this.divider.set(divider);
    }

    private void setPartDivider(String partDivider) {
        this.partDivider.set(partDivider);
    }

    protected Type type;

    public enum Type {
        /**
         * 增加分片
         */
        EXPANSION,

        /**
         * 向右增加分片
         */
        REXPANSION,

        /**
         * 平衡相邻分片的数据
         */
        BALANCE,

        /**
         * 重新分片
         */
        REBUILD,

        /**
         * 合并分片
         */
        MERGE
    }

    public static final String ARCHIVE_SUFFIX = "_sharding_running";

    /**
     * 记录当前任务的执行信息
     */
    public void archive(String runningNode) {
        try {
            Map<String, String> archive = new HashMap<>();
            archive.put("type", type.name());
            archive.put("bucketName", bucketName);
            archive.put("sourceVnode", sourceVnode);
            archive.put("targetVnode", targetVnode);
            archive.put("copyMaxKeys", String.valueOf(copyMaxKeys));
            archive.put("direction", direction.name());
            archive.put("phase", phase.name());
            archive.put("continuation", String.valueOf(continuation.get()));
            archive.put("discard", String.valueOf(discard.get()));
            if (phase.value >= PHASE.UPDATE.value && StringUtils.isNotBlank(divider.get())) {
                archive.put("divider", divider.get());
            }
            if (StringUtils.isNotEmpty(partDivider.get())) {
                archive.put("partDivider", partDivider.get());
            }
            archive.put("runningNode", StringUtils.isNotBlank(runningNode) ? runningNode : ServerConfig.getInstance().getHostUuid());
            POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX).hmset(bucketName + ARCHIVE_SUFFIX, archive);
        } catch (Exception e) {
            log.info("", e);
            throw e;
        }
    }

    public void archive() {
        archive(null);
    }

    /**
     * 删除任务的执行信息
     */
    public void deleteArchive() {
        POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + ARCHIVE_SUFFIX);
    }

    /**
     * 根据任务的存档信息，恢复任务的执行状态
     *
     * @param redisKey 任务存档的key
     * @return 异常中断前的任务
     */
    public static AbstractShardingTaskRunner recovery(String redisKey) {
        Map<String, String> archive = POOL.getCommand(REDIS_TASKINFO_INDEX).hgetall(redisKey);
        String typeString = archive.get("type");
        Type type = Type.valueOf(typeString);
        String bucketName = archive.get("bucketName");
        String sourceVnode = archive.get("sourceVnode");
        String targetVnode = archive.get("targetVnode");
        String copyMaxKeys = archive.get("copyMaxKeys");
        String divider = archive.get("divider");
        String partDivider = archive.get("partDivider");
        String directionString = archive.get("direction");
        String continuation = archive.get("continuation");
        String discard = archive.get("discard");
        AbstractCopyTaskRunner.Direction direction = AbstractCopyTaskRunner.Direction.valueOf(directionString);
        String phaseString = archive.get("phase");
        PHASE phase = PHASE.valueOf(phaseString);
        int startIndex = phase.value >= 2 || phase.value == -1 ? phase.value : 0;
        AbstractShardingTaskRunner runner;
        if (type.equals(Type.EXPANSION)) {
            runner = new ShardingExpansionTaskRunner(bucketName, sourceVnode, targetVnode, Long.parseLong(copyMaxKeys));
        } else if (type.equals(Type.REXPANSION)) {
            runner = new ShardingRightExpansionTaskRunner(bucketName, sourceVnode, targetVnode, Long.parseLong(copyMaxKeys));
        } else if (type.equals(Type.BALANCE)) {
            runner = new ShardingLoadBalanceTaskRunner(bucketName, sourceVnode, targetVnode, Long.parseLong(copyMaxKeys), direction);
        } else if (type.equals(Type.REBUILD)) {
            BooleanSupplier supplier = () -> !(phase.equals(PHASE.REMOVE) && StringUtils.isNotEmpty(continuation) && !Boolean.parseBoolean(continuation));
            runner = new ShardingRebuildTaskRunner(bucketName, sourceVnode, targetVnode, supplier);
        } else if (type.equals(Type.MERGE)) {
            BooleanSupplier supplier = () -> !(phase.equals(PHASE.REMOVE) && StringUtils.isNotEmpty(continuation) && !Boolean.parseBoolean(continuation));
            runner = new ShardingMergeTaskRunner(bucketName, sourceVnode, targetVnode, direction, supplier);
        } else {
            throw new RuntimeException("No such sharding runner type.");
        }
        runner.setStartIndex(startIndex);
        runner.continuation.set(Boolean.parseBoolean(continuation));
        runner.discard.set(Boolean.parseBoolean(discard));
        if (StringUtils.isNotBlank(divider)) {
            runner.setDivider(divider);
        }
        if (StringUtils.isNotEmpty(partDivider)) {
            runner.setPartDivider(partDivider);
        }
        return runner;
    }
}