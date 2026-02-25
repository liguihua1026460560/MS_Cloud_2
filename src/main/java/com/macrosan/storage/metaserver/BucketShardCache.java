package com.macrosan.storage.metaserver;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.REDIS_ACTION_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_BUCKET_INDEX;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

@Log4j2
public class BucketShardCache {

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private static BucketShardCache instance;
    public static final String SYNC_CHANNEL = "history_sync_channel";
    public static BucketShardCache getInstance() {
        if (instance == null) {
            instance = new BucketShardCache();
        }
        return instance;
    }

    private BucketShardCache() { init(); }

    @Getter
    private final Map<String, ObjectSplitTree> cache = new ConcurrentHashMap<>();

    private final ScheduledThreadPoolExecutor repairSchedule = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "cache-repair"));

    private void init() {
        if (initialized.compareAndSet(false, true)) {
            long start = System.currentTimeMillis();
            Map<String, String> bucketIndexMap = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_ACTION_INDEX).hgetall("bucket_index_map");
            ScanArgs scanArgs = new ScanArgs().match("*").limit(10);
            ScanIterator<String> iterator = ScanIterator.scan(RedisConnPool.getInstance().getCommand(SysConstants.REDIS_BUCKETINFO_INDEX), scanArgs);
            while (iterator.hasNext()) {
                String bucket = iterator.next();
                if (BUCKET_NAME_PATTERN.matcher(bucket).matches() && "hash".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).type(bucket))) {
                    String tree = bucketIndexMap.getOrDefault(bucket, null);
                    ObjectSplitTree objectIndexTree = new ObjectSplitTree(bucket, tree);
                    cache.put(bucket, objectIndexTree);
                }
            }
            long end = System.currentTimeMillis();
            repairCache();
            log.info("load bucket index cost {} ms", (end - start));
        }
    }

    public void put(String bucketName, ObjectSplitTree objectSplitTree) {
        cache.put(bucketName, objectSplitTree);
    }

    public void remove(String bucketName) { cache.remove(bucketName); }

    public ObjectSplitTree get(String bucketName) {
        ObjectSplitTree objectSplitTree = cache.get(bucketName);
        if (objectSplitTree == null) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket name:" + bucketName);
        }
        return objectSplitTree;
    }

    public boolean contains(String bucketName) {
        return cache.containsKey(bucketName);
    }

    public void repairCache() {
        try {
            String localHeartIp = ServerConfig.getInstance().getHeartIp1();
            while (true) {
                String bucket = RedisConnPool.getInstance().getShortMasterCommand(REDIS_ACTION_INDEX).rpop(localHeartIp + "_reload_bucket_shard_cache_queue");
                if (bucket == null) {
                    break;
                }
                log.info("reload bucket {} shard cache.", bucket);
                Long exists = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).exists(bucket);
                if (exists != 0) {
                    String serialize = RedisConnPool.getInstance().getCommand(REDIS_ACTION_INDEX).hget("bucket_index_map", bucket);
                    ObjectSplitTree objectSplitTree = new ObjectSplitTree(bucket, serialize);
                    cache.put(bucket, objectSplitTree);
                    objectSplitTree.print();
                }
            }
        } finally {
            repairSchedule.schedule(this::repairCache, 100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 用于监听
     */
    public static void listenSyncing() {
        log.info("start listen history bucket syncing...");
        RedisConnPool.getInstance().getShortMasterCommand(REDIS_ACTION_INDEX)
                .lrem(SYNC_CHANNEL, 0, "END");
        Disposable[] disposables = new Disposable[1];
        disposables[0] = Mono.just(true)
                .publishOn(SCAN_SCHEDULER)
                .flatMapMany(b -> Flux.interval(Duration.ofMillis(100)))
                // 此线程池中使用同步的redis操作，防止数据缓存在响应式缓存中，导致历史桶重复初始化
                .flatMap(l -> Mono.just(Optional.ofNullable(RedisConnPool.getInstance().getShortMasterCommand(REDIS_ACTION_INDEX).rpop(SYNC_CHANNEL)).orElse("")))
                .filter(StringUtils::isNotEmpty)
                .doOnNext(key -> {
                    // 接受到结束符号，停止流
                    if ("END".equals(key)) {
                        disposables[0].dispose();
                        log.info("syncing complete!");
                    }
                })
                .filter(key -> !"END".equals(key))
                .flatMap(bucket -> RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, "storage_strategy")
                        .defaultIfEmpty(StorageStrategy.DEFAULT_STRATEGY_NAME)
                        .flatMap(strategy -> reloadHistoryBucketCache(bucket, strategy).zipWith(Mono.just(bucket))))
                .doOnError(e -> SCAN_SCHEDULER.schedule(BucketShardCache::listenSyncing, 3, TimeUnit.SECONDS))
                .subscribe(tuple2 -> {
                    if (!tuple2.getT1()) {
                        log.error("bucket object index tree init fail, requeue {}!", tuple2.getT2());
                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_ACTION_INDEX).lpush(SYNC_CHANNEL, tuple2.getT2());
                    } else {
                        log.info("bucket {} object index tree init successfully!", tuple2.getT2());
                    }
                });
    }

    private static Mono<Boolean> reloadHistoryBucketCache(String bucket, String strategy) {
        if (StringUtils.isBlank(bucket) || !BUCKET_NAME_PATTERN.matcher(bucket).matches()) {
            return Mono.just(true);
        }
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Flux<Boolean> res = Flux.empty();
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("type", "2")
                .put("bucket", bucket)
                .put("strategy", strategy);
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            res = res.mergeWith(Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_BUCKET_INDEX.name())))
                    .map(r -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                    .onErrorReturn(false));
        }
        res.publishOn(DISK_SCHEDULER).doOnError(log::error).doOnComplete(() -> result.onNext(true)).subscribe();
        return result;
    }

    public static Mono<Boolean> removeBucketCache(String bucket) {
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Flux<Boolean> res = Flux.empty();
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("type", "3")
                .put("bucket", bucket);
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            res = res.mergeWith(Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_BUCKET_INDEX.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                    .onErrorReturn(false));
        }
        res.publishOn(DISK_SCHEDULER).doOnError(log::error).doOnComplete(() -> result.onNext(true)).subscribe();
        return result;
    }

}
