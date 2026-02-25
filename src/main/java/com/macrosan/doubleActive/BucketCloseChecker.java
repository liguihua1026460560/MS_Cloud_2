package com.macrosan.doubleActive;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.deployment.AddClusterUtils;
import com.macrosan.doubleActive.deployment.BucketSyncChecker;
import com.macrosan.doubleActive.deployment.DeployRecord;
import com.macrosan.lifecycle.LifecycleCommandConsumer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.versions.DeleteMarker;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.message.xmlmsg.versions.Version;
import com.macrosan.message.xmlmsg.versions.VersionBase;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.channel.ListVersionsMergeChannel;
import com.macrosan.utils.functional.Tuple2;
import io.lettuce.core.ScanStream;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.HeartBeatChecker.INDEX_STATUS_MAP;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.datasyncIsEnabled;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;
import static com.macrosan.lifecycle.LifecycleCommandConsumer.ACK_TURE;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.ARCHIVE_SUFFIX;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * 删除close状态的桶中的对象
 *
 * @author chengyinfeng
 */
@Log4j2
public class BucketCloseChecker {

    private static final Integer MAX_LIST_AMOUNT = 3000;
    private static int LIST_AMOUNT = 1000;
    private static BucketCloseChecker instance;
    private static final RedisConnPool pool = RedisConnPool.getInstance();

    public static BucketCloseChecker getInstance() {
        if (instance == null) {
            instance = new BucketCloseChecker();
        }
        return instance;
    }

    public static class SyncRequest<T> {
        public T payload;
        public MonoProcessor<Boolean> res;
    }

    /**
     * 用于list线程数量控制，表示还在list中的bucket总数
     */
    static final AtomicInteger deletingBucketNum = new AtomicInteger();
    private static final Set<String> bucketsList = new ConcurrentHashSet<>();
    static AtomicInteger dealingDeleteAmount = new AtomicInteger();
    private static final AtomicBoolean has_obj_deleting = new AtomicBoolean(false);
    private static final UnicastProcessor<SyncRequest<JSONObject>>[] bsObjProcessors = new UnicastProcessor[16];
    private ScheduledFuture<?> scheduledFuture;

    public void init() {
        log.info("start closed bucket obj clear.");
        for (int i = 0; i < bsObjProcessors.length; i++) {
            bsObjProcessors[i] = UnicastProcessor.create(Queues.<SyncRequest<JSONObject>>unboundedMultiproducer().get());
            bsObjProcessors[i].publishOn(SCAN_SCHEDULER)
                    .flatMap(syncRequest ->
                            LifecycleCommandConsumer.expirationOperation(syncRequest.payload)
                                    .map(ACK_TURE::equals)
                                    .doOnNext(result -> syncRequest.res.onNext(result))
                                    .onErrorResume(e -> {
                                        log.error("bucket sync dealObjMeta error,", e);
                                        syncRequest.res.onNext(false);
                                        return Mono.empty();
                                    })
                    )
                    .subscribe();
        }


        Optional.ofNullable(scheduledFuture).ifPresent(scheduledFuture -> scheduledFuture.cancel(false));
        scheduledFuture = SCAN_TIMER.scheduleAtFixedRate(BucketCloseChecker::initStart, 10, 10, TimeUnit.SECONDS);
    }

    private static void initStart() {
        try {
            // 非主节点递归，预备切主。
            if (!MainNodeSelector.checkIfSyncNode()) {
                has_obj_deleting.compareAndSet(true, false);
                bucketsList.clear();
                deletingBucketNum.set(0);
                log.debug("not master, keep going for possibly node switch.");
                return;
            }

            // 恢复中断扫描数据
            has_obj_deleting.compareAndSet(false, true);
            final String maxKey = pool.getCommand(REDIS_SYSINFO_INDEX).hget(CLOSED_BUCKETS_MAP, "maxKey");
            if (StringUtils.isNotBlank(maxKey)) {
                final int i = Integer.parseInt(maxKey);
                if (0 < i && i <= 1000) {
                    LIST_AMOUNT = i;
                } else {
                    LIST_AMOUNT = 1000;
                }
            } else {
                LIST_AMOUNT = 1000;
            }
            log.debug("bucketList： {}, dealingBuckets: {}", bucketsList, deletingBucketNum);
            ScanStream.sscan(pool.getReactive(REDIS_SYSINFO_INDEX), CLOSED_BUCKETS)
                    .publishOn(SCAN_SCHEDULER)
                    .doOnNext(bucketName -> {
                        if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName) == 1 && datasyncIsClosed(bucketName)) {
                            synchronized (bucketsList) {
                                if (pool.getCommand(REDIS_SYSINFO_INDEX).sismember(CLOSED_BUCKETS, bucketName) && bucketsList.add(bucketName)) {
                                    log.info("start bucket synchronization: {}", bucketName);
                                    checkSyncStatus(bucketName);
                                }
                            }
                        } else {
                            log.info("The bucket does not require data sync: {}", bucketName);
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(CLOSED_BUCKETS, bucketName);
                        }
                    })
                    .doOnError(e -> log.error("start bucket closed error", e))
                    .subscribe();

        } catch (Exception e) {
            log.error("initStart error", e);
        }
    }

    private static void checkSyncStatus(String bucketName) {
        pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .publishOn(SCAN_SCHEDULER)
                .defaultIfEmpty(new HashMap<>())
                .doOnNext(map -> {
                    if (map.isEmpty()) {
                        bucketsList.remove(bucketName);
                        return;
                    }

                    if (deletingBucketNum.incrementAndGet() > 5) {
                        bucketsList.remove(bucketName);
                        deletingBucketNum.decrementAndGet();
                        return;
                    }

                    log.info("start bucket delete obj scan, bucket: {}, bucketList: {}, deletingBucketNum: {}", bucketName, bucketsList, deletingBucketNum);
                    scanTrashObjects(bucketName);
                })
                .doOnError(e -> {
                    log.error("checkSyncStatus Error1, ", e);
                    bucketsList.remove(bucketName);
                    deletingBucketNum.decrementAndGet();
                })
                .subscribe();
    }

    private static void scanTrashObjects(String bucketName) {
        log.info("start scan trash objects, bucket {}", bucketName);
        UnicastProcessor<Tuple2<String, String>> bucketVnodeProcessor = UnicastProcessor.create(Queues.<Tuple2<String, String>>unboundedMultiproducer().get());
        bucketVnodeProcessor
                .publishOn(SCAN_SCHEDULER)
                .flatMap(tuple ->
                        pool.getReactive(REDIS_TASKINFO_INDEX).exists(bucketName + ARCHIVE_SUFFIX)
                                .filter(b -> {
                                    if (!has_obj_deleting.get()) {
                                        log.info("Stop obj deleting. {}", bucketName);
                                        deletingBucketNum.set(0);
                                        bucketsList.clear();
                                        bucketVnodeProcessor.onComplete();
                                        return false;
                                    }

                                    if (dealingDeleteAmount.get() > MAX_LIST_AMOUNT) {
                                        log.debug("closed bucket too many listing obj, {}", bucketName);
                                        Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(m -> bucketVnodeProcessor.onNext(tuple));
                                        return false;
                                    }
                                    return true;
                                })
                                .map(b -> tuple)
                )
                .subscribe(tuple2 -> {
                    int maxKey = LIST_AMOUNT;
                    String marker = tuple2.var1;
                    String versionIdMarker = tuple2.var2;
                    UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("bucket", bucketName)
                            .put("maxKeys", maxKey + "")
                            .put("prefix", "")
                            .put("marker", marker)
                            .put("versionIdMarker", versionIdMarker)
                            .put("delimiter", "");
                    ListVersionsResult listVersionsRes = new ListVersionsResult()
                            .setDelimiter("")
                            .setKeyMarker("")
                            .setName(bucketName)
                            .setMaxKeys(maxKey)
                            .setPrefix("");
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
                    listController.publishOn(SCAN_SCHEDULER)
                            .subscribe(i -> {
                                List<String> infoList = storagePool.getBucketVnodeList(bucketName);
                                Disposable[] disposables = new Disposable[1];
                                AtomicBoolean nextList = new AtomicBoolean(false);
                                disposables[0] = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                                        .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME))
                                        .flatMap(tuple -> {
                                            if (tuple.getT1().isEmpty() || !SWITCH_CLOSED.equals(tuple.getT1().get(DATA_SYNC_SWITCH))
                                                    || tuple.getT2().equals(tuple.getT1().get(CLUSTER_NAME))) {
                                                log.info("closed bucket need not delete obj: {}", bucketName);
                                                bucketsList.remove(bucketName);
                                                Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                        .subscribe(a -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("closed_bucket", bucketName), e -> log.error("sRem closed_bucket error"));
                                                return Mono.just(false);
                                            } else {
                                                nextList.compareAndSet(false, true);
                                                ListVersionsMergeChannel channel = (ListVersionsMergeChannel) new ListVersionsMergeChannel(bucketName, listVersionsRes, storagePool, infoList, null)
                                                        .withBeginPrefix("", "", "");
                                                channel.request(msg);
                                                return channel.response();
                                            }
                                        })
                                        .doFinally(f -> listController.onComplete())
                                        .subscribe(b -> {
                                            if (!b) {
                                                if (nextList.get()) {
                                                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple2));
                                                } else {
                                                    bucketVnodeProcessor.onComplete();
                                                }
                                                return;
                                            } else if (listVersionsRes.getVersion().size() == 0) {
                                                log.info("closed bucket delete obj complete: {}", bucketName);
                                                Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                        .subscribe(a -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("closed_bucket", bucketName), e -> log.error("sRem closed_bucket error"));
                                                bucketsList.remove(bucketName);
                                                deletingBucketNum.decrementAndGet();
                                                bucketVnodeProcessor.onComplete();
                                                return;
                                            }
                                            AtomicInteger preCount = new AtomicInteger(Math.min(maxKey, listVersionsRes.getVersion().size()));
                                            AtomicInteger dealCount = new AtomicInteger();
                                            for (VersionBase versionBase : listVersionsRes.getVersion()) {
                                                JSONObject jsonObject = new JSONObject();
                                                jsonObject.put("bucket", bucketName);
                                                jsonObject.put("no_check", bucketName);
                                                if (versionBase instanceof Version) {
                                                    Version version = (Version) versionBase;
                                                    jsonObject.put("object", version.getKey());
                                                    jsonObject.put("versionId", version.getVersionId());
                                                }
                                                if (versionBase instanceof DeleteMarker) {
                                                    checkThisComplete(bucketVnodeProcessor, listVersionsRes, preCount, dealCount);
                                                    continue;
                                                }
                                                SyncRequest<JSONObject> syncRequest = new SyncRequest<>();
                                                syncRequest.payload = jsonObject;
                                                syncRequest.res = MonoProcessor.create();
                                                syncRequest.res
                                                        .publishOn(SCAN_SCHEDULER)
                                                        .timeout(Duration.ofMinutes(5))
                                                        .doOnError(e -> log.error("bucket sync meta syncRequest res error1: {}, {}", bucketName, jsonObject.get("object"), e))
                                                        .doFinally(s -> {
                                                            dealingDeleteAmount.decrementAndGet();
                                                            syncRequest.res.dispose();
                                                            syncRequest.payload = null;
                                                        })
                                                        .subscribe(s -> {
                                                            if (!s) {
                                                                Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple2));
                                                                return;
                                                            }
                                                            checkThisComplete(bucketVnodeProcessor, listVersionsRes, preCount, dealCount);
                                                        }, e -> {
                                                            log.error("bucket sync meta syncRequest res error2: {}, {}, {}", bucketName, jsonObject.get("object"), jsonObject.get("versionId"), e);
                                                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple2));
                                                        });

                                                dealingDeleteAmount.incrementAndGet();
                                                int index = ThreadLocalRandom.current().nextInt(bsObjProcessors.length);
                                                bsObjProcessors[index].onNext(syncRequest);
                                            }
                                        }, e -> {
                                            log.error("delete obj error!", e);
                                            Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(tuple2));
                                        }, () -> {
                                            for (Disposable d : disposables) {
                                                if (null != d) {
                                                    d.dispose();
                                                }
                                            }
                                        });
                            }, e -> {
                                log.error("scanTrashRecords error2, ", e);
                            });
                    listController.onNext("1");
                }, e -> {
                    log.error("listBucketSyncObj Error2, {}", bucketName, e);
                    Mono.delay(Duration.ofSeconds(2)).publishOn(SCAN_SCHEDULER).subscribe(l -> bucketVnodeProcessor.onNext(new Tuple2<>("", "")));
                });
        bucketVnodeProcessor.onNext(new Tuple2<>("", ""));
    }

    private static void checkThisComplete(UnicastProcessor<Tuple2<String, String>> bucketVnodeProcessor, ListVersionsResult versionsResult, AtomicInteger preCount, AtomicInteger dealCount) {
        if (dealCount.incrementAndGet() == preCount.get()) {
            bucketVnodeProcessor.onNext(new Tuple2<>(versionsResult.getNextKeyMarker(), versionsResult.getNextVersionIdMarker()));
        }
    }

    public static boolean datasyncIsClosed(String bucketName) {
        String datasync = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH);
        return SWITCH_CLOSED.equals(datasync);
    }

}
