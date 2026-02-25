package com.macrosan.doubleActive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.getSyncRecordLun;
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.getStampFromRecord;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.syncStamp2DaPrefix;
import static com.macrosan.httpserver.MossHttpClient.INDEX_IPS_ENTIRE_MAP;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;

/**
 * 每个节点单独扫描自己的索引盘，查找并删除“|”开头且桶名不存在的record。
 *
 * @author fanjunxi
 */
@Log4j2
public class RecordScanChecker {

    private static RecordScanChecker instance;

    public static RecordScanChecker getInstance() {
        if (instance == null) {
            instance = new RecordScanChecker();
        }
        return instance;
    }

    public static Set<String> indexLunList = new HashSet<>();

    ScheduledFuture<?> scheduledFuture;

    static AtomicBoolean isSyncing = new AtomicBoolean();

    public void init() {
        Optional.ofNullable(scheduledFuture).ifPresent(s -> s.cancel(false));
        scheduledFuture = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                // eth4异常时不允许进行筛除，防止eth7更新不及时
                if (!HeartBeatChecker.AVAIL_BACKEND_IP_ENTIRE_SET.contains(ServerConfig.getInstance().getHeartIp1())) {
                    stop();
                    return;
                }

                // 有一个站点未同步完成，就不进行记录筛除。
                String hget = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE);
                if (StringUtils.isBlank(hget)) {
                    return;
                }

                Map<Integer, DATA_SYNC_STATE> syncStateMap = Json.decodeValue(hget, new TypeReference<Map<Integer, DATA_SYNC_STATE>>() {
                });
                boolean allSynced = true;
                for (DATA_SYNC_STATE state : syncStateMap.values()) {
                    if (state != DATA_SYNC_STATE.SYNCED) {
                        allSynced = false;
                        break;
                    }
                }

                if (allSynced) {
                    if (isSyncing.get()) {
                        log.debug("already siftout.");
                        return;
                    }
                    isSyncing.compareAndSet(false, true);
                    log.debug("allSynced, start siftout.");
                    Disposable disposable = scanTrashRecords().doFinally(b -> {
                        indexLunList.clear();
                        isSyncing.compareAndSet(true, false);
                    }).subscribe();
                    disposableSet.add(disposable);
                } else {
                    // 不是全部SYNCED，如果有线程在筛除，需要中断，防止不停扫描。
                    if (isSyncing.get() && !disposableSet.isEmpty()) {
                        stop();
                    }
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }, 300, 60, TimeUnit.SECONDS);
    }

    static final Set<Disposable> disposableSet = new ConcurrentHashSet<>();

    public void close() {
        Optional.of(scheduledFuture).ifPresent(scheduledFuture1 -> scheduledFuture1.cancel(true));
    }

    void stop() {
        log.info("Stop siftout.");
        synchronized (disposableSet) {
            DoubleActiveUtil.streamDispose(disposableSet);
            disposableSet.clear();
            isSyncing.compareAndSet(true, false);
        }
    }

    Mono<Boolean> scanTrashRecords() {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        try {
            String node = ServerConfig.getInstance().getHostUuid();
            ScanIterator<String> iterator = ScanIterator.scan(RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX), new ScanArgs().match(node + "@fs*"));
            while (iterator.hasNext()) {
                String lunName = iterator.next().split("@")[1];
                if (lunName.endsWith("index")) {
                    indexLunList.add(lunName);
                }
            }

            AtomicInteger completeCount = new AtomicInteger();
            indexLunList.forEach(lun -> {
                UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
                Disposable subscribe = listController.publishOn(SCAN_SCHEDULER).subscribe(marker -> {
                            if (StringUtils.isBlank(marker)) {
                                listController.onComplete();
                                return;
                            }
                            // eth4异常时不允许进行筛除，防止eth7更新不及时
                            if (!HeartBeatChecker.AVAIL_BACKEND_IP_ENTIRE_SET.contains(ServerConfig.getInstance().getHeartIp1())) {
                                stop();
                                return;
                            }
                            siftOutNoneBucUnsyncRecord(1000, lun, marker)
                                    .subscribe(listController::onNext, e -> log.error("scanTrashRecords error1, ", e));
                        },
                        e -> {
                            log.error("scanTrashRecords error2, ", e);
                            res.onError(e);
                        },
                        () -> {
                            if (completeCount.incrementAndGet() >= indexLunList.size()) {
                                log.debug("finish scan useless unsyncrecord.");
                                res.onNext(true);
                            }
                        }
                );
                if (!isSyncing.get()) {
                    subscribe.dispose();
                }
                disposableSet.add(subscribe);
                listController.onNext(ROCKS_UNSYNCHRONIZED_KEY);
            });
        } catch (Exception e) {
            log.error("", e);
            res.onError(e);
        }
        return res;
    }

    public static Mono<String> siftOutNoneBucUnsyncRecord(int maxKey, String lun, String marker) {
        MonoProcessor<String> res = MonoProcessor.create();
        String nextMarker = "";
        String recordLun = getSyncRecordLun(lun);
        MSRocksDB db = MSRocksDB.getRocksDB(recordLun);
        try {

            try (Slice lowerSlice = new Slice(marker.getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice);
                 MSRocksIterator iterator = db.newIterator(readOptions)) {
                iterator.seek(marker.getBytes());
                int count = 0;
                while (iterator.isValid() && new String(iterator.key()).startsWith(ROCKS_UNSYNCHRONIZED_KEY)) {
                    if (!isSyncing.get()) {
                        break;
                    }
                    UnSynchronizedRecord record = Json.decodeValue(new String(iterator.value()), UnSynchronizedRecord.class);
                    count++;
                    String stamp = getStampFromRecord(record);
                    // 生成时间不足300s的记录不做处理。（更新bucket_sync_set的周期为两分钟。）
                    if (StringUtils.isNotBlank(stamp)) {
                        int nodeNum = INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
                        int nodeAmount = 300 * INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
                        // 没到时间的预提交记录暂不不处理
                        if ((System.currentTimeMillis() - Long.parseLong(stamp)) * nodeNum < nodeAmount) {
                            iterator.next();
                            continue;
                        }
                    } else {
                        String versionStamp = syncStamp2DaPrefix(record.versionNum);
                        String currStamp = syncStamp2DaPrefix(VersionUtil.getVersionNum(false));
                        // EC_version一秒钟+nodeAmount，客户端速度为1MB/s，此即为5G的对象上传完毕前后的EC_version的变化量。
                        int nodeAmount = 300 * INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
                        if (Long.parseLong(currStamp) - Long.parseLong(versionStamp) < nodeAmount) {
                            iterator.next();
                            continue;
                        }
                    }
                    // record.bucket不存在，且为未提交记录
                    if (!SYNC_BUCKET_STATE_MAP.get(LOCAL_CLUSTER_INDEX).containsKey(record.bucket)) {
                        deleteRecord(recordLun, record.rocksKey());
                    }
                    nextMarker = record.rocksKey();
                    if (count >= maxKey) {
                        res.onNext(nextMarker);
                        return res;
                    }
                    iterator.next();
                }

                // 记录扫完了，结束本次list
                res.onNext("");
                return res;
            }
        } catch (Exception e) {
            log.error("", e);
            res.onError(e);
            return res;
        }
    }

    static void deleteRecord(String lun, String key) {
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            writeBatch.delete(key.getBytes());
        };

        BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .doOnError(e -> log.error("", e))
                .subscribe();
    }

}
