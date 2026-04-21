package com.macrosan.component.trigger;

import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.pojo.ComponentStrategy;
import com.macrosan.component.scanners.ComponentRecordScanner;
import com.macrosan.component.scanners.ComponentScanner;
import com.macrosan.component.scanners.MetaDataScanner;
import com.macrosan.component.scanners.RedisKeyScanner;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.ModuleDebug;
import com.macrosan.utils.functional.Tuple2;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.COMP_SCHEDULER;
import static com.macrosan.component.ComponentStarter.COMP_TIMER;
import static com.macrosan.component.pojo.ComponentRecord.Type.DICOM;
import static com.macrosan.component.scanners.ComponentScanner.KEEP_SCANING;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.COUNT_BUCKET_DICOM_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;


/**
 * DICOM压缩 元数据扫描触发器
 * - 定时扫描开启dicom压缩的桶，扫描桶中元数据并生成待处理记录
 *
 * @author zhaoyang
 * @date 2026/04/15
 **/
@Log4j2
public class DicomScanMetaTrigger {
    private static final RedisConnPool POOL = RedisConnPool.getInstance();

    public static final int MAX_BUCKET_PENDING_THRESHOLD = 3000;
    private static final int MAX_PENDING_THRESHOLD = MAX_BUCKET_PENDING_THRESHOLD * 5;
    private static final int SCAN_BATCH_SIZE = 1000;
    public static final String DICOM_RECORD_MARKER = "dcm_compress";
    private static final String MARKER_FIELD = "dicom_scan_marker";

    private final AtomicLong oneRoundScanMetaCount = new AtomicLong();


    private static DicomScanMetaTrigger instance;

    private final MetaDataScanner metaDataScanner;
    private final ComponentStrategy defaultStrategy;
    private final RedisKeyScanner redisKeyScanner;

    public static DicomScanMetaTrigger getInstance() {
        if (instance == null) {
            instance = new DicomScanMetaTrigger();
        }
        return instance;
    }

    private DicomScanMetaTrigger() {
        this.metaDataScanner = MetaDataScanner.getInstance();
        this.defaultStrategy = createDefaultStrategy();
        this.redisKeyScanner = new RedisKeyScanner(DICOM_COMPRESS_BUCKET_KEY_PREFIX + "*", 10);
    }

    private ComponentStrategy createDefaultStrategy() {
        ComponentStrategy strategy = new ComponentStrategy();
        strategy.setType(DICOM);
        strategy.setStrategyName("dicom_compress");
        strategy.setStrategyMark(DICOM_RECORD_MARKER);
        strategy.setProcess("compress,c_true");
        strategy.setCopyUserMetaData(true);
        return strategy;
    }

    public void init() {
        scheduleScan(10, TimeUnit.SECONDS);
    }


    private void scheduleScan(long delay, TimeUnit timeUnit) {
        COMP_TIMER.schedule(this::scanMeta, delay, timeUnit);
    }


    private void scanMeta() {
        try {
            if (!"master".equals(Utils.getRoleState())) {
                KEEP_SCANING.compareAndSet(true, false);
                scheduleScan(1, TimeUnit.MINUTES);
                return;
            }
            KEEP_SCANING.compareAndSet(false, true);
            oneRoundScanMetaCount.set(0);
            if (ModuleDebug.mediaComponentDebug()) {
                log.info("schedule dicom scan bucket metadata");
            }
            redisKeyScanner.scanKey()
                    .flatMap(this::processBucket, 10)
                    .doOnError(log::error)
                    .doFinally(s -> {
                        if (!redisKeyScanner.isScanComplete() && oneRoundScanMetaCount.get() < MAX_PENDING_THRESHOLD) {
                            scheduleScan(50, TimeUnit.MILLISECONDS);
                        } else {
                            scheduleScan(1, TimeUnit.MINUTES);
                        }
                    })
                    .subscribe(oneRoundScanMetaCount::addAndGet);
        } catch (Exception e) {
            scheduleScan(1, TimeUnit.MINUTES);
        }
    }


    /**
     * 处理单个桶
     */
    private Mono<Integer> processBucket(String key) {
        String bucket = key.substring(DICOM_COMPRESS_BUCKET_KEY_PREFIX.length());
        if (ModuleDebug.mediaComponentDebug()) {
            log.info("process {} , start scan metadata", bucket);
        }
        return getBucketPendingRecordCount(bucket)
                .filter(pendingCount -> pendingCount < MAX_BUCKET_PENDING_THRESHOLD)
                .flatMap(pendingCount -> getBucketMarker(bucket))
                .flatMap(mark -> metaDataScanner.scanMeta(bucket, mark, SCAN_BATCH_SIZE).zipWith(Mono.just(mark)))
                .map(t2 -> processMetaDataList(t2.getT1(), bucket, t2.getT2()))
                .doOnError(e -> log.error("process bucket {} error", bucket, e));
    }


    private int processMetaDataList(List<Tuple2<String, MetaData>> list, String bucket, String marker) {
        if (list.isEmpty()) {
            return 0;
        }
        String lastKey = null;
        int count = 0;
        for (Tuple2<String, MetaData> tuple : list) {
            MetaData metaData = tuple.var2();
            if (!tuple.var1().equals(marker) && shouldProcess(metaData)) {
                ComponentScanner.distributeMetaData(DICOM_RECORD_MARKER, defaultStrategy, metaData);
                count++;
            }
            lastKey = tuple.var1();
        }
        updateBucketMarker(bucket, lastKey);
        if (ModuleDebug.mediaComponentDebug()) {
            log.info("scan meta result  bucket:{} size:{}", bucket, count);
        }
        return count;
    }


    private boolean shouldProcess(MetaData metaData) {
        if (metaData.equals(MetaData.ERROR_META) || metaData.equals(MetaData.NOT_FOUND_META)
                || metaData.deleteMark || metaData.deleteMarker) {
            return false;
        }
        if (metaData.key.endsWith(File.separator)) {
            return false;
        }

        return ComponentUtils.isSupportFormat(metaData.key, ComponentRecord.Type.VIDEO);
    }

    private void updateBucketMarker(String bucket, String marker) {
        Mono.just(1)
                .publishOn(COMP_SCHEDULER)
                .subscribe(b -> {
                    POOL.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, MARKER_FIELD, marker);
                });
    }

    private Mono<String> getBucketMarker(String bucket) {
        return POOL.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, MARKER_FIELD)
                .defaultIfEmpty("");
    }

    /**
     * 查询 RocksDB 中待处理的记录数量
     */
    private Mono<Integer> getBucketPendingRecordCount(String bucket) {
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucket);
        return Flux.fromIterable(metaPool.getBucketVnodeList(bucket))
                .flatMap(vnode -> getVnodePendingRecordCount(bucket, vnode, metaPool))
                .collectList()
                .map(list -> list.stream().mapToInt(l -> l).sum());
    }

    private static Mono<Integer> getVnodePendingRecordCount(String bucket, String vnode, StoragePool metaPool) {
        String prefix = ComponentRecord.rocksKeyPrefix(bucket, DICOM.name(), DICOM_RECORD_MARKER, vnode);
        return metaPool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple3 -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("prefix", prefix)
                                    .put("lun", MSRocksDB.getIndexRocksDBLun(tuple3.var2, MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB))
                            )
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<Integer> responseInfo = ClientTemplate.oneResponse(msgs, COUNT_BUCKET_DICOM_RECORD, Integer.class, nodeList);
                    MonoProcessor<Integer> res = MonoProcessor.create();
                    int[] count = new int[]{0};
                    responseInfo.responses.subscribe(payload -> {
                        if (payload.getVar2().equals(SUCCESS)) {
                            count[0] = Math.max(payload.getVar3(), count[0]);
                        }
                    }, res::onError, () -> {
                        if (responseInfo.successNum < metaPool.getK()) {
                            res.onError(new RuntimeException("get vnode pending record count error"));
                        } else {
                            res.onNext(count[0]);
                        }
                    });
                    return res;
                });
    }
}