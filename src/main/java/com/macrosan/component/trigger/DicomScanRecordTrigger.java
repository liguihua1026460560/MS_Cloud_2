package com.macrosan.component.trigger;

import com.macrosan.component.ComponentUtils;
import com.macrosan.component.TaskSender;
import com.macrosan.component.compression.CompressionServerManager;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.scanners.ComponentRecordScanner;
import com.macrosan.component.scanners.RedisKeyScanner;
import com.macrosan.ec.Utils;
import com.macrosan.utils.ModuleDebug;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.component.ComponentStarter.ALL_IP_SET;
import static com.macrosan.component.ComponentStarter.COMP_TIMER;
import static com.macrosan.component.compression.ImageCompressionProcessor.MAX_WAIT_QUEUE_SIZE;
import static com.macrosan.component.scanners.ComponentScanner.KEEP_SCANING;
import static com.macrosan.constants.SysConstants.DICOM_COMPRESS_BUCKET_KEY_PREFIX;

/**
 * DICOM压缩处理记录扫描触发器
 * - 定时触发处理记录的扫描
 *
 * @author zhaoyang
 * @date 2026/04/15
 **/
@Log4j2
public class DicomScanRecordTrigger {
    private static DicomScanRecordTrigger instance;
    private final ComponentRecordScanner recordScanner;
    private final RedisKeyScanner redisKeyScanner;
    private final CompressionServerManager compressionServerManager;

    public static DicomScanRecordTrigger getInstance() {
        if (instance == null) {
            instance = new DicomScanRecordTrigger();
        }
        return instance;
    }

    private DicomScanRecordTrigger() {
        this.recordScanner = ComponentRecordScanner.getInstance();
        this.redisKeyScanner = new RedisKeyScanner(DICOM_COMPRESS_BUCKET_KEY_PREFIX + "*", 10);
        this.compressionServerManager = CompressionServerManager.getInstance();
    }

    public void init() {
        scheduleScan(10, TimeUnit.SECONDS);

    }

    private void scheduleScan(long delay, TimeUnit timeUnit) {
        COMP_TIMER.schedule(this::scanRecord, delay, timeUnit);
    }

    private void scanRecord() {
        try {
            //主站点扫描
            if (!"master".equals(Utils.getRoleState())) {
                KEEP_SCANING.compareAndSet(true, false);
                scheduleScan(1, TimeUnit.MINUTES);
                return;
            }

            KEEP_SCANING.compareAndSet(false, true);
            int allServerTotalConcurrent = compressionServerManager.getAllServerTotalConcurrent();
            int totalWaitQueueSize = ALL_IP_SET.size() * MAX_WAIT_QUEUE_SIZE;
            int maxTotalThroughput = allServerTotalConcurrent + totalWaitQueueSize;

            double threshold = maxTotalThroughput * 0.8;

            if (getProcessingRecordCount() > threshold) {
                if (ModuleDebug.mediaComponentDebug()) {
                    log.info("processing record count:{} maxTotalThroughput:{} , wait 10s", getProcessingRecordCount(), maxTotalThroughput);
                }
                scheduleScan(10, TimeUnit.SECONDS);
                return;
            }

            if (ModuleDebug.mediaComponentDebug()) {
                log.info("start scan dicom record, processing record count:{} maxTotalThroughput:{}", getProcessingRecordCount(), maxTotalThroughput);
            }
            AtomicBoolean shouldStop = new AtomicBoolean();
            redisKeyScanner.scanKey()
                    .map(key -> key.substring(DICOM_COMPRESS_BUCKET_KEY_PREFIX.length()))
                    .concatMap(bucket -> doscan(bucket, shouldStop, threshold), 1)
                    .doFinally(s -> {
                        if (getProcessingRecordCount() == 0 && redisKeyScanner.isScanComplete()) {
                            // 所有桶都没有扫描到数据
                            scheduleScan(1, TimeUnit.MINUTES);
                        } else if (!redisKeyScanner.isScanComplete() && getProcessingRecordCount() < threshold) {
                            // 桶没有扫描完且扫描到的数据不够，则继续扫描
                            scheduleScan(10, TimeUnit.MILLISECONDS);
                        } else {
                            scheduleScan(10, TimeUnit.SECONDS);
                        }
                    })
                    .subscribe(TaskSender::distributeMultiMediaRecord);

        } catch (Exception e) {
            log.error("regular component scan error, ", e);
            scheduleScan(10, TimeUnit.SECONDS);
        }
    }

    private Flux<ComponentRecord> doscan(String bucket, AtomicBoolean shouldStop, double threshold) {
        if (shouldStop.get()) {
            return Flux.empty();
        }
        return recordScanner.scanRecord(ComponentRecord.Type.DICOM, bucket, DicomScanMetaTrigger.DICOM_RECORD_MARKER)
                .doOnComplete(() -> {
                    if (getProcessingRecordCount() >= threshold) {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("bucket {} done, processingCount:{} >= threshold:{}, stop", bucket, getProcessingRecordCount(), threshold);
                        }
                        shouldStop.set(true);
                    }
                });
    }


    public int getProcessingRecordCount() {
        return ComponentUtils.checkDicomRecordMap.size();
    }

}
