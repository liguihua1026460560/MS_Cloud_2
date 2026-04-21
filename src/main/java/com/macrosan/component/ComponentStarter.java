package com.macrosan.component;

import com.macrosan.component.compression.CompressionServerManager;
import com.macrosan.component.scanners.ComponentScanner;
import com.macrosan.component.trigger.DicomScanMetaTrigger;
import com.macrosan.component.trigger.DicomScanRecordTrigger;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;

/**
 * 外部处理模块
 */
@Log4j2
public class ComponentStarter {
    public static final ScheduledThreadPoolExecutor COMP_TIMER = new ScheduledThreadPoolExecutor(PROC_NUM, runnable -> new Thread(runnable, "comp-timer"));

    public final static Scheduler COMP_SCHEDULER;

    public final static Set<String> SUPPORT_IMAGE_FORMAT = new HashSet<>();
    public final static Set<String> SUPPORT_DICOM_FORMAT = new HashSet<>();
    public final static Set<String> SUPPORT_VIDEO_FORMAT = new HashSet<>();

    public static final long DEFAULT_MAX_IMAGE_SIZE = 32 * 1024 * 1024;
    public static volatile long MAX_IMAGE_SIZE = DEFAULT_MAX_IMAGE_SIZE;
    public static volatile boolean DICOM_SUPPORT_DESTINATION = false;

    public enum SUPPORT_IMAGE_FORMAT_ENUM {
        jpg,
        png,
        bmp,
        tiff,
        jpeg,
    }

    public enum SUPPORT_VIDEO_FORMAT_ENUM {
        mp4,
        mkv,
        mov,
        asf,
        avi,
        mxf,
        ts,
        flv,
        wmv,
    }

    static {
        Scheduler scheduler = null;
        try {
            ThreadFactory DISK_THREAD_FACTORY = new MsThreadFactory("comp-scheduler");
            MsExecutor executor = new MsExecutor(PROC_NUM * 4, 16, DISK_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        COMP_SCHEDULER = scheduler;

        for (SUPPORT_IMAGE_FORMAT_ENUM value : SUPPORT_IMAGE_FORMAT_ENUM.values()) {
            SUPPORT_IMAGE_FORMAT.add(value.name());
        }
        for (SUPPORT_VIDEO_FORMAT_ENUM value : SUPPORT_VIDEO_FORMAT_ENUM.values()) {
            SUPPORT_VIDEO_FORMAT.add(value.name());
        }
        SUPPORT_DICOM_FORMAT.add("dcm");
    }

    public static final Set<String> AVAIL_MEDIA_SERVER_IP_SET = new ConcurrentHashSet<>();
    public static final Set<String> AVAIL_MOSS_SERVER_IP_SET = new ConcurrentHashSet<>();

    public static final Set<String> ALL_IP_SET = new HashSet<>();

    /**
     * 初始化所有后端IP用于向插件发起请求，实现分布式处理
     */
    public static void initIpSet() {
        ScanArgs scanArgs = new ScanArgs().match("*");
        KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
        keyScanCursor.setCursor("0");
        KeyScanCursor<String> res;
        RedisConnPool pool = RedisConnPool.getInstance();
        do {
            res = pool.getCommand(REDIS_NODEINFO_INDEX).scan(keyScanCursor, scanArgs);
            for (String uuid : res.getKeys()) {
                String heartIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, HEART_IP);
                if (ServerConfig.getInstance().getHostUuid().equals(uuid)) {
                    heartIp = LOCAL_IP_ADDRESS;
                }
                AVAIL_MEDIA_SERVER_IP_SET.add(heartIp);
                AVAIL_MOSS_SERVER_IP_SET.add(heartIp);
                ALL_IP_SET.add(heartIp);
            }
            keyScanCursor.setCursor(res.getCursor());
        } while (!res.isFinished());

        COMP_TIMER.scheduleAtFixedRate(() -> {
            try {
                AVAIL_MEDIA_SERVER_IP_SET.addAll(ALL_IP_SET);
                AVAIL_MOSS_SERVER_IP_SET.addAll(ALL_IP_SET);
            } catch (Exception e) {

            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    public static void start() {
        initIpSet();
        TaskSender.init();
        startPeriodicRefreshConfig();
        ComponentScanner.getInstance().init();
        CompressionServerManager.getInstance().init();
        DicomScanMetaTrigger.getInstance().init();
        DicomScanRecordTrigger.getInstance().init();
    }

    public static void startPeriodicRefreshConfig() {
        COMP_TIMER.scheduleAtFixedRate(() -> {
            try {
                String maxImageSize = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget("media_config", "max_image_size");
                long tmpMaxImageSize;
                if (StringUtils.isBlank(maxImageSize)) {
                    tmpMaxImageSize = DEFAULT_MAX_IMAGE_SIZE;
                } else {
                    tmpMaxImageSize = Long.parseLong(maxImageSize);
                }
                if (tmpMaxImageSize != MAX_IMAGE_SIZE) {
                    MAX_IMAGE_SIZE = tmpMaxImageSize;
                    log.info("max image size change to {}", MAX_IMAGE_SIZE);
                }

                String dcmSupportDestination = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget("media_config", "dcm_support_destination");
                boolean tmpDicomSupportDestination = Boolean.parseBoolean(dcmSupportDestination);
                if (tmpDicomSupportDestination != DICOM_SUPPORT_DESTINATION) {
                    DICOM_SUPPORT_DESTINATION = tmpDicomSupportDestination;
                    log.info("dicom support destination change to {}", DICOM_SUPPORT_DESTINATION);
                }
            } catch (Exception e) {
                log.error("update max image size error", e);
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

}
