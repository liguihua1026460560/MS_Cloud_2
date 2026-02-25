package com.macrosan.database.rocksdb;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.SstFileManager;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Log4j2
public class MsRocksMonitor {

    public enum DiskUsageState {
        NORMAL,         // 可读可写
        FROZEN          // 冻结（禁止写入和业务删除）
    }

    // 记录被禁止写入的 RocksDB 路径
    private static final Map<String, DiskUsageState> DISK_USAGE_STATES = new ConcurrentHashMap<>();

    // 存储所有需要监控的 RocksDB 路径
    private static final Map<String, SstFileManager> MONITORED_PATHS = new ConcurrentHashMap<>();

    public static void init() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(MsRocksMonitor::checkAllRocksDBs, 0, 10, TimeUnit.SECONDS);
        log.info("RocksDB monitor started.");
    }

    /**
     * 添加一个 RocksDB 路径到监控列表中
     */
    public static void addMonitoredPath(String path, SstFileManager manager) {
        MONITORED_PATHS.put(path, manager);
    }

    /**
     * 检查所有 RocksDB 路径的磁盘空间，并更新状态
     */
    private static void checkAllRocksDBs() {
        for (Map.Entry<String, SstFileManager> path : MONITORED_PATHS.entrySet()) {
            try {
                DiskUsageState state = getDiskUsageState(path.getKey(), path.getValue());
                DISK_USAGE_STATES.put(path.getKey(), state);
                if (state == DiskUsageState.FROZEN) {
                    log.warn("Disk space critically low: RocksDB at {} is frozen.", path);
                }
            } catch (Exception e) {
                log.error("Failed to check disk space for RocksDB at {}", path, e);
            }
        }
    }

    public static boolean allowWrite(String dbPath) {
        DiskUsageState state = DISK_USAGE_STATES.getOrDefault(dbPath, DiskUsageState.NORMAL);
        return state != DiskUsageState.FROZEN;
    }

    public static boolean allowBusinessDelete(String dbPath) {
        DiskUsageState state = DISK_USAGE_STATES.getOrDefault(dbPath, DiskUsageState.NORMAL);
        return state != DiskUsageState.FROZEN;
    }

    public static void enforceWriteAllowed(String dbPath) throws MsException {
        if (!allowWrite(dbPath)) {
            throw new MsException(ErrorNo.NO_ENOUGH_SPACE, "Write blocked due to low disk space.");
        }
    }

    public static void enforceDeleteAllowed(String dbPath) throws MsException {
        if (!allowBusinessDelete(dbPath)) {
            throw new MsException(ErrorNo.NO_ENOUGH_SPACE, "Delete operation blocked due to low disk space.");
        }
    }

    private static DiskUsageState getDiskUsageState(String path, SstFileManager manager) {
        try {
            // 从 Redis 获取配置（带默认值）
            long freezeThresholdBytes = getLongConfig("rocksdb_disk_freeze_threshold_bytes", 200L << 30);
            int freezeThresholdPercent = getIntConfig("rocksdb_disk_freeze_threshold_percent", 90);
            path = "/" + path + "/rocks_db";
            long totalSstSize = manager.getTotalSize();
            File f = new File(path);

            long totalSize = f.getTotalSpace();
            // 计算允许的最大空间
            long maxAllowedSpace = Math.max(totalSize - (10L << 30), totalSize * 95 / 100);

            // 计算最终阈值：maxAllowedSpace - 256GB 或 maxAllowedSpace * 85%
            long allowUsage = Math.max(maxAllowedSpace - freezeThresholdBytes, maxAllowedSpace * freezeThresholdPercent / 100);
            if (totalSstSize >= allowUsage) {
                return DiskUsageState.FROZEN;
            } else {
                return DiskUsageState.NORMAL;
            }
        } catch (Exception e) {
            log.error("get disk usage state fail", e);
            return DiskUsageState.NORMAL;
        }
    }

    private static long getLongConfig(String key, long defaultValue) {
        try {
            String value = RedisConnPool.getInstance()
                    .getCommand(SysConstants.REDIS_SYSINFO_INDEX)
                    .hget("config_rocksdb_monitor", key);
            return value == null ? defaultValue : Long.parseLong(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static int getIntConfig(String key, int defaultValue) {
        try {
            String value = RedisConnPool.getInstance()
                    .getCommand(SysConstants.REDIS_SYSINFO_INDEX)
                    .hget("config_rocksdb_monitor", key);
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static void remove(String lun) {
        MONITORED_PATHS.remove(lun);
    }
}
