package com.macrosan.ec.rebuild;

import com.google.common.collect.Sets;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.ServerConfig;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;


import java.util.HashSet;
import java.util.List;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.database.rocksdb.MSRocksDB.*;

/**
 * @author zhaoyang
 * @date 2025/06/18
 **/
@Log4j2
public class DiskStatusChecker {
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();
    public static final String REBUILD_WAITER_QUEUE = "rebuild_waiter_queue_";
    private static final Set<String> rebuildWaiter = new ConcurrentSkipListSet<>();


    public static final String LOCAL_UUID = ServerConfig.getInstance().getHostUuid();

    public static void startCheck() {
        checkRebuildWaiter();
        ServerConfig.getInstance().getVertx().setPeriodic(10_000, timeId -> {
            checkRebuildWaiter();
        });
    }

    private static void checkRebuildWaiter() {
        try {
            Set<String> waiter = new HashSet<>();
            List<String> queues = redisConnPool.getCommand(REDIS_POOL_INDEX).keys(REBUILD_WAITER_QUEUE + "*");
            for (String queue : queues) {
                Set<String> diskNames = redisConnPool.getCommand(REDIS_POOL_INDEX).smembers(queue);
                waiter.addAll(diskNames.stream().filter(diskName -> diskName.startsWith(LOCAL_UUID)).map(diskName -> diskName.split("@")[1]).collect(Collectors.toSet()));
            }
            if (waiter.isEmpty()) {
                return;
            }
            Sets.SetView<String> newRebuildWaiterSet = Sets.difference(waiter, rebuildWaiter);
            if (newRebuildWaiterSet.isEmpty()){
                return;
            }
            log.info("add lun:{} to rebuild waiter", newRebuildWaiterSet);
            rebuildWaiter.addAll(waiter);
            // 移除rocksdb 实例
            for (String disk : newRebuildWaiterSet) {
                log.info("lun:{} in rebuild waiter queue", disk);
                MSRocksDB.remove(disk);
                if (disk.contains("index")) {
                    MSRocksDB.remove(getSyncRecordLun(disk));
                    MSRocksDB.remove(getComponentRecordLun(disk));
                    MSRocksDB.remove(getRabbitmqRecordLun(disk));
                    MSRocksDB.remove(getSTSTokenLun(disk));
                    MSRocksDB.remove(getAggregateLun(disk));
                } else {
                    BlockDevice.remove(disk);
                }
            }
        } catch (Exception e) {
            log.error("DiskStatusChecker error", e);
        }
    }

    /**
     * 判断磁盘是否在重构等待队列中
     * <p>重构队列中的磁盘肯定是需要重构的，因此该队列中的磁盘将会：
     * 1、移除rocksdb实例，且不会重新创建
     * 2、移除BlockDevice，且不会重新创建
     * 3、该盘新产生的修复消息不再发往mq中，已存在的消息直接ack
     *
     * @param lun fs-SP0-xxx
     * @return true:在重构等待队列中
     */
    public static boolean isRebuildWaiter(String lun) {
        return rebuildWaiter.contains(lun);
    }


    @Getter
    public enum DISK_STATUS {
        NORMAL(0),
        FAULT(1),
        OFFLINE(2);
        private final int status;

        DISK_STATUS(int status) {
            this.status = status;
        }

        public static DISK_STATUS parseStatus(int status) {
            for (DISK_STATUS value : values()) {
                if (value.status == status) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Invalid DISK_STATUS: " + status);
        }
    }
}
