package com.macrosan.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.database.rocksdb.batch.BatchRocksDB.logPrintMap;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

@Log4j2
public class PoolHealth {
    public static Set<String> repairLun = new ConcurrentSkipListSet<>();

    public static Set<String> recoverLun = new ConcurrentSkipListSet<>();

    public static Set<String> MSRocksDBErrorLun = new ConcurrentSkipListSet<>();
    public static Set<String> BatchRocksDBErrorLun = new ConcurrentSkipListSet<>();
    public static boolean isHealth(String lun, Set<String> toAddRepairLun, Set<String> toRemoveRepairLun, String node) {
        String fsName = node + "@" + lun;
        try {
            if (toAddRepairLun.contains(fsName)) {
                repairLun.add(fsName);
                BatchRocksDB.errorLun.add(lun);
                MSRocksDB.errorLun.add(lun);
                logPrintMap.add(lun);
                MSRocksDB.remove(lun);
                return false;
            }
            if (toRemoveRepairLun.contains(fsName)){
                recoverLun.add(lun);
                MSRocksDB.getRocksDB(lun);
                BatchRocksDB.errorLun.remove(lun);
                MSRocksDB.errorLun.remove(lun);
                repairLun.remove(fsName);
                logPrintMap.remove(lun);
                recoverLun.remove(lun);
            }
            boolean health = MSRocksDB.getRocksDB(lun) != null && !MSRocksDB.errorLun.contains(lun) && !BatchRocksDB.errorLun.contains(lun);
            StoragePool pool = StoragePoolFactory.getStoragePoolByDisk(lun);
            //走到这里说明rocksdb打开没问题
            RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).srem("open_error_lun", fsName);
            if (null == pool) {
                log.info("getStoragePoolByDisk error {}", lun);
                return health;
            }

            //缓存盘的第一个分区，可使用容量小于20%的时候，认为不健康。
            if (pool.getVnodePrefix().startsWith("cache")) {
                File f = new File("/" + lun + "/");
                long total = f.getTotalSpace();
                long usable = f.getUsableSpace();
                if (usable * 100 / total < 20) {
                    health = false;
                }
            }

            return health;
        } catch (Exception e) {
            Boolean exist = RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).sismember("open_error_lun", fsName);
            if (!exist){
                RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).sadd("error_lun", fsName);
                RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).sadd("open_error_lun", fsName);
            }
            return false;
        }
    }

    private static final String CUR_NODE = ServerConfig.getInstance().getHostUuid();

    private static void checkDiskHealth(StoragePool pool) {
        Set<String> healthDisk = new HashSet<>();
        //更新repairLun
        Set<String> newRepairLun = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_POOL_INDEX).smembers("repair_pool_lun");
        //需要新增的维护盘（newRepairLun-repairLun)
        Set<String> toAdd = new HashSet<>(newRepairLun);
        toAdd.removeAll(repairLun);
        //需要删除的维护盘(repairLun-newRepairLun)
        Set<String> toRemove = new HashSet<>(repairLun);
        toRemove.removeAll(newRepairLun);

        for (String disk : pool.getCache().lunSet) {
            if (!RemovedDisk.getInstance().contains(disk)) {
                String uuid = disk.split("@")[0];
                if (uuid.equalsIgnoreCase(CUR_NODE)) {
                    if (isHealth(disk.split("@")[1], toAdd, toRemove, uuid)) {
                        healthDisk.add(disk);
                    }
                }
            }
        }
        checkErrorLun();
        RedisConnPool.getInstance()
                .getShortMasterCommand(SysConstants.REDIS_POOL_INDEX)
                .setex("health_disk_" + pool.getVnodePrefix() + "_" + CUR_NODE, 30, Json.encode(healthDisk));
    }

    private static final TypeReference<Set<String>> SetStringType = new TypeReference<Set<String>>() {
    };

    public static void updateHealth(StoragePool pool) {
        try {
            checkDiskHealth(pool);
            Set<String> healthDisk = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_POOL_INDEX)
                    .keys("health_disk_" + pool.getVnodePrefix() + "_*")
                    .stream()
                    .map(key -> {
                        String value = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_POOL_INDEX).get(key);
                        return value == null ? "[]" : value;
                    })
                    .flatMap(s -> Json.decodeValue(s, SetStringType).stream())
                    .collect(Collectors.toSet());

            int unHealthNum = 0;
            Map<String, Tuple2<Integer, Integer>> nodeMap = new HashMap<>();

            for (String disk : pool.getCache().lunSet) {
                if (!RemovedDisk.getInstance().contains(disk)) {
                    String node = disk.split("@")[0];
                    Tuple2<Integer, Integer> tuple = nodeMap.get(node);
                    if (null == tuple) {
                        tuple = new Tuple2<>(0, 0);
                        nodeMap.put(node, tuple);
                    }

                    if (healthDisk.contains(disk)) {
                        tuple.var1++;
                    } else {
                        unHealthNum++;
                        tuple.var2++;
                    }
                }
            }

            HealthState state;
            if (unHealthNum == 0) {
                state = HealthState.Health;
            } else {
                int link = pool.getK() + pool.getM();
                int max;
                if (link % nodeMap.size() == 0) {
                    max = link / nodeMap.size();
                } else {
                    max = link / nodeMap.size() + 1;
                }

                int n = 0;
                for (Tuple2<Integer, Integer> tuple : nodeMap.values()) {
                    n += Math.min(max, tuple.var2);
                }

                if (n <= pool.getM()) {
                    state = HealthState.Down;
                } else {
                    n = 0;
                    for (Tuple2<Integer, Integer> tuple : nodeMap.values()) {
                        n += Math.min(max, tuple.var1);
                    }

                    if (n >= pool.getK()) {
                        state = HealthState.Fault;
                    } else {
                        state = HealthState.Broken;
                    }
                }
            }

            pool.setState(state);
            log.debug("{} {}", pool.getVnodePrefix(), state);
        } catch (Exception e) {
            log.error("update pool health fail {}", pool.getVnodePrefix(), e);
        }

        DISK_SCHEDULER.schedule(() -> updateHealth(pool), 30, TimeUnit.SECONDS);
    }

    public static void checkErrorLun() {
        //找到不能写rocksdb的盘（排除维护中的盘）
        for (String lun : MSRocksDB.errorLun){
            String fsName = CUR_NODE + "@" + lun;
            if (!repairLun.contains(fsName) && !MSRocksDBErrorLun.contains(fsName)){
                RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).sadd("error_lun", fsName);
                MSRocksDBErrorLun.add(fsName);
            }
        }
        for (String lun : BatchRocksDB.errorLun){
            String fsName = CUR_NODE + "@" + lun;
            if (!repairLun.contains(fsName) && !BatchRocksDBErrorLun.contains(fsName)){
                RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_POOL_INDEX).sadd("error_lun", fsName);
                BatchRocksDBErrorLun.add(fsName);
            }
        }
        MSRocksDBErrorLun.removeIf(fsName -> !MSRocksDB.errorLun.contains(fsName.split("@")[1]));
        BatchRocksDBErrorLun.removeIf(fsName -> !BatchRocksDB.errorLun.contains(fsName.split("@")[1]));
    }

    public enum HealthState {
        Health, Down, Fault, Broken,
    }
}
