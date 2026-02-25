package com.macrosan.doubleActive.deployment;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.migrate.SstVnodeDataScannerUtils;
import com.macrosan.httpserver.ServerConfig;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.Checkpoint;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.REDIS_LUNINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.database.rocksdb.MSRocksDB.getSyncRecordLun;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.deployment.AddClusterUtils.node_his_sync;

/**
 * 3DC的环境历史数据同步时需要将已有差异记录复制一份发给复制站点，
 */
@Log4j2
public class RocksDBCheckPoint {

    static final String HIS_DB_PATH = "_checkPoint";

    static Map<Integer, Set<String>> dbMap = new ConcurrentHashMap<>();

    public static void init(Integer index) {
        String node = ServerConfig.getInstance().getHostUuid();
        AtomicBoolean res = new AtomicBoolean(true);
        ScanStream.scan(RedisConnPool.getInstance().getReactive(REDIS_LUNINFO_INDEX), new ScanArgs().match(node + "@fs*-index"))
                .publishOn(SCAN_SCHEDULER)
                .map(lun -> lun.split("@")[1])
                .doOnNext(lun -> {
                    if (!createCheckPointAllNodes(lun, index)) {
                        res.compareAndSet(true, false);
                    }
                })
                .doOnComplete(() -> {
                    if (res.get()) {
                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(node_his_sync, index + "_" + node, "1");
                    } else {
                        Mono.delay(Duration.ofSeconds(10)).subscribe(s -> init(index));
                    }
                }).subscribe();
    }

    public static boolean createCheckPointAllNodes(String lun, Integer index) {
        String checkPointLun = getCheckPointLun(lun, index);
        if (dbMap.computeIfAbsent(index, s -> new ConcurrentHashSet<>()).contains(checkPointLun)) {
            log.info("checkPoint already exists, {}", checkPointLun);
            return true;
        }
        MSRocksDB msRocksDB = MSRocksDB.getRocksDB(getSyncRecordLun(lun));
        String cpPath = "/" + checkPointLun;
        Checkpoint checkpoint = Checkpoint.create(msRocksDB.getRocksDB());
        try {
            checkpoint.createCheckpoint(cpPath);
            MSRocksDB.getRocksDB(checkPointLun);
            dbMap.get(index).add(checkPointLun);
            log.info("create checkpoint: {}", cpPath);
            return true;
        } catch (Exception e) {
            log.error("createCheckPoint error, {}, {}", lun, index, e);
            if ("Directory exists".equals(e.getMessage())) {
                SstVnodeDataScannerUtils.removeCheckPoint(cpPath);
            }
            return false;
        }
    }

    public static String getCheckPointLun(String lun, int index) {
        return getSyncRecordLun(lun) + HIS_DB_PATH + index;
    }

    public static void close(int index) {
        if (dbMap.get(index)==null){
            return;
        }

        dbMap.get(index).forEach(path -> {
            MSRocksDB.remove(path);
            SstVnodeDataScannerUtils.removeCheckPoint("/" + path);
        });
    }

    /**
     * 关闭所有checkPoint，从节点用于在历史数据同步全部结束后调用。
     *
     * @param indexHisSyncMap
     */
    public static void closeAll(Map<Integer, Integer> indexHisSyncMap) {
        String node = ServerConfig.getInstance().getHostUuid();
        ScanStream.scan(RedisConnPool.getInstance().getReactive(REDIS_LUNINFO_INDEX), new ScanArgs().match(node + "@fs*-index"))
                .publishOn(SCAN_SCHEDULER)
                .map(lun -> lun.split("@")[1])
                .doOnNext(lun -> {
                    for (Integer index : indexHisSyncMap.keySet()) {
                        String checkPointLun = getCheckPointLun(lun, index);
                        File file = new File("/" + checkPointLun);
                        if (file.exists()) {
                            MSRocksDB.remove(checkPointLun);
                            SstVnodeDataScannerUtils.removeCheckPoint("/" + checkPointLun);
                            log.info("delete checkpoint under {}", lun);
                        }
                    }
                })
                .doOnError(log::error)
                .subscribe();
    }


}
