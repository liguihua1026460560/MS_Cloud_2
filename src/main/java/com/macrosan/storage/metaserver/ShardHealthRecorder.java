package com.macrosan.storage.metaserver;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.VnodeCache;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className ShardHealthRecorder
 * @date 2023/3/15 15:37
 */
@Log4j2
public class ShardHealthRecorder {

    private static final RedisConnPool POOL = RedisConnPool.getInstance();
    private static final long UPDATE_TIME = 300 * 1000L;
    private static final ScheduledThreadPoolExecutor INDEX_DISK_SCHEDULE = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "shard-health"));

    public static void init() {
        INDEX_DISK_SCHEDULE.schedule(ShardHealthRecorder::updateIndexDiskUsedSize, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 定时更新当前节点索引盘第一分区的使用率
     */
    public static void updateIndexDiskUsedSize() {
        try {
            RedisCommands<String, String> command = POOL.getCommand(SysConstants.REDIS_LUNINFO_INDEX);
            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*-index").limit(10));
            while (iterator.hasNext()) {
                String lun = iterator.next();
                String[] split = lun.split("@");
                String uuid = split[0];
                String disk = split[1];
                if (ServerConfig.getInstance().getHostUuid().equals(uuid)) {
                    File file = new File("/" + disk + "/");
                    BigDecimal usableSpace = BigDecimal.valueOf(file.getUsableSpace());
                    BigDecimal totalSpace = BigDecimal.valueOf(file.getTotalSpace());
                    double rate = usableSpace.divide(totalSpace, 2, RoundingMode.UP).doubleValue();
                    double useRate = rate * 100;
                    POOL.getShortMasterCommand(SysConstants.REDIS_LUNINFO_INDEX).hset(lun, "usable_rate", String.valueOf(useRate));
                }
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            INDEX_DISK_SCHEDULE.schedule(ShardHealthRecorder::updateIndexDiskUsedSize, UPDATE_TIME, TimeUnit.MILLISECONDS);
        }
    }

    public static boolean shardingIsHealth(String bucketName, String vnode) {
        boolean isHealth = true;
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        VnodeCache cache = pool.getCache();
        String linkString = cache.hget(pool.getVnodePrefix() + vnode, "link");
        String[] linkArray = Json.decodeValue(linkString, String[].class);
        for (String link : linkArray) {
            String sUuid = cache.hget(pool.getVnodePrefix() + link, "s_uuid");
            String lunName = cache.hget(pool.getVnodePrefix() + link, "lun_name");
            String lun = sUuid + "@" + lunName;
            String usableRate = Optional.ofNullable(POOL.getCommand(SysConstants.REDIS_LUNINFO_INDEX).hget(lun, "usable_rate"))
                    .orElse("0");
            double parseDouble = Double.parseDouble(usableRate);
            if (parseDouble <= 15) {
                isHealth = false;
                break;
            }
        }
        return isHealth;
    }
}
