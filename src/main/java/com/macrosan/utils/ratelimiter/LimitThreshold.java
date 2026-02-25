package com.macrosan.utils.ratelimiter;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_LUNINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

/**
 * @Author: WANG CHENXING
 * @Date: 2023/2/28
 * @Description: 修改LimitStrategy策略的阈值
 */

@Log4j2
public class LimitThreshold {

    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static String REDIS_KEY = "QoSThreshold";
    private static long no_limit = 0L;
    private static long limit = (long) (1024 * 1024 * 0.25);
    private static long limit_meta = 1;
    private static long adapt = 10;

    private static final Scheduler monitor = Schedulers.fromExecutor(new MsExecutor(1, 1, new MsThreadFactory("bandwidth-monitor")));
    private static final Map<String, Long> recBandwidthForDisk = new HashMap<>();

    public static void init() {//用于初始化每块磁盘的恢复流量带宽，仅针对数据盘
        try {
            RedisCommands<String, String> command = pool.getCommand(REDIS_LUNINFO_INDEX);
            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*"));

            while(iterator.hasNext()) {
                String key = iterator.next();
                String disk = key.split("@")[1];
                String node = key.split("@")[0];
                if ("data".equals(pool.getCommand(REDIS_LUNINFO_INDEX).hget(key, "role")) &&
                        ServerConfig.getInstance().getHostUuid().equals(node)) {//对于本节点的上的数据盘
                    if (pool.getCommand(REDIS_LUNINFO_INDEX).hexists(key, "recover_bandwidth")) {
                        long recBandwidth = Long.parseLong(pool.getCommand(REDIS_LUNINFO_INDEX).hget(key, "recover_bandwidth"));
                        recBandwidthForDisk.put(disk, (recBandwidth * 10 / 1000));
                    } else {
                        recBandwidthForDisk.put(disk, limit);
                    }
                }
            }
            checkDiskBandwidth();
        } catch (Exception e) {
            log.error("init recover bandwidth fail", e);
        }
    }

    private static void checkDiskBandwidth() {
        try {
            RedisCommands<String, String> command = pool.getCommand(REDIS_LUNINFO_INDEX);
            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*"));

            while(iterator.hasNext()) {
                String key = iterator.next();
                String disk = key.split("@")[1];
                String node = key.split("@")[0];
                if (pool.getCommand(REDIS_LUNINFO_INDEX).hexists(key, "recover_bandwidth") &&
                        ServerConfig.getInstance().getHostUuid().equals(node)) {//不存在recBandwidth的话表示未进行设置，仍然使用之前初始化的默认值，不进行更新
                    Map<String, String> lunInfo = pool.getCommand(REDIS_LUNINFO_INDEX).hgetall(key);
                    long recBandwidth = Long.parseLong(lunInfo.get("recover_bandwidth"));
                    if ("data".equals(lunInfo.get("role")) && recBandwidthForDisk.containsKey(disk)) {
                        if ((recBandwidth * 10 / 1000) != recBandwidthForDisk.get(disk)) {
                            recBandwidthForDisk.put(disk, (recBandwidth * 10 / 1000));//这里再转化一下/100
                            log.info("the recover bandwidth of the disk:{} has been updated to {}", disk, recBandwidth);
                        }
                    } else if ("data".equals(lunInfo.get("role")) && !recBandwidthForDisk.containsKey(disk)) {
                        recBandwidthForDisk.put(disk, (recBandwidth * 10 / 1000));
                        log.info("the recover bandwidth of the disk:{} has been updated to {}", disk, recBandwidth);
                    }
                } else if ((!pool.getCommand(REDIS_LUNINFO_INDEX).hexists(key, "recover_bandwidth")) &&
                        ServerConfig.getInstance().getHostUuid().equals(node)) {
                    if ("data".equals(pool.getCommand(REDIS_LUNINFO_INDEX).hget(key, "role")) && recBandwidthForDisk.containsKey(disk) && limit != recBandwidthForDisk.get(disk)) {
                        recBandwidthForDisk.put(disk, limit);
                        log.info("the recover bandwidth of the disk:{} has been updated to default value:{}", disk, limit);
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            monitor.schedule(LimitThreshold::checkDiskBandwidth, 10, TimeUnit.SECONDS);
        }
    }
    /**
     * 手动修改redis中，调用函数更新恢复QoS策略的底层阈值
     * */
    public static void changeThreshold(){
        try {

            Map<String, String> map = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(REDIS_KEY);
            if ( (map.get("no_limit") != null) && (map.get("limit") != null) && (map.get("limit_meta") != null) && (map.get("adapt") != null)){
                no_limit = Long.parseLong(map.get("no_limit"));
                limit = Long.parseLong(map.get("limit"));
                limit_meta = Long.parseLong(map.get("limit_meta"));
                adapt = Long.parseLong(map.get("adapt"));
            }

        } catch (Exception e) {
            no_limit = 0L;
            limit = (long) (1024 * 1024 * 0.25);
            limit_meta = 1;
            adapt = 10;
            log.error("Read Redis error, set the QoS threshold to the default initial value", e);
        }
    }

    public static long getNo_limit() {
        return no_limit;
    }

    public static void setNo_limit(long no_limit) {
        LimitThreshold.no_limit = no_limit;
    }

    public static long getLimit(String disk) {
        return recBandwidthForDisk.getOrDefault(disk, limit);
//        return limit;
    }

    public static void setLimit(long limit) {
        LimitThreshold.limit = limit;
    }

    public static long getLimit_meta() {
        return limit_meta;
    }

    public static void setLimit_meta(long limit_meta) {
        LimitThreshold.limit_meta = limit_meta;
    }

    public static long getAdapt() {
        return adapt;
    }

    public static void setAdapt(long adapt) {
        LimitThreshold.adapt = adapt;
    }
}
