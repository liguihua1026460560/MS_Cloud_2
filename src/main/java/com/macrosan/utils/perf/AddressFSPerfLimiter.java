package com.macrosan.utils.perf;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import static com.macrosan.action.managestream.FSPerformanceService.FS_IP_SET;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

@Log4j2
public class AddressFSPerfLimiter extends BasePerfLimiter {
    private AddressFSPerfLimiter() {
        ServerConfig.getInstance().getVertx().setTimer(1000, l -> syncIps());
    }

    private static AddressFSPerfLimiter instance;

    public static AddressFSPerfLimiter getInstance() {
        if (instance == null) {
            instance = new AddressFSPerfLimiter();
        }
        return instance;
    }

    /**
     * @param key   "fsAddressPerf-" + address;
     * @param value 接口名称-性能配额类型，如nfs3_create-throughput_quota
     * @return
     */
    @Override
    protected Mono<Long> getQuotaHandler(String key, String value) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(key, value).map(Long::parseLong);
    }

    public static ConcurrentHashSet<String> addTaskSet = new ConcurrentHashSet<>();

    public static String getAddTaskKey(String bucket, String ip) {
        return bucket + "_" + ip;
    }

    // 只考虑添加ip的情况。后续如果要umount文件系统后移除ip，改用事务。
    void syncIps() {
        ScanArgs scanAllArg = new ScanArgs().match("fsAddrSet_*");
        ScanStream.scan(pool.getReactive(REDIS_SYSINFO_INDEX), scanAllArg)
                .publishOn(ErasureServer.DISK_SCHEDULER)
                .filter(key -> key.split("_").length == 2)
                .flatMap(key -> pool.getReactive(REDIS_SYSINFO_INDEX).smembers(key)
                        .map(ip -> {
                            String bucket = key.split("_")[1];
                            FS_IP_SET.computeIfAbsent(bucket, k -> new ConcurrentHashSet<>()).add(ip);
                            return new Tuple2<>(bucket, ip);
                        }))
                .doOnNext(tuple2 -> {
                    String addTaskKey = getAddTaskKey(tuple2.var1, tuple2.var2);
                    // redis已有表示是已添加过的桶和ip，跳过sadd
                    addTaskSet.remove(addTaskKey);
                })
                .doOnComplete(() -> {
                    for (String s : addTaskSet) {
                        String key = "fsAddrSet_" + s.split("_")[0];
                        Mono.just(1).publishOn(ErasureServer.DISK_SCHEDULER)
                                .subscribe(l -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(key, s.split("_")[1]));
                    }
                })
                .doOnError(log::error)
                .doFinally(s -> ErasureServer.DISK_SCHEDULER.schedule(this::syncIps, 3, TimeUnit.SECONDS))
                .subscribe();

    }


}
