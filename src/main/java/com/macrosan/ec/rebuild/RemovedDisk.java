package com.macrosan.ec.rebuild;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Set;

@Log4j2
public class RemovedDisk {
    Set<String> set = new ConcurrentHashSet<>();
    private static RemovedDisk instance = new RemovedDisk();

    private RemovedDisk() {
        try {
            Set<String> removedDisks = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_MIGING_V_INDEX)
                    .smembers("removed_disk_set");

            set.addAll(removedDisks);
        } catch (Exception e) {
            log.error("init removed_disk_set fail", e);
        }

        Flux.interval(Duration.ofSeconds(10)).publishOn(ErasureServer.DISK_SCHEDULER).subscribe(l -> {
            try {
                Set<String> diskSet = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_MIGING_V_INDEX)
                        .smembers("removed_disk_set");

                set.addAll(diskSet);
            } catch (Exception e) {
                log.error("update removed_disk_set fail", e);
            }
        });
    }

    public static RemovedDisk getInstance() {
        return instance;
    }

    public boolean contains(String disk) {
        return set.contains(disk);
    }

    public void add(String disk) {
        set.add(disk);
    }
}
