package com.macrosan.ec.rebuild;

import io.lettuce.core.ScriptOutputType;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


import static com.macrosan.ec.rebuild.RebuildCache.REBUILD_CACHE_SCHEDULER;

/**
 * @author zhaoyang
 * @date 2026/01/06
 * @description 重构迁移计数器，先在内存中进行累积，定时写入redis，避免频繁 写入redis
 **/
@Log4j2
public class RebuildCounter {
    private static final Map<String, RebuildCounter> COUNTER_MAP = new java.util.concurrent.ConcurrentHashMap<>();
    private final String runningKey;
    private final AtomicLong taskNumDelta = new AtomicLong(0L);
    private final AtomicLong migrateNumDelta = new AtomicLong(0L);
    private final AtomicLong inodeNumDelta = new AtomicLong(0L);
    private final AtomicLong chunkNumDelta = new AtomicLong(0L);
    private final Disposable disposable;

    public RebuildCounter(String runningKey) {
        this.runningKey = runningKey;
        this.disposable = REBUILD_CACHE_SCHEDULER.schedulePeriodically(this::flush, 200, 200, TimeUnit.MILLISECONDS);
    }

    public static RebuildCounter getCounter(String runningKey) {
        return COUNTER_MAP.computeIfAbsent(runningKey, RebuildCounter::new);
    }

    public void incrementInodeNum(long num) {
        migrateNumDelta.addAndGet(num);
    }

    public void incrementChunkNum(long num) {
        chunkNumDelta.addAndGet(num);
    }


    public void decrementTaskNum(long num) {
        taskNumDelta.addAndGet(-num);
    }

    public void decrementMigrateNum(long num) {
        migrateNumDelta.addAndGet(-num);
    }

    private void flush() {
        try {
            if (taskNumDelta.get() == 0L && migrateNumDelta.get() == 0L && inodeNumDelta.get() == 0L && chunkNumDelta.get() == 0L) {
                if (RebuildRabbitMq.getMaster().exists(runningKey) == 0) {
                    COUNTER_MAP.remove(runningKey);
                    disposable.dispose();
                    log.info("{} is not exists, remove counter", runningKey);
                }
                return;
            }
            String luaScript =
                    "if redis.call('exists', KEYS[1]) == 1 then\n" +
                            "    redis.call('hincrby', KEYS[1], 'taskNum', ARGV[1])\n" +
                            "    redis.call('hincrby', KEYS[1], 'inodeNum', ARGV[2])\n" +
                            "    redis.call('hincrby', KEYS[1], 'chunkNum', ARGV[3])\n" +
                            "    redis.call('hincrby', KEYS[1], 'migrateNum', ARGV[4])\n" +
                            "    return 1\n" +
                            "else\n" +
                            "    return 0\n" +
                            "end";
            Object res = RebuildRabbitMq.getMaster().eval(luaScript, ScriptOutputType.INTEGER, new String[]{runningKey},
                    String.valueOf(taskNumDelta.getAndUpdate(n -> 0L)),
                    String.valueOf(inodeNumDelta.getAndUpdate(n -> 0L)),
                    String.valueOf(chunkNumDelta.getAndUpdate(n -> 0L)),
                    String.valueOf(migrateNumDelta.getAndUpdate(n -> 0L))
            );
            if (Objects.equals(res, 0L)) {
                COUNTER_MAP.remove(runningKey);
                disposable.dispose();
                log.info("{} is not exists, remove counter", runningKey);
            }
        } catch (Exception e) {
            log.error("flush error", e);
        }
    }
}
