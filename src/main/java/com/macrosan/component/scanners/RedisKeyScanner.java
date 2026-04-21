package com.macrosan.component.scanners;

import com.macrosan.database.redis.RedisConnPool;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;

/**
 * redis scan 命令 封装
 * @author zhaoyang
 * @date 2026/04/15
 **/
@Log4j2
public class RedisKeyScanner {
    private final RedisConnPool redisConnPool = RedisConnPool.getInstance();
    private final String keyMatches;
    private final AtomicBoolean scanFinished;
    private final AtomicReference<String> scanCursor;
    private final int limit;

    public RedisKeyScanner(String keyMatches, int limit) {
        this.keyMatches = keyMatches;
        this.limit = limit;
        this.scanFinished = new AtomicBoolean();
        this.scanCursor = new AtomicReference<>(ScanCursor.INITIAL.getCursor());
    }

    public Flux<String> scanKey() {
        return doCollectKeys(new ArrayList<>(), scanCursor.get());
    }

    public boolean isScanComplete() {
        return scanFinished.get();
    }

    private Flux<String> doCollectKeys(List<String> accumulated, String cursor) {
        return redisConnPool.getReactive(REDIS_POOL_INDEX)
                .scan(ScanCursor.of(cursor), ScanArgs.Builder.matches(keyMatches).limit(limit))
                .flatMapMany(scanResult -> {
                    accumulated.addAll(scanResult.getKeys());

                    if (scanResult.isFinished()) {
                        scanCursor.set(ScanCursor.INITIAL.getCursor());
                        scanFinished.set(true);
                        return Flux.fromIterable(accumulated);
                    }

                    if (accumulated.size() >= 10) {
                        scanCursor.set(scanResult.getCursor());
                        return Flux.fromIterable(accumulated);
                    }
                    // 不够，继续扫描
                    return doCollectKeys(accumulated, scanResult.getCursor());
                });
    }
}
