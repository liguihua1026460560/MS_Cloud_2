package com.macrosan.storage.metaserver.move.copy;


import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.storage.metaserver.move.ShardingContext;
import com.macrosan.storage.metaserver.move.scanner.Scanner;
import com.macrosan.utils.functional.Tuple3;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.macrosan.storage.metaserver.ShardingWorker.SHARDING_CONF_REDIS_KEY;

/**
 * @author Administrator
 */
@Log4j2
public abstract class AbstractCopyTaskRunner<T> {

    public static int COPY_MAX_CONCURRENCY = 100;
    static {
        try {
            String copyMaxConcurrency = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_SYSINFO_INDEX).hget(SHARDING_CONF_REDIS_KEY, "copy-max-concurrency");
            if (StringUtils.isNotBlank(copyMaxConcurrency)) {
                COPY_MAX_CONCURRENCY = Integer.parseInt(copyMaxConcurrency);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        log.info("bucket sharding copy-max-concurrency={}",COPY_MAX_CONCURRENCY);
    }

    protected final String bucketName;
    protected final String sourceVnode;
    protected final String targetVnode;

    public Scanner<T> scanner;
    protected final MonoProcessor<Boolean> res = MonoProcessor.create();
    protected final Direction direction;
    protected final StoragePool storagePool;

    public final AtomicReference<String> divider = new AtomicReference<>("");

    /**
     * 问题单SERVER-1323
     * 由于@ partKey不是严格的按照字典排序，所以需要依赖与initPart copy产生的part divider来确定partKey的copy顺序，以防止@ partKey出现缺失的情况
     */
    public final AtomicReference<String> partDivider = new AtomicReference<>("");

    @Setter
    protected ShardingContext context;

    protected AbstractCopyTaskRunner(String bucketName, String sourceVnode, String targetVnode, Direction direction) {
        this.bucketName = bucketName;
        this.sourceVnode = sourceVnode;
        this.targetVnode = targetVnode;
        this.direction = direction;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
    }

    public void run() {
        log.info("copy task was running.");
        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.scan())
                .publishOn(ShardingWorker.SHARDING_SCHEDULER)
                .flatMap(this::copy, COPY_MAX_CONCURRENCY)
                .doOnNext(b -> {
                    if (!b) {
                        throw new RuntimeException("copy task running error!");
                    }
                })
                .doOnComplete(() -> res.onNext(true))
                .subscribe((l) -> {}, e -> {
                    log.error("", e);
                    if (!scanner.isEnd()) {
                        scanner.tryEnd();
                    }
                    res.onNext(false);
                });
    }

    /**
     * 将数据copy至目标节点中
     * @param t 需要被复制的数据
     * @return 是否copy成功
     */
    protected abstract Mono<Boolean> copy(T t);


    protected Mono<Boolean> retryableOperation(Function<List<Tuple3<String, String, String>>, Mono<Boolean>> operation) {
        return storagePool.mapToNodeInfo(targetVnode)
                .flatMap(operation)
                .filter(b -> b)
                .switchIfEmpty(Mono.error(new Exception("Operation returned false or null")))
                .retryBackoff(5, Duration.ofSeconds(3), Duration.ofSeconds(15))
                .doOnError(e -> log.error("copy task retryableOperation error.", e))
                .onErrorReturn(false);
    }

    /**
     * 节点间的迁移方向
     */
    public enum Direction {

        /**
         * 节点向左邻节点迁移数据
         */
        LEFT,

        /**
         * 节点向右邻接点迁移数据
         */
        RIGHT
    }

    public Mono<Boolean> res() { return res; }
}
