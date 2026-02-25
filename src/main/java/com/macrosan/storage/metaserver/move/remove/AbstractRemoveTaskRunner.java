package com.macrosan.storage.metaserver.move.remove;


import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.storage.metaserver.move.scanner.Scanner;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;

import static com.macrosan.storage.metaserver.ShardingWorker.SHARDING_CONF_REDIS_KEY;

/**
 * @author Administrator
 */
@Log4j2
public abstract class AbstractRemoveTaskRunner<T> {

    public static int REMOVE_MAX_CONCURRENCY = 100;
    static {
        try {
            String copyMaxConcurrency = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_SYSINFO_INDEX).hget(SHARDING_CONF_REDIS_KEY, "remove-max-concurrency");
            if (StringUtils.isNotBlank(copyMaxConcurrency)) {
                REMOVE_MAX_CONCURRENCY = Integer.parseInt(copyMaxConcurrency);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        log.info("bucket sharding remove-max-concurrency={}",REMOVE_MAX_CONCURRENCY);
    }

    protected final String bucketName;
    protected final String vnode;
    protected final String separator;
    protected final MonoProcessor<Boolean> res = MonoProcessor.create();
    protected final StoragePool storagePool;
    protected Scanner<T> scanner;

    protected final POSITION position;

    protected AbstractRemoveTaskRunner(String bucketName, String vnode, String separator, POSITION position) {
        if (separator == null) {
            throw new UnsupportedOperationException("fatal error:bucket " + bucketName+ " vnode:" + vnode + "position: " + position.name()+ " AbstractRemoveTaskRunner separator is null.");
        }
        this.bucketName = bucketName;
        this.vnode = vnode;
        this.separator = separator;
        this.position = position;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
    }

    protected boolean canTryEnd(String key) {
        return !position.equals(POSITION.LEFT) && key.compareTo(separator) > 0;
    }

    public void run() {
        log.info("remove is running");
        Mono.delay(Duration.ofSeconds(0))
                .flatMapMany(l -> scanner.scan())
                .publishOn(ShardingWorker.SHARDING_SCHEDULER)
                .flatMap(this::remove, REMOVE_MAX_CONCURRENCY)
                .doOnNext(b -> {
                    if (!b) {
                        throw new RuntimeException("remove task running error!");
                    }
                })
                .doOnComplete(() -> res.onNext(true))
                .subscribe((l) -> {}, e -> {
                    if (!scanner.isEnd()) {
                        scanner.tryEnd();
                    }
                    res.onNext(false);
                });
    }

    /**
     * 从vnode上移除该数据
     * @param t 需要被移除的数据
     * @return 是否移除成功
     */
    protected abstract Mono<Boolean> remove(T t);

    /**
     * @return 任务执行的结果
     */
    public Mono<Boolean> res() { return res; }

    public enum POSITION {
        /**
         * 位于分割线的左边
         */
        LEFT,
        /**
         * 位于分割线的右边
         */
        RIGHT
    }
}
