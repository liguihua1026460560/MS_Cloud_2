package com.macrosan.utils.ratelimiter;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.lifecycle.mq.LifecycleChannels;
import com.macrosan.rabbitmq.RabbitMqChannels;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIFECYCLE_OBJECT_META;
import static com.macrosan.utils.ratelimiter.LimitTimeSet.DEFAULT_STRATEGY;


@Log4j2
public class RecoverLimiter {
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static LimitStrategy limitStrategy = DEFAULT_STRATEGY;
    public static long qosWeight = 1000;
    public static long lifecycleWeight = 1;
    public static long lifecycleDeleteWeight = 1;
    public static long normalWeight = 1;

    private static final RecoverLimiter instance = new RecoverLimiter();

    public static void updateStrategy(LimitStrategy newLimitStrategy){

        // 设置QoS底层阈值
        LimitThreshold.changeThreshold();

        // 如果切换的是业务优先策略，则两种Limiter分开
        if (newLimitStrategy.name().equals(LimitStrategy.LIMIT.name())){
            instance.dataLimiter.updateLimitStrategy(LimitStrategy.LIMIT);
            instance.metaLimiter.updateLimitStrategy(LimitStrategy.LIMIT_META);
        } else {  // 如果切换的是其它策略
            instance.dataLimiter.updateLimitStrategy(newLimitStrategy);
            instance.metaLimiter.updateLimitStrategy(newLimitStrategy);
        }

        try {
            // 调整rabbitmq的并发量
            String ec_rabbit = pool.getCommand(REDIS_SYSINFO_INDEX).get("ec_rabbit");
            String rebuild_rabbit = pool.getCommand(REDIS_SYSINFO_INDEX).get("rebuild_rabbit");
            String lifecycle_rabbit = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_rabbit");

            // 对于业务优先策略的rabbitmq并发量，如果从redis获取的记录不为空
            if (ec_rabbit != null && rebuild_rabbit != null && lifecycle_rabbit != null) {
                RabbitMqChannels.adjustRabbitMqChannel(newLimitStrategy.name(), Integer.parseInt(ec_rabbit));
                RebuildRabbitMq.adjustRebuildMqChannel(newLimitStrategy.name(), Integer.parseInt(rebuild_rabbit));
                LifecycleChannels.adjustLifecycleChannels(newLimitStrategy.name(), Integer.parseInt(lifecycle_rabbit));
                RecoverLimiter.getInstance().getMetaLimiter().setEc_error_ratio(Double.parseDouble(ec_rabbit) / 100.0);
                RecoverLimiter.getInstance().getMetaLimiter().setRebuild_queue_ratio(Double.parseDouble(rebuild_rabbit) / 10.0);
                RecoverLimiter.getInstance().getMetaLimiter().setLifecycle_command_ratio(Double.parseDouble(lifecycle_rabbit) / 200.0);
            } else {  // 为空则设置为默认的情况
                RabbitMqChannels.adjustRabbitMqChannel(newLimitStrategy.name(), 5);
                RebuildRabbitMq.adjustRebuildMqChannel(newLimitStrategy.name(), 2);
                LifecycleChannels.adjustLifecycleChannels(newLimitStrategy.name(), 20);
                RecoverLimiter.getInstance().getMetaLimiter().setEc_error_ratio(0.05);
                RecoverLimiter.getInstance().getMetaLimiter().setRebuild_queue_ratio(0.2);
                RecoverLimiter.getInstance().getMetaLimiter().setLifecycle_command_ratio(0.1);
            }
        } catch (Exception e) {
            log.error("", e);
        }

        //TODO 用于调试，显示当前环境生效的策略
        log.info("The current effective policy in dataLimiter : {}", instance.dataLimiter.getStrategy().name());
        log.info("The current effective policy in metaLimiter : {}", instance.metaLimiter.getStrategy().name());
    }
    public static RecoverLimiter getInstance() {
        return instance;
    }

    public static LimitStrategy getLimitStrategy(){
        return instance.dataLimiter.getStrategy();
    }

    RateLimiter dataLimiter;
    RateLimiter metaLimiter;

    RecoverLimiter() {
        dataLimiter = new RateLimiter(limitStrategy, 200L << 20, "dataLimiter");
        metaLimiter = new RateLimiter(limitStrategy, 50000, "metaLimiter");
    }

    public Mono<Boolean> acquireData(String disk, long size, boolean isLimit) {
        return dataLimiter.acquire(disk, size, isLimit, "data");
    }

    public Mono<Boolean> acquireMeta(boolean isLimit, ErasureServer.PayloadMetaType metaType) {

        // 如果是恢复流量
        if (isLimit){
            if (metaType.name().equalsIgnoreCase(LIFECYCLE_OBJECT_META.name())){
                return metaLimiter.acquire("meta", lifecycleWeight, isLimit, metaType.name());
            } else {
                return metaLimiter.acquire("meta", qosWeight, isLimit, metaType.name());
            }
        }
        return metaLimiter.acquire("meta", normalWeight, isLimit, metaType.name());
    }

    /**
     * 生命周期删除限流时使用
     * */
    public Mono<Boolean> acquireMeta(boolean isLimit) {
        if (isLimit) {
            return metaLimiter.acquire("meta", lifecycleDeleteWeight, isLimit, "LIFECYCLE_DELETE_OBJECT_META");
        }
        return Mono.just(true);
    }

    public long getMetaNormalFlow(){
        return metaLimiter.normalSumFlow;
    }

    public long getDataNormalFlow() {
        return dataLimiter.normalSumFlow;
    }

    public RateLimiter getMetaLimiter(){
        return metaLimiter;
    }

    public RateLimiter getDataLimiter(){
        return dataLimiter;
    }
}
