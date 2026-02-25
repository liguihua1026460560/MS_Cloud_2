package com.macrosan.utils.ratelimiter;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.rebuild.RebuildMessageBroker;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.lifecycle.mq.LifecycleChannels;
import com.macrosan.lifecycle.mq.LifecycleMessageBroker;
import com.macrosan.rabbitmq.MessageBroker;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.rabbitmq.client.*;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.REDIS_MIGING_V_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_VNODE_META;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_VNODE_OBJ;

@Log4j2
@Data
public class RateLimiter {
    private static final long STATISTIC_TIME = 10;
    private static final long SCAN_RECOVER_TIME = 1000 * 2 * 60;  // 2分钟检测一次
    private static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("rate-limiter"));
    private final Map<String, Limiter> limitMap = new ConcurrentHashMap<>();
    private final Map<Thread, Map<String, Statistic>> runMap = new ConcurrentHashMap<>();
    private final Map<String, Statistic> totalMap = new HashMap<>();
    private LimitStrategy strategy;
    private long defaultSize;
    private AtomicInteger printNum = new AtomicInteger();
    public double ec_error_ratio = 0.05;
    public double rebuild_queue_ratio = 0.2;
    public double lifecycle_command_ratio = 0.1;
    public long normalSumFlow = 0L;
    private boolean adaptFlag = false;
    private int lastPrefetchNum = 0;
    private String limiterName;

    public Boolean recoverFlag = false;  // 一旦标志位设置为true，即开启对running的检测
    public Boolean existIndexRebuild = false;
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public LimitStrategy getStrategy(){
        return strategy;
    }
    public void updateLimitStrategy(LimitStrategy newLimitStrategy){
        this.strategy = newLimitStrategy;
    }

    RateLimiter(LimitStrategy strategy, long defaultSize, String limiterName) {
        this.limiterName = limiterName;
        this.strategy = strategy;
        this.defaultSize = defaultSize * STATISTIC_TIME / 1000;
        executor.schedule(this::update, STATISTIC_TIME, TimeUnit.MILLISECONDS);
        if (limiterName.equalsIgnoreCase("metaLimiter")){
            executor.schedule(this::scanRecover, SCAN_RECOVER_TIME, TimeUnit.MILLISECONDS);  // TODO
        }
    }

    /**
     * 定时2分钟读取redis中running是否存在索引池重构，若是则将消息权重置1，否则消息权重置1000
     * */
    public void scanRecover(){
        existIndexRebuild = false;
        try {
            log.debug("recoverFlag:{}", recoverFlag);
            if(recoverFlag){
                String running = "";
                List<String> runningKeys = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).keys("running_*");
                for (String runningKey : runningKeys) {
                    if (!"hash".equals(redisConnPool.getCommand(REDIS_MIGING_V_INDEX).type(runningKey))) {
                        continue;
                    }
                    String operate = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(runningKey, "operate");
                    String poolName = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(runningKey, "poolName");
                    String poolType = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(poolName,"role");

                    if("meta".equals(poolType)) {//如果此时存在索引池重构
                        if ("remove_disk".equals(operate)) {
                            if (!existIndexRebuild) {
                                existIndexRebuild = true;
                            }
                            running = runningKey;
                            log.debug("runnningKey:{}", running);
                            break;
                        } else if("expand".equals(operate)){
                            String qosFlag = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget("running", "QoSFlag");
                            if ("1".equals(qosFlag)) {
                                if (!existIndexRebuild) {
                                    existIndexRebuild = true;
                                }
                                running = runningKey;
                                break;
                            }
                        }
                    }
                }
                if (existIndexRebuild && StringUtils.isNotEmpty(running)){
                    String operate = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(running, "operate");
                    String disk = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(running, "diskName");
                    if ((operate != null) && (disk != null)){
                        switch (strategy){
                            case LIMIT_META:
                                adjustWeight(operate, disk, 1);
                                break;
                            case ADAPT:
                                long max = limitMap.get("meta").max.get();
                                adjustWeight(operate, disk, max);  // 消息权重仅和 metaLimiter 有关
                                break;
                            default:
                                RecoverLimiter.qosWeight = 1000;
                        }
                    }

                } else {  // 检测不到 running 字段，证明当前无重平衡，关闭对redis的读取；后可改为连续6分钟检测不到running，
                    RecoverLimiter.qosWeight = 1000;
                    recoverFlag = false;
                    log.info("Data recover finished, QoS weight set to: {}", RecoverLimiter.qosWeight);
                }
            }

        } catch (Exception e) {
            log.error("MetaLimiter: scan redis running error. ", e);
            RecoverLimiter.qosWeight = 1000;  // 一旦出错权重调回1000
        } finally {
            log.debug(RecoverLimiter.qosWeight);
            executor.schedule(this::scanRecover, SCAN_RECOVER_TIME, TimeUnit.MILLISECONDS);  // TODO 合入代码修改为2分钟
        }
    }


    public void adjustWeight(String operate, String disk, long weight){
        if (operate.equalsIgnoreCase("remove_disk") && disk.endsWith("index")) {  // 索引池重构
            RecoverLimiter.qosWeight = weight;
            log.debug("pool_index removeDisk {}", RecoverLimiter.qosWeight);
        } else if (operate.equalsIgnoreCase("expand")) {  // 索引池扩副本
            String poolName = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget("running", "poolName");
            String prefix = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(poolName, "prefix");
            String qosFlag = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget("running", "QoSFlag");
            if (prefix.startsWith("meta") && qosFlag.equalsIgnoreCase("1")) {
                RecoverLimiter.qosWeight = weight;
                log.debug("pool_index expand {}", RecoverLimiter.qosWeight);
            }
        } else {
            RecoverLimiter.qosWeight = 1000;
        }
        log.debug(" QoS Weight: {}", RecoverLimiter.qosWeight);
    }


    public void update() {
        try {
            long execTime = System.nanoTime();
            Map<String, Statistic> curMap = new HashMap<>();
            for (Map<String, Statistic> map : runMap.values()) {
                for (String key : map.keySet()) {
                    Statistic s = curMap.computeIfAbsent(key, k -> new Statistic());
                    Statistic cur = map.get(key);
                    long n = cur.normal.get();
                    s.normal.addAndGet(n);
                    cur.normal.addAndGet(-n);

                    n = cur.limit.get();
                    s.limit.addAndGet(n);
                    cur.limit.addAndGet(-n);

                    n = cur.delay.get();
                    s.delay.addAndGet(n);
                    cur.delay.addAndGet(-n);

                    n = cur.flush.get();
                    s.flush.addAndGet(n);
                    cur.flush.addAndGet(-n);

                    Iterator<AcquireTask> iterator = cur.tasks.iterator();
                    while (iterator.hasNext()) {
                        AcquireTask task = iterator.next();
                        if (tryAcquire(task, execTime)) {
                            task.end();
                            s.flush.addAndGet(task.size);
                            iterator.remove();
                        }
                    }
                }
            }


            boolean print = printNum.incrementAndGet() % 6000  == 0;  // 当前该节点各磁盘的流量统计情况Map，一个Entry一块磁盘
            long crtNormalFlowSum = 0;  // 当前curMap统计到的各磁盘的业务流量和
            for (String key : curMap.keySet()) {
                Limiter limiter = limitMap.computeIfAbsent(key, k -> new Limiter());
                Statistic s = curMap.get(key);

                Statistic t = totalMap.computeIfAbsent(key, k -> new Statistic());

                t.normal.addAndGet(s.normal.get());
                t.limit.addAndGet(s.limit.get());  // 如果每次进入的流量都比max低，limit会越来越大，此时将不会有限制效果
                t.delay.addAndGet(s.delay.get());
                t.flush.addAndGet(s.flush.get());
                t.normalList.add(s.normal.get());
                if (t.normalList.size() > 1000) {
                    t.normalList.pop();
                }

                long max;
                long sum = t.normalList.stream().mapToLong(l -> l).sum();  // normalList 过去监测到的正常业务流量的统计结果
                if (sum > 0) {
                    sum = t.normalList.stream().mapToLong(l -> l == 0 ? defaultSize : l).sum();
                    if (limiterName.equalsIgnoreCase("metaLimiter")) {  // 如果是元数据限流器

                        // 自适应策略仅当数据盘无落盘流量时放开限制，解决元数据限流器受定时消息一直限制的影响
                        if (adaptFlag && (RecoverLimiter.getInstance().getDataNormalFlow() == 0)){
                            max = strategy.getLimit(0, key);
                        } else {
                            max = strategy.getLimit(sum / t.normalList.size(), key);
                        }

                    } else {  // 如果是数据限流器
                        max = strategy.getLimit(sum / t.normalList.size(), key);
                    }
                } else {
                    max = strategy.getLimit(0, key);
                }

                long tmp = limiter.cur.get();
                if (tmp > limiter.max.get()) {
                    tmp = limiter.max.get();
                }

                limiter.max.set(max);
                limiter.cur.addAndGet(-tmp);  // 当前流量归零，阈值会影响清零的速度
                crtNormalFlowSum = crtNormalFlowSum + sum;

                if (print) {
                    //TODO 流量的统计信息 只用于调试
                    log.debug("{}: flush {} delay {} limit {} normal {} sum {} Flag {} sum0 {} threshold {} limit0 {}", key, t.flush, t.delay, t.limit, t.normal, normalSumFlow, adaptFlag, sum, max, s.limit);
                }
            }

            normalSumFlow = crtNormalFlowSum;
            if (print) {
                log.debug("Adapt strategy adjusts rabbitmq qos: {} Flag {} lastPrefetchNum {} dataNormal {}", limiterName, adaptFlag, lastPrefetchNum, RecoverLimiter.getInstance().getDataNormalFlow());
                adjustRabbitMqChannel();
            }

        } catch (Exception e) {
            log.error("", e);
        } finally {
            executor.schedule(this::update, STATISTIC_TIME, TimeUnit.MILLISECONDS);
        }
    }


    /**
     * 当策略为ADAPT时动态调整RabbitMq的并发量
     * adaptFlag为true则表示当前为ADAPT需要动态更新rabbitMQ并发量；
     * adaptFlag为false则表示当前为其它策略无需更新rabbitMQ并发量；
     * @author: wangchenxing
     * */
    public void adjustRabbitMqChannel(){
        if (adaptFlag){
            double ratio = ec_error_ratio;
            double rebuild_ratio = rebuild_queue_ratio;
            double lifecycle_ratio = lifecycle_command_ratio;
            // 因为只有dataLimiter可以清晰监控正常流量，所以通过dataLimiter中的normalFlow判断正常业务的有无
            long dataNormalFlow = RecoverLimiter.getInstance().getDataNormalFlow();
            if (dataNormalFlow > 0){
                ratio = ec_error_ratio;
                rebuild_ratio = rebuild_queue_ratio;
                lifecycle_ratio = lifecycle_command_ratio;
            } else {
                ratio = 1.0;
                rebuild_ratio = 1.0;
                lifecycle_ratio = 1.0;
            }
            int prefetchNum = (int) (100 * ratio);
            int rebuild_prefetchNum = (int) (10 * rebuild_ratio);
            int lifecycle_prefetchNum = (int) (200 * lifecycle_ratio);
            // 仅当prefetchNum出现变化时再更改实际的rabbitMQ并发量，减少调用basicQos的次数
            if (prefetchNum != lastPrefetchNum){

                /** EC_ERROR队列 **/
                Map<Channel, MessageBroker> brokerMap = ObjectConsumer.brokerMap;
                for (Channel channel : brokerMap.keySet()) {
                    try {
                        MessageBroker curBroker = brokerMap.get(channel);
                        curBroker.adjustAdaptQoS(prefetchNum);
                        log.debug(curBroker.queueName + " Adjust rabbitmq concurrency based on ADAPT policy: " + dataNormalFlow + " QoS: " + curBroker.getQos().get());
                    } catch (Exception e) {
                        MessageBroker curBroker = brokerMap.get(channel);
                        log.error("{} Adjust rabbitMQ channel qos prefetch num error. QoS: {} {}", curBroker.queueName, curBroker.getQos().get(), e);
                    }
                }

                /** rebuild-queue队列 **/
                ArrayList<Channel> removeList = new ArrayList<>();  // 用于保存需移除的掉线磁盘队列
                Map<Channel, RebuildMessageBroker> rebuildBrokerMap = RebuildRabbitMq.brokerMap;
                for (Channel channel : rebuildBrokerMap.keySet()) {
                    boolean isFine = true;
                    try {
                        RebuildMessageBroker curRebildBroker = rebuildBrokerMap.get(channel);
                        AMQP.Queue.DeclareOk OK = channel.queueDeclarePassive(curRebildBroker.queueName);
                    } catch (Exception e) {
                        isFine = false;
                        removeList.add(channel);
                        synchronized (RebuildRabbitMq.getWarnMap()){
                            RebuildRabbitMq.putWarnMap(rebuildBrokerMap.get(channel).consumerTag, rebuildBrokerMap.get(channel).queueName);
                        }
                        log.info("Queue removed, no need to switch. QueueName: {}, ConsumerTag: {}.", rebuildBrokerMap.get(channel).queueName, rebuildBrokerMap.get(channel).consumerTag);
                    }

                    if (isFine){
                        try {
                            RebuildMessageBroker curRebildBroker = rebuildBrokerMap.get(channel);
                            curRebildBroker.adjustAdaptQoS(rebuild_prefetchNum);
                            log.debug(curRebildBroker.queueName + " Adjust rebuild rabbitmq concurrency based on ADAPT policy: " + dataNormalFlow + " QoS: " + curRebildBroker.getQos().get());
                        } catch (Exception e) {
                            RebuildMessageBroker curRebildBroker = rebuildBrokerMap.get(channel);
                            log.error("{} Adjust rebuild rabbitMQ channel qos prefetch num error. QoS: {} {}", curRebildBroker.queueName, curRebildBroker.getQos().get(), e);
                        }
                    }
                }

                if (removeList.size() > 0) {
                    for (int i = 0; i < removeList.size(); i++) {
                        rebuildBrokerMap.remove(removeList.get(i));
                    }
                }

                /** lifecycle_command队列 **/
                Map<Channel, LifecycleMessageBroker> lifecycleBrokerMap = LifecycleChannels.brokerMap;
                for (Channel channel : lifecycleBrokerMap.keySet()) {
                    try {
                        LifecycleMessageBroker lifecycleBroker = lifecycleBrokerMap.get(channel);
                        lifecycleBroker.adjustAdaptQoS(lifecycle_prefetchNum);
                        log.debug(lifecycleBroker.queueName + " Adjust lifecycle rabbitmq concurrency based on ADAPT policy: " + dataNormalFlow + " QoS: " + lifecycleBroker.getQos().get());
                    } catch (Exception e) {
                        LifecycleMessageBroker lifecycleBroker = lifecycleBrokerMap.get(channel);
                        log.error("{} Adjust lifecycle rabbitMQ channel qos prefetch num error. QoS: {} {}", lifecycleBroker.queueName, lifecycleBroker.getQos().get(), e);
                    }
                }

            }
            lastPrefetchNum = prefetchNum;
        }
    }

    public boolean tryAcquire(AcquireTask task, long execTime) {
        if (execTime >= task.endWaitTime) {
            return true;
        }

        Limiter limiter = limitMap.getOrDefault(task.key, new Limiter());
        long limit = limiter.max.get();
        if (limit > 0) {
            if (limiter.tryUpdate(task.size, limit)) {
                return true;
            }
        } else {
            return true;
        }

        return false;
    }

    public Mono<Boolean> acquire(String key, long size, boolean isLimit, String metaType) {
        Map<String, Statistic> map = runMap.get(Thread.currentThread());
        if (null == map) {
            synchronized (runMap) {
                map = runMap.computeIfAbsent(Thread.currentThread(), k -> new ConcurrentHashMap<>());
            }
        }

        Statistic statistic = map.computeIfAbsent(key, k -> new Statistic());
        if (isLimit) {
            statistic.limit.addAndGet(size);
            Limiter limiter = limitMap.getOrDefault(key, new Limiter());
            long limit = limiter.max.get();
            if (limit > 0) {
                if (limiter.tryUpdate(size, limit)) {
                    statistic.flush.addAndGet(size);
                    return Mono.just(true);
                } else {
                    if (limiterName.equals("metaLimiter")){
                        if (metaType.equalsIgnoreCase(LIST_VNODE_META.name()) || metaType.equalsIgnoreCase(LIST_VNODE_OBJ.name())){  // 如果是list消息，且定时消息还未开启
                            recoverFlag = true;
                        }
                    }
                    statistic.delay.addAndGet(size);
                    AcquireTask task = new AcquireTask(key, size);
                    statistic.tasks.add(task);
                    return task.res;
                }
            } else {
                return Mono.just(true);
            }
        } else {
            statistic.normal.addAndGet(size);
            return Mono.just(true);
        }
    }

    private static class AcquireTask {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        long startWaitTime = System.nanoTime();
        long endWaitTime = startWaitTime + 10_000_000_000L; //10s
        String key;
        long size;

        public AcquireTask(String key, long size) {
            this.key = key;
            this.size = size;
        }

        public void end() {
            ErasureServer.DISK_SCHEDULER.schedule(() -> this.res.onNext(true));
        }
    }

    private static class Statistic {
        AtomicLong normal = new AtomicLong();
        AtomicLong limit = new AtomicLong();
        AtomicLong delay = new AtomicLong();
        AtomicLong flush = new AtomicLong();
        LinkedList<Long> normalList = new LinkedList<>();
        ConcurrentLinkedQueue<AcquireTask> tasks = new ConcurrentLinkedQueue<>();
    }

    private static class Limiter {
        AtomicLong cur = new AtomicLong();
        AtomicLong max = new AtomicLong();

        boolean tryUpdate(long size, long max) {  // 无论限制有多小，第一个byte[]数组都会被放过去
            long prev, next;
            do {
                prev = cur.get();
                next = prev + size;
                if (prev > max) {
                    return false;
                }
            } while (!cur.compareAndSet(prev, next));  // 第一个进来的数值时cur都是0，那么就会循环一次，会让cur等于next

            return true;
        }
    }
}
