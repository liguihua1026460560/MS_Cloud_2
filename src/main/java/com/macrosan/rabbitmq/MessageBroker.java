package com.macrosan.rabbitmq;

import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.ratelimiter.LimitStrategy;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.rabbitmq.RabbitMqUtils.normalAck;
import static com.macrosan.rabbitmq.RabbitMqUtils.normalRequeue;


/**
 * @Author: WANG CHENXING
 * @Date: 2023/3/7
 * @Description: 用于拦截rabbitmq消息，并自定义控制并发量，用于解决直接修改basicQoS带来的消息重复消费问题及避免频繁取消订阅带来的不稳定影响
 */

@Log4j2
@Data
public class MessageBroker {

    /**
     * Broker 对应的 channel
     **/
    public Channel channel;

    /**
     * Broker 对应 channel 的队列名
     **/
    public String queueName;

    /**
     * 数据恢复优先策略并发数
     **/
    public static int NO_LIMIT_NUM = 100;

    /**
     * 业务优先策略并发数
     **/
    public static int LIMIT_NUM = 5;

    /**
     * 是否为队列初始化时的策略切换
     **/
    private boolean initFlag = false;

    /**
     * 当前 Broker 的并发量，初始值等于 channel 本身的 prefetch count
     **/
    private AtomicInteger qos = new AtomicInteger(100);

    /**
     * 当前 Broker 实际在处理的消息数量
     **/
    private AtomicInteger runner = new AtomicInteger(0);

    /**
     * 无界队列，用于存储拦截的消息
     **/
    public UnicastProcessor<Tuple2<Envelope, byte[]>> processor = UnicastProcessor.create(Queues.<Tuple2<Envelope, byte[]>>unboundedMultiproducer().get());

    /**
     * 订阅关系
     **/
    private final Subscription[] subscription = new Subscription[1];

    /**
     * 订阅者
     **/
    private Subscriber subscriber = new Subscriber() {

        @Override
        public void onSubscribe(Subscription s) {
            s.request(5);  // 初始值与业务优先数值一样
            subscription[0] = s;
        }

        @Override
        public void onNext(Object o) {
            runner.incrementAndGet();
            try {
                Tuple2<Envelope, byte[]> tuple2 = (Tuple2<Envelope, byte[]>) o;
                ObjectConsumer.getInstance().dealMsg(channel, tuple2.var1, tuple2.var2, RabbitMqUtils.ERROR_MQ_TYPE.EC_ERROR).subscribe(aBoolean -> {
                    int req = qos.get() - runner.decrementAndGet();
                    if (req > 0) {
                        subscription[0].request(req > 1 ? 2 : 1);
                    }

                }, e -> {
                    runner.decrementAndGet();
                    if (e != null && e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                        normalAck(getChannel(), tuple2.var1);
                        return;
                    }
                    log.error("", e);
                    normalRequeue(getChannel(), tuple2.var1);
                });
            } catch (Exception e) {
                if (o instanceof String) {  // 当消息都卡住不动时，可通过脚本拉取新消息
                    String str = (String) o;
                    int num = Integer.parseInt((String) str.split("&")[1]);
                    subscription[0].request(num);
                    log.info("Pull message: {}", num);
                } else {
                    Tuple2<Envelope, byte[]> tuple2 = (Tuple2<Envelope, byte[]>) o;
                    log.error("Object consumer error", e);
                    final int runnerNum = runner.decrementAndGet();
                    if (e.getMessage() != null && e.getMessage().contains("no such bucket name")) {
                        normalAck(getChannel(), tuple2.var1);
                    } else {
                        normalRequeue(getChannel(), tuple2.var1);
                    }
                    int req = qos.get() - runnerNum;
                    if (req > 0) {
                        subscription[0].request(req > 1 ? 2 : 1);
                    }
                }
            }

        }

        @Override
        public void onError(Throwable t) {
            log.error("", t);
        }

        @Override
        public void onComplete() {
            log.info("complete");
        }
    };

    /**
     * 构造函数：在生成 Broker 实例的时候就建立订阅关系
     **/
    public MessageBroker(Channel channel, String queue) {
        this.channel = channel;
        this.queueName = queue;
        processor.subscribe(subscriber);
    }

    /**
     * 调整队列的并发量，业务优先和数据恢复优先直接切换至对应的并发量
     */
    public synchronized void adjustQoS(String limitStrategy, int limit_num, boolean... isInit) {
        try {
            String initStr = "";
            if (isInit.length == 1) {
                initStr = "Init: ";
                if (initFlag) {
                    return;
                }
            }
            if (limitStrategy.equalsIgnoreCase(LimitStrategy.ADAPT.name())) {
                log.info(initStr + queueName + " Adjust rabbitmq concurrency based on ADAPT policy QoS: {}", qos.get());

            } else if (limitStrategy.equalsIgnoreCase(LimitStrategy.NO_LIMIT.name())) {
                int addNum = NO_LIMIT_NUM - qos.get();
                qos.addAndGet(addNum);
                log.info(initStr + queueName + " Adjust rabbitmq concurrency based on NO_LIMIT policy QoS: {}", qos.get());

            } else if (limitStrategy.equalsIgnoreCase(LimitStrategy.LIMIT.name())) {
                LIMIT_NUM = limit_num;  // 修改业务优先策略并发数
                int addNum = LIMIT_NUM - qos.get();
                qos.addAndGet(addNum);
                log.info(initStr + queueName + " Adjust rabbitmq concurrency based on LIMIT policy QoS: {}", qos.get());
            }
            initFlag = true;
        } catch (Exception e) {
            log.error("Adjust message concurrency error", e);
        }
    }


    /**
     * 调整队列的并发量，业务优先和数据恢复优先直接切换至对应的并发量，自适应策略开启自适应开关 adaptFlag
     */
    public void adjustAdaptQoS(int prefetchNum) {
        try {
            int addNum = prefetchNum - qos.get();
            qos.addAndGet(addNum);
        } catch (Exception e) {
            log.error("Adjust message concurrency error", e);
        }
    }

    /**
     * 增加远程调试方法，可手动拉取消息
     */
    public void pullMessage(int num) {
        String s = "pull&" + num;
        subscriber.onNext(s);
    }
}
