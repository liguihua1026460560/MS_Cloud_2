package com.macrosan.lifecycle.mq;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.lifecycle.LifecycleCommandConsumer;
import com.macrosan.lifecycle.LifecycleService;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqChannels;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.mapToMsg;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.DEL_COMPONENT_RECORD;


public class LifecycleChannels {
    public static final Logger logger = LogManager.getLogger("LifecycleChannels.class");

    protected static ServerConfig config = ServerConfig.getInstance();
    protected static RedisConnPool pool = RedisConnPool.getInstance();

    private static final String HOST_UUID = config.getHostUuid();
    private static final String COMMAND_QUEUE_NAME = "lifecycle_command";
    private static final String COMMAND_ROUTINGKEY = "command_routingKey";
    private static final String COMMAND_PRODUCE = "produce";
    private static final String COMMAND_CONSUMER = "consume";

    private static final String EXCHANGE = "lifecycle_exchange";
    private static final Map<String, Channel> commandChannelMap = new ConcurrentHashMap<>();
    public static final Set<String> AVAIL_CHANNEL_NODE_SET = new ConcurrentHashSet<>();
    private static final Set<String> ALL_HEART_IP_SET = new ConcurrentHashSet<>();
    private static int LIFECYCLE_PREFETCH = 200;
    public static Map<Channel, LifecycleMessageBroker> brokerMap = new ConcurrentHashMap<>();  // channel 对应存储 messageBroker TODO
    public static LifecycleOverwriteHandler overwriteHandler = new LifecycleOverwriteHandler();

    public static void adjustLifecycleChannels(String limitStrategy, int limit_num){
        for (Channel channel : brokerMap.keySet()) {
            brokerMap.get(channel).adjustQoS(limitStrategy, limit_num);
        }
    }

    private static Channel openCommandChannel(String ip, String type) {
        try {
            Channel channel = RabbitMqChannels.getLifecycleChannel(ip);
            channel.queueDeclare(COMMAND_QUEUE_NAME, false, false, false, null);
            channel.exchangeDeclare(EXCHANGE, "direct");
            channel.queueBind(COMMAND_QUEUE_NAME, EXCHANGE, COMMAND_ROUTINGKEY);
            channel.basicQos(LIFECYCLE_PREFETCH);
            commandChannelMap.put(ip + type, channel);
            return channel;
        } catch (IOException | TimeoutException e) {
            logger.error("openCommandChannel error.");
            return null;
        }
    }

    public static void publishCommand(JsonObject jsonObject) throws IOException {
        Objects.requireNonNull(jsonObject);
        //把信息写成json放到消息队列中
        //logger.info(jsonObject+ "-----------input message queues");
        byte[] bytes = jsonObject.toString().getBytes();
        Channel channel = getSharedCommandChannel(COMMAND_PRODUCE);
        synchronized (COMMAND_ROUTINGKEY) {
            channel.basicPublish(EXCHANGE, COMMAND_ROUTINGKEY, MessageProperties.TEXT_PLAIN, bytes);
        }
    }

    public static void publishCommand(JsonObject jsonObject, String ip) throws IOException {
        Objects.requireNonNull(jsonObject);
        //把信息写成json放到消息队列中
        //logger.info(jsonObject+ "-----------input message queues");
        byte[] bytes = jsonObject.toString().getBytes();
        Channel channel = getSharedCommandChannel(ip, COMMAND_PRODUCE);
        synchronized (COMMAND_ROUTINGKEY) {
            channel.basicPublish(EXCHANGE, COMMAND_ROUTINGKEY, MessageProperties.TEXT_PLAIN, bytes);
        }
    }

    public static void initChannel() {
        try {
            getSharedCommandChannel(COMMAND_CONSUMER);
            ALL_HEART_IP_SET.forEach(ip -> getSharedCommandChannel(ip, COMMAND_PRODUCE));
            logger.info("init lifecycle channel.");
        } catch (Exception e) {
            logger.error("init lifecycle channel failed.", e);
        }
    }

    public static void startCommandConsumer() {
        try {
            Channel channel = getSharedCommandChannel(COMMAND_CONSUMER);
            LifecycleCommandConsumer consumer = new LifecycleCommandConsumer(channel);
            LifecycleMessageBroker broker = new LifecycleMessageBroker(channel, COMMAND_QUEUE_NAME, consumer);
            consumer.messageBroker = broker;
            channel.basicConsume(COMMAND_QUEUE_NAME, false, consumer);
            brokerMap.put(channel, consumer.messageBroker);
            initLifecycleQoS(broker);
            logger.info("startCommandConsumer.");
        } catch (Exception e) {
            ServerConfig.getInstance().getVertx().setTimer(10_000, s -> startCommandConsumer());
            logger.error("startCommandConsumer failed.", e);
        }
    }

    /**
     * 生命周期队列初始化失败重试成功后执行，确保此时生命周期队列的并发能准确切换
     * 依照切换时限流器中的策略设置并发，若出错则以默认并发(20)设置
     **/
    private static void initLifecycleQoS(LifecycleMessageBroker broker){
        String limitStrategy = "";
        try {
            String lifecycle_rabbit = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_rabbit");
            limitStrategy = RecoverLimiter.getInstance().getDataLimiter().getStrategy().name();
            if (null != lifecycle_rabbit && !limitStrategy.isEmpty()) {
                broker.adjustQoS(limitStrategy, Integer.parseInt(lifecycle_rabbit),true);
            }
        } catch (Exception e) {
            logger.error("", e);
            broker.adjustQoS(limitStrategy, 20);
        }
    }

    private static Channel getSharedCommandChannel(String type) {
        return getSharedCommandChannel(LOCAL_IP_ADDRESS, type);
    }

    private static Channel getSharedCommandChannel(String ip, String type) {
        if (commandChannelMap.containsKey(ip + type)) {
            return commandChannelMap.get(ip + type);
        }
        return openCommandChannel(ip, type);
    }

    public static void init() {
        ScanArgs scanArgs = new ScanArgs().match("*");
        KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
        keyScanCursor.setCursor("0");
        KeyScanCursor<String> res;
        do {
            res = pool.getCommand(REDIS_NODEINFO_INDEX).scan(keyScanCursor, scanArgs);
            for (String uuid : res.getKeys()) {
                String heartIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, HEART_IP);
                if (HOST_UUID.equals(uuid)) {
                    heartIp = LOCAL_IP_ADDRESS;
                }
                AVAIL_CHANNEL_NODE_SET.add(heartIp);
                ALL_HEART_IP_SET.add(heartIp);
            }
            keyScanCursor.setCursor(res.getCursor());
        } while (!res.isFinished());

        if (overwriteHandler != null) {
            overwriteHandler.init();
        }

        ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(1,
                runnable -> new Thread(runnable, "heartIp-mq-check"));
        scheduledThreadPool.scheduleAtFixedRate(() -> {
            try {
                AVAIL_CHANNEL_NODE_SET.addAll(ALL_HEART_IP_SET);
            } catch (Exception e) {

            }
        }, 0, 10, TimeUnit.MINUTES);
    }

    public static LifecycleOverwriteHandler getOverwriteHandler() {
        return overwriteHandler;
    }

    public static class LifecycleOverwriteHandler {
        private final UnicastProcessor<Tuple2<String, MetaData>> processor = UnicastProcessor.create(Queues.<Tuple2<String, MetaData>>unboundedMultiproducer().get());
        public void init() {
            processor
                    .publishOn(LifecycleService.getScheduler())
                    .flatMap(tuple2 -> deleteOverwriteLifeKey(tuple2.var1, tuple2.var2))
                    .subscribe();
        }

        public void handle(String vnode, MetaData data) {
            int maxKeys = 20_000;
            if (processor.size() < maxKeys) {
                processor.onNext(new Tuple2<>(vnode, data));
            }
        }

        public long size() {
            return processor.size();
        }

        private Mono<Boolean> deleteOverwriteLifeKey(String vnode, MetaData metaData) {
            MonoProcessor<Boolean> res = MonoProcessor.create();
            try {
                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
                String lifeCycleMetaKey = Utils.getLifeCycleMetaKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(vnode).block();
                List<SocketReqMsg> msgs = mapToMsg(lifeCycleMetaKey, "", nodeList);
                ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DEL_COMPONENT_RECORD, String.class, nodeList);
                responseInfo.responses.subscribe(s -> {
                }, e -> {
                    logger.error("", e);
                    res.onNext(false);
                }, () -> {
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        res.onNext(true);
                    } else {
                        res.onNext(false);
                    }
                });
            } catch (Exception e) {
                logger.debug("", e);
                res.onNext(false);
            }
            return res;
        }

    }
}