package com.macrosan.rabbitmq;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.error.ErrorConstant.ECErrorType;
import com.macrosan.ec.error.ErrorFunctionCache;
import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.ec.rebuild.*;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.PropertiesUtil;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.RECOVER_VNODE_DATA;
import static com.macrosan.ec.error.ErrorFunctionCache.NODE_LIST_TYPE_REFERENCE;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.rebuild.RebuildCheckpointManager.REBUILD_COMPLETE_STATUS;
import static com.macrosan.ec.rebuild.RebuildCheckpointManager.VNODE_COMPLETED_SET_PREFIX;
import static com.macrosan.ec.rebuild.RebuildRabbitMq.REBUILD_QUEUE_NAME_PREFIX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.rabbitmq.RabbitMqChannels.CONSUMER_SCHEDULER;
import static com.macrosan.rabbitmq.RabbitMqChannels.initConsumerChannelByIp;
import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.ERROR_MQ_TYPE.*;

/**
 * 对象相关错误消息的消费者
 *
 * @author fanjunxi
 */
@Log4j2
public class ObjectConsumer {
    private static final RedisConnPool REDIS_CONN_POOL = RedisConnPool.getInstance();

    private static final ObjectConsumer INSTANCE = new ObjectConsumer();

    public static ObjectConsumer getInstance() {
        return INSTANCE;
    }

    public static Map<Channel, MessageBroker> brokerMap = new ConcurrentHashMap<>();

    public static Map<String, List<Channel>> channelMap = new HashMap<>();

    public void init(String uuid) {
        String heartIp = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
        // HEART_IP_LIST位置不可移动，加节点时会再次init加入ip；6208需HEART_IP_LIST与RabbitMqUtils一起初始化，保证Rabbit_MQ_HEALTH及时更新，为使不重复加入判断
        try {
            synchronized (HEART_IP_LIST) {
                if (!HEART_IP_LIST.contains(heartIp)) {
                    RabbitMqUtils.HEART_IP_LIST.add(heartIp);
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        RabbitMqUtils.IP_UUID_MAP.put(heartIp, uuid);
        boolean success = "1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(ServerConfig.getInstance().getHostUuid(), "isRemoved"));
        if ("1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(ServerConfig.getInstance().getHostUuid(), "isRemoved"))) {
            //当前节点待移除不再在其他节点初始化队列
            return;
        }
        initConsumers(heartIp);
    }

    public void remove(String uuid) {
        String heartIp = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
        removeQueues(heartIp, uuid);
        RabbitMqUtils.IP_UUID_MAP.remove(heartIp);
        try {
            synchronized (HEART_IP_LIST) {
                if (HEART_IP_LIST.contains(heartIp)) {
                    RabbitMqUtils.HEART_IP_LIST.remove(heartIp);
                    channelMap.getOrDefault(heartIp, new ArrayList<>()).forEach(channel -> {
                        try {
                            log.info("close consumer channel {}, {}", heartIp, channel);
                            log.info("close consumer channel connection {}, {}", heartIp, channel.getConnection());
                            AutorecoveringChannel channel1 = (AutorecoveringChannel) channel;
                            if (channel1.isOpen()) {
                                channel1.close();
                            }
                        } catch (Exception e) {
                            log.error("close consumer channel error!", e);
                        }
                    });
                    log.info("====start close ec and lifecycle connection");
                    RabbitMqChannels.closeConnection(heartIp);
                    log.info("====start close notification connection");
                    RabbitMqNotification.closeConnection(heartIp);
                    log.info("====start close Rebuild connection");
                    RebuildRabbitMq.closeRebuildConnection(heartIp);
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }

    }

    public void init() {
        //从表8拿到各节点的uuid
        List<String> tempNodeList = scanKeys(REDIS_NODEINFO_INDEX).collectList().block();
        //根据各uuid获得所有后端ip，并将两者关联。
        if (tempNodeList != null) {
            tempNodeList.forEach(hostuuid -> {
                init(hostuuid);
            });
        } else {
            log.error("get REDIS_NODEINFO failed.");
        }
        msgConsumer(CURRENT_IP, LOW_SPEED);
        //每个ip下初始化consumer。
//        RabbitMqUtils.HEART_IP_LIST.forEach(this::initConsumers);

        //开始磁盘状态周期性更新
        getDiskStatusPeriodically();
//        scheduledThreadPoolExecutor.schedule(this::CheckCache, 30, TimeUnit.SECONDS);
        log.info("init consumers complete.");
    }

//    public void CheckCache() {
//        log.info("HEART_IP_LIST :{}", HEART_IP_LIST);
//        log.info("node cache=====: {}", NodeCache.getCache());
//        log.info("rootSecretKeyCache=====: {}", RootSecretKeyManager.getIpUuidMap());
//        scheduledThreadPoolExecutor.schedule(this::CheckCache, 300, TimeUnit.SECONDS);
//    }

    public void addHeartIp(String uuid) {
        String heartIp = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
        try {
            synchronized (HEART_IP_LIST) {
                if (!HEART_IP_LIST.contains(heartIp)) {
                    RabbitMqUtils.HEART_IP_LIST.add(heartIp);
                    log.info("add new node heart ip {} successfully", heartIp);
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        RabbitMqUtils.IP_UUID_MAP.put(heartIp, uuid);
//        initConsumers(heartIp);
    }


    /**
     * 初始化consumer。
     * 这里初始化的consumer所在的队列在别的节点上。本地不会消费本地生产的信息
     *
     * @param ip 要消费的节点的heart_ip
     */
    private void initConsumers(String ip) {
        msgConsumer(ip, EC_ERROR);
        msgConsumer(ip, EC_BUFFERED_ERROR);
    }

    /**
     * 直接在本地rabbitmq删除被移除节点消费的队列
     *
     * @param ip
     */
    public void removeQueues(String ip, String uuid) {//考虑下要不要带存储池//0006
        log.info("begin remove public queue");
        removeEcQueues(ip, EC_ERROR);
        removeEcQueues(ip, EC_BUFFERED_ERROR);

        //遍历存储池
        List<String> poolList = new ArrayList<>();
        RedisCommands<String, String> command = REDIS_CONN_POOL.getCommand(REDIS_POOL_INDEX);
        ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("storage_*"));

        while (iterator.hasNext()) {
            String storage = iterator.next();
            String poolName = command.hget(storage, "pool");
            poolList.add(poolName);
        }

        log.info("begin remove pool queue");
        removeEcQueuesForPool(ip, EC_ERROR, poolList);
        removeEcQueuesForPool(ip, EC_BUFFERED_ERROR, poolList);
        //删除被移除节点磁盘的相关信息

        removeRebuildQueues(uuid);


    }

    private void removeEcQueues(String ip, ERROR_MQ_TYPE errorMqType) {
        String queueName = errorMqType.queue() + getErrorMqSuffix(ip);
        Channel channel = initConsumerChannelByIp(CURRENT_IP, errorMqType);//连接本地的rabbitmq
        try {
            channel.queueDelete(queueName);
            log.info("del queue:{} success", queueName);
        } catch (Exception e) {
        }
        try {
            channel.close();
        } catch (Exception e) {
            log.error("", e);
        }

    }

    private void removeEcQueuesForPool(String ip, ERROR_MQ_TYPE errorMqType, List<String> poolList) {
        Channel channel = initConsumerChannelByIp(CURRENT_IP, errorMqType);//连接本地的rabbitmq
        for (String poolName : poolList) {
            String queueName = errorMqType.queue() + "_" + poolName + END_STR + getErrorMqSuffix(ip);
            log.info("del queue:{}", queueName);
            try {
                channel.queueDelete(queueName);
                log.info("del queue:{} success", queueName);
            } catch (IOException e) {

            }
        }
        try {
            channel.close();
        } catch (Exception e) {
            log.error("", e);
        }

    }

    private void removeRebuildQueues(String uuid) {
        //初始化连接
        try {
            Address address = new Address("127.0.0.1", 5672);
            Channel channel = new ConnectionFactory().newConnection(Arrays.asList(address)).createChannel();

            //这里改成从表12获取缓存的待移除磁盘信息
            Map<String, String> removedLunInfo = REDIS_CONN_POOL.getCommand(REDIS_MIGING_V_INDEX).hgetall("remove_node_lun_" + uuid);
            if (removedLunInfo != null) {
                for (Map.Entry<String, String> entry : removedLunInfo.entrySet()) {
                    String disk = entry.getKey();
                    String poolName = entry.getValue();
                    String queueName = REBUILD_QUEUE_NAME_PREFIX + "-" + disk + "-" + poolName + END_STR;
                    channel.queueDelete(queueName);
                    log.info("del queue:{} success", queueName);
                }
            }

            channel.close();
        } catch (IOException | TimeoutException e) {
            log.error("delete queue error", e);
        }
    }

    public void initPoolConsumers(String poolName) {
        List<String> tempNodeList = scanKeys(REDIS_NODEINFO_INDEX).collectList().block();
        //根据各uuid获得所有后端ip，并将两者关联。
        if (tempNodeList != null) {
            tempNodeList.forEach(uuid -> {
                String heartIp = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
                msgConsumer(heartIp, poolName, EC_ERROR);
                msgConsumer(heartIp, poolName, EC_BUFFERED_ERROR);
            });
        } else {
            log.error("get REDIS_NODEINFO failed.");
        }
        // 死信队列只由本节点进行消费
        RebuildDeadLetter.getInstance().msgConsumerForPool(CURRENT_IP, poolName,DEAD_LETTER);
    }

    public void initNodePoolConsumer(String uuid, String poolName) {
        String heartIp = REDIS_CONN_POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
        msgConsumer(heartIp, poolName, EC_ERROR);
        msgConsumer(heartIp, poolName, EC_BUFFERED_ERROR);
    }


    /**
     * 获取channel
     *
     * @param ip 目标连接的ip
     */
    private Channel getChannelInstance(String ip, String queueName, String routingKey, ERROR_MQ_TYPE type) throws IOException {
        //根据ip获得连接的信道
        Channel channel = initConsumerChannelByIp(ip, type);
        if (channel == null) {
            throw new IOException("create rabbitmq connnection failed, ip: " + ip);
        }
        /*
         * 声明一个队列
         * 第一个参数表示这个信道连接哪个队列
         * 第二个参数表示是否持久化，当这个参数设置为true，即使你的服务器关了从新开数据还是存在的
         * 第三个参数表示是否独占队列（排他），为 true 则设置队列为排他的。如果一个队列被声明为排 他队列，该队列仅对首次声明它的连接可见，并在连接断开时自动删除
         * 第四个参数表示队列脱离绑定时是否自动删除
         * 第五个参数表示扩展参数，可设置为null
         */
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, OBJ_EXCHANGE, routingKey);
        channel.addShutdownListener(e -> {
            if (!e.isInitiatedByApplication()) {
                log.error("{} {} object consumer channel shutdown.", ip, queueName, e);
            }
        });
        return channel;
    }

    /**
     * 去其他节点启动本节点的consumer, 接收的消息为SocketReqMsg，返回Mono<Boolean>
     *
     * @param ip 监听的ip
     */
    private void msgConsumer(String ip, ERROR_MQ_TYPE errorMqType) {
        try {
            String queueName = errorMqType.queue() + getErrorMqSuffix(CURRENT_IP);
            String routingKey = errorMqType.key() + getErrorMqSuffix(CURRENT_IP);
            Channel channel = getChannelInstance(ip, queueName, routingKey, errorMqType);
            channelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
            Consumer consumer;

            if (errorMqType.name().equalsIgnoreCase(EC_ERROR.name())) {  // EC_ERROR队列消息进行拦截
                MessageBroker messageBroker = new MessageBroker(channel, queueName);
                brokerMap.put(channel, messageBroker);
                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) {
                        try {
                            super.handleDelivery(consumerTag, envelope, properties, body);
                            Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(envelope, body);
                            messageBroker.processor.onNext(tuple2);
                        } catch (Exception e) {
                            log.error("Object consumer error", e);
                        }
                    }
                };
                initECErrorQoS(messageBroker);

            } else {  // EC_BUFFERED 和 LOW_SPEED队列消息不进行拦截
                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) {
                        try {
                            super.handleDelivery(consumerTag, envelope, properties, body);
                            dealMsg(channel, envelope, body, errorMqType);
                        } catch (Exception e) {
                            log.error("Object consumer error", e);
                        }
                    }
                };
            }

            /* 声明：basicConsume的autoAck为false时：没有手动应答，在没有指定qos时，消费者可以获取所有消息；
               当指定qos时，只能获取指定条数，没法继续获取下一条了，因为没有收到消费确认不会继续进行 */
            channel.basicConsume(queueName, false, consumer);

        } catch (IOException e) {
            log.error("Init Consumer failed, {}", e.getMessage());
            if (StringUtils.isNotEmpty(getUUID(ip))) {
                if (!"1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(ip), "isRemoved"))) {
                    ADD_NODE_SCHEDULER.schedule(() -> msgConsumer(ip, errorMqType), 10, TimeUnit.SECONDS);
                }
            }
//            ServerConfig.getInstance().getVertx().setTimer(10_000, s -> msgConsumer(ip, errorMqType));
        }

    }

    private long getMessageCount(Channel channel, String queueName, long messageCount) {
        try {
            messageCount = channel.queueDeclarePassive(queueName).getMessageCount();
        } catch (IOException e) {
            log.debug("queue: {} is not existing", queueName);
        }
        return messageCount;
    }

    private void msgConsumer(String ip, String poolName, ERROR_MQ_TYPE errorMqType) {//这里需要将EC_ERROR队列按照存储池来区分不同的队列，直接增加存储池对应的EC_error队列
        try {
            String queueName = errorMqType.queue() + "_" + poolName + END_STR + getErrorMqSuffix(CURRENT_IP);
            String oldName = errorMqType.queue() + "_" + poolName + getErrorMqSuffix(CURRENT_IP);
            String routingKey = errorMqType.key() + "_" + poolName + getErrorMqSuffix(CURRENT_IP);
            Channel channel0 = getChannelInstance(ip, oldName, routingKey + "_old", errorMqType);
            channelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel0);
            channel0.queueUnbind(oldName, OBJ_EXCHANGE, routingKey);
            Channel channel = getChannelInstance(ip, queueName, routingKey, errorMqType);
            channelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
            Consumer consumer0;
            Consumer consumer;
            long messageCount = 0;

            messageCount = getMessageCount(channel0, oldName, messageCount);
            if (errorMqType.name().equalsIgnoreCase(EC_ERROR.name())) {  // EC_ERROR队列消息进行拦截
                if (messageCount > 0) {
                    log.info("init consumer for old queue {}", oldName);
                    MessageBroker messageBroker0 = new MessageBroker(channel0, oldName);
                    brokerMap.put(channel0, messageBroker0);
                    consumer0 = new DefaultConsumer(channel0) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope,
                                                   AMQP.BasicProperties properties, byte[] body) {
                            try {
                                super.handleDelivery(consumerTag, envelope, properties, body);
                                Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(envelope, body);
                                messageBroker0.processor.onNext(tuple2);
                            } catch (Exception e) {
                                log.error("Object consumer error", e);
                            }
                        }
                    };
                    initECErrorQoS(messageBroker0);
                    channel0.basicConsume(oldName, false, consumer0);
                }

                MessageBroker messageBroker = new MessageBroker(channel, queueName);
                brokerMap.put(channel, messageBroker);
                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) {
                        try {
                            super.handleDelivery(consumerTag, envelope, properties, body);
                            Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(envelope, body);
                            messageBroker.processor.onNext(tuple2);
                        } catch (Exception e) {
                            log.error("Object consumer error", e);
                        }
                    }
                };
                initECErrorQoS(messageBroker);

            } else {  // EC_BUFFERED 和 LOW_SPEED队列消息不进行拦截
                if (messageCount > 0) {
                    log.info("init consumer for old queue {}", oldName);
                    consumer0 = new DefaultConsumer(channel0) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope,
                                                   AMQP.BasicProperties properties, byte[] body) {
                            try {
                                super.handleDelivery(consumerTag, envelope, properties, body);
                                dealMsg(channel0, envelope, body, errorMqType);
                            } catch (Exception e) {
                                log.error("Object consumer error", e);
                            }
                        }
                    };
                    channel0.basicConsume(oldName, false, consumer0);
                }

                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) {
                        try {
                            super.handleDelivery(consumerTag, envelope, properties, body);
                            dealMsg(channel, envelope, body, errorMqType);
                        } catch (Exception e) {
                            log.error("Object consumer error", e);
                        }
                    }
                };
            }

            /* 声明：basicConsume的autoAck为false时：没有手动应答，在没有指定qos时，消费者可以获取所有消息；
               当指定qos时，只能获取指定条数，没法继续获取下一条了，因为没有收到消费确认不会继续进行 */
            channel.basicConsume(queueName, false, consumer);
            if (messageCount > 0) {
                //这里增加一个map存放消息还未清空的队列，供后面另起线程检测队列是否清空然后删除队列
                log.info("need delete set add queue:{}", oldName);
                DeleteQueue queue = new DeleteQueue(channel0, oldName);
                ClearMqQueue.needClearQueue.add(queue);//需要携带IP信息
            } else {
                channel0.queueDelete(oldName);
            }

        } catch (IOException e) {
            log.error("Init Consumer failed, {}", e.getMessage());
            if (StringUtils.isNotEmpty(getUUID(ip))) {
                if (!"1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(ip), "isRemoved"))) {
                    ADD_NODE_SCHEDULER.schedule(() -> msgConsumer(ip, poolName, errorMqType), 10, TimeUnit.SECONDS);
                }
            }
//            ServerConfig.getInstance().getVertx().setTimer(10_000, s -> msgConsumer(ip, errorMqType));
        }

    }

    public static void initECErrorQoS(MessageBroker broker) {
        String limitStrategy = "";
        try {
            String ec_rabbit = REDIS_CONN_POOL.getCommand(REDIS_SYSINFO_INDEX).get("ec_rabbit");
            limitStrategy = RecoverLimiter.getInstance().getDataLimiter().getStrategy().name();
            if (null != ec_rabbit && !limitStrategy.isEmpty()) {
                broker.adjustQoS(limitStrategy, Integer.parseInt(ec_rabbit), true);
            }
        } catch (Exception e) {
            log.error("", e);
            broker.adjustQoS(limitStrategy, 5);
        }
    }

    public AtomicLong msgRunning = new AtomicLong(0);
    public static final int MAX_MSG_RUNNING = PropertiesUtil.getProperties().getIntegerProperty("macrosan.rabbit.running", 1200);

    public Mono<Boolean> dealMsg(Channel channel, Envelope envelope, byte[] body, ERROR_MQ_TYPE errorMqType) {
        if (EC_ERROR == errorMqType) {
            long running = msgRunning.getAndUpdate(l -> {
                if (l >= MAX_MSG_RUNNING) {
                    return l;
                } else {
                    return l + 1;
                }
            });

            if (running >= MAX_MSG_RUNNING) {
                //用rabbitmq的线程池调度
                return Mono.delay(Duration.ofSeconds(60), CONSUMER_SCHEDULER)
                        .flatMap(l -> dealMsg(channel, envelope, body, errorMqType));
            }

            try {
                return dealMsg0(channel, envelope, body, errorMqType)
                        .doFinally(s -> {
                            msgRunning.decrementAndGet();
                        });
            } catch (Throwable e) {
                msgRunning.decrementAndGet();
                throw new RuntimeException(e);
            }
        } else {
            try {
                return dealMsg0(channel, envelope, body, errorMqType);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 分发消息至 normalQueue、bufferedQueue以及lowSpeedQueue的逻辑
     */
    public Mono<Boolean> dealMsg0(Channel channel, Envelope envelope, byte[] body, ERROR_MQ_TYPE errorMqType) throws IOException {
        String message = new String(body, StandardCharsets.UTF_8);
        SocketReqMsg msg = Json.decodeValue(message, SocketReqMsg.class);
        ECErrorType errorType = ECErrorType.valueOf(msg.getMsgType());
        log.debug(errorType);
        Function<SocketReqMsg, Mono<Boolean>> function = ErrorFunctionCache.get(errorType);

        //msgType不存在，当作ack消费掉
        if (null == function) {
            log.error("{} don't has handler", errorType);
            channel.basicAck(envelope.getDeliveryTag(), false);
            return Mono.just(true);
        }

        if (null == msg.get("lun")) {
            normalQueueConsumer(true, channel, envelope, body, function, msg, errorMqType);
            return Mono.just(true);
        }
        //错误盘中是否存在未移除盘
        final boolean[] diskNotRemoved = {false};
        final boolean[] diskNotFault = {false};
        List<String> disks = Arrays.stream(msg.get("lun").split("#"))
                .map(lun -> {
                    String diskName = lun.contains("@") ? lun : getDiskName(CURRENT_IP, lun);
                    if (!RemovedDisk.getInstance().contains(diskName)) {
                        diskNotRemoved[0] = true;
                    }
                    if (!DiskStatusChecker.isRebuildWaiter(lun.contains("@") ? lun.split("@")[1] : lun)) {
                        diskNotFault[0] = true;
                    }
                    return diskName;
                })
                .collect(Collectors.toList());


        //异常消息相关的盘都没有在RemovedDisk里，即还未进行盘移除的流程。
        if (!diskNotRemoved[0] || !diskNotFault[0]) {
            //恢复vnode数据的消息重新发布到其他盘进行重试
            if (errorType == ECErrorType.RECOVER_VNODE_DATA) {
                Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(b -> {
                    String str = msg.get("nodeList");
                    List<Tuple3<String, String, String>> nodeList = Json.decodeValue(str, NODE_LIST_TYPE_REFERENCE);
                    int curIndex = 0;
                    for (int i = 0; i < nodeList.size(); i++) {
                        String curDisk = getDiskName(nodeList.get(i).var1, nodeList.get(i).var2);
                        if (disks.contains(curDisk)) {
                            curIndex = i;
                            break;
                        }
                    }

                    curIndex++;
                    int errorIndex = Integer.parseInt(msg.get("errorIndex"));
                    while (curIndex == errorIndex) {
                        curIndex++;
                    }

                    if (curIndex >= nodeList.size()) {
                        RebuildRabbitMq.getMaster().hincrby("running_" + msg.get("poolQueueTag"), "scanNum", -1);
                        log.info("can not recover vnode {} data", msg);
                    } else {
                        msg.put("lun", nodeList.get(curIndex).var2);
                        ObjectPublisher.basicPublish(nodeList.get(curIndex).var1, msg, RECOVER_VNODE_DATA);
                    }
                });
            }


            if (errorType == ECErrorType.REMOVE_DISK) {
                Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(b -> RebuildRabbitMq.getMaster().hincrby("running_" + msg.get("poolQueueTag"), "vnodeNum", -1));
            }

            if (errorType == ECErrorType.ADD_DISK) {
                Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(b -> {
                    String poolQueueTag = msg.get("poolQueueTag");
                    String runningKey = "running_" + poolQueueTag;
                    String operate = RebuildRabbitMq.getMaster().hget(runningKey, "operate");
                    MigrateUtil.checkAndHandleAddDiskCompletion(poolQueueTag, msg.get("vnode"), runningKey, operate);
                });
            }

            normalAck(channel, envelope);
            return Mono.just(true);
        }

        boolean isDiskAvailable = diskIsAvailable(disks);

        if (errorMqType == EC_ERROR) {
            return normalQueueConsumer(isDiskAvailable, channel, envelope, body, function, msg, errorMqType);
        } else if (errorMqType == EC_BUFFERED_ERROR) {
            bufferedQueueConsumer(isDiskAvailable, channel, envelope, body, function, msg, disks.get(0));
        } else {
            lowSpeedQueueConsumer(channel, envelope, body, function, msg);
        }

        return Mono.just(true);
    }


    private Mono<Boolean> normalQueueConsumer(boolean isAvailable, Channel channel, Envelope envelope, byte[] body,
                                              Function<SocketReqMsg, Mono<Boolean>> function, SocketReqMsg msg, ERROR_MQ_TYPE errorMqType) {

        MonoProcessor<Boolean> res = MonoProcessor.create();

        //磁盘不可用的消息发布往缓冲队列。
        if (!isAvailable) {
            String bufferedRoutingKey = EC_BUFFERED_ERROR.key() + envelope.getRoutingKey().substring(errorMqType.key().length());
//            String bufferedRoutingKey = EC_BUFFERED_ERROR.key() + getErrorMqSuffix(CURRENT_IP);
            requeueToAnotherQueue(channel, bufferedRoutingKey, body, envelope);
            res.onNext(true);
        } else {
            function.apply(msg).publishOn(DISK_SCHEDULER).subscribe(hasConsumed -> {
                        //直接ack，避免消息重复。如果还有异常前面的步骤会重新publish，
                        normalAck(channel, envelope);
                        res.onNext(true);
                    },
                    //在function中若出现错误(如getObjectBytes失败)不直接放回,而是再次发布，因为异常也可能是所在磁盘问题导致（如修复数据时getObject失败报错），
                    // 不会通过前面的代码发布，而实际上并没有成功修复。
                    e -> {
                        if (e instanceof MsException && e.getMessage() != null
                                && (e.getMessage().contains("version time is not init") || e.getMessage().contains("Cannot get correct syncStamp"))) {
                            DISK_SCHEDULER.schedule(() -> {
                                normalRequeue(channel, envelope);
                                res.onNext(true);
                            }, 30, TimeUnit.SECONDS);
                        } else if (e instanceof MsException && ErrorNo.REPEATED_KEY != ((MsException) e).getErrCode()) {
                            log.error("queue error 1, ", e);
                            normalAck(channel, envelope);
                            ObjectPublisher.publish(CURRENT_IP, msg, ECErrorType.valueOf(msg.getMsgType()));
                            res.onNext(true);
                        } else if (e instanceof NoPrintLogException) {
                            normalRequeue(channel, envelope);
                            res.onNext(true);
                        } else {
                            if (e instanceof TimeoutException) {
                                log.error("queue error 2 {},errorType:{}", e.getMessage(), errorMqType);
                            } else {
                                if (!(e instanceof MsException)) {
                                    log.error("queue error 2, ", e);
                                } else {
                                    if (ErrorNo.REPEATED_KEY != ((MsException) e).getErrCode()) {
                                        log.error("queue error 2, ", e);
                                    }
                                }
                            }
                            //其他的错误，如超时、已有同样的消息正在处理等，将消息放回队列。
                            normalRequeue(channel, envelope);
                            res.onNext(true);
                        }
                    });
        }

        return res;

    }

    private void bufferedQueueConsumer(boolean isAvailable, Channel limitedChannel, Envelope envelope, byte[] body,
                                       Function<SocketReqMsg, Mono<Boolean>> function, SocketReqMsg msg, String diskName) {

        // 检查对应节点的RabbitMQ健康情况，如果cg_sp0的占用率大于85%，则直接在buffer队列消费而不转到EC_ERROR队列
        Boolean isFine = RabbitMqUtils.isLimitSpace(limitedChannel.getConnection().getAddress().getHostAddress());

        // 磁盘可用且MQ未满则将消息发往普通队列
        if (isAvailable && isFine) {
            String routingKey = EC_ERROR.key() + envelope.getRoutingKey().substring(EC_BUFFERED_ERROR.key().length());
//            String routingKey = EC_ERROR.key() + getErrorMqSuffix(CURRENT_IP);
            requeueToAnotherQueue(limitedChannel, routingKey, body, envelope);
        } else {
            function.apply(msg).publishOn(DISK_SCHEDULER).subscribe(needBuffer -> {
                        //异常解决（true），ack，盘状态复原。异常未解决(false)，延迟ack
                        if (needBuffer) {
                            normalAck(limitedChannel, envelope);
                            cleanErrorAmount(diskName);
                        } else {
                            Mono.delay(Duration.ofMinutes(5)).subscribe(s -> normalAck(limitedChannel, envelope));
                        }
                    },
                    e -> {
                        log.error("buffered queue error . ", e);
                        //在function中若出现MsException，延时放回队尾。
                        String currentRoutineKey = envelope.getRoutingKey();
//                        String currentRoutineKey = EC_BUFFERED_ERROR.key() + getErrorMqSuffix(CURRENT_IP);
                        if (e instanceof MsException && ((MsException) e).getErrCode() != ErrorNo.REPEATED_KEY) {
                            Mono.delay(Duration.ofMinutes(5)).publishOn(DISK_SCHEDULER).subscribe(s -> requeueToAnotherQueue(limitedChannel, currentRoutineKey, body, envelope));
                        } else {
                            requeueToAnotherQueue(limitedChannel, currentRoutineKey, body, envelope);
                        }
                    });
        }
    }

    private void lowSpeedQueueConsumer(Channel channel, Envelope envelope, byte[] body,
                                       Function<SocketReqMsg, Mono<Boolean>> function, SocketReqMsg msg) {
        function.apply(msg).publishOn(DISK_SCHEDULER).subscribe(hasConsumed -> {
                    //延迟再ack，降低消费速度。
                    Mono.delay(Duration.ofMillis(10)).publishOn(DISK_SCHEDULER).subscribe(s -> normalAck(channel, envelope));
                },
                //在function中若出现错误(如getObjectBytes失败)不直接放回,而是再次发布，因为异常也可能是所在磁盘问题导致（如修复数据时getObject失败报错），
                // 不会通过前面的代码发布，而实际上并没有成功修复。
                e -> {
                    log.error("low speed queue error, ", e);
                    if (e instanceof MsException && ErrorNo.REPEATED_KEY != ((MsException) e).getErrCode()) {
                        normalAck(channel, envelope);
                        ObjectPublisher.publish(CURRENT_IP, msg, ECErrorType.valueOf(msg.getMsgType()));
                    } else {
                        //其他的错误，如超时、已有同样的消息正在处理等，将消息放回队列。
                        normalRequeue(channel, envelope);
                    }
                });
    }
}
