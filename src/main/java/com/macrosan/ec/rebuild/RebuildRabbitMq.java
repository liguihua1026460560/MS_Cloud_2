package com.macrosan.ec.rebuild;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.error.StatisticErrorHandler;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.AutoRecoveryShutdown;
import com.macrosan.rabbitmq.ClearMqQueue;
import com.macrosan.rabbitmq.DeleteQueue;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.DelaySet;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.ErrorFunctionCache.NODE_LIST_TYPE_REFERENCE;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.CREATE_REBUILD_QUEUE;
import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class RebuildRabbitMq {
    private static int PORT = 5672;
    public static String REBUILD_QUEUE_NAME_PREFIX = "rebuild-queue";
    private static String REBUILD_QUEUE_KEY_PREFIX = "rebuild-queue-key";
    private static int QOS = 10;
    public static Map<Channel, RebuildMessageBroker> brokerMap = new ConcurrentHashMap<>();
    private final Set<String> consumerDisk = new ConcurrentHashSet<>();
    private final Map<String, Channel> channels = new HashMap<>();
    public static Map<String, List<Connection>> rebuildConnectionMap = new HashMap<>();
    public static Map<String, List<Channel>> rebuildChannelMap = new HashMap<>();
    private ConnectionFactory factory;
    private ThreadLocal<Random> random = ThreadLocal.withInitial(Random::new);
    private static long DEALYPRINT = 200L;
    private static Map<String, String> warnMap = new HashMap<>();  // 已移除rebuild队列map <consumerTag, queueName>

    public static Map<String, String> getWarnMap() {
        return warnMap;
    }

    public static void putWarnMap(String consumerTag, String queueName) {
        warnMap.put(consumerTag, queueName);
    }

    private static ThreadLocal<StatefulRedisConnection<String, String>> threadLocal = ThreadLocal.withInitial(() ->
            RedisConnPool.getInstance().getSharedConnection(REDIS_MIGING_V_INDEX).newMaster());

    public static RedisCommands<String, String> getMaster() {
        if (Thread.currentThread().getName().startsWith("lettuce")) {
            log.error("get sync redis connection on lettuce thread ", new Throwable());
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get sync redis connection on lettuce thread");
        }

        StatefulRedisConnection<String, String> connection = threadLocal.get();
        try {
            connection.sync().ping();
        } catch (Exception e) {

        }
        return connection.sync();
    }

    Channel getNewLocalChannel() throws IOException, TimeoutException {
        Address address = new Address("127.0.0.1", PORT);
        Channel channel = factory.newConnection(Arrays.asList(address)).createChannel();
        channel.exchangeDeclare(OBJ_EXCHANGE, "direct", true, false, null);
        return channel;
    }

    private final DelaySet<String> failSet = new DelaySet<>();


    public Channel getChannel(String ip) throws IOException, TimeoutException {
        if (failSet.contains(ip)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get rabbitmq channel with " + ip + " fail");
        }

        String key = Thread.currentThread().getName() + ip;
        Channel channel = channels.get(key);
        if (null == channel) {
            synchronized (channels) {
                channel = channels.get(key);
                if (null == channel) {
                    Address address = new Address(ip, PORT);
                    Connection connection = factory.newConnection(Arrays.asList(address));
                    connection.addShutdownListener(cause -> {
                        if (cause instanceof ShutdownSignalException) {//这里做一个定时任务定时查询节点的状态
                            AutoRecoveryShutdown.checkNeedRecover(ip, connection);
                        }
                        log.info("conn hostName:{}, conn:{}", ip, connection);
                    });
                    rebuildConnectionMap.computeIfAbsent(ip, k -> new ArrayList<Connection>()).add(connection);
                    channel = connection.createChannel();
                    channel.exchangeDeclare(OBJ_EXCHANGE, "direct", true, false, null);
                    channels.put(key, channel);
                }
            }

            channel.addShutdownListener(e -> {
                log.error("rebuild channel shutdown, ", e);
            });
            rebuildChannelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
        }

        return channel;
    }

    public static void closeRebuildConnection(String ip) {
        List<Channel> channels = rebuildChannelMap.get(ip);
        if (null != channels) {
            for (Channel channel : channels) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (Exception e) {
                    log.error("close rebuild channel error", e);
                }
            }
        }

        List<Connection> connections  = rebuildConnectionMap.get(ip);
        if (null != channels) {
            for (Connection connection : connections) {
                try {
                    boolean b = connection instanceof AutorecoveringConnection;
                    log.info("close connection type: {}", b);
                    if (connection.isOpen()) {
                        connection.close();
                    }
                } catch (Exception e) {
                    log.error("close rebuild connection error", e);
                }
            }
        }

    }

    public void publish(String ip, ReBuildTask task) throws IOException, TimeoutException {
        try {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("disk", task.disk);
            String poolName = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hget(task.disk, "pool_name");

            if (!consumerDisk.contains(task.disk)) {
                Flux<Boolean> res = Flux.empty();
                for (String i : HEART_IP_LIST) {
                    Mono<Boolean> mono = RSocketClient.getRSocket(i, BACK_END_PORT)
                            .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), CREATE_REBUILD_QUEUE.name())))
                            .timeout(Duration.ofSeconds(30))
                            .map(r -> true)
                            .onErrorReturn(true);

                    res = res.mergeWith(mono);
                }

                res.collectList().publishOn(Schedulers.immediate()).subscribe(l -> {
                    try {
                        Channel channel = getChannel(ip);
                        String key = REBUILD_QUEUE_KEY_PREFIX + "-" + task.disk + "-" + poolName;
                        channel.basicPublish(OBJ_EXCHANGE, key, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(task).getBytes());
                    } catch (Exception e) {
                        log.error("", e);
                    }
                });
            } else {
                Channel channel = getChannel(ip);
                String key = REBUILD_QUEUE_KEY_PREFIX + "-" + task.disk + "-" + poolName;
                channel.basicPublish(OBJ_EXCHANGE, key, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(task).getBytes());
            }
        } catch (Exception e) {
            //经过延时后如果重试失败才继续延时，上次延时未结束不继续延时
            if (failSet.timeout(ip)) {
                failSet.add(ip, 10_000);
            }
            throw e;
        }
    }

    public void publish(ReBuildTask task) {
        while (true) {
            int index = random.get().nextInt(HEART_IP_LIST.size());
            try {
                publish(HEART_IP_LIST.get(index), task);
                break;
            } catch (Exception e) {
                log.error("publish msg with {} fail", HEART_IP_LIST.get(index), e);
            }
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

    public void consumer(String disk) {
        try {
            synchronized (consumerDisk) {
                if (!consumerDisk.contains(disk)) {
                    String poolName = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hget(disk, "pool_name");
                    Channel localChannel = getNewLocalChannel();
                    String oldName1 = REBUILD_QUEUE_NAME_PREFIX + "-" + disk;
                    String oldName0 = REBUILD_QUEUE_NAME_PREFIX + "-" + disk + "-" + poolName;
                    String name = REBUILD_QUEUE_NAME_PREFIX + "-" + disk + "-" + poolName + END_STR;
                    String key = REBUILD_QUEUE_KEY_PREFIX + "-" + disk + "-" + poolName;
                    //判断旧队列是否存在或者是否包含消息
                    //处理升级初始化磁盘队列时移除旧版本创建的磁盘队列
                    long messageCount = -1;
                    Channel localChannel1 = getNewLocalChannel();
                    messageCount = getMessageCount(localChannel1, oldName1, messageCount);

                    long messageCount0 = -1;
                    Channel localChannel0 = getNewLocalChannel();
                    messageCount0 = getMessageCount(localChannel0, oldName0, messageCount0);

                    if (messageCount > 0) {
                        log.info("init consumer for old queue {}", oldName1);
                        localChannel1.queueDeclare(oldName1, true, false, false, null);
                        localChannel1.queueUnbind(oldName1, OBJ_EXCHANGE, key);//保留旧队列的同时需要解绑原routingKey
                        localChannel1.queueBind(oldName1, OBJ_EXCHANGE, key + "_old1");
                        localChannel1.basicQos(QOS);
                        RebuildMessageBroker messageBroker1 = new RebuildMessageBroker(localChannel1, oldName1);
                        MsgConsumer msgConsumer1 = new MsgConsumer(localChannel1, messageBroker1);
                        brokerMap.put(localChannel1, messageBroker1);
                        localChannel1.basicConsume(oldName1, msgConsumer1);
                        initRebuildQoS(messageBroker1);
                        log.info("need delete set add queue:{}", oldName1);
                        DeleteQueue queue = new DeleteQueue(localChannel1, oldName1);
                        ClearMqQueue.needClearQueue.add(queue);
                    } else if (messageCount == 0){
                        localChannel1.queueDelete(oldName1);
                    }
                    if (messageCount0 > 0) {
                        log.info("init consumer for old queue {}", oldName0);
                        localChannel0.queueDeclare(oldName0, true, false, false, null);
                        localChannel0.queueUnbind(oldName0, OBJ_EXCHANGE, key);
                        localChannel0.queueBind(oldName0, OBJ_EXCHANGE, key + "_old0");
                        localChannel0.basicQos(QOS);
                        RebuildMessageBroker messageBroker0 = new RebuildMessageBroker(localChannel0, oldName0);
                        MsgConsumer msgConsumer0 = new MsgConsumer(localChannel0, messageBroker0);
                        brokerMap.put(localChannel0, messageBroker0);
                        localChannel0.basicConsume(oldName0, msgConsumer0);
                        initRebuildQoS(messageBroker0);
                        log.info("need delete set add queue:{}", oldName0);
                        DeleteQueue queue = new DeleteQueue(localChannel0, oldName0);
                        ClearMqQueue.needClearQueue.add(queue);
                    } else if (messageCount0 == 0){
                        localChannel0.queueDelete(oldName0);
                    }

                    localChannel.queueDeclare(name, true, false, false, null);
                    localChannel.queueBind(name, OBJ_EXCHANGE, key);
                    localChannel.basicQos(QOS);

                    RebuildMessageBroker messageBroker = new RebuildMessageBroker(localChannel, name);
                    MsgConsumer msgConsumer = new MsgConsumer(localChannel, messageBroker);
                    brokerMap.put(localChannel, messageBroker);

                    String curConsumerTag = localChannel.basicConsume(name, msgConsumer);
                    messageBroker.setConsumerTag(curConsumerTag);
                    localChannel.addShutdownListener(e -> {
                        Mono.delay(Duration.ofMillis(DEALYPRINT)).subscribe(l -> {
                            Boolean isWarn = warnMap.containsKey(curConsumerTag);
                            if (warnMap.size() != 0 && isWarn != null && isWarn) {
                                log.warn("The queue {} is invalidated because the corresponding disk has been removed out.", name);
                                synchronized (warnMap) {
                                    warnMap.remove(curConsumerTag);
                                }
                            } else {
                                log.error("disk rebuild channel {} shutdown. The consumerTag is {}.", name, curConsumerTag);
                            }
                        });
                    });
                    initRebuildQoS(messageBroker);
                    consumerDisk.add(disk);

                }
            }
        } catch (Exception e) {
//            ServerConfig.getInstance().getVertx().setTimer(10_000L, s -> consumer(disk));
            ADD_NODE_SCHEDULER.schedule(() -> consumer(disk), 10, TimeUnit.SECONDS);
            log.error("", e);
        }
    }

    /**
     * rebuild队列初始化失败重试成功后执行，确保此时rebuild队列的并发能准确切换
     * 依照切换时限流器中的策略设置并发，若出错则以默认并发(2)设置
     **/
    private static void initRebuildQoS(RebuildMessageBroker broker) {
        String limitStrategy = "";
        synchronized (RecoverLimiter.getInstance().getDataLimiter().getStrategy()) {
            try {
                String rebuild_rabbit = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("rebuild_rabbit");
                limitStrategy = RecoverLimiter.getInstance().getDataLimiter().getStrategy().name();
                if (null != rebuild_rabbit && !limitStrategy.isEmpty()) {
                    broker.adjustQoS(limitStrategy, Integer.parseInt(rebuild_rabbit), true);
                }
            } catch (Exception e) {
                log.error("", e);
                broker.adjustQoS(limitStrategy, 2);
            }
        }
    }

    /**
     * channel已初始化再次切换并发量时使用，根据生效的策略限制rebuildRabbitMQ并发量，更新频次当前为1分钟一次
     */
    public static void adjustRebuildMqChannel(String limitStrategy, int limit_num) {

        // 用于保存需移除的掉线磁盘队列
        ArrayList<Channel> removeList = new ArrayList<>();
        for (Channel channel : brokerMap.keySet()) {
            boolean flag = true;
            try {
                AMQP.Queue.DeclareOk OK = channel.queueDeclarePassive(brokerMap.get(channel).queueName);  // 检查队列是否被移除
            } catch (Exception e) {
                flag = false;
                removeList.add(channel);
                synchronized (warnMap) {
                    warnMap.put(brokerMap.get(channel).consumerTag, brokerMap.get(channel).queueName);  // 被移除队列加入warnMap
                }
                log.info("Queue removed, no need to switch. QueueName: {}, ConsumerTag: {}.", brokerMap.get(channel).queueName, brokerMap.get(channel).consumerTag);
            }

            if (flag) {
                brokerMap.get(channel).adjustQoS(limitStrategy, limit_num);
            }
        }

        for (int i = 0; i < removeList.size(); i++) {
            brokerMap.remove(removeList.get(i));
        }
    }


    RebuildRabbitMq(ThreadPoolExecutor reBuildThreadPool) {
        factory = new ConnectionFactory();
        factory.setPassword("guest");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setSharedExecutor(reBuildThreadPool);
        factory.setNetworkRecoveryInterval(60_000);

        ThreadFactory threadFactory = new ThreadFactory() {
            AtomicInteger num = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "rabbitmq-heart-thread-" + num.getAndIncrement());
            }
        };

        factory.setHeartbeatExecutor(new ScheduledThreadPoolExecutor(1, threadFactory));
        initConsumer();
    }

    public void initConsumer() {
        List<String> diskList = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).keys("*fs*");

        for (String disk : diskList) {
            //判断当前盘的存储池是否存在，不存在就不创建队列
            if (1 == RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).exists(disk)) {
                String pool_name = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hget(disk, "pool_name");
                if (1 != RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).exists(pool_name)) {
                    continue;
                }
            }
            consumer(disk);
        }
    }

    private static class MsgConsumer extends DefaultConsumer {

        public RebuildMessageBroker messageBroker;

        MsgConsumer(Channel channel, RebuildMessageBroker broker) {
            super(channel);
            messageBroker = broker;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            try {
                Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(envelope, body);
                messageBroker.processor.onNext(tuple2);
            } catch (Exception e) {
                log.error("unexpected error, requeue msg. ", e);
                normalRequeue(getChannel(), envelope);
            }
        }
    }

    /**
     * 消息分发逻辑
     **/
    public static Mono<Boolean> dealMsg(Channel channel, Envelope envelope, byte[] body) throws IOException {
        String message = new String(body, StandardCharsets.UTF_8);
        ReBuildTask task = Json.decodeValue(message, ReBuildTask.class);

        if (RemovedDisk.getInstance().contains(task.disk)) {
            normalAck(channel, envelope);
            return Mono.just(true);
        }

        String[] diskLink = task.getDiskLink();
        for (String disk : diskLink) {
            int num = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX)
                    .keys("rebuild_running_" + disk + "_*").size();
            if (num > QOS * HEART_IP_LIST.size() + QOS) {
                normalRequeue(channel, envelope);
                return Mono.just(true);
            }
        }

        String[] linkKeys = new String[diskLink.length];

        for (int i = 0; i < diskLink.length; i++) {
            String disk = diskLink[i];
            linkKeys[i] = "rebuild_running_" + disk + "_" + task.vnode + "_" + System.currentTimeMillis();

            getMaster().setex(linkKeys[i], 15 * 60, task.map.toString());
        }

        List<Tuple3<String, String, String>> nodeList = new ArrayList<>();
        if (task.map.containsKey("nodeList")) {
            nodeList = Json.decodeValue(task.map.get("nodeList"),
                    NODE_LIST_TYPE_REFERENCE);
        }

        StoragePool pool = StoragePoolFactory.getStoragePool(task.pool, null);
//        String strategyName = "storage_" + task.pool;
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(task.pool);
//        if (StringUtils.isEmpty(poolQueueTag)) {
//            String strategyName = "storage_" + task.pool;
//            poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
        String runningKey = "running_" + poolQueueTag;
        Mono<Boolean> taskRes;

        switch (task.type) {
            case OBJECT_FILE:
                String errorIndex = task.map.get("errorIndex");
                String fileName = task.map.get("fileName");
                String endIndex = task.map.get("endIndex");
                String fileSize = task.map.get("fileSize");
                String metaKey = task.map.get("metaKey");
                String vnodeNum = String.valueOf(task.vnode);
                String lun = task.map.get("lun");

                String crypto = task.map.get("crypto");
                String secretKey = task.map.get("secretKey");
                String lastAccessStamp = task.map.get("lastAccessStamp");

                taskRes = TaskHandler.rebuildObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, task.map.get("timestamp"), lastAccessStamp)
                        .doOnNext(b -> {
                            if (b) {
                                RebuildSpeed.add(Long.parseLong(fileSize));
                            }
                        });
                break;
            case OBJECT_META:
                taskRes = TaskHandler.rebuildObjMeta(task.map.get("bucket"), task.map.get("object"), task.map.get("versionId"), nodeList, task.map.get("snapshotMark"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case INIT_PART_UPLOAD:
                taskRes = TaskHandler.rebuildInitPartUpload(task.map.get("bucket"), task.map.get("object"),
                                task.map.get("uploadId"), nodeList, task.map.get("snapshotMark"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case PART_UPLOAD:
                PartInfo partInfo = Json.decodeValue(task.map.get("value"), PartInfo.class);
                taskRes = TaskHandler.rebuildPartUpload(partInfo, nodeList)
                        .timeout(Duration.ofMinutes(1));
                break;
            case STATISTIC:
                taskRes = StatisticErrorHandler.putMinuteRecord(task.map.get("value"))
                        .timeout(Duration.ofMinutes(1));
                break;
            case BUCKET_STORAGE:
                taskRes = Mono.just(true);
                break;
            case SYNC_RECORD:
                taskRes = TaskHandler.rebuildSyncRecord(task.map.get("value"));
                break;
            case DEDUP_INFO:
                taskRes = TaskHandler.rebuildDeduplicateInfo(task.map.get("realKey"), task.map.get("value"));
                break;
            case COMPONENT_RECORD:
                taskRes = TaskHandler.rebuildComponentRecord(task.map.get("value"));
                break;
            case NFS_INODE:
                taskRes = TaskHandler.rebuildInode(task.map.get("inodeKey"), task.map.get("inodeValue"));
                break;
            case NFS_CHUNK:
                taskRes = TaskHandler.rebuildChunkFile(task.map.get("chunkKey"), task.map.get("chunkValue"));
                break;
            case STS_TOKEN:
                taskRes = TaskHandler.rebuildSTSToken(task.map.get("value"));
                break;
            default:
                log.error("no such rebuild task type {}", task.type);
                taskRes = Mono.just(false);
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();

        taskRes.doFinally(b -> {
            try {
                normalAck(channel, envelope);

                long last = getMaster().hincrby(runningKey, "taskNum", -1L);
                getMaster().hincrby(runningKey, "migrateNum", -1L);
                for (int i = 0; i < diskLink.length; i++) {
                    getMaster().del(linkKeys[i]);
                }
                res.onNext(true);

            } catch (Exception e) {
                log.error("", e);
                res.onNext(true);
            }
        }).subscribe(b -> {
            if (!b) {
                log.info("rebuild {} fail", message);
            }
        }, e -> {
            log.error("rebuild {} fail ", message, e);
            res.onNext(true);
        });

        return res;
    }

}
