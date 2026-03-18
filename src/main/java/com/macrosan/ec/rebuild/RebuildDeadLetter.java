package com.macrosan.ec.rebuild;/**
 * @author niechengxing
 * @create 2025-08-04 14:03
 */

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.error.StatisticErrorHandler;
import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.AutoRecoveryShutdown;
import com.macrosan.rabbitmq.ClearMqQueue;
import com.macrosan.rabbitmq.DeleteQueue;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import com.rabbitmq.client.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.macrosan.constants.SysConstants.END_STR;
import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;
import static com.macrosan.ec.error.ErrorFunctionCache.NODE_LIST_TYPE_REFERENCE;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.rebuild.ReBuildTask.Type.OBJECT_FILE;
import static com.macrosan.ec.rebuild.RebuildRabbitMq.rebuildChannelMap;
import static com.macrosan.ec.rebuild.RebuildRabbitMq.rebuildConnectionMap;
import static com.macrosan.rabbitmq.RabbitMqChannels.CONSUMER_THREAD_POOL;
import static com.macrosan.rabbitmq.RabbitMqChannels.initConsumerChannelByIp;
import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.ERROR_MQ_TYPE.DEAD_LETTER;

/**
 * @program: MS_Cloud
 * @description: 用于数据池重构的类死信队列初始化，提供重试处理
 * @author: niechengxing
 * @create: 2025-08-04 10:03
 */
@Log4j2
public class RebuildDeadLetter {
    private static RebuildDeadLetter instance = new RebuildDeadLetter();
    private static int PORT = 5672;
    //    private static int QOS = 10;
    private Map<String, Channel> channelMap = new HashMap<>();
    private ConnectionFactory factory;

    public RebuildDeadLetter() {
        factory = new ConnectionFactory();
        factory.setPassword("guest");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(30_000);
        factory.setSharedExecutor(CONSUMER_THREAD_POOL);
    }

    public static RebuildDeadLetter getInstance() {
        return instance;
    }

    public void init() {

        try {
            Channel channel = initConsumerChannelByIp(CURRENT_IP, DEAD_LETTER);
            if (channel == null) {
                throw new RuntimeException("init dead letter queue channel error");
            }
            String queueName = DEAD_LETTER.queue() + getErrorMqSuffix(CURRENT_IP);
            // 全局队列存在，则判断是否存在消息，如果没有消息则删除队列
            if (channel.queueDeclarePassive(queueName).getMessageCount() == 0) {
                channel.queueDelete(queueName);
                channel.close();
            } else {
                // 全局队列存在且存在消息，则启动消费者
                msgConsumer(CURRENT_IP, DEAD_LETTER);
                // 消息消费完成后删除队列
                DeleteQueue queue = new DeleteQueue(channel, queueName);
                ClearMqQueue.needClearQueue.add(queue);
            }
        } catch (Exception e) {
            if (e instanceof IOException && e.getCause() != null && e.getCause().getMessage() != null && e.getCause().getMessage().contains("no queue 'DEAD_LETTER_QUEUE_")) {
                // 队列不存在，则无需进行处理
                return;
            }
            log.error("dead letter queue init error", e);
            if (StringUtils.isNotEmpty(getUUID(CURRENT_IP))) {
                if (!"1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(CURRENT_IP), "isRemoved"))) {
                    ADD_NODE_SCHEDULER.schedule(this::init, 10, TimeUnit.SECONDS);
                }
            }

        }
    }

    /**
     * 创建全局死信队列消费者 （后续消息发往存储池相关的死信队列，全局队列在消息消费完成后删除）
     *
     * @param ip          本地节点ip
     * @param errorMqType 队列类型
     */
    private void msgConsumer(String ip, ERROR_MQ_TYPE errorMqType) {
        String queueName = errorMqType.queue() + getErrorMqSuffix(CURRENT_IP);
        String routingKey = errorMqType.key() + getErrorMqSuffix(CURRENT_IP);
        createConsumer(ip, errorMqType, queueName, routingKey);
    }

    /**
     * 创建存储池相关的死信队列消费者
     *
     * @param ip          本地节点ip
     * @param poolName    存储池名称
     * @param errorMqType 队列类型
     */
    public void msgConsumerForPool(String ip, String poolName, ERROR_MQ_TYPE errorMqType) {
        String queueName = errorMqType.queue() + "_" + poolName + END_STR + getErrorMqSuffix(CURRENT_IP);
        String routingKey = errorMqType.key() + "_" + poolName + getErrorMqSuffix(CURRENT_IP);
        createConsumer(ip, errorMqType, queueName, routingKey);
        log.info("create dead letter queue consumer for pool:{}", queueName);
    }

    private void createConsumer(String ip, ERROR_MQ_TYPE errorMqType, String queueName, String routingKey) {
        try {
            Channel channel = getChannelInstance(ip, queueName, routingKey, errorMqType);
            ObjectConsumer.channelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
            Consumer consumer;

            consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) {
                    try {
                        super.handleDelivery(consumerTag, envelope, properties, body);
                        dealMsg(channel, envelope, body);
                    } catch (Exception e) {
                        log.error("Object consumer error", e);
                        normalRequeue(channel, envelope);
                    }
                }
            };

            /* 声明：basicConsume的autoAck为false时：没有手动应答，在没有指定qos时，消费者可以获取所有消息；
               当指定qos时，只能获取指定条数，没法继续获取下一条了，因为没有收到消费确认不会继续进行 */
            channel.basicConsume(queueName, false, consumer);
            log.info("Init Consumer success, {}", queueName);
        } catch (IOException e) {
            log.error("Init Consumer failed, {}", e.getMessage());
            if (StringUtils.isNotEmpty(getUUID(ip))) {
                if (!"1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(ip), "isRemoved"))) {
                    ADD_NODE_SCHEDULER.schedule(() -> createConsumer(ip, errorMqType, queueName, routingKey), 10, TimeUnit.SECONDS);
                }
            }
        }

    }

    private Channel getChannelInstance(String ip, String queueName, String routingKey, ERROR_MQ_TYPE type) throws IOException {
        //根据ip获得连接的信道
        Channel channel = initConsumerChannelByIp(ip, type);
        if (channel == null) {
            throw new IOException("create rabbitmq connnection failed, ip: " + ip);
        }
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, OBJ_EXCHANGE, routingKey);
        channel.addShutdownListener(e -> {
            if (!e.isInitiatedByApplication()) {
                log.error("{} {} object consumer channel shutdown.", ip, queueName, e);
            }
        });
        return channel;
    }

    public void dealMsg(Channel channel, Envelope envelope, byte[] body) throws IOException {
        String message = new String(body, StandardCharsets.UTF_8);
//        ReBuildTask task = Json.decodeValue(message, ReBuildTask.class);
        JsonObject jsonObject = new JsonObject(message);
        log.debug("dealMsg: {}", jsonObject);
        if (jsonObject.containsKey("type")) {
            //这里处理重构的失败重试消息
            ReBuildTask task = Json.decodeValue(message, ReBuildTask.class);

            if (RemovedDisk.getInstance().contains(task.disk)) {
                normalAck(channel, envelope);
                return;
            }

            List<Tuple3<String, String, String>> nodeList = new ArrayList<>();
            if (task.map.containsKey("nodeList")) {
                nodeList = Json.decodeValue(task.map.get("nodeList"),
                        NODE_LIST_TYPE_REFERENCE);
            }

            StoragePool pool = StoragePoolFactory.getStoragePool(task.pool, null);

            Mono<Boolean> taskRes;
            log.debug("task map:{}", task.map);

            switch (task.type) {
                case OBJECT_FILE:
                    String errorIndex = task.map.get("errorIndex");//表示离线盘上的错误vnode的索引，重构后的数据块需要存至新映射到新盘上的该vnode中
                    String fileName = task.map.get("fileName");
                    String endIndex = task.map.get("endIndex");
                    String fileSize = task.map.get("fileSize");
                    String metaKey = task.map.get("metaKey");
                    String vnodeNum = String.valueOf(task.vnode);
                    String lun = task.map.get("lun");

                    String crypto = task.map.get("crypto");
                    String secretKey = task.map.get("secretKey");
                    String flushStamp = task.map.get("flushStamp");
                    String lastAccessStamp = task.map.get("lastAccessStamp");
                    taskRes = TaskHandler.rebuildObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, flushStamp, lastAccessStamp)
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
                case AGGREGATE_META:
                    taskRes = TaskHandler.rebuildAggregateMeta(task.map.get("key"));
                    break;
                default:
                    log.error("no such rebuild task type {}", task.type);
                    taskRes = Mono.just(false);
            }

            taskRes.subscribe(b -> {
                log.debug("task {} finish, result:{}", task.map.get("metaKey"), b);
                if (!b) {
                    if (OBJECT_FILE.equals(task.type)) {
                        //如果目标数据磁盘离线超6分钟则不再进行重构直接ack
                        if (DiskStatusChecker.isRebuildWaiter(task.disk.split("@")[1])) {
                            normalAck(channel, envelope);
                            return;
                        }
                        long retryEndTime;
                        if (StringUtils.isEmpty(task.map.get("retryEndTime"))) {
                            retryEndTime = System.currentTimeMillis() + 8 * 60_000;//从进入死信队列后第一次重构失败开始累计8分钟
                            task.map.put("retryEndTime", String.valueOf(retryEndTime));
                            log.info("rebuild {} fail", task);
                            log.info("Retry rebuild {} about {}!", task.map.get("metaKey"), task.map.get("fileName"));
                            Mono.delay(Duration.ofSeconds(2)).subscribe(s -> {
                                publishRebuildTask(task);
                                normalAck(channel, envelope);
                            });
                        } else {
                            retryEndTime = Long.parseLong(task.map.get("retryEndTime"));
                            if (System.currentTimeMillis() < retryEndTime) {
                                Mono.delay(Duration.ofSeconds(2)).subscribe(s -> {
                                    publishRebuildTask(task);
                                    normalAck(channel, envelope);
                                    log.debug("Retry rebuild publish to dead letter queue {} about {}!", task.map.get("metaKey"), task.map.get("fileName"));
                                });

                                //1.需要初始化消费者，只考虑在前端包启动的时候进行初始化。
                                //2.然后就是对代码进行测试，包括目标盘离线未超过6分钟重新插回后时候能够正常修复

                            } else {
                                //超过8分钟后为避免因消费时间过长导致磁盘上线后队列末尾的消息直接被丢失，增加一次消费机会
                                if (StringUtils.isEmpty(task.map.get("lastRetry"))) {
                                    task.map.put("lastRetry", "true");
                                    Mono.delay(Duration.ofSeconds(2)).subscribe(s -> {
                                        publishRebuildTask(task);
                                        normalAck(channel, envelope);
                                        log.info("Last Retry rebuild publish to dead letter queue {} about {}!", task.map.get("metaKey"), task.map.get("fileName"));
                                    });
                                } else {//最后一次超过8分钟的重试失败，不再做重试处理，直接ack
                                    normalAck(channel, envelope);
                                }
                            }
                        }

                    } else {
                        log.info("rebuild {} fail", task);
                        normalAck(channel, envelope);
                    }
                } else {
                    normalAck(channel, envelope);
                }
            }, e -> {
                log.error("rebuild {} fail ", message, e);
                normalRequeue(channel, envelope);
            });
        } else {
            //这里处理迁移的失败重试消息
            SocketReqMsg msg = Json.decodeValue(message, SocketReqMsg.class);
            MigrateType migrateType = MigrateType.valueOf(msg.getMsgType());

            //消息的处理逻辑直接复用migrateUtil中migrateFile0方法，在此方法中会区分本地磁盘是否能够正常使用而确定是在本地获取数据进行迁移重试还是在其他节点获取数据进行迁移
            Mono<Boolean> taskRes;
            String fileMetaStr;
            FileMeta fileMeta;
            switch (migrateType) {
                case MIGRATE_FILE_LOCAL:
                    fileMetaStr = msg.get("fileMeta");
                    fileMeta = Json.decodeValue(fileMetaStr, FileMeta.class);
                    taskRes = MigrateUtil.migrateFile0(fileMeta, msg.get("srcDisk"), msg.get("dstIP"), msg.get("dstDisk"), msg.get("poolType"), true, Integer.parseInt(msg.get("retryNum")));
                    break;
                case MIGRATE_FILE_REMOTE:
                    fileMetaStr = msg.get("fileMeta");
                    fileMeta = Json.decodeValue(fileMetaStr, FileMeta.class);
                    taskRes = MigrateUtil.migrateFileFromOther(fileMeta, msg.get("srcDisk"), msg.get("dstIP"), msg.get("dstDisk"), msg.get("poolType"), Integer.parseInt(msg.get("retryNum")));
                    break;
                default:
                    log.error("no such migrate task type {}", migrateType);
                    taskRes = Mono.just(false);
            }

            taskRes.subscribe(b -> {
                //这里应该不管结果如何都正常ack掉
                if (!b) {
//                    log.info("rebuild {} fail", task);
                    normalAck(channel, envelope);
                } else {
                    normalAck(channel, envelope);
                }
            }, e -> {
                log.error("migrate {} fail ", message, e);
                normalRequeue(channel, envelope);
            });

        }

    }

    public void publishRebuildTask(ReBuildTask task) {
        try {
            Channel channel = getChannel("127.0.0.1");
            String key = DEAD_LETTER.key() + "_" + StoragePoolFactory.getPoolNameByPrefix(task.pool) + getErrorMqSuffix(CURRENT_IP);
            channel.basicPublish(OBJ_EXCHANGE, key, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(task).getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public void publishMigrateTask(SocketReqMsg msg) {
        try {
            Channel channel = getChannel("127.0.0.1");
            String key = DEAD_LETTER.key() + "_" + StoragePoolFactory.getPoolNameByPrefix(msg.get("poolType")) + getErrorMqSuffix(CURRENT_IP);
            channel.basicPublish(OBJ_EXCHANGE, key, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(msg).getBytes());
        } catch (Exception e) {
            log.error("", e);
        }

    }

    public Channel getChannel(String ip) throws IOException, TimeoutException {
        String key = Thread.currentThread().getName() + ip;
        Channel channel = channelMap.get(key);
        if (null == channel) {
            synchronized (channelMap) {
                channel = channelMap.get(key);
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
                    channelMap.put(key, channel);
                }
            }

            channel.addShutdownListener(e -> {
                log.error("rebuild channel shutdown, ", e);
            });
            rebuildChannelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
        }

        return channel;
    }

    public enum MigrateType {
        MIGRATE_FILE_LOCAL,
        MIGRATE_FILE_REMOTE,
    }

}

