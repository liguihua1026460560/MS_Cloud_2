package com.macrosan.rabbitmq;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.ReadWriteLock;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.SshClientUtils;
import com.macrosan.utils.property.PropertyReader;
import com.rabbitmq.client.*;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.rebuild.RebuildRabbitMq.REBUILD_QUEUE_NAME_PREFIX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.rabbitmq.RabbitMqUtils.DISK_CONSUMER_STATUS.AVAILABLE;
import static com.macrosan.rabbitmq.RabbitMqUtils.DISK_CONSUMER_STATUS.UNAVAILABLE;

/**
 * rabbitmq工具类
 *
 * @author fanjunxi
 * @date 2020年5月22日
 */
@Log4j2
public class RabbitMqUtils {
    private static RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public static final String CURRENT_IP = ServerConfig.getInstance().getHeartIp1();

    public static final String OBJ_EXCHANGE = "objExchange";

    public static final String DISK_CONSUMER_STATUS_FIELD = "disk_consumer_status";
    public static final String DISK__STATUS_FIELD = "state";

    static final int DEFAULT_RABBITMQ_PORT = 5672;

    private static final Map<String, Boolean> RABBIT_MQ_HEALTH = new ConcurrentHashMap<>();

    private static int SCAN_QUEUE_TIME = 600;

    /**
     * 所有后端ip，该集合会首先在RabbitMqUtils初始化时加入ip，以使RABBIT_MQ_HEALTH能够及时更新，而后会在ObjectConsumer初始化时再次更新
     */
    public static List<String> HEART_IP_LIST = new ArrayList<>();

    /**
     * 每个ip对应的hostUuid。<ip, uuid>
     */
    static Map<String, String> IP_UUID_MAP = new ConcurrentHashMap<>();

    static {
        try {
            List<String> tempNodeList = scanKeys(REDIS_NODEINFO_INDEX).collectList().block();
            if (tempNodeList != null) {
                for (String hostuuid : tempNodeList) {
                    String heartIp = redisConnPool.getCommand(REDIS_NODEINFO_INDEX).hget(hostuuid, SysConstants.HEART_IP);
                    RabbitMqUtils.HEART_IP_LIST.add(heartIp);
                    RabbitMqUtils.IP_UUID_MAP.put(heartIp, hostuuid);
                }
            } else {
                log.error("get REDIS_NODEINFO failed.");
            }
        } catch (Exception e) {
            log.error("init HEART_IP_LIST failed.", e);
        }
    }

    public static void init() {
        SshClientUtils.exec("rabbitmqctl set_policy moss-lazy \".*\" '{\"queue-mode\":\"lazy\"}' --apply-to queues\n");
        ReadWriteLock.readLock(true);
        PropertyReader propertyReader = new PropertyReader(PUBLIC_CONF_FILE);
        String diskUsedLimit = propertyReader.getPropertyAsString("mq_disk_used_limit");
        ReadWriteLock.unLock(true);
        diskUsedLimit = StringUtils.isEmpty(diskUsedLimit) ? "85" : diskUsedLimit;
        int usedLimit = Integer.parseInt(diskUsedLimit);

        ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(1,
                runnable -> new Thread(runnable, "mqDiskLimitCheck"));

        String rabbitmqIP = CURRENT_IP;
        scheduledThreadPool.scheduleAtFixedRate(() -> {
            try {
                File f = new File("/cg_sp0/");
                long total = f.getTotalSpace();
                long used = f.getTotalSpace() - f.getUsableSpace();
                if (used * 100 / total >= usedLimit) {
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_ROCK_INDEX).setex("rabbitmq_" + rabbitmqIP, 30, "0");
                } else {
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_ROCK_INDEX).setex("rabbitmq_" + rabbitmqIP, 30, "1");
                }
            } catch (Exception e) {
                log.error("update mq disk space fail", e);
            }

            try {
                for (String ip : HEART_IP_LIST) {
                    String status = RedisConnPool.getInstance().getCommand(REDIS_ROCK_INDEX).get("rabbitmq_" + ip);
                    if ("1".equals(status)) {
                        RABBIT_MQ_HEALTH.put(ip, true);
                    } else if ("0".equals(status)) {
                        RABBIT_MQ_HEALTH.put(ip, false);
                    }
                }
            } catch (Exception e) {
                log.error("get mq health fail", e);
            }
        }, 0, 10, TimeUnit.SECONDS);


        ScheduledThreadPoolExecutor queueChecker = new ScheduledThreadPoolExecutor(1,
                runnable -> new Thread(runnable, "rabbitmq-queue-check"));

        queueChecker.scheduleAtFixedRate(() -> {
            try {
                final List<Address> addressList = RabbitMqUtils.HEART_IP_LIST.stream().map(Address::new).collect(Collectors.toList());
                final String uuid = ServerConfig.getInstance().getHostUuid();
                boolean isDiskRemoved = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).exists("removed_disk_set") > 0;
                if (isDiskRemoved) {
                    Set<String> diskSet = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).smembers("removed_disk_set");
                    for (String curDisk : diskSet) {
                        String poolName = RedisConnPool.getInstance().getCommand(REDIS_MIGING_V_INDEX).hget("removed_disk_pool", curDisk);
                        final String queueName = REBUILD_QUEUE_NAME_PREFIX + "-" + curDisk + "-" + poolName + END_STR;
                        for (Address address : addressList) {
                            if (curDisk.contains(uuid)) {
                                final Channel channel = new ConnectionFactory().newConnection(Arrays.asList(address)).createChannel();
                                try {
                                    // 如果该队列已不存在，则channel.queueDeclarePassive会报错进入catch，且为空；若该队列存在，则会继续执行，并删除该队列
                                    AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queueName);
                                    String queue = declareOk.getQueue();
                                    log.info("remove queue: {}, address: {}", queue, address);
                                    channel.queueDelete(queueName);
                                } catch (Exception e) {
                                    if (!(e.getMessage() == null && e instanceof IOException)) {
                                        log.error("check and delete queue error: {}, {}", address, queueName, e);
                                    }
                                } finally {
                                    channel.getConnection().close();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }, 0, SCAN_QUEUE_TIME, TimeUnit.SECONDS);

    }

    public static boolean isLimitSpace(String ip) {
        if ("127.0.0.1".equals(ip)) {
            ip = CURRENT_IP;
        }

        return RABBIT_MQ_HEALTH.getOrDefault(ip, true);
    }

    /**
     * When a message is requeued, it will be placed to its original position in its queue, if possible.
     * If not (due to concurrent deliveries and acknowledgements from other consumers when multiple
     * consumers share a queue), the message will be requeued to a position closer to queue head.
     */
    public static void normalRequeue(Channel channel, Envelope envelope) {
        try {
            channel.basicReject(envelope.getDeliveryTag(), true);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 将消息重新发布到指定队列的队尾。
     */
    public static void requeueToAnotherQueue(Channel channel, String routingKey, byte[] body, Envelope envelope) {
        boolean published = false;
        try {
            channel.basicPublish(OBJ_EXCHANGE, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, body);
            published = true;
        } catch (Exception e) {
            log.error("", e);
        }

        try {
            if (published) {
                channel.basicAck(envelope.getDeliveryTag(), false);
            } else {
                channel.basicReject(envelope.getDeliveryTag(), true);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static void normalAck(Channel channel, Envelope envelope) {
        try {
            channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public enum DISK_CONSUMER_STATUS {
        AVAILABLE,
        UNAVAILABLE;
    }

    public enum ERROR_MQ_TYPE {
        EC_ERROR,
        EC_BUFFERED_ERROR,
        LOW_SPEED,
        DEAD_LETTER;

        public String queue() {
            return this.name() + "_QUEUE";
        }

        public String key() {
            return this.name() + "_KEY";
        }
    }

    /**
     * 该次启动后每个盘接收到的异常消息数量
     */
    private static final Map<String, AtomicInteger> DISK_ERROR_AMOUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 每个disk的状态
     */
    private static final Map<String, DISK_CONSUMER_STATUS> DISK_CONSUMER_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 每个盘收到的待修复消息个数阈值，超过此数字的修复消息将放入缓冲队列。
     */
    private static final int BUFFER_THRESHOLD = 100;

    /**
     * 消息发布所在队列名称的后缀，格式为 _目标ip的最后一位
     *
     * @param dstIp 该条消息预备在哪个节点消费
     */
    public static String getErrorMqSuffix(String dstIp) {
        String[] splitIp = dstIp.split("\\.");
        return "_" + splitIp[splitIp.length - 1];
    }

    /**
     * 获得盘名
     *
     * @param ip      异常消息要发往的ip
     * @param lunName 异常消息要处理的文件/数据所在的盘
     */
    public static String getDiskName(String ip, String lunName) {
        return IP_UUID_MAP.get(ip) + "@" + lunName;
    }

    public static String getUUID(String ip) {
        return IP_UUID_MAP.get(ip);
    }

    /**
     * 磁盘下的异常消息计数+1, 如果超出阈值且磁盘状态为AVAILABLE，更新磁盘状态为UNAVAILABLE。
     */
    static void addErrorMsgAmount(String diskName) {
        int amount = DISK_ERROR_AMOUNT_MAP.computeIfAbsent(diskName, k -> new AtomicInteger()).incrementAndGet();
        if (diskIsAvailable(diskName) && amount >= BUFFER_THRESHOLD) {
            updateDiskStatus(diskName, UNAVAILABLE);
        }
    }

    /**
     * 缓冲队列中消息成功消费后，相应盘的错误消息数清零，盘状态设置为available
     */
    static void cleanErrorAmount(String diskName) {
        DISK_ERROR_AMOUNT_MAP.put(diskName, new AtomicInteger());
        updateDiskStatus(diskName, AVAILABLE);
    }

    /**
     * 获取缓存中的disk状态。缓存中的磁盘状态将定时更新。如果没有，返回true。
     * 不能为响应式，会出现盘状态异常，消息正在处理意外丢失的问题。
     */
    public static Boolean diskIsAvailable(String diskName) {
        if (diskName == null) {
            return true;
        }
        DISK_CONSUMER_STATUS statusInCache = DISK_CONSUMER_STATUS_MAP.get(diskName);
        return statusInCache == null || statusInCache == AVAILABLE;
    }

    public static Boolean diskIsAvailable(List<String> diskName) {
        if (diskName == null || diskName.isEmpty()) {
            return true;
        }

        boolean res = false;
        for (String disk : diskName) {
            res |= diskIsAvailable(disk);
        }

        return res;
    }

    private static UnicastProcessor<Tuple2<String, DISK_CONSUMER_STATUS>> updateDiskStatusRequest = UnicastProcessor.create(
            Queues.<Tuple2<String, DISK_CONSUMER_STATUS>>unboundedMultiproducer().get());

    static {
        updateDiskStatusRequest.publishOn(DISK_SCHEDULER).subscribe(t0 -> {
            try {
                Map<String, Tuple2<String, DISK_CONSUMER_STATUS>> request = new HashMap<>();
                request.put(t0.var1, t0);
                Tuple2<String, DISK_CONSUMER_STATUS> next = updateDiskStatusRequest.poll();
                while (next != null) {
                    request.put(next.var1, next);
                    next = updateDiskStatusRequest.poll();
                }

                request.values().forEach(t -> {
                    String diskName = t.var1;
                    DISK_CONSUMER_STATUS status = t.var2;
                    DISK_CONSUMER_STATUS_MAP.put(diskName, status);
                    if (redisConnPool.getCommand(REDIS_LUNINFO_INDEX).exists(diskName) == 1 && !RemovedDisk.getInstance().contains(diskName)) {
                        redisConnPool.getShortMasterCommand(REDIS_LUNINFO_INDEX)
                                .hset(diskName, DISK_CONSUMER_STATUS_FIELD, status.name());
                    }
                });
            } catch (Exception e) {
                log.error("update disk status fail", e);
            }
        });
    }

    /**
     * 修改缓存和redis中的disk状态。
     */
    public static void updateDiskStatus(String diskName, DISK_CONSUMER_STATUS status) {
        updateDiskStatusRequest.onNext(new Tuple2<>(diskName, status));
    }

    /**
     * scan指定dataBase下的key
     */
    static Flux<String> scanKeys(int dataBase) {
        ScanArgs scanAllArg = new ScanArgs().match("*");
        return ScanStream.scan(redisConnPool.getReactive(dataBase), scanAllArg);
    }

    /**
     * 定期获取磁盘状态，十秒一次。
     * 定期更新磁盘状态为AVAILABLE
     */
    static void getDiskStatusPeriodically() {
        ScanArgs scanAllArg = new ScanArgs().match("*fs*");
        ScanArgs scanThisArg = new ScanArgs().match(ServerConfig.getInstance().getHostUuid() + "*fs*");

        //每次启动jar包都将本节点磁盘设置为AVAILABLE，防止消息滞留在缓冲队列里。
        ScanStream.scan(redisConnPool.getReactive(REDIS_LUNINFO_INDEX), scanThisArg)
                .doOnNext(diskName -> updateDiskStatus(diskName, AVAILABLE))
                .subscribe();

        ServerConfig.getInstance().getVertx().setPeriodic(120_000, timeId -> {
            ScanStream.scan(redisConnPool.getReactive(REDIS_LUNINFO_INDEX), scanThisArg)
                    .subscribe(diskName -> updateDiskStatus(diskName, AVAILABLE));
        });

        ServerConfig.getInstance().getVertx().setPeriodic(10_000, timeId -> {
            ScanStream.scan(redisConnPool.getReactive(REDIS_LUNINFO_INDEX), scanAllArg)
                    .doOnNext(diskName ->
                            redisConnPool.getReactive(REDIS_LUNINFO_INDEX).hget(diskName, DISK_CONSUMER_STATUS_FIELD)
                                    .map(DISK_CONSUMER_STATUS::valueOf)
                                    .subscribe(status -> {
                                        DISK_CONSUMER_STATUS_MAP.put(diskName, status);
                                    })
                    )
                    .doOnError(e -> log.error("", e))
                    .subscribeOn(DISK_SCHEDULER)
                    .subscribe();
        });
    }
}
