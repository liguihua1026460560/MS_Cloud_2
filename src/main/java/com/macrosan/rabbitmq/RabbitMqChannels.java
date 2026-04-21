package com.macrosan.rabbitmq;

import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.ratelimiter.LimitStrategy;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.ForgivingExceptionHandler;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.ERROR_MQ_TYPE.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.ERROR_MQ_TYPE.DEAD_LETTER;

@Log4j2
public class RabbitMqChannels {

    public static String queueName = "MatadataQueue";
    public static String routingKey = "routingKey";
    public static String exchange = "exchange";
    private static ConcurrentHashMap<String, Channel> channelConcurrentMap = new ConcurrentHashMap<>();

    public static Map<String, List<Connection>> connectionMap = new HashMap<>();
    public static Map<String, List<Channel>> channelMap = new HashMap<>();
    public static Map<String, Long> notPrintExcept = new ConcurrentHashMap<>();


    public static Channel getLifecycleChannel(String ip) throws IOException, TimeoutException {
        Connection connection = getLocalGuestConnection(ip);
        Channel channel = connection.createChannel();
        channelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
        return channel;
    }

    private static Connection getLocalGuestConnection(String ip) throws IOException, TimeoutException {
        synchronized ("getLocalGuestConnection") {
            log.info("Create local rabbitMq guest connection.");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(ip);
            factory.setUsername("guest");
            factory.setPassword("guest");
            factory.setPort(5672);

            factory.setSharedExecutor(CONSUMER_THREAD_POOL);

            // 恢复连接机制，如果连接超时会自动进行恢复
            factory.setAutomaticRecoveryEnabled(true);
            Connection connection = factory.newConnection();
            connection.addShutdownListener(cause -> {
                if (cause instanceof ShutdownSignalException) {//这里做一个定时任务定时查询节点的状态
                    AutoRecoveryShutdown.checkNeedRecover(ip, connection);
                }
                String message = "conn1 hostName:" + ip + ", conn: " + connection;
                if (!notPrintExcept.containsKey(message)) {
                    notPrintExcept.compute(message, (k, v) -> {
                        if (null == v) {
                            log.info("{}", message);
                            v = System.currentTimeMillis();
                        }

                        return v;
                    });
                }
            });
            connectionMap.computeIfAbsent(ip, k -> new ArrayList<Connection>()).add(connection);
            return connection;
        }
    }


    /**
     * 保存各节点rabbitmq的channel。每个线程有一个channel的集合
     * [1*线程, n*[ip, Channel]]
     */
    private static final Map<Integer, Map<String, Channel>> THREAD_NODES_CHANNEL_MAP = new ConcurrentHashMap<>();

    /**
     * 保存各节点rabbitmq的channel。每个线程有一个channel的集合。该集合中的channel有qos限制。
     * [线程, [ip, Channel]]
     */
    private static final Map<Integer, Map<String, Channel>> LIMITED_THREAD_NODES_CHANNEL_MAP = new ConcurrentHashMap<>();

    /**
     * 用于消费者的线程池，对于新的连接使用此线程池进行调度。
     */
    public static final MsExecutor CONSUMER_THREAD_POOL;
    public static final Scheduler CONSUMER_SCHEDULER;
    /**
     * 本地向一个ip的队列最多建立的channel数，用于publish
     */
    private static final int PUBLISH_CHANNEL_AMOUNT = 10;


    static {
        ThreadFactory namedThreadFactory = new DefaultThreadFactory("rabbitmq-error-handler");
        CONSUMER_THREAD_POOL = new MsExecutor(20, 1, namedThreadFactory);
        CONSUMER_SCHEDULER = Schedulers.fromExecutor(CONSUMER_THREAD_POOL);
    }

    private static Map<Integer, Map<String, Channel>> getChannelMap(ERROR_MQ_TYPE type) {
        return type == EC_ERROR ? THREAD_NODES_CHANNEL_MAP : LIMITED_THREAD_NODES_CHANNEL_MAP;
    }

    /**
     * 出现异常publish时，根据ip和错误类型获得Channel。如果该channel不存在，则进行初始化。
     */
    public static Channel initChannelByIp(String ip, ERROR_MQ_TYPE type) {
        String thread = Thread.currentThread().getName();
        int index = thread.hashCode() % PUBLISH_CHANNEL_AMOUNT;
        Map<String, Channel> channelsMap = getChannelMap(type)
                .computeIfAbsent(index, k -> new ConcurrentHashMap<>(HEART_IP_LIST.size()));
        Channel cachedChannel = channelsMap.get(ip);
        // 检查缓存的 channel 是否有效，如果已关闭则移除并重新创建
        if (cachedChannel != null && !cachedChannel.isOpen()) {
            log.info(ip + "cachedChannel is not open, remove it and create a new one.");
            channelsMap.remove(ip, cachedChannel);
        }
        return channelsMap.computeIfAbsent(ip, key -> {
            Channel channel = initSingleChannel(ip, type);
            return channel;
        });
    }

    /**
     * 创建异常处理队列的consumer所需的channel。一个consumer只需要一个channel。
     */
    public static Channel initConsumerChannelByIp(String ip, ERROR_MQ_TYPE type) {
        return initSingleChannel(ip, type);
    }

    private static ForgivingExceptionHandler handleChannelExceptions() {
        return new ForgivingExceptionHandler() {
            @Override
            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
                if (exception instanceof ConnectException) {
                    // no-op
                } else {
                    String message = exception.getMessage();
                    if (null != message && message.contains("No route to host")) {
                        if (!notPrintExcept.containsKey(message)) {
                            notPrintExcept.compute(message, (k, v) -> {
                                if (null == v) {
                                    log.error("Caught an exception during connection recovery! {}", exception.getMessage());
                                    v = System.currentTimeMillis();
                                }

                                return v;
                            });
                        }
                    } else {
                        log.error("Caught an exception during connection recovery! {}", exception.getMessage());
                    }
                }
            }

        };
    }

    private static Channel initSingleChannel(String ip, ERROR_MQ_TYPE type) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ip);
        factory.setPassword("guest");
        factory.setPort(DEFAULT_RABBITMQ_PORT);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(30_000);
        factory.setSharedExecutor(CONSUMER_THREAD_POOL);
        factory.setExceptionHandler(handleChannelExceptions());

        try {
            Connection connection = factory.newConnection();
            connection.addShutdownListener(cause -> {
                if (cause instanceof ShutdownSignalException) {//这里做一个定时任务定时查询节点的状态
                    AutoRecoveryShutdown.checkNeedRecover(ip, connection);
                }
                String message = "conn2 hostName:" + ip + ", conn: " + connection;
                if (!notPrintExcept.containsKey(message)) {
                    notPrintExcept.compute(message, (k, v) -> {
                        if (null == v) {
                            log.info("{}", message);
                            v = System.currentTimeMillis();
                        }

                        return v;
                    });
                }
            });
            connectionMap.computeIfAbsent(ip, k -> new ArrayList<Connection>()).add(connection);
//            boolean bb = connection instanceof AutorecoveringConnection;
//            log.info("connection type:{}", bb);
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(OBJ_EXCHANGE, "direct", true, false, null);
            if (EC_ERROR == type) {
                channel.basicQos(100);
            } else if (EC_BUFFERED_ERROR == type) {
                channel.basicQos(1);
            } else if (LOW_SPEED == type) {
                channel.basicQos(1);
            } else if (DEAD_LETTER == type) {
                channel.basicQos(10);
            }
            return channel;
        } catch (IOException | TimeoutException e) {
            //如果一个节点的后端网口不可连接，该ip下的channel将不存在。
            log.error("create single channel error, thread: {}, ip: {}, {}", Thread.currentThread().getName(), ip, e.getMessage());
            return null;
        }
    }

    public static Channel initChannelByIp_(String ip) {
        return initSingleChannel(ip, null);
    }

    /**
     * channel已初始化再次切换并发量时使用，根据生效的策略限制rabbitMQ并发量，更新频次当前为1分钟一次
     */
    public static void adjustRabbitMqChannel(String limitStrategy, int limit_num) {

        // 调用该方式时首先将ADAPT方法(RateLimiter)的标志位adaptFlag置为false
        RecoverLimiter.getInstance().getMetaLimiter().setAdaptFlag(false);
        RecoverLimiter.getInstance().getMetaLimiter().setLastPrefetchNum(0);
        if (limitStrategy.equalsIgnoreCase(LimitStrategy.ADAPT.name())) {
            RecoverLimiter.getInstance().getMetaLimiter().setAdaptFlag(true);
        }

        Map<Channel, MessageBroker> brokerMap = ObjectConsumer.brokerMap;
        for (Channel channel : brokerMap.keySet()) {
            brokerMap.get(channel).adjustQoS(limitStrategy, limit_num);
        }

    }

    public static void closeConnection(String ip) {
        List<Channel> channels = channelMap.get(ip);
        if (null != channels) {
            for (Channel channel : channels) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (Exception e) {
                    log.error("close channel error", e);
                }
            }
        }
        List<Connection> connectionList = connectionMap.get(ip);
        if (connectionList != null) {
            for (Connection connection : connectionList) {
                try {
                    boolean b = connection instanceof AutorecoveringConnection;
                    log.info("close connection type: {}", b);
                    if (connection.isOpen()) {
                        connection.close();
                    }
                } catch (IOException e) {
                    log.error("close connection error, ip: {}, {}", ip, e.getMessage());
                }
            }
        }
    }

}
