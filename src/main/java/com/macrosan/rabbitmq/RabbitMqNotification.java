package com.macrosan.rabbitmq;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RabbitMqNotification {

    private static final Map<Thread, Map<String, Tuple2<Channel, AtomicBoolean>>> map =
            new ConcurrentHashMap<>();

    private static final Map<Thread,Map<String,Tuple2<KafkaProducer,AtomicBoolean>>> kafkaMap =
            new ConcurrentHashMap<>();
    private static final MsThreadFactory factory = new MsThreadFactory("notifi-rabbit");
    public static final MsExecutor executor = new MsExecutor(2, 2, factory);
    public static Map<String, List<Connection>> notificationConnectionMap = new HashMap<>();
    public static Map<String, List<Channel>> notificationChannelMap = new HashMap<>();

    static {
        executor.schedule(RabbitMqNotification::check, 5, TimeUnit.MINUTES);
    }

    private static void check() {
        for (Thread thread : map.keySet()) {
            Map<String, Tuple2<Channel, AtomicBoolean>> channelMap = map.get(thread);
            for (String key : channelMap.keySet()) {
                Tuple2<Channel, AtomicBoolean> tuple2 = channelMap.get(key);
                if (tuple2 != null) {
                    if (tuple2.var2.get()) {
                        tuple2.var2.set(false);
                    } else {
                        tryClose(thread, key);
                    }
                }
            }
        }

        for (Thread thread : kafkaMap.keySet()){
            Map<String, Tuple2<KafkaProducer,AtomicBoolean>> producer = kafkaMap.get(thread);
            for (String key: producer.keySet()){
                Tuple2<KafkaProducer,AtomicBoolean> tuple2 = producer.get(key);
                if (tuple2 != null){
                    if (tuple2.var2.get()) {
                        tuple2.var2.set(false);
                    } else {
                        tryClose(thread, key);
                    }
                }
            }
        }

        executor.schedule(RabbitMqNotification::check, 5, TimeUnit.MINUTES);
    }

    /**
     * 每次或channel时，将正在使用的标记设为true
     */
    public static Channel getNotificationChannel(String ip, String userName, String password, Integer port) {
        final Thread thread = Thread.currentThread();
        String key = String.join("_", ip, String.valueOf(port), userName, password);

        Map<String, Tuple2<Channel, AtomicBoolean>> channelMap = map.computeIfAbsent(thread, k -> new ConcurrentHashMap<>());

        Tuple2<Channel, AtomicBoolean> tuple2 = channelMap.get(key);
        if (tuple2 == null) {
            synchronized (thread) {
                tuple2 = channelMap.get(key);
                if (tuple2 == null) {
                    Channel channel = openNotificationChannel(ip, userName, password, port);
                    if (channel != null) {
                        tuple2 = new Tuple2<>(channel, new AtomicBoolean(true));
                        channelMap.put(key, tuple2);
                    }
                }
            }
        }

        if (tuple2 == null || !tuple2.var1.isOpen()) {
            tryClose(thread, key);
            return null;
        }

        tuple2.var2.set(true);
        return tuple2.var1;
    }

    private static void tryClose(Thread thread, String key) {
        Map<String, Tuple2<Channel, AtomicBoolean>> channelMap = map.get(thread);
        Map<String, Tuple2<KafkaProducer, AtomicBoolean>> producerMap = kafkaMap.get(thread);
        if (channelMap != null){
            synchronized (thread) {
                Tuple2<Channel, AtomicBoolean> tuple2 = channelMap.get(key);
                if (tuple2 != null) {
                    if (!tuple2.var1.isOpen() || !tuple2.var2.get()) {
                        try {
                            tuple2.var1.getConnection().close();
                            channelMap.remove(key);
                        } catch (Exception e) {

                        }
                    }
                }
            }
        }
        if (producerMap != null){
            synchronized (thread) {
                Tuple2<KafkaProducer, AtomicBoolean> tuple2 = producerMap.get(key);
                if (tuple2 != null){
                    if (!tuple2.var2.get()){
                        try {
                            tuple2.var1.close();
                            producerMap.remove(key);
                        }catch (Exception e){

                        }
                    }
                }
            }
        }

    }


    public static Channel openNotificationChannel(String ip, String userName, String password, Integer port) {
        try {
            Connection connection = getConnection(ip, userName, password, port);
            Channel channel = connection.createChannel();
            notificationChannelMap.computeIfAbsent(ip, k -> new ArrayList<Channel>()).add(channel);
            return channel;
        } catch (Exception e) {
            log.info("open rabbitmq channel fail {}", e.getMessage());
            return null;
        }
    }

    public static Connection getConnection(String ip, String userName, String password, Integer port) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ip);
        factory.setPort(port);
        factory.setUsername(userName);
        factory.setPassword(password);
        factory.setSharedExecutor(executor);
        factory.setAutomaticRecoveryEnabled(true);
        Connection connection = factory.newConnection();
        connection.addShutdownListener(cause -> {
            if (cause instanceof ShutdownSignalException) {//这里做一个定时任务定时查询节点的状态
                AutoRecoveryShutdown.checkNeedRecover(ip, connection);
            }
            log.info("conn hostName:{}, conn:{}", ip, connection);
        });
        notificationConnectionMap.computeIfAbsent(ip, k -> new ArrayList<Connection>()).add(connection);
        return connection;
    }

    public static void closeConnection(String ip) {
        List<Channel> channels = notificationChannelMap.get(ip);
        if (null != channels) {
            for (Channel channel : channels) {
                try {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                } catch (Exception e) {
                    log.error("close notification channel error", e);
                }
            }
        }

        List<Connection> connections  = notificationConnectionMap.get(ip);
        if (null != channels) {
            for (Connection connection : connections) {
                try {
                    boolean b = connection instanceof AutorecoveringConnection;
                    log.info("close connection type: {}", b);
                    if (connection.isOpen()) {
                        connection.close();
                    }
                } catch (Exception e) {
                    log.error("close notification connection error", e);
                }
            }
        }
    }


    public static KafkaProducer getKafkaProducer(String ip,int port,String topic){
        final  Thread thread = Thread.currentThread();
        String key = String.join("_",ip,String.valueOf(port),topic);
        Map<String,Tuple2<KafkaProducer,AtomicBoolean>> producerMap = kafkaMap.computeIfAbsent(thread,k -> new ConcurrentHashMap<>());

        Tuple2<KafkaProducer,AtomicBoolean> tuple2 = producerMap.get(key);

        if (tuple2 == null){
            synchronized (thread){
                tuple2 = producerMap.get(key);
                if (tuple2 == null){
                    KafkaProducer producer = openNotificationProducer(ip,port);
                    if (producer != null){
                        tuple2 = new Tuple2<>(producer, new AtomicBoolean(true));
                        producerMap.put(key,tuple2);
                    }
                }
            }
        }

        if (tuple2 == null){
            tryClose(thread,key);
            return null;
        }

        tuple2.var2.set(true);
        return tuple2.var1;
    }

    private static KafkaProducer openNotificationProducer(String ip,int port){
        Properties prop = new Properties();

        //设置broker的连接配置
        prop.put("bootstrap.servers",ip + ":" + port);
        //对消息进行序列化
        prop.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //设置ack模式
        prop.put("acks","all");
        //设置失败重试次数
        prop.put("retries",3);
        prop.put("retry.backoff.ms","10000");
        prop.put("batch.size",1048576);
        prop.put("linger.ms",20);
        prop.put("buffer.memory",16777216);
        prop.put("reconnect.backoff.max.ms","5000");
        prop.put("reconnect.backoff.ms","1000");
        prop.put("request.timeout.ms","10000");
        //创建生成者
        return new KafkaProducer<>(prop);
    }

    public static void closeKafkaClient(JsonObject result){
        String topic = result.getString("Topic");
        String kafkaTopic = RedisConnPool.getInstance().getCommand(2).hget("kafka_configuration",topic);
        JsonObject kafkaResult = new JsonObject(kafkaTopic);
        String ip = kafkaResult.getString("address").split(":")[0];
        String port = kafkaResult.getString("address").split(":")[1];
        String kafkaTop = kafkaResult.getString("topic");
        String kafkaKey = String.join("_",ip,port,kafkaTop);
        for (Thread thread : kafkaMap.keySet()){
            Map<String, Tuple2<KafkaProducer,AtomicBoolean>> producer = kafkaMap.get(thread);
            for (String key: producer.keySet()){
                if (key.equals(kafkaKey)){
                    producer.get(key).var1.close();
                    producer.remove(key);
                }
            }
        }
    }
}
