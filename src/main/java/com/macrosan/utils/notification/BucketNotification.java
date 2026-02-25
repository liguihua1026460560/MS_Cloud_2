package com.macrosan.utils.notification;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.action.managestream.BucketNotificationService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MessageSend;
import com.macrosan.message.jsonmsg.NotificationInfo;
import com.macrosan.message.xmlmsg.section.ObjectsList;
import com.macrosan.rabbitmq.RabbitMqNotification;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.trash.TrashUtils;
import com.rabbitmq.client.Channel;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;


public class BucketNotification {
    private static Map<String, String> bucketConfigurationInfo = new ConcurrentHashMap<>();
    private static Map<String, String> topicConfiguration = new ConcurrentHashMap<>();
    private static Map<String, String> queueConfiguration = new ConcurrentHashMap<>();
    private static Map<String, String> kafkaConfiguration = new ConcurrentHashMap<>();
    public static Queue<MessageSend> messageToSend = new ConcurrentLinkedQueue<>();
    static RedisConnPool pool = RedisConnPool.getInstance();
    private static final long NOTIFICATION_TIME = 60L;
    static Logger logger = LogManager.getLogger("Notification.BucketNotification");


    private static BucketNotification instance;

    public static BucketNotification getInstance() {
        if (instance == null) {
            instance = new BucketNotification();
        }
        return instance;
    }


    public void start() {
        updateConfig();
        sendMessage();
        TrashUtils.getBucketTrash();
    }

    public static Mono<String[]> checkNotification(MsHttpRequest request) {

        String bucketName = request.getBucketName();
        if ((bucketName != null) && (bucketConfigurationInfo.get(bucketName) != null)) {
            String[] event = judgeEventType(request);
            if (event[0] != null && (event[1] != null || event[4] != null || event[5] != null)) {
                return Mono.just(event);
            }

        }
        return Mono.empty();
    }

    public static void updateConfiguration(){
        bucketConfigurationInfo = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("bucket_notification");
        topicConfiguration = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("topic_configuration");
        queueConfiguration = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("queue_configuration");
        kafkaConfiguration = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("kafka_configuration");
    }

    private void updateConfig(){
        updateConfiguration();
        ErasureServer.DISK_SCHEDULER.schedule(this::updateConfig, NOTIFICATION_TIME, TimeUnit.SECONDS);
    }

    public static void addMess(MsHttpRequest request, String[] event) {
        String bucketName = request.getBucketName();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        String time = simpleDateFormat.format(date);
        String region = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "region_name");
        String userId = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "user_id");

        JSONObject requestParameters = new JSONObject();
        requestParameters.put("sourceIPAddress", request.remoteAddress().host() + ":" + request.remoteAddress().port());

        JSONObject userIdentity = new JSONObject();
        userIdentity.put("PrincipalId", request.getUserId());

        JSONObject responseElements = new JSONObject(new LinkedHashMap<>());
        responseElements.put("x-amz-request-id", request.response().headers().get(X_AMZ_REQUEST_ID));

        JSONObject object = new JSONObject(new LinkedHashMap<>());
        object.put("key", request.getObjectName());
        if (request.response().headers().get(ETAG) != null) {
            object.put("eTag", request.response().headers().get(ETAG).replace("\"", ""));
            object.put("size", request.bytesRead());
        } else if (request.getMember("etag") != null) {
            object.put("eTag", request.getMember("etag"));
        }
        if (request.response().headers().get(X_AMX_VERSION_ID) != null) {
            object.put("versionId", request.response().headers().get(X_AMX_VERSION_ID));
        }
        JSONObject moss = new JSONObject(new LinkedHashMap<>());
        moss.put("Version", "1.0");
        moss.put("configurationId", event[2]);
        JSONObject bucket = new JSONObject();
        bucket.put("name", bucketName);
        JSONObject ownerIdentity = new JSONObject();

        ownerIdentity.put("PrincipalId", userId);
        bucket.put("ownerIdentity", ownerIdentity);
        bucket.put("mrn", "mrn::moss:::" + bucketName);
        moss.put("bucket", bucket);
        moss.put("object", object);

        JSONObject mess = new JSONObject(new LinkedHashMap<>());
        mess.put("eventVersion", "V1.0");
        mess.put("eventSource", "MOSS");
        mess.put("eventRegion", region);
        mess.put("eventTime", time);
        mess.put("eventName", event[0]);
        mess.put("userIdentity", userIdentity);
        mess.put("requestParameters", requestParameters);
        mess.put("responseElements", responseElements);
        mess.put("moss", moss);

        JSONObject records = new JSONObject();
        records.put("Records", new Object[]{mess});

        try {
            sendValue(event, records);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static String[] judgeEventType(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String info = bucketConfigurationInfo.get(bucketName);
        JsonObject jsonObject = new JsonObject(info);

        jsonObject.remove("EventNum");


        final String[] result = {null, null, null, null, null, null};
        jsonObject.forEach(res -> {
            String value = String.valueOf(res.getValue());
            JsonObject event = new JsonObject(value);
            int eventNum = event.getInteger("Event");
            JsonObject regex = event.getJsonObject("Regex");
            String type = event.getString("Type");

            String prefix = null;
            String suffix = null;
            if (regex != null && !regex.isEmpty()) {
                prefix = regex.getString("Prefix");
                suffix = regex.getString("Suffix");
            }


            String resultStr = judgeEventType(eventNum, request, prefix, suffix);
            if (resultStr != null && !resultStr.isEmpty()) {
                result[0] = resultStr;
                result[1] = event.getString("Topic");
                result[2] = res.getKey();
                result[3] = type;
                result[4] = event.getString("Queue");
            }

        });

        return result;
    }

    /**
     * 判断事件类型
     *
     * @param eventNum 事件名
     * @param request  请求
     * @param prefix   前缀
     * @param suffix   后缀
     * @return
     */
    private static String judgeEventType(int eventNum, MsHttpRequest request, String prefix, String suffix) {
        if ((2 == (2 & eventNum)) && NotificationFilter.judgeCopy(request, prefix, suffix)) {
            return "ObjectCreated:Copy";
        }

        if ((1 == (1 & eventNum)) && NotificationFilter.judgePut(request, prefix, suffix)) {
            return "ObjectCreated:Put";
        }


        if ((4 == (4 & eventNum)) && NotificationFilter.judgeMultiComplete(request, prefix, suffix)) {
            return "ObjectCreated:CompleteMultipartUpload";
        }

        if ((16 == (16 & eventNum)) && NotificationFilter.judgeDeleteMarkerCreated(request, prefix, suffix)) {
            return "ObjectRemoved:DeleteMarkerCreated";
        }

        if ((8 == (8 & eventNum)) && NotificationFilter.judgeMultiDelete(request)) {
            String str = "ObjectRemoved:multiDelete";
            if (!StringUtils.isBlank(prefix)) {
                str = StringUtils.join(str, ":", prefix);
            } else {
                str = StringUtils.join(str, ":");
            }
            if (!StringUtils.isBlank(suffix)) {
                str = StringUtils.join(str, ":", suffix);
            } else {
                str = StringUtils.join(str, ":");
            }

            return str;
        }

        if ((8 == (8 & eventNum)) && NotificationFilter.judgeDelete(request, prefix, suffix)) {
            return "ObjectRemoved:Delete";
        }

        return null;
    }

    /**
     * 将请求信息临时存储到队列中
     *
     * @param info
     */
    public static void saveInfoToQueue(NotificationInfo info, boolean status) {
        List<ObjectsList> deletedList = info.getDeletedList();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        String time = simpleDateFormat.format(date);
        String bucketName = info.getMsHttpRequest().getBucketName();
        String region = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "region_name");
        String userId = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "user_id");
        deletedList.forEach(res -> {
            String key = res.getKey();

            if (key != null && !key.isEmpty() && NotificationFilter.checkPrefixAndSuffix(key, info.getPrefix(), info.getSuffix())) {
                MessageSend send = new MessageSend();
                send.setBucketName(bucketName);
                send.setRegion(region);
                send.setUserId(userId);
                send.setConfigurationId(info.getEvent()[2]);
                send.setPrincipalId(info.getMsHttpRequest().getUserId());
                send.setRequestId(info.getMsHttpRequest().response().headers().get(X_AMZ_REQUEST_ID));
                send.setSourceIPAddress(info.getMsHttpRequest().host() + ":" + info.getMsHttpRequest().remoteAddress().port());
                send.setTime(time);

                if (StringUtils.isNotEmpty(res.getVersionId()) || !status) {
                    send.setVersion(res.getVersionId());
                    send.setEventName("ObjectRemoved:Delete");
                } else {
                    send.setEventName("ObjectRemoved:DeleteMarkerCreated");
                }
                send.setObjectName(key);
                send.setEvent(info.getEvent());

                messageToSend.add(send);
            }
        });
    }

    /**
     * 发送桶通知
     */
    public static void sendMessage() {
        while (!messageToSend.isEmpty()) {
            MessageSend send = messageToSend.poll();
            ErasureServer.DISK_SCHEDULER.schedule(() -> addMultiDeleteMess(send));
        }
        ErasureServer.DISK_SCHEDULER.schedule(BucketNotification::sendMessage, 5, TimeUnit.SECONDS);

    }

    public static void addMultiDeleteMess(MessageSend send) {

        JSONObject requestParameters = new JSONObject();
        requestParameters.put("sourceIPAddress", send.getSourceIPAddress());

        JSONObject userIdentity = new JSONObject();
        userIdentity.put("PrincipalId", send.getPrincipalId());

        JSONObject responseElements = new JSONObject(new LinkedHashMap<>());
        responseElements.put("x-amz-request-id", send.getRequestId());

        JSONObject object = new JSONObject(new LinkedHashMap<>());
        object.put("key", send.getObjectName());
        if (send.getVersion() != null && send.getVersion().isEmpty()) {
            object.put("VersionId", send.getVersion());
        }
        JSONObject moss = new JSONObject(new LinkedHashMap<>());
        moss.put("Version", "1.0");
        moss.put("configurationId", send.getConfigurationId());
        JSONObject bucket = new JSONObject();
        bucket.put("name", send.getBucketName());
        JSONObject ownerIdentity = new JSONObject();

        ownerIdentity.put("PrincipalId", send.getUserId());
        bucket.put("ownerIdentity", ownerIdentity);
        bucket.put("mrn", "mrn::moss:::" + send.getBucketName());
        moss.put("bucket", bucket);
        moss.put("object", object);

        JSONObject mess = new JSONObject(new LinkedHashMap<>());
        mess.put("eventVersion", "V1.0");
        mess.put("eventSource", "MOSS");
        mess.put("eventRegion", send.getRegion());
        mess.put("eventTime", send.getTime());
        mess.put("eventName", send.getEventName());
        mess.put("userIdentity", userIdentity);
        mess.put("requestParameters", requestParameters);
        mess.put("responseElements", responseElements);
        mess.put("moss", moss);

        JSONObject records = new JSONObject();
        records.put("Records", new Object[]{mess});

        try {
            sendValue(send.getEvent(), records);
        } catch (Exception e) {
            logger.error("Send message fail. " + e);
        }

    }

    private static final Queue<Tuple3<String[], JSONObject, Integer>> queue = Queues.<Tuple3<String[], JSONObject, Integer>>get(50_0000).get();

    private static final AtomicBoolean sending = new AtomicBoolean(false);

    private static void sendValue(String[] event, JSONObject records) {
        boolean offer;
        synchronized (queue) {
            offer = queue.offer(new Tuple3<>(event, records, 0));
        }

        if (offer) {
            RabbitMqNotification.executor.submit(() -> {
                if (sending.compareAndSet(false, true)) {
                    try {
                        Tuple3<String[], JSONObject, Integer> task = queue.poll();
                        while (task != null) {
                            try {

                                if (!sendValue0(task.var1, task.var2)) {
                                    task.var3++;
                                    if (task.var3 > 2){
                                        logger.error("The message retry send 3 times, the message is :" + task.var2.toString());
                                    }else {
                                        queue.offer(task);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Send message fail. " + e);
                            }
                            task = queue.poll();
                        }
                    } finally {
                        sending.set(false);
                    }
                }
            });
        } else {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "rabbitmq notify queue overflow");
        }
    }

    /**
     * 根据不同类型发送rabbitmq消息
     *
     * @param event
     * @param records
     * @throws IOException
     */
    private static boolean sendValue0(String[] event, JSONObject records) throws IOException {
        if ("SNS".equals(event[3])) {
            //使用SNS发送消息
            JsonObject topicInfo = new JsonObject(topicConfiguration.get(event[1]));
            String topicIp = topicInfo.getString("address").split(":")[0];
            Integer topicPort = Integer.valueOf(topicInfo.getString("address").split(":")[1]);

            String user = topicInfo.getString("user");
            String password = topicInfo.getString("password");
            String exchange = topicInfo.getString("exchange");
            String routingKey = topicInfo.getString("routingKey");
            Channel channel = RabbitMqNotification.getNotificationChannel(topicIp, user, password, topicPort);
            if (channel == null) {
                logger.error("Add sns message {} to Queue fail", records.toString());
            } else {
                channel.basicPublish(exchange, routingKey, null, records.toString().getBytes(StandardCharsets.UTF_8));
                return true;
            }
        } else if ("SQS".equals(event[3])) {
            //使用SQS发送信息
            JsonObject topicInfo = new JsonObject(queueConfiguration.get(event[4]));
            String topicIp = topicInfo.getString("address").split(":")[0];
            Integer topicPort = Integer.valueOf(topicInfo.getString("address").split(":")[1]);

            String user = topicInfo.getString("user");
            String password = topicInfo.getString("password");
            String queue = topicInfo.getString("queue");
            Channel channel = RabbitMqNotification.getNotificationChannel(topicIp, user, password, topicPort);
            if (channel == null) {
                logger.error("Add sqs message {} to Queue fail", records.toString());
            } else {
                channel.basicPublish("", queue, null, records.toString().getBytes(StandardCharsets.UTF_8));
                return true;
            }
        } else if ("Kafka".equals(event[3])) {
            JsonObject kafkaInfo = new JsonObject(kafkaConfiguration.get(event[1]));
            String topicIp = kafkaInfo.getString("address").split(":")[0];
            int topicPort = Integer.parseInt(kafkaInfo.getString("address").split(":")[1]);

            String topic = kafkaInfo.getString("topic");
            KafkaProducer producer = RabbitMqNotification.getKafkaProducer(topicIp, topicPort, topic);
            try {
                producer.send(new ProducerRecord(topic, records.toString()));
                return true;
            } catch (Exception e) {
                logger.error("send message to kafka fail. The message is {}", records.toString());
                producer.close();
                return false;
            }
        }
        return false;
    }
}
