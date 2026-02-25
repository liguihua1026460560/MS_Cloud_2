package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.notification.*;
import com.macrosan.rabbitmq.RabbitMqNotification;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.notification.BucketNotification;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.rabbitmq.client.*;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.rabbitmq.RabbitMqNotification.closeKafkaClient;


/**
 * @author zhangzhixin
 */
public class BucketNotificationService extends BaseService {

    private static BucketNotificationService instance = null;
    private static String[] events = new String[7];
    private static final String BUCKET_NOTIFICATION = "bucket_notification";
    private static final String TOPIC_CONFIGURATION = "topic_configuration";
    private static final String QUEUE_CONFIGURATION = "queue_configuration";
    private static final String KAFKA_CONFIGURATION = "kafka_configuration";
    static Logger logger = LogManager.getLogger("Notification.BucketNotificationService");

    private BucketNotificationService() {
        super();
    }

    public static BucketNotificationService getInstance() {
        if (instance == null) {
            instance = new BucketNotificationService();
        }
        return instance;
    }

    public ResponseMsg putBucketNotification(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        if (checkBody(paramMap.get(BODY))){
            throw new MsException(ErrorNo.MALFORMED_ERROR,"The XML you provided was not wellformed or did not validate against our published schema.");
        }
        NotificationConfiguration notification = (NotificationConfiguration) JaxbUtils.toObject(paramMap.get(BODY));
        String method = "PutBucketNotification";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        logger.debug("policyResult = {}", policyResult);
        checkBucket(bucketName, userId);
        //检测与文件功能是否冲突
        if (bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketTrash");
        }

        if (notification != null && notification.getTopicConfiguration() == null && notification.getQueueConfiguration() == null && notification.getKafkaConfiguration() == null) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_NOTIFICATION, bucketName);
            return new ResponseMsg(SUCCESS_STATUS);
        }

        if (notification != null && notification.getTopicConfiguration() != null) {
            //判断是不是SNS配置
            List<TopicConfiguration> topicConfigurations = notification.getTopicConfiguration();
            setSNS(topicConfigurations, bucketName, paramMap);
        }

        if (notification != null && notification.getQueueConfiguration() != null) {
            //判断是不是SQS配置
            List<QueueConfiguration> queueConfigurations = notification.getQueueConfiguration();
            setSQS(queueConfigurations, bucketName, paramMap);
        }

        if (notification != null && notification.getKafkaConfiguration() != null) {
            //判断是不是Kafka配置
            List<KafkaConfiguration> kafkaConfigurations = notification.getKafkaConfiguration();
            setKafka(kafkaConfigurations, bucketName, paramMap);
        }

        BucketNotification.updateConfiguration();

        return new ResponseMsg(SUCCESS_STATUS);
    }

    public ResponseMsg getBucketNotification(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        String result = pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_NOTIFICATION, bucketName);
        Map<String,String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()){
            throw new MsException(ErrorNo.NO_SUCH_BUCKET,"The bucket not exist");
        }
        String method = "GetBucketNotification";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        logger.debug("policyResult = {}", policyResult);
        checkBucket(bucketName, userId);

        NotificationConfiguration configuration = new NotificationConfiguration();
        if (result == null) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setData(configuration);
        }
        JsonObject jsonObject = new JsonObject(result);
        jsonObject.remove("EventNum");

        List<TopicConfiguration> topicConfigurations = new LinkedList<>();
        List<QueueConfiguration> queueConfigurations = new LinkedList<>();
        List<KafkaConfiguration> kafkaConfigurations = new LinkedList<>();
        jsonObject.forEach(res -> {
            String value = String.valueOf(res.getValue());
            JsonObject event = new JsonObject(value);
            String type = event.getString("Type");
            if ("SNS".equals(type)) {
                topicConfigurations.add(returnSNSSetting(res.getKey(), event,bucketInfo));
            } else if ("SQS".equals(type)) {
                queueConfigurations.add(returnSQSSetting(res.getKey(), event,bucketInfo));
            } else if ("Kafka".equals(type)){
                kafkaConfigurations.add(returnKafkaSetting(res.getKey(), event,bucketInfo));
            }
        });

        configuration.setTopicConfiguration(topicConfigurations);
        configuration.setQueueConfiguration(queueConfigurations);
        configuration.setKafkaConfiguration(kafkaConfigurations);

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setData(configuration);
    }

    /**
     * 检测目标桶是否存在，且此次请求是否有权限调用接口
     *
     * @param bucketName 桶名
     * @param userId 用户名
     */
    private void checkBucket(String bucketName, String userId) {
        MsAclUtils.checkIfAnonymous(userId);

        userCheck(userId,bucketName);
    }

    /**
     * 控制台接口
     * 获取环境中存在的topic
     */
    public ResponseMsg getTopic(UnifiedMap<String, String> param) {
        String type = param.get("type");
        TopicSetting setting = new TopicSetting();
        if ("sns".equals(type)){
            Map<String, String> result = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(TOPIC_CONFIGURATION);
            List<String> topics = new LinkedList<>(result.keySet());
            setting.setTopic(topics);
        }else if ("sqs".equals(type)){
            Map<String, String> result = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(QUEUE_CONFIGURATION);
            List<String> queues = new LinkedList<>(result.keySet());
            setting.setTopic(queues);
        }else if ("kafka".equals(type)){
            Map<String, String> result = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(KAFKA_CONFIGURATION);
            List<String> queues = new LinkedList<>(result.keySet());
            setting.setTopic(queues);
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setData(setting);
    }

    /**
     * 控制台接口
     * 删除指定桶的指定事件
     *
     * @param param 请求参数
     * @return 请求结果
     */
    public ResponseMsg deleteBucketNotification(UnifiedMap<String, String> param) {
        String res = param.get("eventId");
        String userId = param.get(USER_ID);
        String bucketName = param.get(BUCKET_NAME);
        String value = pool.getCommand(REDIS_SYSINFO_INDEX).hget("bucket_notification", param.get(BUCKET_NAME));
        Map<String,String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(param.get(BUCKET_NAME));
        if (bucketInfo.isEmpty()){
            throw new MsException(ErrorNo.NO_SUCH_BUCKET,"The bucket not exist");
        }

        checkBucket(bucketName, userId);

        JsonObject jsonObject = new JsonObject(value);
        String method = "DeleteNotification";
        int policyResult = PolicyCheckUtils.getPolicyResult(param, param.get(BUCKET_NAME), userId, method);
        logger.debug("policyResult = {}", policyResult);
        AtomicInteger eventNums = new AtomicInteger(jsonObject.getInteger("EventNum"));
        JsonObject event = jsonObject.getJsonObject(res);

        if (!event.isEmpty() && event.getString("Type").equals("Kafka")){
            closeKafkaClient(event);
        }

        jsonObject.remove(res);
        jsonObject.remove("EventNum");

        if (jsonObject.isEmpty()){
            eventNums.set(0);
        }else {
            eventNums.set(getEventNums(jsonObject));
        }

        if (eventNums.get() == 0) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel("bucket_notification", param.get(BUCKET_NAME));
        } else {
            jsonObject.put("EventNum", eventNums.get());
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("bucket_notification", param.get(BUCKET_NAME), jsonObject.toString());
        }

        BucketNotification.updateConfiguration();

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 发送测试消息
     *
     * @param topic topic名字
     * @param param 参数名
     * @throws IOException
     */
    private static boolean sendTestMess(String key, String topic, UnifiedMap<String, String> param) throws IOException, InterruptedException {
        JsonObject message = new JsonObject();
        message.put("Service", "MOSS");
        message.put("Event", "TestEvent");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        message.put("Time", simpleDateFormat.format(date));
        message.put("Bucket", param.get(BUCKET_NAME));
        message.put("RequestId", param.get(REQUESTID));
        message.put("HostId", config.getHostUuid());


        String topicInfo = pool.getCommand(REDIS_SYSINFO_INDEX).hget(key, topic);
        JsonObject jsonObject = new JsonObject(topicInfo);

        testIpAndPort(jsonObject.getString("address"));

        String topicIp = jsonObject.getString("address").split(":")[0];
        int topicPort = Integer.parseInt(jsonObject.getString("address").split(":")[1]);
        String user = jsonObject.getString("user");
        String password = jsonObject.getString("password");
        if (QUEUE_CONFIGURATION.equals(key)) {
            String queue = jsonObject.getString("queue");
            Channel channel = RabbitMqNotification.getNotificationChannel(topicIp, user, password, topicPort);
            if (channel == null) {
                return true;
            }

            final boolean[] result = {false};
            channel.confirmSelect();
            ReturnListener returnListener = channel.addReturnListener(new ReturnCallback() {
                @Override
                public void handle(Return returnMessage) {
                    result[0] = true;
                    logger.error("The queue name is error");
                }
            });
            channel.basicPublish("", queue,true, null, message.toString().getBytes(StandardCharsets.UTF_8));

            if (!channel.waitForConfirms()){
                return true;
            }

            return result[0];
        } else if (TOPIC_CONFIGURATION.equals(key)){
            String exchange = jsonObject.getString("exchange");
            String routingKey = jsonObject.getString("routingKey");
            Channel channel = RabbitMqNotification.getNotificationChannel(topicIp, user, password, topicPort);
            if (channel == null) {
                return true;
            }
            final boolean[] result = {false};
            channel.confirmSelect();
            ReturnListener returnListener = channel.addReturnListener(new ReturnCallback() {
                @Override
                public void handle(Return returnMessage) {
                    result[0] = true;
                    logger.error("The routing key is error");
                }
            });
            channel.basicPublish(exchange, routingKey,true, null, message.toString().getBytes(StandardCharsets.UTF_8));

            if (ServerConfig.isVm()){
                try {
                    if (!channel.waitForConfirms(15)){
                        return true;
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            } else {
                if (!channel.waitForConfirms()){
                    return true;
                }
            }
            return result[0];
        } else if (KAFKA_CONFIGURATION.equals(key)){
            String kafkaTopic = jsonObject.getString("topic");

            KafkaProducer producer = RabbitMqNotification.getKafkaProducer(topicIp,topicPort,kafkaTopic);
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, message.toString());

            try {
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null){
                            throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"send message fail");
                        }
                    }
                }).get();
            }catch (Exception e){
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * 检测事件是否存在
     *
     * @param event
     * @return
     */
    private static boolean checkEvent(String event) {
        events[0] = "ObjectCreated:Put";
        events[1] = "ObjectCreated:Copy";
        events[2] = "ObjectCreated:CompleteMultipartUpload";
        events[3] = "ObjectCreated:*";
        events[4] = "ObjectRemoved:Delete";
        events[5] = "ObjectRemoved:DeleteMarkerCreated";
        events[6] = "ObjectRemoved:*";

        for (int i = 0; i < events.length; i++) {
            if (event.equals(events[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取修改后的事件编号
     *
     * @param event      当前事件编号
     * @param idEventNum eventId下的事件编号
     * @return
     */
    private static int changeEventInfo( String event, int idEventNum) {

        int eventInfo = getEventNum(event);


        if ((idEventNum & eventInfo) == 0) {
            idEventNum = idEventNum | eventInfo;
        }


        return idEventNum;
    }

    /**
     * 获取指定事件的编号
     *
     * @param event
     * @return
     */
    private static int getEventNum(String event) {
        switch (event) {
            case "ObjectCreated:Put":
                return 1;
            case "ObjectCreated:Copy":
                return 2;
            case "ObjectCreated:CompleteMultipartUpload":
                return 4;
            case "ObjectCreated:*":
                return 7;
            case "ObjectRemoved:Delete":
                return 8;
            case "ObjectRemoved:DeleteMarkerCreated":
                return 16;
            case "ObjectRemoved:*":
                return 24;
            default:
                return 0;
        }
    }

    /**
     * 获取编号对应的事件名
     *
     * @param eventNum
     * @return
     */
    private static LinkedList getEvents(int eventNum) {
        LinkedList<String> event = new LinkedList<>();
        if ((eventNum & 1) == 1) {
            event.add("ObjectCreated:Put");
        }

        if ((eventNum & 2) == 2) {
            event.add("ObjectCreated:Copy");
        }

        if ((eventNum & 4) == 4) {
            event.add("ObjectCreated:CompleteMultipartUpload");
        }

        if ((eventNum & 8) == 8) {
            event.add("ObjectRemoved:Delete");
        }

        if ((eventNum & 16) == 16) {
            event.add("ObjectRemoved:DeleteMarkerCreated");
        }

        return event;
    }

    /**
     * 测试ip和port是否能连接上
     *
     * @param address
     */
    public static void testIpAndPort(String address) {
        Socket socket = new Socket();
        String[] info = address.split(":");
        try {
            socket.connect(new InetSocketAddress(info[0], Integer.parseInt(info[1])), 200);
            if (!socket.isConnected()) {
                throw new MsException(ErrorNo.CONNECTED_FAIL, "Connect the host fail");
            }
        } catch (IOException e) {
            throw new MsException(ErrorNo.CONNECTED_FAIL, "Connect the host fail");
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                throw new MsException(ErrorNo.CONNECTED_FAIL, "Connect the host fail");
            }
        }
    }

    private static void checkRegex(String str) {
        if (str.length() > 1024) {
            throw new MsException(ErrorNo.PARAM_IS_ILLEGAL, "The str length illegal");
        }
    }

    /**
     * 设置SQS配置
     *
     * @param sqs 请求中的SQS参数
     */
    private static void setSQS(List<QueueConfiguration> sqs, String bucketName, UnifiedMap<String, String> paramMap) {

        AtomicInteger eventNum = new AtomicInteger(0);
        JsonObject events = new JsonObject();
        String oldSetting = pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_NOTIFICATION, bucketName);
        if (oldSetting != null && !oldSetting.isEmpty()) {
            events = new JsonObject(oldSetting);
            eventNum.set(events.getInteger("EventNum"));
        }

        JsonObject finalEvents = events;
        sqs.forEach(queueConfiguration -> {
            String id = queueConfiguration.getId();
            if (id == null || id.isEmpty()) {
                id = "event-" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
            }

            //观察id是否存在，如果存在进入修改流程
            if (finalEvents.getJsonObject(id) != null) {
                finalEvents.remove(id);
            }

            String queueMrn = queueConfiguration.getQueue();

            //检测mrn的格式和内容是否合规
            checkMrn(queueMrn, bucketName, "sqs");

            String[] queueParams = queueMrn.split(":");

            JsonObject jsonObject = new JsonObject();
            jsonObject.put("Queue", queueParams[3]);
            List<String> event = queueConfiguration.getEvent();
            int idEventNum = 0;
            for (String res : event) {
                if (res.startsWith("s3")) {
                    String[] result = res.split(":");
                    res = result[1] + ":" + result[2];
                }
                //检测事件类型是否存在
                if (!checkEvent(res)) {
                    throw new MsException(ErrorNo.EVENT_INPUT_ERROR, "The event is illegal");
                }

                idEventNum = changeEventInfo(res, idEventNum);
            }
            jsonObject.put("Event", idEventNum);

            //配置相关的前后缀
            JsonObject regex = new JsonObject();
            AtomicReference<String> prefix = new AtomicReference<>("");
            AtomicReference<String> suffix = new AtomicReference<>("");
            if (queueConfiguration.getFilter() != null && queueConfiguration.getFilter().getS3Key() != null && queueConfiguration.getFilter().getS3Key().getFilterRule() != null) {
                queueConfiguration.getFilter().getS3Key().getFilterRule().forEach(filter -> {
                    switch (filter.getName()) {
                        case "prefix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Prefix", filter.getValue());
                            }
                            prefix.set(filter.getValue());
                            break;
                        case "suffix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Suffix", filter.getValue());
                            }
                            suffix.set(filter.getValue());
                            break;
                        default:
                            throw new MsException(ErrorNo.FILTER_NAME_ERROR, "filter rule name must be either prefix or suffix");
                    }
                });
            }
            judgeOverlap(finalEvents, event, prefix.get(), suffix.get());
            jsonObject.put("Regex", regex);
            jsonObject.put("Type", "SQS");
            boolean result = true;
            try {
                result =  sendTestMess(QUEUE_CONFIGURATION, queueParams[3], paramMap);
                finalEvents.put(id, jsonObject);
            } catch (Exception e) {
                throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"send message fail");
            }finally {
                if (result){
                    throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"send message fail");
                }
            }
        });

        //将请求中的参数传递存储到redis中
        events = finalEvents;
        eventNum.set(getEventNums(events));
        events.put("EventNum", eventNum.get());
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(BUCKET_NOTIFICATION, bucketName, events.toString());

    }

    /**
     * 设置SNS配置
     *
     * @param sns 请求中的SNS参数
     */
    private static void setSNS(List<TopicConfiguration> sns, String bucketName, UnifiedMap<String, String> paramMap) {
        //获取请求中每一个topic中的属性
        AtomicInteger eventNum = new AtomicInteger(0);
        JsonObject events = new JsonObject();
        String oldSetting = pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_NOTIFICATION, bucketName);
        if (oldSetting != null && !oldSetting.isEmpty()) {
            events = new JsonObject(oldSetting);
            eventNum.set(events.getInteger("EventNum"));
        }

        JsonObject finalEvents = events;

        sns.forEach(topicConfiguration -> {
            String id = topicConfiguration.getId();
            if (id == null || id.isEmpty()) {
                id = "event-" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
            }

            //当eventId与原有的相同时，进入修改流程
            if (finalEvents.getJsonObject(id) != null) {
                finalEvents.remove(id);
            }


            checkMrn(topicConfiguration.getTopic(), bucketName, "sns");

            String[] topic = topicConfiguration.getTopic().split(":");

            List<String> event = topicConfiguration.getEvent();
            if (event.isEmpty()) {
                throw new MsException(ErrorNo.EVENT_INPUT_ERROR, "The event is empty");
            }

            JsonObject jsonObject = new JsonObject();
            jsonObject.put("Topic", topic[3]);
            //配置相关的事件
            int idEventNum = 0;
            for (String res : event) {
                if (res.startsWith("s3")) {
                    String[] result = res.split(":");
                    res = result[1] + ":" + result[2];
                }
                //检测事件类型是否存在
                if (!checkEvent(res)) {
                    throw new MsException(ErrorNo.EVENT_INPUT_ERROR, "The event is illegal");
                }
                idEventNum = changeEventInfo(res, idEventNum);
            }
            jsonObject.put("Event", idEventNum);

            JsonObject regex = new JsonObject();
            AtomicReference<String> prefix = new AtomicReference<>("");
            AtomicReference<String> suffix = new AtomicReference<>("");
            if (topicConfiguration.getFilter() != null && topicConfiguration.getFilter().getS3Key() != null && topicConfiguration.getFilter().getS3Key().getFilterRule() != null) {
                topicConfiguration.getFilter().getS3Key().getFilterRule().forEach(filter -> {
                    switch (filter.getName()) {
                        case "prefix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Prefix", filter.getValue());
                            }
                            prefix.set(filter.getValue());
                            break;
                        case "suffix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Suffix", filter.getValue());
                            }
                            suffix.set(filter.getValue());
                            break;
                        default:
                            throw new MsException(ErrorNo.FILTER_NAME_ERROR, "filter rule name must be either prefix or suffix");
                    }
                });
            }
            judgeOverlap(finalEvents, event, prefix.get(), suffix.get());
            jsonObject.put("Regex", regex);
            jsonObject.put("Type", "SNS");
            boolean result = false;
            try {
                result = sendTestMess(TOPIC_CONFIGURATION, topic[3], paramMap);
                finalEvents.put(id, jsonObject);
            } catch (Exception e) {
                throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"Send message fail");
            }finally {
                if (result){
                    throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"Send message fail");
                }
            }

        });

        //将请求中的参数传递存储到redis中
        events = finalEvents;
        eventNum.set(getEventNums(events));
        events.put("EventNum", eventNum.get());
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(BUCKET_NOTIFICATION, bucketName, events.toString());
    }

    private static void setKafka(List<KafkaConfiguration> kafka, String bucketName, UnifiedMap<String, String> paramMap) {
        AtomicInteger eventNum = new AtomicInteger(0);
        JsonObject events = new JsonObject();
        String oldSetting = pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_NOTIFICATION, bucketName);
        if (oldSetting != null && !oldSetting.isEmpty()) {
            events = new JsonObject(oldSetting);
            eventNum.set(events.getInteger("EventNum"));
        }

        JsonObject finalEvents = events;
        kafka.forEach(kafkaConfiguration -> {
            String id = kafkaConfiguration.getId();
            if (id == null || id.isEmpty()) {
                id = "event-" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
            }

            //观察id是否存在，如果存在进入修改流程
            if (finalEvents.getJsonObject(id) != null) {
                finalEvents.remove(id);
            }

            String queueMrn = kafkaConfiguration.getTopic();

            //检测mrn的格式和内容是否合规
            checkMrn(queueMrn, bucketName, "kafka");

            String[] queueParams = queueMrn.split(":");

            JsonObject jsonObject = new JsonObject();
            jsonObject.put("Topic", queueParams[3]);
            List<String> event = kafkaConfiguration.getEvent();
            int idEventNum = 0;
            for (String res : event) {
                if (res.startsWith("s3")) {
                    String[] result = res.split(":");
                    res = result[1] + ":" + result[2];
                }
                //检测事件类型是否存在
                if (!checkEvent(res)) {
                    throw new MsException(ErrorNo.EVENT_INPUT_ERROR, "The event is illegal");
                }

                idEventNum = changeEventInfo(res, idEventNum);
            }
            jsonObject.put("Event", idEventNum);

            //配置相关的前后缀
            JsonObject regex = new JsonObject();
            AtomicReference<String> prefix = new AtomicReference<>("");
            AtomicReference<String> suffix = new AtomicReference<>("");
            if (kafkaConfiguration.getFilter() != null && kafkaConfiguration.getFilter().getS3Key() != null && kafkaConfiguration.getFilter().getS3Key().getFilterRule() != null) {
                kafkaConfiguration.getFilter().getS3Key().getFilterRule().forEach(filter -> {
                    switch (filter.getName()) {
                        case "prefix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Prefix", filter.getValue());
                            }
                            prefix.set(filter.getValue());
                            break;
                        case "suffix":
                            checkRegex(filter.getValue());
                            if (!StringUtils.isBlank(filter.getValue())){
                                regex.put("Suffix", filter.getValue());
                            }
                            suffix.set(filter.getValue());
                            break;
                        default:
                            throw new MsException(ErrorNo.FILTER_NAME_ERROR, "filter rule name must be either prefix or suffix");
                    }
                });
            }
            judgeOverlap(finalEvents, event, prefix.get(), suffix.get());
            jsonObject.put("Regex", regex);
            jsonObject.put("Type", "Kafka");
            boolean result = true;
            try {
                result = sendTestMess(KAFKA_CONFIGURATION, queueParams[3], paramMap);
                finalEvents.put(id, jsonObject);
            } catch (Exception e) {
                throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"send message fail");
            }finally {
                if (!result){
                    throw new MsException(ErrorNo.CREATE_CHANNEL_FAIL,"send message fail");
                }
            }
        });

        //将请求中的参数传递存储到redis中
        events = finalEvents;
        eventNum.set(getEventNums(events));
        events.put("EventNum", eventNum.get());
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(BUCKET_NOTIFICATION, bucketName, events.toString());

    }
    private static int getEventNums(JsonObject events) {
        AtomicInteger num = new AtomicInteger(0);
        events.stream().forEach(res ->{
            JsonObject json = (JsonObject) res.getValue();
            int eventNum = json.getInteger("Event");
            if ((eventNum & num.get()) == 0) {
                num.set(eventNum | num.get());
            }
        });
        return num.get();
    }

    /**
     * 检查队列名是否合规
     *
     * @param queueName 队列名
     */
    private static void checkQueueName(String queueName) {
        if (queueName == null || queueName.isEmpty()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The queue name is empty");
        }

        if (queueName.length() > 100) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The queue name length more than 100 chars");
        }

        char[] chars = queueName.toCharArray();
        for (char a : chars) {
            if ((a < 48) || (a > 123) || (a < 97 && a > 90) || (a > 57 && a < 65)) {
                if (a != 45 && a != 95) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "The queue name is illegal");
                }
            }
        }
    }

    /**
     * 检查mrn是否合规
     *
     * @param mrn        mrn名
     * @param bucketName 桶名
     */
    private static void checkMrn(String mrn, String bucketName, String type) {
        if (mrn == null || mrn.isEmpty()) {
            throw new MsException(ErrorNo.THE_MRN_ILLEGAL, "The mrn queue is null");
        }
        String[] params = mrn.split(":");
        if (params.length != 4) {
            throw new MsException(ErrorNo.THE_MRN_ILLEGAL, "The mrn is illegal");
        }
        if (!"mrn".equals(params[0])) {
            throw new MsException(ErrorNo.THE_MRN_ILLEGAL, "the param isn't start with mrn");
        }

        if (!params[1].equals(type)) {
            throw new MsException(ErrorNo.THE_MRN_ILLEGAL, "the notification type not right");
        }

        String userId = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "user_id");
        if (!params[2].equals(userId)) {
            throw new MsException(ErrorNo.TOPIC_OR_QUEUE_NOT_EXIST, "The user id mismatch");
        }

        String result = null;
        if ("sns".equals(type)) {
            result = pool.getCommand(REDIS_SYSINFO_INDEX).hget(TOPIC_CONFIGURATION, params[3]);
        } else if ("sqs".equals(type)) {
            result = pool.getCommand(REDIS_SYSINFO_INDEX).hget(QUEUE_CONFIGURATION, params[3]);
        } else if ("kafka".equals(type)){
            result = pool.getCommand(REDIS_SYSINFO_INDEX).hget(KAFKA_CONFIGURATION, params[3]);
        }

        if (result == null) {
            throw new MsException(ErrorNo.TOPIC_OR_QUEUE_NOT_EXIST, "The setting don't exist");
        }

        //检测队列名是否合规
        if ("sqs".equals(params[1])) {
            checkQueueName(params[3]);
        }

    }

    /**
     * 返回解析到的SNS配置
     *
     * @param jsonObject
     * @return
     */
    private static TopicConfiguration returnSNSSetting(String id, JsonObject jsonObject,Map<String,String> bucketInfo) {
        TopicConfiguration topicConfiguration = new TopicConfiguration();
        String topic = getTopic(bucketInfo,"sns",jsonObject.getString("Topic"));
        topicConfiguration.setTopic(topic);

        LinkedList<String> event = getEvents(jsonObject.getInteger("Event"));
        topicConfiguration.setEvent(event);
        topicConfiguration.setId(id);
        topicConfiguration.setFilter(returnFilter(jsonObject));

        return topicConfiguration;
    }

    /**
     * 返回解析到的SQS配置
     *
     * @param jsonObject
     * @return
     */
    private static QueueConfiguration returnSQSSetting(String id, JsonObject jsonObject,Map<String,String> bucketInfo) {
        QueueConfiguration queueConfiguration = new QueueConfiguration();
        String topic = getTopic(bucketInfo,"sqs",jsonObject.getString("Queue"));
        queueConfiguration.setQueue(topic);

        LinkedList<String> event = getEvents(jsonObject.getInteger("Event"));
        queueConfiguration.setEvent(event);
        queueConfiguration.setId(id);
        queueConfiguration.setFilter(returnFilter(jsonObject));

        return queueConfiguration;
    }

    private static KafkaConfiguration returnKafkaSetting(String id, JsonObject jsonObject,Map<String,String> bucketInfo){
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        String topic = getTopic(bucketInfo,"kafka",jsonObject.getString("Topic"));
        kafkaConfiguration.setTopic(topic);

        LinkedList<String> event = getEvents(jsonObject.getInteger("Event"));
        kafkaConfiguration.setEvent(event);
        kafkaConfiguration.setId(id);
        kafkaConfiguration.setFilter(returnFilter(jsonObject));

        return kafkaConfiguration;
    }

    /**
     * 返回解析Filter
     *
     * @param eventValue
     * @return
     */
    private static Filter returnFilter(JsonObject eventValue) {
        Filter filter = new Filter();
        S3Key s3Key = new S3Key();
        List<FilterRule> filterRules = new LinkedList<>();
        if (eventValue.getValue("Regex") != null) {
            JsonObject regex = new JsonObject(String.valueOf(eventValue.getJsonObject("Regex")));
            if (regex.getString("Prefix") != null) {
                FilterRule prefix = new FilterRule();
                prefix.setName("prefix");
                prefix.setValue(regex.getString("Prefix"));
                filterRules.add(prefix);
            }

            if (regex.getString("Suffix") != null) {
                FilterRule suffix = new FilterRule();
                suffix.setName("suffix");
                suffix.setValue(regex.getString("Suffix"));
                filterRules.add(suffix);
            }

        }

        s3Key.setFilterRule(filterRules);
        filter.setS3Key(s3Key);

        return filter;
    }

    private static void judgeOverlap(JsonObject events, List<String> event, String pre, String suf) {
        if (!events.isEmpty()) {
            events.remove("EventNum");
            events.remove("Type");
            event.forEach(result -> {
                events.forEach(res -> {

                    JsonObject oneEvent = (JsonObject) res.getValue();
                    int eventNum = oneEvent.getInteger("Event");
                    if ((eventNum & getEventNum(result)) != 0) {
                        JsonObject regex = oneEvent.getJsonObject("Regex");
                        if (regex.isEmpty() || regex == null) {
                            throw new MsException(ErrorNo.EVENT_REPETITION, "The prefix overlap");
                        }
                        String prefix = regex.getString("Prefix");
                        String suffix = regex.getString("Suffix");

                        if (prefix == null) {
                            prefix = "";
                        }
                        if (suffix == null) {
                            suffix = "";
                        }

                        boolean prefixOverlap = (prefix.startsWith(pre) || pre.startsWith(prefix));
                        boolean suffixOverlap = (suffix.endsWith(suf) || suf.endsWith(suffix));
                        if (prefixOverlap && suffixOverlap){
                            throw new MsException(ErrorNo.EVENT_REPETITION, "The regex overlap");
                        }
                    }
                });
            });

        }
    }


    private static String getTopic(Map<String,String> bucketInfo,String type,String topic){
        String userId = bucketInfo.get("user_id");
        return "mrn:" + type + ":" + userId + ":" + topic;
    }

    private static boolean checkBody(String body){
        String regex = "<Id>(.*?)&.*?<\\/Id>";
        String event = "<Event>(.*?)&.*?<\\/Event>";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
        java.util.regex.Pattern pattern1 = Pattern.compile(event);
        Matcher matcher = pattern.matcher(body);
        Matcher matcher1 = pattern1.matcher(body);
        return (matcher.find() || matcher1.find());
    }

    public static boolean getBucketKafka(String bucket){
        String result = pool.getCommand(2).hget(BUCKET_NOTIFICATION,bucket);
        if (result.contains("Kafka")){
            return true;
        }
        return false;
    }

}
