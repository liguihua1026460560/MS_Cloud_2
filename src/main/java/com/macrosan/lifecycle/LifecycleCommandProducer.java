package com.macrosan.lifecycle;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.lifecycle.mq.LifecycleChannels;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.lifecycle.*;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.lifecycle.LifecycleUtils;
import com.macrosan.utils.listutil.DefaultListOperationFactory;
import com.macrosan.utils.listutil.ListOperation;
import com.macrosan.utils.listutil.ListOptions;
import com.macrosan.utils.listutil.ListType;
import com.macrosan.utils.listutil.context.ListObjectsContext;
import com.macrosan.utils.listutil.context.ListOperationContext;
import com.macrosan.utils.listutil.interpcy.DeadLinePolicy;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.rabbitmq.client.Envelope;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.HeartBeatChecker.syncPolicy;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_TIME;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.lifecycle.LifecycleService.getEndStamp;
import static com.macrosan.lifecycle.LifecycleService.setStartStamp;
import static com.macrosan.storage.metaserver.ShardingWorker.bucketHasShardingTask;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;
import static com.macrosan.utils.lifecycle.LifecycleUtils.*;


/**
 * @author admin
 */
@Log4j2
public class LifecycleCommandProducer implements Job {

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    private static String localUuid = ServerConfig.getInstance().getHostUuid();

    private static final Map<String, Map<String, Set<Tuple3<String, String, Integer>>>> CUR_VERSION_DAYS_MOVE_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<String, Map<String, Set<Tuple2<String, String>>>> CUR_VERSION_DATE_MOVE_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<String, Map<String, Set<Tuple2<String, String>>>> HISTORY_VERSION_MOVE_BUCKET_MAP = new ConcurrentHashMap<>(30);

    /***用于自动定义元数据分层存储***/
    private static final Map<String, Map<String, Set<Tuple2<String, String>>>> HISTORY_VERSION_LAYERED_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<String, Map<String, Set<Tuple3<String, String, Integer>>>> CUR_VERSION_DAYS_LAYERED_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final Map<String, Map<String, Set<Tuple2<String, String>>>> CUR_VERSION_DATE_LAYERED_BUCKET_MAP = new ConcurrentHashMap<>(30);

    private static final long ONE_DAY_TIMESTAMPS = (60 * 60 * 24 * 1000);

    private static final int RECORD_NUM = 100000;

    private static final int LOW_OCCUPANCY = 20;

    private static final int MEDIUM_OCCUPANCY = 40;

    private static final int HIGH_OCCUPANCY = 60;

    private static final int FULL_OCCUPANCY = 80;

    public static String MQ_DISK_USED_RATE = "0";

    public static Semaphore semaphore = new Semaphore(10000);

    private static final List<String[]> MOVE_BUCKET_LIST = new ArrayList<>();

    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, String>>() {
            };

    public LifecycleCommandProducer() {
        log.info("LifecycleCommandProducer is constructed.");
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        String lifecycleEndDate = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_end_date");
        lifecycleEndDate = StringUtils.isEmpty(lifecycleEndDate) ? "7:00" : lifecycleEndDate;
        String lifecycleStartDate = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_start_date");
        lifecycleStartDate = StringUtils.isEmpty(lifecycleStartDate) ? "00:00" : lifecycleStartDate;
        // 通知其他节点更新endStamp
        informOtherNodeUpdate(lifecycleStartDate, lifecycleEndDate);
    }

    private void informOtherNodeUpdate(String lifecycleStartDate, String lifecycleEndDate) {
        long endStamp = getLifecycleEndStamp(lifecycleStartDate, lifecycleEndDate);
        List<String> ipList = NodeCache.getCache().keySet().stream().sorted().map(NodeCache::getIP).collect(Collectors.toList());
        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(ipList.size());
        for (String ip : ipList) {
            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
            nodeList.add(tuple3);
        }
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("lifecycleEndStamp", endStamp + ""))
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_TIME, String.class, nodeList);
        Map<Boolean, AtomicInteger> map = new ConcurrentHashMap<>();
        map.computeIfAbsent(true, k -> new AtomicInteger());
        map.computeIfAbsent(false, k -> new AtomicInteger());
        responseInfo.responses
                .timeout(Duration.ofSeconds(30))
                .subscribe(s -> {
                    if (s.var2 == ErasureServer.PayloadMetaType.SUCCESS) {
                        map.get(true).incrementAndGet();
                    } else {
                        map.get(false).incrementAndGet();
                    }
                }, e -> {
                    map.get(false).incrementAndGet();
                    log.error("informOtherNodeUpdate error", e);
                }, () -> {
                    if (map.get(false).get() + map.get(true).get() == nodeList.size()) {
                        setStartStamp(getLifecycleStartStamp(lifecycleStartDate));
                        log.info("LifecycleCommandProducer starts, the date {} to {} ", lifecycleStartDate, lifecycleEndDate);
                        startDealExpirationObject();
                    }
                });
    }

    /**
     * 启动处理过期对象业务，获取桶-遍历桶-选择客户端处理
     */
    private void startDealExpirationObject() {
        CUR_VERSION_DAYS_MOVE_BUCKET_MAP.clear();
        CUR_VERSION_DATE_MOVE_BUCKET_MAP.clear();
        HISTORY_VERSION_MOVE_BUCKET_MAP.clear();
        CUR_VERSION_DATE_LAYERED_BUCKET_MAP.clear();
        CUR_VERSION_DAYS_LAYERED_BUCKET_MAP.clear();
        HISTORY_VERSION_LAYERED_BUCKET_MAP.clear();
        MOVE_BUCKET_LIST.clear();
        //获取redis表7中所有的keys(包括桶和桶权限)，桶的存储类型为hash
        List<String> bucketKeys = pool.getCommand(REDIS_BUCKETINFO_INDEX).keys("*");
        for (String bucketName : bucketKeys) {
            if (System.currentTimeMillis() >= getEndStamp()) {
                logger.info("execute lifecycle time end!!!");
                break;
            }
            String bucketKeyType = pool.getCommand(REDIS_BUCKETINFO_INDEX).type(bucketName);
            if (!"hash".equals(bucketKeyType)) {
                continue;
            }
            String localRegion = ServerConfig.getInstance().getRegion();
            Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
            String bucketRegion = bucketInfo.get(REGION_NAME);
            if (StringUtils.isNotEmpty(bucketRegion) && !localRegion.equals(bucketRegion)) {
                continue;
            }
            selectExecuteUuid(bucketInfo, bucketName);
        }
        filterTransitionObjectByStamps();
    }

    /**
     * 遍历 Rule，获取 Prefix和 Condition
     * 其中，Condition有四种类型：Expiration 和 NoncurrentVersionExpiration，Transition 和 NoncurrentVersionTransition
     * Expiration 是配置过期时间的Container
     * NoncurrentVersionExpiration 是配置历史版本过期时间的Container
     * Transition 是配置迁移时间的Container
     * NoncurrentVersionTransition 是配置历史版本迁移时间的Container
     */
    private void dealLifecycleRule(String bucket, String vnode, Map<String, String> bucketInfo) {
        String prefix;
        Map<String, String> tagMap = new HashMap<>();
        String filterType;
        try {
            LifecycleConfiguration lifecycleConfiguration = getLifecycleConfiguration(bucket);
            if (lifecycleConfiguration == null) {
                return;
            }
            //获取所有rule
            for (Rule rule : lifecycleConfiguration.getRules()) {
                //判断该条rule是否可用
                if (!Rule.ENABLED.equals(rule.getStatus())) {
                    continue;
                }
                //获取对象名前缀
                if (rule.getFilter() != null) {
                    prefix = rule.getFilter().getPrefix();
                } else {
                    prefix = rule.getPrefix();
                }

                if (StringUtils.isEmpty(prefix)) {//当Filter中的prefix不存在 互斥
                    if (null != rule.getFilter()) {
                        List<Tag> tags = rule.getFilter().getTags();//阿里云中的规则tag可以设置多个
                        if (tags != null) {
                            tagMap = getTagMap(tags);
                        }
                    }
                }

                for (Condition condition : rule.getConditionList()) {
                    if (condition instanceof Expiration) {
                        log.info("processExpiration,bucket:" + bucket + " Expiration:" + condition);
                        processExpiration(bucket, prefix, tagMap, condition, vnode, bucketInfo);
                    } else if (condition instanceof Transition) {
                        log.info("processTransition,bucket:" + bucket + " Transition:" + condition);
                        processTransition(bucket, prefix, tagMap, condition, vnode);
                    } else if (condition instanceof NoncurrentVersionExpiration) {
                        log.info("processNoncurrentVersionExpiration,bucket:" + bucket + " NoncurrentVersionExpiration:" + condition);
                        processNoncurrentVersionExpiration(bucket, prefix, tagMap, condition, vnode, bucketInfo);
                    } else if (condition instanceof NoncurrentVersionTransition) {
                        log.info("processNoncurrentVersionTransition,bucket:" + bucket + " NoncurrentVersionTransition:" + condition);
                        processNoncurrentVersionTransition(bucket, prefix, tagMap, condition, vnode);
                    } else {
                        log.error("Unsurported lifecycle operation." + condition.getClass().getName());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Filter Object failed ! ", e);
        }
    }

    private Map<String, String> getTagMap(List<Tag> tags) {
        Map<String, String> tagMap = new HashMap<>();
        for (Tag tag : tags) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }

    /**
     * 处理 Expiration，计算得到时间戳
     */
    private void processExpiration(String bucket, String prefix, Map<String, String> tagMap, Condition condition, String vnode, Map<String, String> bucketInfo) {
        // 主站点执行对象的生命周期同步给其他站点，从站点需要自己执行关于桶日志，桶清单信息的过期
        boolean iffFlag;
        if (StringUtils.isNotEmpty(syncPolicy) && !"0".equals(syncPolicy)) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, LIFE_EXPIRATION_FLAG, "1");
            iffFlag = false;
        } else {
            iffFlag = !DoubleActiveUtil.isBucketCluster(bucketInfo);
        }
        log.info("Let's process latest Version Expiration！！");
        Integer days = ((Expiration) condition).getDays();
        String date = ((Expiration) condition).getDate();

        if (date != null) {
            if (LifecycleUtils.dateActivated(date)) {
                long timestamps = LifecycleUtils.parseDateToTimestamps(date);
                if (!tagMap.isEmpty()) {
                    filterObjectByTag(bucket, prefix, JSONObject.toJSONString(tagMap), timestamps, "", false, null, "TAG", iffFlag);
                } else {
                    filterObjectByStamps(bucket, prefix, timestamps, "", false, null, iffFlag);
                }
            }
            return;
        }

        if (days != null) {
            long timestamps = LifecycleUtils.daysToDeadlineTimestamps(days);
            if (!tagMap.isEmpty()) {
                filterObjectByTag(bucket, prefix, JSONObject.toJSONString(tagMap), timestamps, "", false, days, "TAG", iffFlag);
            } else {
                filterObjectByStamps(bucket, prefix, timestamps, "", false, days, iffFlag);
            }
        }
    }

    private void selectExecuteUuid(Map<String, String> bucketInfo, String bucketName) {
        try {
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnode = storagePool.getBucketVnodeId(bucketName);
            String link = pool.getCommand(REDIS_MAPINFO_INDEX).hget(storagePool.getVnodePrefix() + bucketVnode, "link");
            String[] vnodes = {};
            String executeUuid = null;
            if (link != null) {
                vnodes = link.substring(1, link.length() - 1).split(",");
            }
            //选择客户端，首选桶的vnode所在节点，如果该节点不在线，则按顺序向后选择
            for (String vnode1 : vnodes) {
                String nodeUuid = pool.getCommand(REDIS_MAPINFO_INDEX).hget(storagePool.getVnodePrefix() + vnode1, VNODE_S_UUID);
                String state = pool.getCommand(REDIS_NODEINFO_INDEX).hget(nodeUuid, NODE_SERVER_STATE);
                if ("1".equals(state)) {
                    executeUuid = nodeUuid;
                    break;
                }
            }
            if (localUuid.equals(executeUuid)) {
                try {
                    //处理生命周期rule
                    dealLifecycleRule(bucketName, bucketVnode, bucketInfo);
                } catch (Exception e) {
                    log.error("The host uuid: {} get bucket info failed !", localUuid, e);
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    /**
     * 处理 Transition，计算得到时间戳
     */
    private void processTransition(String bucket, String prefix, Map<String, String> tagMap, Condition condition, String vnode) {
        log.info("Let's process latest Version Transition！！");
        Integer days = ((Transition) condition).getDays();
        String date = ((Transition) condition).getDate();
        String storageStrategy = ((Transition) condition).getStorageClass();
        Objects.requireNonNull(storageStrategy);
        String filterType;

        if (date != null) {
            if (LifecycleUtils.dateActivated(date)) {
                if (!tagMap.isEmpty()) {//只有tag不为空时,使用tagMap作为筛选条件
                    filterType = "TAG";
                    String tagStr = JSONObject.toJSONString(tagMap);
                    long timestamps = LifecycleUtils.parseDateToTimestamps(date);
                    MOVE_BUCKET_LIST.add(new String[]{bucket, tagStr, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(false), null, filterType});
                    CUR_VERSION_DATE_LAYERED_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                            .computeIfAbsent(tagStr, i -> new ConcurrentHashSet<>()).add(new Tuple2<>(String.valueOf(timestamps), storageStrategy));
                } else {
                    filterType = "PREFIX";
                    long timestamps = LifecycleUtils.parseDateToTimestamps(date);
                    MOVE_BUCKET_LIST.add(new String[]{bucket, prefix, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(false), null, filterType});
                    CUR_VERSION_DATE_MOVE_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                            .computeIfAbsent(prefix, i -> new ConcurrentHashSet<>()).add(new Tuple2<>(String.valueOf(timestamps), storageStrategy));
                }
            }
            return;
        }

        if (days != null) {
            long timestamps = LifecycleUtils.daysToDeadlineTimestamps(days);
            if (!tagMap.isEmpty()) {
                filterType = "TAG";
                String tagStr = JSONObject.toJSONString(tagMap);
                MOVE_BUCKET_LIST.add(new String[]{bucket, tagStr, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(false), days.toString(), filterType});
                CUR_VERSION_DAYS_LAYERED_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                        .computeIfAbsent(tagStr, i -> new ConcurrentHashSet<>()).add(new Tuple3<>(String.valueOf(timestamps), storageStrategy, days));
            } else {
                filterType = "PREFIX";
                MOVE_BUCKET_LIST.add(new String[]{bucket, prefix, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(false), days.toString(), filterType});
                CUR_VERSION_DAYS_MOVE_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                        .computeIfAbsent(prefix, i -> new ConcurrentHashSet<>()).add(new Tuple3<>(String.valueOf(timestamps), storageStrategy, days));
            }
        }
    }

    private void filterTransitionObjectByStamps() {
        MOVE_BUCKET_LIST.forEach(args -> {
            log.info("move bucket info: {}, {}, {}, {} , {}, {}, {}, {}", args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
            if ("PREFIX".equals(args[7])) {
                filterObjectByStamps(args[0], args[1], Long.valueOf(args[2]), args[4], Boolean.valueOf(args[5]),
                        StringUtils.isNotEmpty(args[6]) ? Integer.valueOf(args[6]) : null, false);
            } else if ("TAG".equals(args[7])) {
                log.info("Process1");
                filterObjectByTag(args[0], args[1], args[1], Long.valueOf(args[2]), args[4], Boolean.valueOf(args[5]),
                        StringUtils.isNotEmpty(args[6]) ? Integer.valueOf(args[6]) : null, args[7], false);
            }
        });
    }

    /**
     * 删除历史版本过期的对象，计算得到时间戳
     */
    private void processNoncurrentVersionExpiration(String bucket, String prefix, Map<String, String> tagMap, Condition condition, String vnode, Map<String, String> bucketInfo) {
        boolean iffFlag;
        if (StringUtils.isNotEmpty(syncPolicy) && !"0".equals(syncPolicy)) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, LIFE_EXPIRATION_FLAG, "1");
            iffFlag = false;
        } else {
            iffFlag = !DoubleActiveUtil.isBucketCluster(bucketInfo);
        }
        log.info("Let's process non current Version Expiration ！！");
        Integer days = ((NoncurrentVersionExpiration) condition).getNoncurrentDays();
        long timestamps = LifecycleUtils.daysToDeadlineTimestamps(days);
        boolean versionStatus = pool.getCommand(REDIS_BUCKETINFO_INDEX).hexists(bucket, BUCKET_VERSION_STATUS);
        if (!versionStatus) {
            log.info("multiple versions are not enabled. bucket: {}", bucket);
            return;
        }
        if (!tagMap.isEmpty()) {
            filterObjectByTag(bucket, prefix, JSONObject.toJSONString(tagMap), timestamps, "", true, days, "TAG", iffFlag);
        } else {
            filterObjectByStamps(bucket, prefix, timestamps, "", true, days, iffFlag);
        }
    }

    /**
     * 删除历史版本过期的对象，计算得到时间戳
     */
    private void processNoncurrentVersionTransition(String bucket, String prefix, Map<String, String> tagMap, Condition condition, String vnode) {
        log.info("Let's process non current Version Transition ！！");
        Integer days = ((NoncurrentVersionTransition) condition).getNoncurrentDays();
        String storageStrategy = ((NoncurrentVersionTransition) condition).getStorageClass();
        Objects.requireNonNull(storageStrategy);
        long timestamps = LifecycleUtils.daysToDeadlineTimestamps(days);
        boolean versionStatus = pool.getCommand(REDIS_BUCKETINFO_INDEX).hexists(bucket, BUCKET_VERSION_STATUS);
        if (!versionStatus) {
            log.info("multiple versions are not enabled. bucket: {}", bucket);
            return;
        }
        String filterType;
        if (!tagMap.isEmpty()) {
            filterType = "TAG";
            String tagStr = JSONObject.toJSONString(tagMap);
            MOVE_BUCKET_LIST.add(new String[]{bucket, tagStr, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(true), null, filterType});
            HISTORY_VERSION_LAYERED_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                    .computeIfAbsent(tagStr, i -> new ConcurrentHashSet<>()).add(new Tuple2<>(String.valueOf(timestamps), storageStrategy));
        } else {
            filterType = "PREFIX";
            MOVE_BUCKET_LIST.add(new String[]{bucket, prefix, String.valueOf(timestamps), vnode, storageStrategy, String.valueOf(true), null, filterType});
            HISTORY_VERSION_MOVE_BUCKET_MAP.computeIfAbsent(bucket, i -> new ConcurrentHashMap<>())
                    .computeIfAbsent(prefix, i -> new ConcurrentHashSet<>()).add(new Tuple2<>(String.valueOf(timestamps), storageStrategy));
        }
    }

    /**
     * 封装json信息，发送到消息队列
     */
    private void produceObject(String bucket, String object, String storageStrategy, String versionId, String status, String storage, String snapshotMark) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("bucket", bucket);
        jsonObject.put("object", object);
        jsonObject.put("targetStorageStrategy", storageStrategy);
        jsonObject.put("versionId", versionId);
        jsonObject.put("status", status);
        Optional.ofNullable(snapshotMark).ifPresent(v -> jsonObject.put("snapshotMark", v));
        if (StringUtils.isNotEmpty(storageStrategy)) {
            if (checkTransition(storageStrategy, storage)) {
                return;
            }
        }
        String ip = LOCAL_IP_ADDRESS;
        if (StringUtils.isNotEmpty(storageStrategy)) {
            String[] publishIps = LifecycleChannels.AVAIL_CHANNEL_NODE_SET.toArray(new String[0]);
            final int currentIndex = ThreadLocalRandom.current().nextInt(0, publishIps.length);
            ip = publishIps[currentIndex];
        }
        try {
            LifecycleChannels.publishCommand(jsonObject, ip);
        } catch (Exception e) {
            if (LOCAL_IP_ADDRESS.equals(ip)) {
                log.error("publish object error, bucket: {}, key: {}.", bucket, object, e);
                return;
            }
            LifecycleChannels.AVAIL_CHANNEL_NODE_SET.remove(ip);
            try {
                LifecycleChannels.publishCommand(jsonObject, LOCAL_IP_ADDRESS);
            } catch (Exception e1) {
                log.error("publish object error, bucket: {}, key: {}.", bucket, object, e1);
            }
        }
    }

    private void produceObject(String bucket, String object, String storageStrategy, String versionId, String status, String storage, String snapshotMark, long timestamps) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("bucket", bucket);
        jsonObject.put("object", object);
        jsonObject.put("targetStorageStrategy", storageStrategy);
        jsonObject.put("versionId", versionId);
        jsonObject.put("status", status);
        jsonObject.put("timestamps", timestamps);
        Optional.ofNullable(snapshotMark).ifPresent(v -> jsonObject.put("snapshotMark", v));
        if (StringUtils.isNotEmpty(storageStrategy)) {
            if (checkTransition(storageStrategy, storage)) {
                return;
            }
        }
        String ip = LOCAL_IP_ADDRESS;
        if (StringUtils.isNotEmpty(storageStrategy)) {
            String[] publishIps = LifecycleChannels.AVAIL_CHANNEL_NODE_SET.toArray(new String[0]);
            final int currentIndex = ThreadLocalRandom.current().nextInt(0, publishIps.length);
            ip = publishIps[currentIndex];
        }
        try {
            LifecycleChannels.publishCommand(jsonObject, ip);
        } catch (Exception e) {
            if (LOCAL_IP_ADDRESS.equals(ip)) {
                log.error("publish object error, bucket: {}, key: {}.", bucket, object, e);
                return;
            }
            LifecycleChannels.AVAIL_CHANNEL_NODE_SET.remove(ip);
            try {
                LifecycleChannels.publishCommand(jsonObject, LOCAL_IP_ADDRESS);
            } catch (Exception e1) {
                log.error("publish object error, bucket: {}, key: {}.", bucket, object, e1);
            }
        }
    }

    /**
     * 封装json信息，发送到消费者
     */
    private void produceObjectToMemoryConsumer(String bucket, String object, String storageStrategy, String versionId, String status, String storage, String snapshotMark) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bucket", bucket);
        jsonObject.put("object", object);
        jsonObject.put("targetStorageStrategy", storageStrategy);
        jsonObject.put("versionId", versionId);
        jsonObject.put("status", status);
        Optional.ofNullable(snapshotMark).ifPresent(v -> jsonObject.put("snapshotMark", v));
        if (StringUtils.isNotEmpty(storageStrategy)) {
            if (checkTransition(storageStrategy, storage)) {
                return;
            }
        }
        try {
            semaphore.acquire();
            Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(null, jsonObject.toJSONString().getBytes(StandardCharsets.UTF_8));
            LifecycleCommandConsumer.messageBroker.processor.onNext(tuple2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 封装json信息，发送到消费者
     */
    private void produceObjectToMemoryConsumer(String bucket, String object, String storageStrategy, String versionId, String status, String storage, String snapshotMark, long timestamps) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bucket", bucket);
        jsonObject.put("object", object);
        jsonObject.put("targetStorageStrategy", storageStrategy);
        jsonObject.put("versionId", versionId);
        jsonObject.put("status", status);
        jsonObject.put("timestamps", timestamps);
        Optional.ofNullable(snapshotMark).ifPresent(v -> jsonObject.put("snapshotMark", v));
        if (StringUtils.isNotEmpty(storageStrategy)) {
            if (checkTransition(storageStrategy, storage)) {
                return;
            }
        }
        try {
            semaphore.acquire();
            Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(null, jsonObject.toJSONString().getBytes(StandardCharsets.UTF_8));
            LifecycleCommandConsumer.messageBroker.processor.onNext(tuple2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void produceObject(MetaData metaData, String versionId, String storageStrategy, long timestamps, String status, boolean isHistoryVersion, Integer days, boolean allocateToMq) {
        String bucketName = metaData.bucket;
        String objectName = metaData.key;
        String storage = metaData.storage;
        if (StringUtils.isEmpty(storageStrategy)) {
            if (allocateToMq) {
                produceObject(bucketName, objectName, storageStrategy, versionId, status, metaData.getStorage(), metaData.snapshotMark, timestamps);
            } else {
                produceObjectToMemoryConsumer(bucketName, objectName, storageStrategy, versionId, status, metaData.getStorage(), metaData.snapshotMark, timestamps);
            }
            return;
        }
        if (metaData.deleteMarker) {
            return;
        }

        Tuple2<String, String> strategyTuple = new Tuple2<>(String.valueOf(timestamps), storageStrategy);
        if (isHistoryVersion) {
            Map<String, Set<Tuple2<String, String>>> prefixMap = HISTORY_VERSION_MOVE_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            for (Map.Entry<String, Set<Tuple2<String, String>>> entry : prefixMap.entrySet()) {
                if (StringUtils.isEmpty(entry.getKey()) || objectName.startsWith(entry.getKey())) {
                    for (Tuple2<String, String> tuple : entry.getValue()) {
                        if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                && strategyTuple.var1.compareTo(tuple.var1) > 0) {
                            return;
                        }
                    }
                }
            }
            Map<String, Set<Tuple2<String, String>>> tagMap = HISTORY_VERSION_LAYERED_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            for (Map.Entry<String, Set<Tuple2<String, String>>> entry : tagMap.entrySet()) {
                if (metaData.getJsonUserMetaData() != null && isContain(getUserMetaMap(metaData), Json.decodeValue(entry.getKey(), MAP_TYPE_REFERENCE))) {
                    //若当前扫描到的对象中包含规则Map中的tag，就进行比较
                    for (Tuple2<String, String> tuple2 : entry.getValue()) {
                        if (!tuple2.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple2.var1) < 0
                                && strategyTuple.var1.compareTo(tuple2.var1) > 0) {//迁移策略不同且已过期的对象，且当前正在执行的规则的日期更大，则跳过这次处理
                            //时间日期更大说明之前时间日期小的规则更早触发执行了，无需再重复处理
                            return;
                        }
                    }
                }
            }
        } else {
            Map<String, Set<Tuple3<String, String, Integer>>> prefixDaysMap = CUR_VERSION_DAYS_MOVE_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            Map<String, Set<Tuple2<String, String>>> prefixDateMap = CUR_VERSION_DATE_MOVE_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            Map<String, Set<Tuple2<String, String>>> tagDateMap = CUR_VERSION_DATE_LAYERED_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            Map<String, Set<Tuple3<String, String, Integer>>> tagDaysMap = CUR_VERSION_DAYS_LAYERED_BUCKET_MAP.getOrDefault(bucketName, new ConcurrentHashMap<>());
            if (days != null) {
                // 迁移的策略不一致，天数满足迁移时间条件，判断一下两条策略时间戳，大的就不执行
                for (Map.Entry<String, Set<Tuple3<String, String, Integer>>> entry : prefixDaysMap.entrySet()) {
                    if (StringUtils.isEmpty(entry.getKey()) || objectName.startsWith(entry.getKey())) {
                        for (Tuple3<String, String, Integer> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(tuple.var1) > 0) {
                                return;
                            }
                        }
                    }
                }
                for (Map.Entry<String, Set<Tuple3<String, String, Integer>>> entry : tagDaysMap.entrySet()) {
                    if (metaData.getJsonUserMetaData() != null && isContain(getUserMetaMap(metaData), Json.decodeValue(entry.getKey(), MAP_TYPE_REFERENCE))) {
                        for (Tuple3<String, String, Integer> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(tuple.var1) > 0) {
                                return;
                            }
                        }
                    }
                }

                // 迁移的策略不一致，日期和天数满足迁移时间条件，天数时间戳进行转换，判断一下两条策略时间戳，小的就不执行
                long stamp = (Long.parseLong(metaData.stamp) + days * ONE_DAY_TIMESTAMPS);
                for (Map.Entry<String, Set<Tuple2<String, String>>> entry : prefixDateMap.entrySet()) {
                    if (StringUtils.isEmpty(entry.getKey()) || objectName.startsWith(entry.getKey())) {
                        for (Tuple2<String, String> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && Long.toString(stamp).compareTo(tuple.var1) < 0) {
                                return;
                            }
                        }
                    }
                }
                for (Map.Entry<String, Set<Tuple2<String, String>>> entry : tagDateMap.entrySet()) {
                    if (metaData.getJsonUserMetaData() != null && isContain(getUserMetaMap(metaData), Json.decodeValue(entry.getKey(), MAP_TYPE_REFERENCE))) {
                        for (Tuple2<String, String> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && Long.toString(stamp).compareTo(tuple.var1) < 0) {
                                return;
                            }
                        }
                    }
                }

            } else {
                // 迁移的策略不一致，日期满足迁移时间条件，判断一下两条策略时间戳，小的或相同就不执行
                for (Map.Entry<String, Set<Tuple2<String, String>>> entry : prefixDateMap.entrySet()) {
                    if (StringUtils.isEmpty(entry.getKey()) || objectName.startsWith(entry.getKey())) {
                        for (Tuple2<String, String> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(tuple.var1) < 0) {
                                return;
                            }
                        }
                    }
                }
                for (Map.Entry<String, Set<Tuple2<String, String>>> entry : tagDateMap.entrySet()) {
                    if (metaData.getJsonUserMetaData() != null && isContain(getUserMetaMap(metaData), Json.decodeValue(entry.getKey(), MAP_TYPE_REFERENCE))) {
                        for (Tuple2<String, String> tuple : entry.getValue()) {
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(tuple.var1) < 0) {
                                return;
                            }
                        }
                    }
                }

                // 迁移的策略不一致，日期满足迁移时间条件，将其他策略天数时间戳进行转换，判断一下两条策略时间戳，小的或相同就不执行
                for (Map.Entry<String, Set<Tuple3<String, String, Integer>>> entry : prefixDaysMap.entrySet()) {
                    if (StringUtils.isEmpty(entry.getKey()) || objectName.startsWith(entry.getKey())) {
                        for (Tuple3<String, String, Integer> tuple : entry.getValue()) {
                            long stamp = (Long.parseLong(metaData.stamp) + tuple.var3 * ONE_DAY_TIMESTAMPS);
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(Long.toString(stamp)) < 0) {
                                return;
                            }
                        }
                    }
                }
                for (Map.Entry<String, Set<Tuple3<String, String, Integer>>> entry : tagDaysMap.entrySet()) {
                    if (metaData.getJsonUserMetaData() != null && isContain(getUserMetaMap(metaData), Json.decodeValue(entry.getKey(), MAP_TYPE_REFERENCE))) {
                        for (Tuple3<String, String, Integer> tuple : entry.getValue()) {
                            long stamp = (Long.parseLong(metaData.stamp) + tuple.var3 * ONE_DAY_TIMESTAMPS);
                            if (!tuple.var2.equals(strategyTuple.var2) && metaData.stamp.compareTo(tuple.var1) < 0
                                    && strategyTuple.var1.compareTo(Long.toString(stamp)) < 0) {
                                return;
                            }
                        }
                    }
                }
            }
        }
        if (allocateToMq) {
            produceObject(bucketName, objectName, storageStrategy, versionId, status, metaData.getStorage(), metaData.snapshotMark);
        } else {
            produceObjectToMemoryConsumer(bucketName, objectName, storageStrategy, versionId, status, metaData.getStorage(), metaData.snapshotMark);
        }
    }

    /**
     * 检测需要迁移的存储策略是否是当前的存储策略
     */
    private boolean checkTransition(String storageStrategy, String storage) {
        StorageStrategy storageStrategy1 = POOL_STRATEGY_MAP.get(storageStrategy);
        String dataStr = storageStrategy1.redisMap.get("data");

        if (StringUtils.isNotEmpty(dataStr)) {
            String[] data = Json.decodeValue(dataStr, String[].class);
            for (String str : data) {
                if (storage.equals(str)) {
                    return true;
                }
            }
        }
        return false;
    }

    private LifecycleConfiguration getLifecycleConfiguration(String bucket) {

        String lifecycleXml = pool.getCommand(SysConstants.REDIS_SYSINFO_INDEX).hget("bucket_lifecycle_rules", bucket);

        if (lifecycleXml == null) {
            return null;
        }
        return (LifecycleConfiguration) JaxbUtils.toObject(lifecycleXml);
    }

    /**
     * 此方法用于根据tag匹配获取对象
     *
     * @param bucketName
     * @param prefix
     * @param tagStr
     * @param timestamps
     * @param storageStrategy
     * @param isHistoryVersion
     * @param days
     */
    private void filterObjectByTag(String bucketName, String prefix, String tagStr, long timestamps, String
            storageStrategy, boolean isHistoryVersion, Integer days, String filterType, boolean iffFlag) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        int startIndex = 0;
        Map<String, String> tagMap = JSONObject.parseObject(tagStr, Map.class);
        AtomicInteger nextRunNumber = new AtomicInteger(startIndex);//tags条件可以直接从头开始遍历
        AtomicInteger completeNums = new AtomicInteger(bucketVnodeList.size());
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        boolean isMove = StringUtils.isNotEmpty(storageStrategy);
        String key = "_" + tagStr + "_" + timestamps;
        List<String> recordList = new LinkedList<>();
        if (!isMove) {
            String serialize = storagePool.getBucketShardCache().get(bucketName).serialize();
            pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hsetnx(SERIALIZE_RECORD, bucketName + key, serialize);
        }
        shardProcessor.subscribe(vnode -> {
            AtomicInteger findNum = new AtomicInteger();
            int maxKey = 1000;
            String stampMarker = timestamps + 1 + "";
            String status = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS))
                    .orElse("NULL");
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            String beginPrefix = getLifeCycleStamp(vnode, bucketName, "0");
            String recordKey = getLifecycleRecord(bucketName, vnode, filterType, tagMap, prefix, isMove, isHistoryVersion, timestamps, days);
            recordList.add(recordKey);
            String lifecycleRecord = pool.getCommand(REDIS_TASKINFO_INDEX).hget(bucketName + LIFECYCLE_RECORD, recordKey);
            AtomicBoolean restartSign = new AtomicBoolean(StringUtils.isNotEmpty(lifecycleRecord));
            if (restartSign.get()) {
                beginPrefix = lifecycleRecord;
            }
            UnicastProcessor<SocketReqMsg> listController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            listController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxKeys", String.valueOf(maxKey))
                    .put("prefix", prefix)
                    .put("stamp", stampMarker)
                    .put("isHistoryVersion", isHistoryVersion ? "0" : "1")
                    .put("retryTimes", "0")
                    .put("beginPrefix", beginPrefix));
            log.info("-------------------------- begin get Object by Tag -----------{}---------{}---------", beginPrefix, iffFlag);

            listController.subscribe(msg -> {
                if ("error".equals(msg.getMsgType())) {
                    throw new MsException(UNKNOWN_ERROR, "");
                }
                storagePool.mapToNodeInfo(vnode)
                        .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            msg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());

                            ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                    String, MetaData>[]>() {
                            }, infoList);
                            LifecycleClientHandler lifecycleClientHandler = new LifecycleClientHandler(storagePool, listController, responseInfo, nodeArr[0], reqMsg);
                            responseInfo.responses.publishOn(LifecycleService.getScheduler()).subscribe(lifecycleClientHandler::handleResponse, e -> log.error("", e),
                                    () -> lifecycleClientHandler.completeResponse(bucketName, recordKey, restartSign, lifecycleRecord));
                            lifecycleClientHandler.res.subscribe(metaDataList -> {
                                if (metaDataList.size() > 0) {
                                    int num = findNum.addAndGet(metaDataList.size());
                                    if (num > RECORD_NUM && metaDataList.size() > 1000) {
                                        log.debug("put lifecycle record: {}", metaDataList.get(0).getMetaData().getKey());
                                        putLifecycleRecord(bucketName, vnode, recordKey, metaDataList.get(0).getMetaData(), pool);
                                        findNum.set(0);
                                    }
                                }
                                if (metaDataList.size() > 1000) {
                                    MetaData metaData = metaDataList.get(1000).getMetaData();
                                    String curScanPrefix = getLifeCycleMetaKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp);
                                    if (StringUtils.isNotEmpty(lifecycleRecord) && !restartSign.get() && curScanPrefix.compareTo(lifecycleRecord) >= 0) {
                                        reScan(bucketName, key, completeNums, shardProcessor, recordList);
                                    }
                                } else {
                                    if (!restartSign.get()) {
                                        reScan(bucketName, key, completeNums, shardProcessor, recordList);
                                    }
                                }
                                AtomicInteger metaDataNum = new AtomicInteger(Math.min(maxKey, metaDataList.size()));
                                for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                    MetaData metaData = metaDataList.get(i).getMetaData();
                                    String versionId = metaData.getVersionId();
                                    if (metaData.isDiscard()) {
                                        metaDataNum.decrementAndGet();
                                        continue;
                                    }
                                    if (metaData.deleteMark) {
                                        metaDataNum.decrementAndGet();
                                        continue;
                                    }
                                    if (iffFlag) {
                                        if (StringUtils.isNotBlank(metaData.getSysMetaData())) {
                                            Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                            });
                                            if (!sysMap.containsKey(NO_SYNCHRONIZATION_KEY) || !sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE)) {
                                                continue;
                                            }
                                        } else {
                                            continue;
                                        }
                                    }
                                    Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
                                    MetaData metaData1 = storagePool.mapToNodeInfo(bucketVnodeIdTuple.var1)
                                            .flatMap(nodeList -> ErasureClient.getLifecycleMetaVersion(metaData.bucket, metaData.key, versionId, nodeList, null, metaData.snapshotMark))
                                            .timeout(Duration.ofSeconds(30)).block();

                                    //判断自定义元数据不包含tags的对象不进行处理
                                    if (metaData.deleteMark || metaData1.getJsonUserMetaData() == null || (metaData1.getJsonUserMetaData() != null && !isContain(getUserMetaMap(metaData1), tagMap)) || metaData1.isUnView(msg.get("currentSnapshotMark"))) {
                                        metaDataNum.decrementAndGet();
                                        continue;//未设置用户元数据的对象不参与分层处理
                                    }
                                    //未开启多版本
                                    if ("NULL".equals(status)) {
                                        continue;
                                    } else if (!isHistoryVersion) {
                                        //获取最新的
                                        if (!metaData.latest) {
                                            metaDataNum.decrementAndGet();
                                        }
                                    } else {
                                        if (metaData.latest) {
                                            metaDataNum.decrementAndGet();
                                        }
                                    }
                                }
                                Tuple2<Integer, Integer> tuple2 = allocateMetaData(metaDataNum.get());
                                AtomicInteger completeNum = new AtomicInteger(0);
                                for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                    MetaData metaData = metaDataList.get(i).getMetaData();
                                    String versionId = metaData.getVersionId();
                                    if (metaData.isDiscard()) {
                                        LifecycleChannels.getOverwriteHandler().handle(vnode, metaData);
                                        continue;
                                    }
                                    if (metaData.deleteMark) {
                                        continue;
                                    }
                                    Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(metaData.bucket, metaData.key);
                                    MetaData metaData1 = storagePool.mapToNodeInfo(bucketVnodeIdTuple.var1)
                                            .flatMap(nodeList -> ErasureClient.getLifecycleMetaVersion(metaData.bucket, metaData.key, versionId, nodeList, null, metaData.snapshotMark))
                                            .timeout(Duration.ofSeconds(30)).block();

                                    //判断自定义元数据不包含tags的对象不进行处理
                                    log.debug("userMetadata:{} of object:{}", metaData1.getJsonUserMetaData(), metaData1.getKey());
                                    log.debug("tagMap: {}", tagMap);
                                    if (metaData.deleteMark || metaData1.getJsonUserMetaData() == null || (metaData1.getJsonUserMetaData() != null && !isContain(getUserMetaMap(metaData1), tagMap)) || metaData1.isUnView(msg.get("currentSnapshotMark"))) {
                                        continue;//未设置用户元数据的对象不参与分层处理
                                    }
                                    log.debug("produceObject:{}", metaData1.key);
                                    //未开启多版本
                                    if ("NULL".equals(status)) {
                                        produceObject(metaData1, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNum.getAndIncrement() < tuple2.var1);
                                    } else if (!isHistoryVersion) {
                                        //获取最新的
                                        if (!metaData.latest) {
                                            continue;
                                        }
                                        if (!metaData.deleteMarker) {
                                            //插入删除标记
                                            produceObject(metaData1, null, storageStrategy, timestamps, status, isHistoryVersion, days, completeNum.getAndIncrement() < tuple2.var1);
                                        } else {
                                            produceObject(metaData1, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNum.getAndIncrement() < tuple2.var1);
                                        }
                                    } else {
                                        if (metaData.latest) {
                                            continue;
                                        }
                                        produceObject(metaData1, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNum.getAndIncrement() < tuple2.var1);
                                    }
                                }
                            });
                        });
            }, log::error);
        }, log::error);
        for (int i = startIndex; i < bucketVnodeList.size(); ++i) {
            int next = nextRunNumber.getAndIncrement();
            log.info("bucket vnode index:{}", next);
            shardProcessor.onNext(bucketVnodeList.get(next));
        }
    }

    private Tuple2<Integer, Integer> allocateMetaData(int metaDataNum) {
        Tuple2<Integer, Integer> allocateTuple;
        int usedRate = Integer.parseInt(MQ_DISK_USED_RATE);
        //磁盘使用率小于阈值时，全部放MQ
        if (usedRate < LOW_OCCUPANCY) {
            allocateTuple = new Tuple2<>(metaDataNum, 0);
        } else if (usedRate < MEDIUM_OCCUPANCY) {
            int v = (int) (metaDataNum * 0.25);
            allocateTuple = new Tuple2<>(metaDataNum - v, v);
        } else if (usedRate < HIGH_OCCUPANCY) {
            int v = (int) (metaDataNum * 0.5);
            allocateTuple = new Tuple2<>(metaDataNum - v, v);
        } else if (usedRate < FULL_OCCUPANCY) {
            int v = (int) (metaDataNum * 0.75);
            allocateTuple = new Tuple2<>(metaDataNum - v, v);
        } else {
            allocateTuple = new Tuple2<>(0, metaDataNum);
        }
        return allocateTuple;
    }

    private Map<String, String> getUserMetaMap(MetaData metaData) {
        //map是直接从对象元数据中获取到的，用户元数据的key的前缀都是x-amz-meta，需要去除后再比较
        Map<String, String> resMap = new HashMap<>();
        Map<String, String> map = Json.decodeValue(metaData.userMetaData, MAP_TYPE_REFERENCE);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String k = entry.getKey().toLowerCase();
            if (k.startsWith(USER_META)) {
                String key = entry.getKey().substring(USER_META.length());
                resMap.put(key, entry.getValue());
            }
        }
        return resMap;
    }

    private boolean isContain(Map<String, String> map1, Map<String, String> map2) {
//        log.info(map1);
//        log.info(map2);
        //先判断map1中keyset是否完全包含map2中的key
        Set<String> map1Keys = map1.keySet();
        Set<String> map2Keys = map2.keySet();
        if (map1Keys.containsAll(map2Keys)) {//当map1的keyset包含map2的时，继续判断；不包含则直接返回false
            for (Map.Entry<String, String> entry1 : map1.entrySet()) {
                for (Map.Entry<String, String> entry2 : map2.entrySet()) {
                    if (entry1.getKey().equals(entry2.getKey())) {//key相同时比较映射的value
                        boolean res = entry1.getValue().equals(entry2.getValue());
                        if (!res) {//相同key对应的value如果不同则返回false
                            return false;
                        }
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private void filterObjectByStamps(String bucketName, String prefix, long timestamps, String storageStrategy,
                                      boolean isHistoryVersion, Integer days, boolean iffFlag) {
        String status = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS))
                .orElse("NULL");
        String scanMode = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget("lifecycle_configuration", "scan_mode")).orElse("time");
        if (StringUtils.isNotEmpty(prefix) && scanMode.equals("prefix")) {
            filterObjectByPrefix(bucketName, prefix, timestamps, storageStrategy, isHistoryVersion, status, iffFlag);
            return;
        }
        String filterType = "PREFIX";
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String startVnodeId = storagePool.getBucketVnodeId(bucketName, prefix);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        int startIndex = bucketVnodeList.indexOf(startVnodeId);
        AtomicInteger nextRunNumber = new AtomicInteger(startIndex);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        boolean isMove = StringUtils.isNotEmpty(storageStrategy);
        shardProcessor.subscribe(vnode -> {
            AtomicInteger findNum = new AtomicInteger();
            int maxKey = 1000;
            String stampMarker = timestamps + 1 + "";
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            String beginPrefix = getLifeCycleStamp(vnode, bucketName, "0");
            String recordKey = getLifecycleRecord(bucketName, vnode, filterType, null, prefix, isMove, isHistoryVersion, timestamps, days);
            String lifecycleRecord = pool.getCommand(REDIS_TASKINFO_INDEX).hget(bucketName + LIFECYCLE_RECORD, recordKey);
            AtomicBoolean restartSign = new AtomicBoolean(StringUtils.isNotEmpty(lifecycleRecord));
            if (restartSign.get()) {
                beginPrefix = lifecycleRecord;
            }
            UnicastProcessor<SocketReqMsg> listController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            listController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxKeys", String.valueOf(maxKey))
                    .put("prefix", prefix)
                    .put("stamp", stampMarker)
                    .put("isHistoryVersion", isHistoryVersion ? "0" : "1")
                    .put("retryTimes", "0")
                    .put("beginPrefix", beginPrefix));
            log.info("-------------------------- begin get Object  -----------{}---------{}---------", beginPrefix, iffFlag);

            listController.subscribe(msg -> {
                if ("error".equals(msg.getMsgType())) {
                    throw new MsException(UNKNOWN_ERROR, "");
                }
                storagePool.mapToNodeInfo(vnode)
                        .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            msg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());

                            ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                    String, MetaData>[]>() {
                            }, infoList);
                            LifecycleClientHandler lifecycleClientHandler = new LifecycleClientHandler(storagePool, listController, responseInfo, nodeArr[0], reqMsg);
                            responseInfo.responses.publishOn(LifecycleService.getScheduler()).subscribe(lifecycleClientHandler::handleResponse, e -> log.error("", e),
                                    () -> lifecycleClientHandler.completeResponse(bucketName, recordKey, restartSign, lifecycleRecord));
                            lifecycleClientHandler.res.subscribe(metaDataList -> {
                                if (metaDataList.size() > 0) {
                                    int num = findNum.addAndGet(metaDataList.size());
                                    if (num > RECORD_NUM && metaDataList.size() > 1000) {
                                        log.debug("put lifecycle record: {}", metaDataList.get(0).getMetaData().getKey());
                                        putLifecycleRecord(bucketName, vnode, recordKey, metaDataList.get(0).getMetaData(), pool);
                                        findNum.set(0);
                                    }
                                }
                                AtomicInteger metaDataNum = new AtomicInteger(Math.min(maxKey, metaDataList.size()));
                                for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                    MetaData metaData = metaDataList.get(i).getMetaData();
                                    String versionId = metaData.getVersionId();
                                    if (metaData.isDiscard()) {
                                        metaDataNum.decrementAndGet();
                                        continue;
                                    }
                                    if (metaData.deleteMark || (StringUtils.isNotEmpty(prefix) && !metaData.key.startsWith(prefix)) || metaData.isUnView(msg.get("currentSnapshotMark"))) {
                                        metaDataNum.decrementAndGet();
                                        continue;
                                    }
                                    if (iffFlag) {
                                        if (StringUtils.isNotBlank(metaData.getSysMetaData())) {
                                            Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                            });
                                            if (!sysMap.containsKey(NO_SYNCHRONIZATION_KEY) || !sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE)) {
                                                metaDataNum.decrementAndGet();
                                                continue;
                                            }
                                        } else {
                                            metaDataNum.decrementAndGet();
                                            continue;
                                        }
                                    }
                                    // 生命周期生产订阅者
                                    //未开启多版本
                                    if ("NULL".equals(status)) {
                                    } else if (!isHistoryVersion) {
                                        //获取最新的
                                        if (!metaData.latest) {
                                            metaDataNum.decrementAndGet();
                                        }
                                    } else {
                                        if (metaData.latest) {
                                            metaDataNum.decrementAndGet();
                                        }
                                    }
                                }
                                Tuple2<Integer, Integer> tuple2 = allocateMetaData(metaDataNum.get());
                                AtomicInteger completeNums = new AtomicInteger(0);
                                for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                    MetaData metaData = metaDataList.get(i).getMetaData();
                                    String versionId = metaData.getVersionId();
                                    if (metaData.isDiscard()) {
                                        LifecycleChannels.getOverwriteHandler().handle(vnode, metaData);
                                        continue;
                                    }
                                    if (metaData.deleteMark || (StringUtils.isNotEmpty(prefix) && !metaData.key.startsWith(prefix)) || metaData.isUnView(msg.get("currentSnapshotMark"))) {
                                        continue;
                                    }
                                    if (iffFlag) {
                                        if (StringUtils.isNotBlank(metaData.getSysMetaData())) {
                                            Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                            });
                                            if (!sysMap.containsKey(NO_SYNCHRONIZATION_KEY) || !sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE)) {
                                                continue;
                                            }
                                        } else {
                                            continue;
                                        }
                                    }
                                    //未开启多版本
                                    if ("NULL".equals(status)) {
                                        produceObject(metaData, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNums.getAndIncrement() < tuple2.var1);
                                    } else if (!isHistoryVersion) {
                                        //获取最新的
                                        if (!metaData.latest) {
                                            continue;
                                        }
                                        if (!metaData.deleteMarker) {
                                            //插入删除标记
                                            produceObject(metaData, null, storageStrategy, timestamps, status, isHistoryVersion, days, completeNums.getAndIncrement() < tuple2.var1);
                                        } else {
                                            produceObject(metaData, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNums.getAndIncrement() < tuple2.var1);
                                        }
                                    } else {
                                        if (metaData.latest) {
                                            continue;
                                        }
                                        produceObject(metaData, versionId, storageStrategy, timestamps, status, isHistoryVersion, days, completeNums.getAndIncrement() < tuple2.var1);
                                    }
                                }
                            });
                        });
            }, log::error);
        }, log::error);
        for (int i = startIndex; i < bucketVnodeList.size(); ++i) {
            int next = nextRunNumber.getAndIncrement();
            shardProcessor.onNext(bucketVnodeList.get(next));
        }
    }

    private void reScan(String bucketName, String key, AtomicInteger completeNums, UnicastProcessor<String> shardProcessor, List<String> recordList) {
        String beginSerialize = "";
        String curSerialize = "";
        boolean exists = pool.getCommand(REDIS_TASKINFO_INDEX).hexists(SERIALIZE_RECORD, bucketName + key);
        boolean isSharding = false;
        if (completeNums != null && exists && completeNums.decrementAndGet() == 0) {
            curSerialize = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache().get(bucketName).serialize();
            log.info("------------ cur serialize info  -----------{}", curSerialize);
            beginSerialize = pool.getCommand(REDIS_TASKINFO_INDEX).hget(SERIALIZE_RECORD, bucketName + key);
            log.info("------------ begin serialize info  -----------{}", beginSerialize);
            isSharding = ShardingWorker.contains(bucketName) || bucketHasShardingTask(bucketName);
            if (!beginSerialize.equals(curSerialize) || isSharding) {
                List<String> bucketVnodeList1 = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketVnodeList(bucketName);
                completeNums.set(bucketVnodeList1.size());
                for (String recordKey : recordList) {
                    pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(bucketName + LIFECYCLE_RECORD, recordKey);
                }
                pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(SERIALIZE_RECORD, bucketName + key, curSerialize);
                recordList.clear();
                for (String vnode : bucketVnodeList1) {
                    shardProcessor.onNext(vnode);
                    log.info("bucket: {}, vnode: {}, rescan list object !!!", bucketName, vnode);
                }
            }
        }
        if (completeNums != null && exists && completeNums.get() == 0 && beginSerialize.equals(curSerialize) && !isSharding) {
            pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(SERIALIZE_RECORD, bucketName + key);
        }
    }

    private void filterObjectByPrefix(String bucketName, String prefix, long timestamps, String storageStrategy, boolean isHistoryVersion, String versionStatus, boolean iffFlag) {
        log.info("filterObjectByPrefix bucketName:{}, prefix:{}, timestamps:{}, storageStrategy:{}, isHistoryVersion:{}, versionStatus:{}, iffFlag:{}", bucketName, prefix, timestamps, storageStrategy, isHistoryVersion, versionStatus, iffFlag);
        DeadLinePolicy<MetaData> deadLinePolicy = new DeadLinePolicy<>(getEndStamp());
        ListOperationContext<MetaData> context = new ListObjectsContext(bucketName, prefix, deadLinePolicy, new ListOptions());
        ListOperation<MetaData> operation;
        if (!isHistoryVersion) {
            operation = DefaultListOperationFactory.create(ListType.LSFILE, context);
        } else {
            operation = DefaultListOperationFactory.create(ListType.LSVERSIONS, context);
        }
        AtomicLong adder = new AtomicLong();
        operation.apply("")
                .publishOn(LifecycleService.getScheduler())
                .filter(metaData -> !metaData.deleteMark && metaData.key.startsWith(prefix) && metaData.stamp.compareTo(String.valueOf(timestamps)) < 0)
                .filter(metaData -> {
                    if (iffFlag) {
                        if (StringUtils.isNotBlank(metaData.getSysMetaData())) {
                            Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                            });
                            return sysMap.containsKey(NO_SYNCHRONIZATION_KEY) && sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE);
                        } else {
                            return false;
                        }
                    }
                    return true;
                })
                .doOnComplete(() -> log.info("list objects complete"))
                .subscribe(metaData -> {
                    boolean produceToMq = false;
                    try {
                        int used = Integer.parseInt(MQ_DISK_USED_RATE);
                        if (used < MEDIUM_OCCUPANCY) {
                            produceToMq = true;
                        } else if (used < HIGH_OCCUPANCY) {
                            produceToMq = adder.incrementAndGet() % 2 == 0;
                        } else if (used < FULL_OCCUPANCY) {
                            produceToMq = adder.incrementAndGet() % 5 == 0;
                        } else {
                            produceToMq = false;
                        }
                    } catch (NumberFormatException ignored) {}
                    //未开启多版本
                    String versionId = metaData.getVersionId();
                    if ("NULL".equals(versionStatus)) {
                        produceObject(metaData, versionId, storageStrategy, timestamps, versionStatus, isHistoryVersion, 0, produceToMq);
                    } else if (!isHistoryVersion) {
                        //获取最新的
                        if (!metaData.latest) {
                            return;
                        }
                        if (!metaData.deleteMarker) {
                            //插入删除标记
                            produceObject(metaData, null, storageStrategy, timestamps, versionStatus, false, 0, produceToMq);
                        } else {
                            produceObject(metaData, versionId, storageStrategy, timestamps, versionStatus, false, 0, produceToMq);
                        }
                    } else {
                        if (metaData.latest) {
                            return;
                        }
                        produceObject(metaData, versionId, storageStrategy, timestamps, versionStatus, true, 0, produceToMq);
                    }
                });
    }
}