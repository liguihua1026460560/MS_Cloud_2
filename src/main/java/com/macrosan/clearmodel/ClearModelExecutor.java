package com.macrosan.clearmodel;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.lifecycle.LifecycleCommandConsumer;
import com.macrosan.lifecycle.mq.LifecycleChannels;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.clear.ClearConfigration;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.clearmodel.ClearModelUtils.calculateResult;
import static com.macrosan.clearmodel.ClearModelUtils.isCurrentTimeInRange;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.BucketInfo.NOT_FOUND_BUCKET_INFO;
import static com.macrosan.utils.lifecycle.LifecycleUtils.*;


@Log4j2
public class ClearModelExecutor implements Job {

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    private static final ExecutorService COMMAND_PRODUCER_EXECUTOR = new ThreadPoolExecutor(PROC_NUM * 2, Integer.MAX_VALUE,
            15L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(),
            new DefaultThreadFactory("clear_model_thread"));

    private static reactor.core.scheduler.Scheduler scheduler = Schedulers.fromExecutor(COMMAND_PRODUCER_EXECUTOR);

    public static final String MODEL_DEFAULT = "DEFAULT";
    public static final String DELETE_MARK = "cleaning_mark";

    public static boolean scanKey = false;
    public static boolean isMultiCluster = false;

    public static final String NODE_NAME = ServerConfig.getInstance().getHostUuid();
    public static final String LOCK_KEY = NODE_NAME + "executing";

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        try {
            String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
            isMultiCluster = StringUtils.isNotBlank(masterCluster);
            if (scanKey) {
                log.debug("scanKey is true,not scan again");
                return;
            }
            scanKey = true;
            Map<String, String> nodesState = getNodesState();
            RedisCommands<String, String> command = pool.getCommand(REDIS_BUCKETINFO_INDEX);
            ScanArgs scanArgs = new ScanArgs();
            scanArgs.limit(10);
            ScanIterator<String> scan = ScanIterator.scan(command, scanArgs);
            while (scan.hasNext()) {
                String bucketName = scan.next();
                if (NODE_NAME.equals(getExecuteNode(bucketName,getNodesState()))) {
                    String originalNode = String.format("%04d", Math.abs(bucketName.hashCode() % nodesState.size()) + 1);
                    if (!NODE_NAME.equals(originalNode)){
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(originalNode + "executing");
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName,DELETE_MARK);
                    }
                    boolean flag = true;
                    while (flag){
                        Long exists = pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).exists(LOCK_KEY);
                        if (exists == 1){
                            TimeUnit.MINUTES.sleep(1);
                        }else {
                            flag = false;
                        }
                    }
                    startDealExpirationObject(bucketName);
                }
            }
            scanKey = false;
        } catch (Exception e) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
            throw new RuntimeException(e);
        }
    }

    public String getExecuteNode(String bucketName, Map<String, String> nodeState) {
        int index = Math.abs(bucketName.hashCode() % nodeState.size()) + 1;
        String executeNode = String.format("%04d", index);
        String state = nodeState.get(executeNode);
        if (state.equals("1") ){
            return executeNode;
        }else {
            for (int i = 0; i < nodeState.size(); i++) {
                index = index % nodeState.size() + 1;
                if ("1".equals(nodeState.get(String.format("%04d", index)))){
                    return String.format("%04d", index);
                }
            }
        }
        return executeNode;
    }

    private Map<String, String> getNodesState(){
        Map<String, String> nodesState = new HashMap<>();
        AtomicInteger maxNodeIndex = new AtomicInteger();
        pool.getCommand(REDIS_NODEINFO_INDEX).keys("*").forEach(key -> {
            int index = Integer.parseInt(key);
            if (index > maxNodeIndex.get()){
                maxNodeIndex.set(index);
            }
        });
        for (int index = 1; index <= maxNodeIndex.get(); index++) {
            String nodeName = String.format("%04d", index);
            if (pool.getCommand(REDIS_NODEINFO_INDEX).exists(nodeName) != 0){
                nodesState.put(nodeName, pool.getCommand(REDIS_NODEINFO_INDEX).hget(nodeName, "server_state"));
            }else {
                nodesState.put(nodeName, "0");
            }
        }
        return nodesState;
    }

    private void startDealExpirationObject(String bucketName) {
        //获取redis表7中所有的keys(包括桶和桶权限)，桶的存储类型为hash
        String bucketKeyType = pool.getCommand(REDIS_BUCKETINFO_INDEX).type(bucketName);
        if (!"hash".equals(bucketKeyType)) {
            return;
        }
        String localRegion = ServerConfig.getInstance().getRegion();
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        String bucketRegion = bucketInfo.get(REGION_NAME);
        if (StringUtils.isNotEmpty(bucketRegion) && !localRegion.equals(bucketRegion)) {
            return;
        }
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)){
            return;
        }
        String deteleMark = bucketInfo.get(DELETE_MARK);
        if (StringUtils.isNotEmpty(deteleMark)) {
            return;
        }
        boolean Trigger = getBucketTrigger(bucketName);
        if (Trigger){
            log.info("start clear " + bucketName);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).setnx(LOCK_KEY, "1");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, DELETE_MARK, NODE_NAME);
            executeClear(bucketInfo, bucketName);
        }
    }

    private void executeClear(Map<String, String> bucketInfo, String bucketName) {
        try {
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnode = storagePool.getBucketVnodeId(bucketName);
            try {
                dealCleanRule(bucketName, bucketVnode, bucketInfo);
            } catch (Exception e) {
                log.error("dealCleanRule error", e);
            }
        } catch (Exception e) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
            log.error("executeClear error",e);
        }
    }

    private void dealCleanRule(String bucket, String vnode, Map<String, String> bucketInfo) {
        String prefix = "";
        try {
            String xmlConfig = pool.getCommand(REDIS_SYSINFO_INDEX).get(bucket + "_clear_model");
            ClearConfigration cleanConfigration = (ClearConfigration) JaxbUtils.toObject(xmlConfig);
            String model = cleanConfigration.getModel();
            //默认清理模式
            if (MODEL_DEFAULT.equals(model)){
                boolean iffFlag = false;
                long timestamps = System.currentTimeMillis();
                filterObjectByDefault(bucket, prefix, timestamps, "", false, null, iffFlag);
            }
        } catch (Exception e) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucket, DELETE_MARK);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
            log.error("the clear model accured exception when list object!", e);
        }
    }

    private boolean getBucketTrigger(String bucketName) {
        String configXml = pool.getCommand(REDIS_SYSINFO_INDEX).get(bucketName + "_clear_model");
        if (StringUtils.isEmpty(configXml)){
            return false;
        }
        String objNumFlag = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "objnum_flag"))
                .orElse("0");
        String quotaFlag = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "quota_flag"))
                .orElse("0");
        if (objNumFlag.equals("0") && quotaFlag.equals("0")){
            return false;
        }
        ClearConfigration clearConfigration = (ClearConfigration) JaxbUtils.toObject(configXml);
        if (StringUtils.isNotEmpty(clearConfigration.getStartTime()) && StringUtils.isNotEmpty(clearConfigration.getEndTime())){
            boolean currentTimeInRange = isCurrentTimeInRange(clearConfigration.getStartTime(), clearConfigration.getEndTime());
            if (!currentTimeInRange){
                return false;
            }
        }
        boolean objNumAttainQuota = objNumFlag.equals("1") || objNumFlag.equals("5");
        boolean capacityAttainQuota = quotaFlag.equals("1") || quotaFlag.equals("5");
        BucketInfo bucketInfo = ErasureClient.reduceBucketInfo(bucketName).block();
        if (objNumAttainQuota){
            String softObjnumValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_objnum_value")).orElse("0");
            long softQuotaObjNum = Long.parseLong(softObjnumValue);
            long usedObjNum = Long.parseLong(bucketInfo.getObjectNum());
            if (StringUtils.isEmpty(softObjnumValue) || softObjnumValue.equals("0")){
                objNumAttainQuota = false;
            }
            if (softQuotaObjNum > usedObjNum){
                objNumAttainQuota = false;
            }
        }
        if (capacityAttainQuota){
            String softQuotaValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_quota_value")).orElse("0");
            long softQuotaCapacity = Long.parseLong(softQuotaValue);
            long usedCapacity = Long.parseLong(bucketInfo.getBucketStorage());
            if (StringUtils.isEmpty(softQuotaValue) || softQuotaValue.equals("0")){
                capacityAttainQuota = false;
            }
            if (softQuotaCapacity > usedCapacity){
                capacityAttainQuota = false;
            }
        }
        if ((objNumAttainQuota || capacityAttainQuota)){
            return true;
        }
        return false;
    }

    private void filterObjectByDefault(String bucketName, String prefix, long timestamps, String storageStrategy,
                                      boolean isHistoryVersion, Integer days, boolean iffFlag) throws Exception {
        String filterType = "PREFIX";
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String startVnodeId = storagePool.getBucketVnodeId(bucketName, prefix);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        int startIndex = bucketVnodeList.indexOf(startVnodeId);
        AtomicInteger nextRunNumber = new AtomicInteger(startIndex);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        boolean isMove = StringUtils.isNotEmpty(storageStrategy);

        String softQuotaObjectNum = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_objnum_value")).orElse("0");
        String softQuotaCapacity = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_quota_value")).orElse("0");

        AtomicLong deleteNum = new AtomicLong(0L);
        AtomicLong deleteCapacity = new AtomicLong(0L);
        AtomicLong haveDeletedNum = new AtomicLong(0L);
        AtomicLong haveDeletedCapacity = new AtomicLong(0L);

        AtomicReference<String> startTime = new AtomicReference<>();
        AtomicReference<String> endTime = new AtomicReference<>();

        BucketInfo bucketInfo = ErasureClient.reduceBucketInfo(bucketName).block();
        if (bucketInfo.equals(ERROR_BUCKET_INFO) || bucketInfo.equals(NOT_FOUND_BUCKET_INFO)){
            throw new Exception("the usedObjectNum or usedQuate is null");
        }
        ClearConfigration clearConfigration = (ClearConfigration) JaxbUtils.toObject(pool.getCommand(REDIS_SYSINFO_INDEX).get(bucketName + "_clear_model"));
        if (StringUtils.isNotEmpty(clearConfigration.getStartTime()) && StringUtils.isNotEmpty(clearConfigration.getEndTime())){
            startTime.set(clearConfigration.getStartTime());
            endTime.set(clearConfigration.getEndTime());
        }
        if (!"0".equals(softQuotaObjectNum)){
            if (StringUtils.isNotEmpty(clearConfigration.getObjectRemaining())) {
                long remain = calculateResult(Long.parseLong(softQuotaObjectNum),clearConfigration.getObjectRemaining());
                deleteNum.set(Long.parseLong(bucketInfo.getObjectNum()) - remain);
            }else {
                deleteNum.set(Long.parseLong(bucketInfo.getObjectNum()) - Long.parseLong(softQuotaObjectNum));
            }
        }
        if (!"0".equals(softQuotaCapacity)){
            if (StringUtils.isNotEmpty(clearConfigration.getCapacityRemaining())) {
                long remain = calculateResult(Long.parseLong(softQuotaCapacity),clearConfigration.getCapacityRemaining());
                deleteCapacity.set(Long.parseLong(bucketInfo.getBucketStorage()) - remain);
            }else {
                deleteCapacity.set(Long.parseLong(bucketInfo.getBucketStorage()) - Long.parseLong(softQuotaCapacity));
            }
        }

        shardProcessor.subscribe(vnode -> {
            int maxKey = 1000;
            String stampMarker = timestamps + 1 + "";
            String status = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS))
                    .orElse("NULL");
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            String beginPrefix = getLifeCycleStamp(vnode, bucketName, "0");
            String recordKey = getLifecycleRecord(bucketName, vnode, filterType, null, prefix, isMove, isHistoryVersion, timestamps, days);
            String lifecycleRecord = "";
            AtomicBoolean restartSign = new AtomicBoolean(false);

            UnicastProcessor<SocketReqMsg> listController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            UnicastProcessor<JSONObject> deleteController = UnicastProcessor.create(Queues.<JSONObject>unboundedMultiproducer().get());

            Semaphore semaphore = new Semaphore(10000);
            listController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxKeys", String.valueOf(maxKey))
                    .put("prefix", prefix)
                    .put("stamp", stampMarker)
                    .put("isHistoryVersion", isHistoryVersion ? "0" : "1")
                    .put("retryTimes", "0")
                    .put("beginPrefix", beginPrefix));
            log.info("-------------------------- begin get Object  -----------{}---------{}--------{}", bucketName, timestamps, vnode);

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
                            ClearModelClientHandler clearModelClientHandler = new ClearModelClientHandler(storagePool, listController, responseInfo, nodeArr[0], reqMsg);
                            responseInfo.responses
                                    .publishOn(scheduler)
                                    .subscribe(clearModelClientHandler::handleResponse,
                                            e -> log.error("", e),
                                            () -> clearModelClientHandler.completeResponse(bucketName, recordKey, restartSign, lifecycleRecord));
                            clearModelClientHandler.res.subscribe(metaDataList -> {
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

                                    try {
                                        semaphore.acquire();
                                        deleteController.onNext(produceObject(metaData, versionId, storageStrategy, timestamps, status, isHistoryVersion, days));
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    if (isMultiCluster){
                                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
                                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
                                        listController.onComplete();
                                        return;
                                    }
                                    if (deleteNum.get() != 0){
                                        if (haveDeletedNum.incrementAndGet()>deleteNum.get() && deleteCapacity.get() == 0){
                                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
                                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
                                            log.info("list object complete");
                                            listController.onComplete();
                                            return;
                                        }
                                    }
                                    if (deleteCapacity.get() != 0){
                                        if (haveDeletedCapacity.addAndGet(metaData.endIndex - metaData.startIndex + 1) > deleteCapacity.get() && deleteNum.get() == 0 ){
                                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
                                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
                                            log.info("list object complete");
                                            listController.onComplete();
                                            return;
                                        }
                                    }
                                    if (haveDeletedNum.get() > deleteNum.get() && haveDeletedCapacity.get() > deleteCapacity.get()){
                                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
                                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
                                        log.info("list object complete");
                                        listController.onComplete();
                                        return;
                                    }
                                    if (StringUtils.isNotEmpty(startTime.get()) && StringUtils.isNotEmpty(endTime.get())){
                                        if (!isCurrentTimeInRange(startTime.get(), endTime.get())){
                                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
                                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
                                            log.info("list object complete");
                                            listController.onComplete();
                                            return;
                                        }
                                    }
                                }
                            });
                        });
            }, log::error);

            deleteController
                    .flatMap(jsonObject -> LifecycleCommandConsumer.expirationOperation(jsonObject).doFinally(signalType -> semaphore.release()))
                    .subscribe();
        }, log::error);

        for (int i = startIndex; i < bucketVnodeList.size(); ++i) {
            int next = nextRunNumber.getAndIncrement();
            shardProcessor.onNext(bucketVnodeList.get(next));
        }
    }


    private JSONObject produceObject(MetaData metaData, String versionId, String storageStrategy, long timestamps, String status, boolean isHistoryVersion, Integer days) {

        String bucketName = metaData.bucket;
        String objectName = metaData.key;
        return produceObject(bucketName, objectName, storageStrategy, versionId, status, metaData.getStorage(), metaData.snapshotMark);
    }

    private JSONObject produceObject(String bucket, String object, String storageStrategy, String versionId, String status, String storage, String snapshotMark) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bucket", bucket);
        jsonObject.put("object", object);
        jsonObject.put("targetStorageStrategy", storageStrategy);
        jsonObject.put("versionId", versionId);
        jsonObject.put("status", status);
        jsonObject.put("from", "clearModel");
        Optional.ofNullable(snapshotMark).ifPresent(v->jsonObject.put("snapshotMark",v));
        return jsonObject;
    }

}