package com.macrosan.component;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.pojo.ComponentStrategy;
import com.macrosan.component.pojo.ComponentTask;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.*;
import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.lifecycle.LifecycleCommandConsumer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import io.vertx.core.MultiMap;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.*;
import static com.macrosan.component.pojo.ComponentRecord.ERROR_COMPONENT_RECORD;
import static com.macrosan.component.pojo.ComponentRecord.NOT_FOUND_COMPONENT_RECORD;
import static com.macrosan.component.pojo.ComponentRecord.Type.IMAGE;
import static com.macrosan.component.pojo.ComponentRecord.Type.VIDEO;
import static com.macrosan.constants.ServerConstants.AUTHORIZATION;
import static com.macrosan.constants.ServerConstants.COMPONENT_RECORD_INNER_MARKER;
import static com.macrosan.constants.SysConstants.ROCKS_COMPONENT_IMAGE_KEY;
import static com.macrosan.constants.SysConstants.ROCKS_COMPONENT_VIDEO_KEY;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.ECUtils.updateRocksKey;
import static com.macrosan.ec.ErasureClient.updateMetaDataAcl;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_DELETE_COMPONENT_RECORD;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_MULTI_MEDIA_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.utils.authorize.AuthorizeV2.getAwsAk;

@Log4j2
public class ComponentUtils {

    public final static int RECORD_SET_MAX_SIZE = 500;
    public final static int VIDEO_RECORD_SET_MIN_SIZE = 10;
    public final static int VIDEO_RECORD_SET_MAX_SIZE = 30;


    /**
     * 存放图像处理记录的map  k为getMapKey()方法获取  v为一个元组，第一个为时间戳，第二个为记录
     */
    public static final Map<String, Tuple2<Long, ComponentRecord>> checkRecordMap = new ConcurrentHashMap<>();

    /**
     * 存放视频处理记录的map  k为getMapKey()方法获取  v为一个元组，第一个为时间戳，第二个为记录
     */
    public static final Map<String, Tuple2<Long, ComponentRecord>> checkVideoRecordMap = new ConcurrentHashMap<>();

    // 记录过期时间 10分钟
    public static final Long RECORD_EXPIRE_TIME = 1000 * 60 * 15L;

    public static final Set<String> COMPONENT_ROCKS_PREFIX_SET = new HashSet<>();

    static {
        COMPONENT_ROCKS_PREFIX_SET.add(ROCKS_COMPONENT_VIDEO_KEY);
        // 与ROCKS_PART_PREFIX冲突，因此使用!/进行判断
        COMPONENT_ROCKS_PREFIX_SET.add(ROCKS_COMPONENT_IMAGE_KEY + File.separator);
    }

    public static boolean isComponentRecordLun(String rocksKey) {
        return COMPONENT_ROCKS_PREFIX_SET.stream().anyMatch(rocksKey::startsWith);
    }

    /**
     * 检查记录是否过期
     */
    public static void checkIfRecordExpire() {
        Map<String, Tuple2<Long, ComponentRecord>> allRecordMap = new ConcurrentHashMap<>(); // 用于存储所有记录映射关系
        allRecordMap.putAll(checkRecordMap);
        allRecordMap.putAll(checkVideoRecordMap);
        allRecordMap.forEach((k, v) -> {
            if (System.currentTimeMillis() - v.var1 > RECORD_EXPIRE_TIME) {
                log.info("recordMap定时任务，移除key：{}", k);
                endRecordOperation(v.var2);
            }
        });
    }

    /**
     * 更新map中的记录时间戳
     *
     * @param record
     */
    public static void addExpire(ComponentRecord record) {
        if (record.taskMarker.startsWith(COMPONENT_RECORD_INNER_MARKER) && VIDEO.equals(record.strategy.getType())) {
            checkVideoRecordMap.put(getMapKey(record), new Tuple2<>(System.currentTimeMillis(), record));
        } else {
            checkRecordMap.put(getMapKey(record), new Tuple2<>(System.currentTimeMillis(), record));
        }
    }

    /**
     * 获取记录在map中的key
     *
     * @param record
     * @return
     */
    public static String getMapKey(ComponentRecord record) {
        if (record.strategy.getType().equals(VIDEO)) {
            return record.getBucket() + File.separator + record.getObject() + File.separator + record.versionId + File.separator + record.taskMarker + File.separator + record.syncStamp;
        }
        return record.getBucket() + File.separator + record.getObject() + File.separator + record.versionId + File.separator + record.taskMarker;
    }

    /**
     * 返回true表示该record未在队列中
     */
    public static boolean startRecordOperation(ComponentRecord record) {
        if (record.taskMarker.startsWith(COMPONENT_RECORD_INNER_MARKER) && VIDEO.equals(record.strategy.getType())) {
            return checkVideoRecordMap.putIfAbsent(getMapKey(record), new Tuple2<>(System.currentTimeMillis(), record)) == null;
        } else {
            return checkRecordMap.putIfAbsent(getMapKey(record), new Tuple2<>(System.currentTimeMillis(), record)) == null;
        }
    }

    /**
     * 处理结果返回后，移除该record
     */
    public static void endRecordOperation(ComponentRecord record) {
        if (record.taskMarker.startsWith(COMPONENT_RECORD_INNER_MARKER) && VIDEO.equals(record.strategy.getType())) {
            checkVideoRecordMap.remove(getMapKey(record));
        } else {
            checkRecordMap.remove(getMapKey(record));
        }
    }


    public static ComponentRecord createComponentRecord(String bucket, String object, MetaData metaData, MsHttpRequest request) {
        ComponentRecord record = new ComponentRecord();
        record.getStrategy().initFromRequest(request);
        record.setBucket(bucket)
                .setObject(object)
                .setVersionId(metaData.versionId)
                .setVersionNum(metaData.versionNum)
                .setSyncStamp(metaData.syncStamp)
                .setAk(StringUtils.isNotBlank(request.getHeader(AUTHORIZATION)) ? getAwsAk(request.getHeader(AUTHORIZATION)) : "");

        return record;
    }

    public static ComponentRecord createComponentRecord(String bucket, String object, MetaData metaData,
                                                        ComponentTask task, Map<String, String> headersMap) {
        ComponentRecord record = new ComponentRecord();
        record.setStrategy(task.getStrategy())
                .setBucket(bucket)
                .setObject(object)
                .setVersionId(metaData.versionId)
                .setVersionNum(metaData.versionNum)
                .setSyncStamp(metaData.syncStamp)
                .setTaskMarker(task.getMarker())
//                .setAk(ak)
//                .setSk(sk)
                .setTaskName(task.taskName)
                .setHeaderMap(headersMap);

        return record;
    }

    public static ComponentRecord createComponentRecord(MetaData metaData,
                                                        ComponentStrategy strategy, String marker, Map<String, String> headersMap) {
        ComponentRecord record = new ComponentRecord();
        record.setStrategy(strategy)
                .setBucket(metaData.bucket)
                .setObject(metaData.key)
                .setVersionId(metaData.versionId)
                .setVersionNum(metaData.versionNum)
                .setSyncStamp(metaData.syncStamp)
                .setTaskMarker(marker)
                .setHeaderMap(headersMap);

        return record;
    }

    public static Mono<Boolean> putComponentRecord(ComponentRecord record, MsHttpRequest request) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;

        return storagePool.mapToNodeInfo(bucketVnode)
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(record.bucket))
                .publishOn(COMP_SCHEDULER)
                .flatMap(tuple2 -> {
                    List<Tuple3<String, String, String>> nodeList = tuple2.getT1();
                    Boolean isSyncSwitchOff = tuple2.getT2();
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple -> {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("key", record.rocksKey())
                                        .put("value", Json.encode(record))
                                        .put("versionNum", VersionUtil.getVersionNum(isSyncSwitchOff))
                                        .put("lun", MSRocksDB.getComponentRecordLun(tuple.var2))
                                        .put("poolQueueTag", poolQueueTag);
                                return msg;
                            })
                            .collect(Collectors.toList());

                    return putRecord(storagePool, msgs, PUT_MULTI_MEDIA_RECORD, ERROR_PUT_MULTI_MEDIA_RECORD, nodeList, request);
                });
    }

    private static Mono<Boolean> putRecord(StoragePool storagePool, List<SocketReqMsg> msgs, ErasureServer.PayloadMetaType type, ErrorConstant.ECErrorType errorType,
                                           List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {

        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);
        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum < storagePool.getK()) {
                //多个节点写入失败，放入消息队列中
                publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
                res.onNext(false);
                return;
            }
            if (responseInfo.successNum != nodeList.size()) {
                //只要写不成功，均写入消息队列
                publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
            }
            res.onNext(true);
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res;
    }

    public static Mono<Tuple2<ComponentRecord, Tuple2<ErasureServer.PayloadMetaType, ComponentRecord>[]>> getComponentRecord(ComponentRecord record) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ECUtils.getRocksKey(storagePool, record.rocksKey()
                        , ComponentRecord.class, GET_COMPONENT_RECORD, NOT_FOUND_COMPONENT_RECORD, ERROR_COMPONENT_RECORD
                        , null, ComponentRecord::getVersionNum, Comparator.comparing(a -> a.versionNum),
                        (a, b, c, d) -> Mono.just(1), nodeList, null));
    }

    public static Mono<Integer> updateComponentRecord(ComponentRecord record, List<Tuple3<String, String, String>> nodeList,
                                                      MsHttpRequest request, Tuple2<ErasureServer.PayloadMetaType, ComponentRecord>[] res) {
        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<ErasureServer.PayloadMetaType, ComponentRecord> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        String lastVersionNum = VersionUtil.getLastVersionNum(record.getVersionNum());
        record.setVersionNum(lastVersionNum);
        for (Tuple2<ErasureServer.PayloadMetaType, ComponentRecord> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(lastVersionNum);
            }
        }

        return updateRocksKey(StoragePoolFactory.getMetaStoragePool(record.bucket), oldVersionNum, record.rocksKey(),
                Json.encode(record), UPDATE_COMPONENT_RECORD, ERROR_PUT_MULTI_MEDIA_RECORD, nodeList, request, null);
    }

    public static boolean hasMultiMediaParam(MultiMap params) {
        for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
            if (params.contains(type.name)) {
                return true;
            }
        }
        return false;
    }

    public static Mono<Boolean> deleteComponentRecord(ComponentRecord record) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        storagePool.mapToNodeInfo(bucketVnode)
                .publishOn(COMP_SCHEDULER)
                .subscribe(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple -> {
                                SocketReqMsg msg = new SocketReqMsg("", 0)
                                        .put("key", record.rocksKey());
                                msg.put("lun", MSRocksDB.getComponentRecordLun(tuple.var2));
                                return msg;
                            })
                            .collect(Collectors.toList());
                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DEL_COMPONENT_RECORD, String.class, nodeList);
                    responseInfo.responses.subscribe(s -> {
                            },
                            e -> log.error("delComponentRecord error, ", e),
                            () -> {
                                if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                                    res.onNext(true);
                                } else {
                                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                            .put("value", Json.encode(record))
                                            .put("bucket", record.bucket)
                                            .put("key", record.rocksKey)
                                            .put("poolQueueTag", poolQueueTag);
                                    publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_DELETE_COMPONENT_RECORD);
                                    res.onNext(responseInfo.successNum >= storagePool.getK());
                                }
                            });
                });
        return res;
    }

    /**
     * 该对象已按策略进行过处理
     */
    public static boolean hasDealt(MetaData metaData, ComponentStrategy strategy) {
        return metaData.strategySet != null && metaData.strategySet.contains(strategy.strategyMark);
    }

    public static boolean isImageObject(String objectName) {
        String[] split = objectName.split("\\.");
        if (split.length == 0) {
            return false;
        }
        String type = split[split.length - 1].toLowerCase();
        return SUPPORT_IMAGE_FORMAT.contains(type);
    }

    public static String getRadomAvailIp() {
        Set<String> set = AVAIL_IP_SET;
        if (AVAIL_IP_SET.isEmpty()) {
            set = ALL_IP_SET;
        }

        String[] publishIps = set.toArray(new String[0]);
        final int currentIndex = ThreadLocalRandom.current().nextInt(0, publishIps.length);
        return publishIps[currentIndex];
    }

    /**
     * 根据策略中的process类型判断对象格式是否受支持
     *
     * @param objectName
     * @param type
     * @return
     */
    public static boolean isSupportFormat(String objectName, ComponentRecord.Type type) {
        String[] split = objectName.split("\\.");
        if (split.length == 0) {
            return false;
        }
        String format = split[split.length - 1].toLowerCase();
        switch (type) {
            case IMAGE:
                return SUPPORT_IMAGE_FORMAT.contains(format);
            case VIDEO:
                return true;
            default:
                return false;
        }
    }

    /**
     * 根据record类型 动态调整set最大值
     *
     * @param type
     * @param taskMarker
     * @return
     */
    public static boolean isSetFullForRecordType(ComponentRecord.Type type, String taskMarker) {
        if (taskMarker.startsWith(COMPONENT_RECORD_INNER_MARKER) && VIDEO.equals(type)) {
            if (checkRecordMap.size() >= RECORD_SET_MAX_SIZE * 0.75) {
                //图片处理任务较多   视频截帧最多10个
                return checkVideoRecordMap.size() >= VIDEO_RECORD_SET_MIN_SIZE;
            } else {
                return checkVideoRecordMap.size() >= VIDEO_RECORD_SET_MAX_SIZE;
            }
        } else if (IMAGE.equals(type)) {
            // 如果视频截帧到达30个，图片数量最大值也应该减少
            if (checkVideoRecordMap.size() >= VIDEO_RECORD_SET_MAX_SIZE * 0.75) {
                return checkRecordMap.size() >= RECORD_SET_MAX_SIZE * 0.75;
            } else {
                return checkRecordMap.size() >= RECORD_SET_MAX_SIZE;
            }
        }
        return false;
    }

    /**
     * 根据策对象名获取对象的类型
     *
     * @param objectName 对象名
     * @return 对象类型
     */
    public static String getObjectType(String objectName) {
        String[] split = objectName.split("\\.");
        if (split.length == 0) {
            return null;
        }
        String format = split[split.length - 1].toLowerCase();
        if (SUPPORT_IMAGE_FORMAT.contains(format)) {
            return IMAGE.name;
        }
        if (SUPPORT_VIDEO_FORMAT.contains(format)) {
            return VIDEO.name;
        }
        return null;
    }

    /**
     * 图像处理完成后，删除源对象
     *
     * @param record 图像处理记录
     * @return 删除结果
     */
    public static Mono<? extends Boolean> deleteSourceObject(ComponentRecord record) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bucket", record.bucket);
        jsonObject.put("object", record.object);
        jsonObject.put("versionId", record.versionId);
        jsonObject.put("mediaDeleteSource","");
        return MsObjVersionUtils.versionStatusReactive(record.bucket)
                .doOnNext(status -> jsonObject.put("status", status))
                .flatMap(b -> LifecycleCommandConsumer.expirationOperation(jsonObject))
                .map(b -> b == 1);
    }

    /**
     * 图像处理完成后，更新源对象元数据，标记源对象已被该策略处理过
     *
     * @param record     图像处理记录
     * @param bucketPool 存储池
     * @return 更新结果
     */
    public static Mono<Boolean> updateSourceObjectMetaData(ComponentRecord record, StoragePool bucketPool) {
        return Mono.just(true)
                .flatMap(a -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket, record.object)))
                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(record.bucket, record.object,
                        record.versionId, bucketVnodeList, null).zipWith(Mono.just(bucketVnodeList)))
                .flatMap(tuple2 -> {
                    Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(record.bucket, record.object);
                    String bucketVnode = bucketVnodeIdTuple.var1;
                    // 判断当前节点是否正在进行扩容，是否开启双写
                    String migrateVnode = bucketVnodeIdTuple.var2;

                    // 该对象（图像）已被处理完毕，更新元数据中的strategySet，
                    MetaData metaData = tuple2.getT1().var1;
                    if (metaData.deleteMark || metaData.deleteMarker) {
                        // 图像处理完之后还没更新元数据时已经被删除了，或被置为删除标记了，则不再更新元数据
                        return Mono.just(true);
                    }
                    List<Tuple3<String, String, String>> bucketNodeList = tuple2.getT2();
                    if (metaData.strategySet == null) {
                        metaData.strategySet = new HashSet<>();
                    }
                    metaData.strategySet.add(record.strategy.strategyMark);
                    return Mono.just(migrateVnode != null)
                            .flatMap(b -> {
                                if (b && !migrateVnode.equals(bucketVnode)) {
                                    String migrateMetaKey = Utils.getVersionMetaDataKey(migrateVnode, record.bucket, record.object, record.versionId);
                                    UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                                    MonoProcessor<Boolean> updateRes = MonoProcessor.create();
                                    AtomicInteger retryCount = new AtomicInteger(0);
                                    retryProcessor
                                            .flatMap(retry -> bucketPool.mapToNodeInfo(migrateVnode))
                                            .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(record.bucket, record.object, record.versionId, migrateVnodeList, null).zipWith(Mono.just(migrateVnodeList)))
                                            .flatMap(resTuple2 -> ErasureClient.updateMetaData(migrateMetaKey, metaData.clone(), resTuple2.getT2(), null, resTuple2.getT1().var2))
                                            .timeout(Duration.ofSeconds(30))
                                            .doOnNext(r -> {
                                                if (r == 1) {
                                                    updateRes.onNext(true);
                                                    retryProcessor.onComplete();
                                                } else if (r == 2 && retryCount.get() < 3) {
                                                    retryProcessor.onNext(retryCount.incrementAndGet());
                                                    log.debug("retry update meta,count:{}", retryCount.get());
                                                } else {
                                                    updateRes.onNext(false);
                                                    retryProcessor.onComplete();
                                                    log.error("update new mapping meta storage error! {}", r);
                                                }
                                            })
                                            .doOnError(e -> {
                                                updateRes.onNext(false);
                                                log.error("update mig component metadata error", e);
                                            })
                                            .subscribe();
                                    retryProcessor.onNext(retryCount.get());
                                    return updateRes;
                                }
                                return Mono.just(true);
                            })
                            .filter(b -> b)
                            .flatMap(b -> {
                                UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                                MonoProcessor<Boolean> updateRes = MonoProcessor.create();
                                AtomicInteger retryCount = new AtomicInteger(0);
                                retryProcessor
                                        .flatMap(retry -> updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, record.bucket,
                                                record.object, metaData.versionId), metaData, bucketNodeList, null, tuple2.getT1().var2))
                                        .doOnNext(res -> {
                                            if (res == 1) {
                                                updateRes.onNext(true);
                                                retryProcessor.onComplete();
                                            } else if (res == 2 && retryCount.get() < 3) {
                                                retryProcessor.onNext(retryCount.incrementAndGet());
                                                log.debug("retry update meta,count:{}", retryCount.get());
                                            } else {
                                                updateRes.onNext(false);
                                                retryProcessor.onComplete();
                                                log.error("update new mapping meta storage error! {}", res);
                                            }
                                        }).doOnError(e -> {
                                            updateRes.onNext(false);
                                            log.error("update mig component metadata error", e);
                                        }).subscribe();

                                retryProcessor.onNext(retryCount.get());
                                return updateRes;
                            })
                            .timeout(Duration.ofSeconds(30))
                            .doOnError(e -> log.error("update component metadata error.", e))
                            .onErrorReturn(false);

                });
    }

    public static boolean isBoolean(String str) {
        return "true".equals(str) || "false".equals(str);
    }
}
