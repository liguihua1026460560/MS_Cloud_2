package com.macrosan.component.scanners;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.TaskSender;
import com.macrosan.component.enums.ErrorEnum;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.pojo.ComponentTask;
import com.macrosan.component.utils.ErrorRecordUtil;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.storage.client.ListAllLifecycleClientHandler;
import com.macrosan.storage.client.ListMultiMediaTaskHandler;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.*;
import static com.macrosan.component.ComponentUtils.checkIfRecordExpire;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_COMPONENT_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * 多媒体对象处理开启扫描。扫描到的Record将通过TaskSender发往组件处理。
 */
@Log4j2
public class ComponentScanner {
    private static final RedisConnPool POOL = RedisConnPool.getInstance();

    private static ComponentScanner instance;

    public static ComponentScanner getInstance() {
        if (instance == null) {
            instance = new ComponentScanner();

        }
        return instance;
    }

    /**
     * 处理定时任务扫描到的metaData，生成componentRecord
     */
    private static UnicastProcessor<Tuple2<ComponentTask, MetaData>>[] objProcessors = new UnicastProcessor[16];
    private static final int RECORD_NUM = 10000;


    /**
     * 初始化一个常规的ComponentRecord扫描，以及根据每个ComponentTask，生成一个对应的扫描
     */
    public void init() {
        for (int i = 0; i < objProcessors.length; i++) {
            objProcessors[i] = UnicastProcessor.create(Queues.<Tuple2<ComponentTask, MetaData>>unboundedMultiproducer().get());
            objProcessors[i].publishOn(COMP_SCHEDULER)
                    .filter(tuple2 -> {
                        if (ComponentRecord.Type.VIDEO.equals(tuple2.var1.strategyType)) {
                            return true;
                        }
                        MetaData metaData = tuple2.var2;
                        long dataSize = metaData.endIndex - metaData.startIndex + 1;
                        if (dataSize > MAX_IMAGE_SIZE) {
                            log.debug("image too large,image size:{}", dataSize);
                            ErrorRecordUtil.putErrorRecord(ErrorEnum.IMAGE_SIZE_ERROR, metaData, tuple2.var1).subscribe();
                            return false;
                        }
                        return true;
                    })
                    .subscribe(tuple2 -> {
                        // 符合条件的对象都根据task中的strategy生成一个record
                        ComponentTask componentTask = tuple2.var1;
                        MetaData metaData = tuple2.var2;

                        // 由元数据获取对象相关ak sk。
                        Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                        });
                        String userId = sysMap.get("owner");

                        Map<String, String> map = new HashMap<>();
                        long dataSize = metaData.endIndex - metaData.startIndex + 1;
                        map.put(CONTENT_LENGTH, String.valueOf(dataSize));
                        map.put("userId", userId);
                        ComponentRecord record = ComponentUtils.createComponentRecord(metaData.bucket, metaData.key, metaData, componentTask, map);
                        ComponentUtils.putComponentRecord(record, null)
                                .subscribe(b -> {
                                    if (!b) {
                                        //todo f 异常处理
                                        log.error("putComponentRecord error1, {}", record);
                                    }
                                }, e -> log.error("putComponentRecord error2, {}", record));
                    }, e -> log.error("dealObjMeta error,", e));
        }

        regularScan();
        objectLevelScan();
        taskScan();
        scanDeleteTaskSet();
    }

    /**
     * 是否继续扫描
     */
    private static final AtomicBoolean KEEP_SCANING = new AtomicBoolean();

    /**
     * 保存各桶是否在进行某个组件的扫描。
     */
    static final Map<String, Set<ComponentRecord.Type>> isScanningMap = new ConcurrentHashMap<>();

    /**
     * 有几个组件的扫描流程正在进行。
     */
    static final AtomicInteger componentCount = new AtomicInteger();

    private void regularScan() {
        //一个线程常规扫描所有record。按桶名来开启扫描。
        AtomicBoolean isRunning = new AtomicBoolean();
        COMP_TIMER.scheduleWithFixedDelay(() -> {
            try {
                //主站点扫描
                if (!"master".equals(Utils.getRoleState())) {
                    KEEP_SCANING.compareAndSet(true, false);
                    return;
                }

                if (!isRunning.compareAndSet(false, true)) {
                    return;
                }
                KEEP_SCANING.compareAndSet(false, true);

                ScanStream.scan(POOL.getReactive(REDIS_POOL_INDEX), new ScanArgs().match(COMPONENT_TASK_REDIS_PREFIX + "*"))
                        .publishOn(COMP_SCHEDULER)
                        .doFinally(s -> isRunning.compareAndSet(true, false))
                        .flatMap(taskKey -> POOL.getReactive(REDIS_POOL_INDEX).hgetall(taskKey)
                                .doOnNext(task -> {
                                    if (StringUtils.isEmpty(task.get("bucket"))) {
                                        Mono.just(true).publishOn(COMP_SCHEDULER)
                                                .subscribe(b -> {
                                                    POOL.getShortMasterCommand(REDIS_POOL_INDEX).del(taskKey);
                                                });
                                    }
                                }))
                        .filter(task -> StringUtils.isNotEmpty(task.get("bucket")) && BUCKET_NAME_PATTERN.matcher(task.get("bucket")).matches())
                        .subscribe(taskMap -> {
                            ComponentTask componentTask = new ComponentTask(taskMap);
                            String lockKey = componentTask.getBucket() + componentTask.getMarker();
                            if (!isScanningMap.containsKey(lockKey)) {
                                isScanningMap.put(lockKey, new ConcurrentHashSet<>());
                            }
                            for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
                                scanRecord(type, componentTask.getBucket(), componentTask.getMarker());
                            }
                        }, e -> log.error("regular component scan error 2, ", e));

            } catch (Exception e) {
                log.error("regular component scan error, ", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 对象级别的record扫描   扫描固定前缀的record
     */
    private void objectLevelScan() {
        AtomicBoolean isRunning = new AtomicBoolean();
        COMP_TIMER.scheduleWithFixedDelay(() -> {
            try {
                //主站点扫描
                if (!"master".equals(Utils.getRoleState())) {
                    KEEP_SCANING.compareAndSet(true, false);
                    return;
                }

                if (!isRunning.compareAndSet(false, true)) {
                    return;
                }
                KEEP_SCANING.compareAndSet(false, true);
                // todo 扫描完后移除bucket
                POOL.getReactive(REDIS_POOL_INDEX).smembers(COMPONENT_RECORD_BUCKET_SET)
                        .publishOn(COMP_SCHEDULER)
                        .doFinally(s -> isRunning.compareAndSet(true, false))
                        .filter(bucket -> StringUtils.isNotEmpty(bucket) && BUCKET_NAME_PATTERN.matcher(bucket).matches())
                        .subscribe(bucket -> {
                            String lockKey = bucket + COMPONENT_RECORD_INNER_MARKER;
                            if (!isScanningMap.containsKey(lockKey)) {
                                isScanningMap.put(lockKey, new ConcurrentHashSet<>());
                            }
                            for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
                                scanRecord(type, bucket, COMPONENT_RECORD_INNER_MARKER);
                            }
                        }, e -> log.error("object component scan error 2, ", e));

            } catch (Exception e) {
                log.error("regular component scan error, ", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 将根据桶名和组件类型扫描，扫描到的record发往TaskSender.
     */
    public static void scanRecord(ComponentRecord.Type type, String bucketName, String taskMarker) {

        String lockKey = bucketName + taskMarker;
        synchronized (isScanningMap.get(lockKey)) {
            if (isScanningMap.get(lockKey).contains(type)) {
                log.debug("component is already scanning, bucket:{}, type:{}", bucketName, type.name());
                return;
            }
            isScanningMap.get(lockKey).add(type);
        }
        try {
            UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnode = metaPool.getBucketVnodeId(bucketName);
            TypeReference<Tuple2<String, ComponentRecord>[]> reference = new TypeReference<Tuple2<String, ComponentRecord>[]>() {
            };

            listController.publishOn(COMP_SCHEDULER)
                    .flatMap(marker -> Mono.just(marker).zipWith(metaPool.mapToNodeInfo(bucketVnode)))
                    .doFinally(v -> {
                        isScanningMap.get(lockKey).remove(type);
                    })
                    .subscribe(tuple2 -> {
                        if (!KEEP_SCANING.get()) {
                            listController.onComplete();
                            return;
                        }

                        if (ComponentUtils.isSetFullForRecordType(type,taskMarker)) {
                            log.debug("delay record scan");
                            Mono.delay(Duration.ofSeconds(1)).publishOn(COMP_SCHEDULER).subscribe(s -> listController.onNext(tuple2.getT1()));
                            return;
                        }

                        String marker = tuple2.getT1();
                        List<Tuple3<String, String, String>> nodeList = tuple2.getT2();

                        List<SocketReqMsg> msgs = nodeList.stream().map(info ->
                                new SocketReqMsg("", 0)
                                        .put("bucket", bucketName)
                                        .put("maxKeys", String.valueOf(ListMultiMediaTaskHandler.MAX_KEY))
                                        .put("type", type.name())
                                        .put("marker", marker)
                                        .put("lun", MSRocksDB.getComponentRecordLun(info.var2))
                                        .put("taskMarker", taskMarker)
                        ).collect(Collectors.toList());

                        ClientTemplate.ResponseInfo<Tuple2<String, ComponentRecord>[]> responseInfo =
                                ClientTemplate.oneResponse(msgs, LIST_COMPONENT_RECORD, reference, nodeList);
                        ListMultiMediaTaskHandler clientHandler = new ListMultiMediaTaskHandler(responseInfo, nodeList, bucketName);
                        Disposable subscribe = responseInfo.responses.publishOn(COMP_SCHEDULER)
                                .subscribe(clientHandler::handleResponse, listController::onError, clientHandler::handleComplete);
                        AtomicLong count = new AtomicLong(0L);
                        clientHandler.listProcessor
                                .doOnNext(list -> {
                                    if (list.size() == 0) {
                                        listController.onComplete();
                                    }
                                })
                                .flatMapMany(Flux::fromIterable)
                                .flatMap(componentRecord -> {
                                    String destination = componentRecord.strategy.destination;
                                    if (StringUtils.isBlank(destination)) {
                                        return Mono.just(componentRecord).zipWith(Mono.just(true));
                                    }
                                    String destinationBucket = destination.split("/")[0];
                                    return POOL.getReactive(REDIS_BUCKETINFO_INDEX).exists(destinationBucket)
                                            .flatMap(res -> Mono.just(componentRecord).zipWith(Mono.just(res == 1L)));
                                })
                                .flatMap(t2 -> {
                                    if (!t2.getT2()) {
                                        ComponentUtils.endRecordOperation(t2.getT1());
                                        return ComponentUtils.deleteComponentRecord(t2.getT1())
                                                .map(b -> t2.getT1());

                                    }
                                    TaskSender.distributeMultiMediaRecord(t2.getT1());
                                    return Mono.just(t2.getT1());
                                })
                                .subscribe(t2 -> {
                                    long curCount = count.addAndGet(1);
                                    if (curCount == clientHandler.getCount()) {
                                        if (clientHandler.getCount() < ListMultiMediaTaskHandler.MAX_KEY) {
                                            listController.onComplete();
                                        } else {
                                            listController.onNext(clientHandler.getNextMarker());
                                        }
                                    }
                                });
                    });
            listController.onNext("");
        } catch (MsException e) {
            //有异常，则从isScanningMap中移除---此处一般为桶不存在时抛出的异常
            isScanningMap.get(lockKey).remove(type);
        }
    }

    /**
     * 当前有几个账户在扫描task
     */
    AtomicInteger runningAccountCount = new AtomicInteger();

    /**
     * 当前所有在扫描的task数量统计
     */
    AtomicInteger runningTaskCount = new AtomicInteger();

    static final Integer MAX_RUNNING_ACOUNT = 100;

    static final Integer MAX_RUNNING_TASK = 100;

    static final Set<String> taskScanningMap = new ConcurrentSkipListSet<>();

    public void taskScan() {
        // 一个线程每分钟检测是否有task到了执行时间。根据task扫描元数据生成record后，将record写入rocksDB。
        AtomicBoolean isRunning = new AtomicBoolean();
        COMP_TIMER.scheduleAtFixedRate(() -> {
            //主站点扫描
            if (!"master".equals(Utils.getRoleState())) {
                KEEP_SCANING.compareAndSet(true, false);
                return;
            }

            if (!isRunning.compareAndSet(false, true)) {
                return;
            }
            String today = LocalDate.now().toString();
            KEEP_SCANING.compareAndSet(false, true);
            checkIfRecordExpire();//定时检查map中的record是否长时间没被移除
            ScanStream.scan(POOL.getReactive(REDIS_POOL_INDEX), new ScanArgs().match(ComponentTask.REDIS_KEY + "*"))
                    .publishOn(COMP_SCHEDULER)
                    .doFinally(s -> isRunning.compareAndSet(true, false))
                    .flatMap(key -> POOL.getReactive(REDIS_POOL_INDEX).hgetall(key))
                    .filter(taskMap -> !today.equals(taskMap.get("lastTime")))
                    .doOnNext(value -> {
                        ComponentTask componentTask = new ComponentTask(value);
                        if (!componentTask.allowStart()) {
                            return;
                        }
                        componentTask.strategyMono().publishOn(COMP_SCHEDULER).subscribe(strategy -> scanMeta(componentTask, today));
                    })
                    .doOnError(e -> log.error("scan component task error, ", e))
                    .subscribe();
        }, 30, 60, TimeUnit.SECONDS);
    }

    private static void scanMeta(ComponentTask componentTask, String today) {
        synchronized (taskScanningMap) {
            if (taskScanningMap.contains(componentTask.taskRedisKey())) {
                log.debug("task is already scanning, {}", componentTask.taskRedisKey());
                return;
            }
            taskScanningMap.add(componentTask.taskRedisKey());
        }

        log.info("start component Task, {}", componentTask);

        try {
            String bucketName = componentTask.getBucket();
            String taskMarker = componentTask.getMarker();
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            List<String> bucketVnodeList = metaStoragePool.getBucketVnodeList(bucketName);
            UnicastProcessor<SocketReqMsg> allListController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            AtomicInteger findNum = new AtomicInteger();
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
            int maxKey = 1000;
            // startTime为0表示立即执行
            String stampMarker;
            if ("0".equals(componentTask.startTime)) {
                stampMarker = String.valueOf(System.currentTimeMillis());
            } else {
                stampMarker = String.valueOf(componentTask.calendarFromStartTime().getTimeInMillis());
            }
            String stamp = "0";
            String beginPrefix = Optional.ofNullable(POOL.getCommand(REDIS_POOL_INDEX).hget(componentTask.taskRedisKey(), taskMarker + "beginPrefix")).orElse("");
            allListController
                    .doOnComplete(() -> POOL.getReactive(REDIS_POOL_INDEX).hget(componentTask.taskRedisKey(), "marker")
                            .publishOn(COMP_SCHEDULER)
                            .subscribe(redisTaskMarker -> {
                                if (Objects.equals(taskMarker, redisTaskMarker)) {
                                    POOL.getShortMasterCommand(REDIS_POOL_INDEX).hset(componentTask.taskRedisKey(), "lastTime", today);
                                }
                            }))
                    .doFinally(s -> taskScanningMap.remove(componentTask.taskRedisKey()))
                    .flatMap(socketReqMsg -> POOL.getReactive(REDIS_POOL_INDEX).hget(componentTask.taskRedisKey(), "marker")
                            .defaultIfEmpty("")
                            .doOnNext(redisTaskMarker -> {
                                if (!Objects.equals(taskMarker, redisTaskMarker)) {
                                    allListController.onComplete();
                                }
                            })
                            .map(b -> socketReqMsg)
                    )
                    .subscribe(msg -> {
                        if (!KEEP_SCANING.get()) {
                            allListController.onComplete();
                            return;
                        }
                        Mono.just(bucketVnodeList.toArray(new String[0]))
                                .flatMapMany(Flux::fromArray)
                                .flatMap(bucketVnode -> {
                                    String keySuffix = msg.dataMap.getOrDefault("beginPrefix", "");
                                    String curBeginPrefix = StringUtils.isNotEmpty(keySuffix) ? ROCKS_LIFE_CYCLE_PREFIX + bucketVnode + keySuffix
                                            : getLifeCycleStamp(bucketVnode, bucketName, stamp);
                                    return metaPool.mapToNodeInfo(bucketVnode)
                                            .publishOn(COMP_SCHEDULER)
                                            .flatMap(infoList -> {
                                                String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                                                List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)
                                                        .put("beginPrefix", curBeginPrefix).put("vnode", nodeArr[0])).collect(Collectors.toList());

                                                ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                                        String, MetaData>[]>() {
                                                }, infoList);
                                                ListAllLifecycleClientHandler clientHandler = new ListAllLifecycleClientHandler(responseInfo, nodeArr[0], reqMsg, bucketName);
                                                responseInfo.responses.publishOn(COMP_SCHEDULER).subscribe(clientHandler::handleResponse, e -> log.error("", e),
                                                        clientHandler::completeResponse);
                                                return clientHandler.res;
                                            });
                                })
                                .collectList()
                                .map(ComponentScanner::getMetaList)
                                .doOnNext(metaDataList -> {
                                    for (int i = 0; i < Math.min(maxKey, metaDataList.size()); i++) {
                                        MetaData lifeMeta = metaDataList.get(i).var2;
                                        if (lifeMeta.equals(MetaData.ERROR_META) || lifeMeta.equals(MetaData.NOT_FOUND_META)
                                                || lifeMeta.deleteMark || lifeMeta.deleteMarker || !lifeMeta.latest) {
                                            continue;
                                        }

                                        if (ComponentUtils.hasDealt(lifeMeta, componentTask.getStrategy())
                                                // 目录
                                                || lifeMeta.key.endsWith(File.separator)
                                                // 不符合定时任务的前缀要求
                                                || (StringUtils.isNotEmpty(componentTask.prefix) && !lifeMeta.key.startsWith(componentTask.prefix))
                                                // 格式不支持
                                                || !ComponentUtils.isSupportFormat(lifeMeta.key, componentTask.getStrategy().type)
                                                // 上一次任务执行时已经处理过
                                                || Objects.equals(metaDataList.get(i).var1, beginPrefix)) {
                                            continue;
                                        }
                                        int index = ThreadLocalRandom.current().nextInt(objProcessors.length);
                                        objProcessors[index].onNext(new Tuple2<>(componentTask, lifeMeta));
                                    }

                                    if (metaDataList.size() > 0) {
                                        int metaListNum = Math.min(1001, metaDataList.size());
                                        int num = findNum.addAndGet(metaListNum);
                                        if (num > RECORD_NUM || metaDataList.size() <= 1000) {
                                            String key = metaDataList.get(metaListNum - 1).var1;
                                            log.info("put component scanner key: {}", key);
                                            Mono.just(true)
                                                    .publishOn(COMP_SCHEDULER)
                                                    .subscribe(a -> POOL.getReactive(REDIS_POOL_INDEX).hget(componentTask.taskRedisKey(), "marker")
                                                            .publishOn(COMP_SCHEDULER)
                                                            .subscribe(redisTaskMarker -> {
                                                                if (Objects.equals(taskMarker, redisTaskMarker)) {
                                                                    POOL.getShortMasterCommand(REDIS_POOL_INDEX).hset(componentTask.taskRedisKey(), taskMarker + "beginPrefix", key);
                                                                    findNum.set(0);
                                                                }
                                                            }));

                                        }
                                    }

                                    if (metaDataList.size() <= 1000) {
                                        allListController.onComplete();
                                    } else {
                                        allListController.onNext(reqMsg.put("bucket", bucketName)
                                                .put("maxKeys", String.valueOf(maxKey))
                                                // 列出的对象名不超过该stamp生成的rocksKey前缀
                                                .put("stamp", stampMarker)
                                                .put("retryTimes", "0")
                                                .put("beginPrefix", metaDataList.get(1000).var1));
                                    }
                                })
                                .doOnError(e -> {
                                    log.error("list component metadata error.", e);
                                    allListController.onError(e);
                                })
                                .subscribe();
                    }, e -> {
                    });
            allListController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxKeys", String.valueOf(maxKey))
                    .put("beginPrefix", beginPrefix)
                    // 列出的对象名不超过该stamp生成的rocksKey前缀
                    .put("stamp", stampMarker));

        } catch (MsException e) {
            //有异常，则从taskScanningMap中移除该task，并更新task上一次执行时间
            taskScanningMap.remove(componentTask.taskRedisKey());
            POOL.getReactive(REDIS_POOL_INDEX).exists(componentTask.taskRedisKey())
                    .publishOn(COMP_SCHEDULER)
                    .subscribe(b -> {
                        if (b == 1) {
                            POOL.getShortMasterCommand(REDIS_POOL_INDEX).hset(componentTask.taskRedisKey(), "lastTime", today);
                        }
                    });
        }

    }

    public static List<Tuple2<String, MetaData>> getMetaList(List<List<LifecycleClientHandler.Counter>> list) {
        List<Tuple2<String, MetaData>> metaDataList = new LinkedList<>();
        for (List<LifecycleClientHandler.Counter> counterList : list) {
            if (metaDataList.isEmpty()) {
                //第一次直接写入
                counterList.forEach(c -> metaDataList.add(new Tuple2<>(c.getKey().substring(c.getKey().indexOf("/")), c.getMetaData())));
            } else {
                ListIterator<Tuple2<String, MetaData>> iterator = metaDataList.listIterator();
                Tuple2<String, MetaData> curCounter;
                //如果不是第一个响应的节点，遍历这个节点的响应结果
                for (LifecycleClientHandler.Counter c : counterList) {
                    //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                    //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                    String newKey = c.getKey().substring(c.getKey().indexOf("/"));
                    if (!iterator.hasNext()) {
                        iterator.add(new Tuple2<>(newKey, c.getMetaData()));
                        continue;
                    }
                    //新响应的节点返回的记录的key
                    curCounter = iterator.next();
                    int i = curCounter.var1.compareTo(newKey);
                    if (i > 0) {
                        //如果新来的key比链表中当前的key小，将新来的记录加到当前key前面
                        iterator.previous();
                        curCounter = new Tuple2<>(newKey, c.getMetaData());
                        iterator.add(curCounter);
                    } else if (i < 0) {
                        //如果新来的key比链表中当前的key大，遍历链表：
                        //如果有key和当前一致的，对应的counter.c++
                        //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                        //如果到最后一直是比当前小的，就在最后插入这条记录
                        int j = -1;
                        while (iterator.hasNext()) {
                            curCounter = iterator.next();
                            j = curCounter.var1.compareTo(newKey);
                            if (j == 0) {
                                break;
                            }

                            if (j > 0) {
                                iterator.previous();
                                curCounter = new Tuple2<>(newKey, c.getMetaData());
                                iterator.add(curCounter);
                                break;
                            }
                        }

                        if (j < 0) {
                            curCounter = new Tuple2<>(newKey, c.getMetaData());
                            iterator.add(curCounter);
                        }
                    }
                }
            }
        }
        if (metaDataList.size() < 1002) {
            return metaDataList;
        } else {
            return metaDataList.subList(0, 1001);
        }
    }

    public static final AtomicBoolean KEEP_DELETE_SCANING = new AtomicBoolean();

    static final Map<String, Set<ComponentRecord.Type>> SCANNING_DELETE_TASK_MAP = new ConcurrentHashMap<>();

    /**
     * 定时扫描已被删除的task列表，将rocksdb中还没处理的record进行删除
     */
    public void scanDeleteTaskSet() {
        COMP_TIMER.scheduleAtFixedRate(() -> {
            AtomicBoolean isRunning = new AtomicBoolean();
            //主站点扫描
            if (!"master".equals(Utils.getRoleState())) {
                KEEP_DELETE_SCANING.compareAndSet(true, false);
                return;
            }

            if (!isRunning.compareAndSet(false, true)) {
                return;
            }
            KEEP_DELETE_SCANING.compareAndSet(false, true);

            POOL.getReactive(REDIS_POOL_INDEX).smembers(COMPONENT_TASK_DELETE_SET)
                    .publishOn(COMP_SCHEDULER)
                    .doFinally(s -> isRunning.compareAndSet(true, false))
                    .map(JSON::parseObject)
                    .subscribe(deleteTask -> {
                        String bucketName = deleteTask.getString("bucket");
                        String marker = deleteTask.getString("marker");
                        String lockKey = bucketName + marker;
                        if (!SCANNING_DELETE_TASK_MAP.containsKey(lockKey)) {
                            SCANNING_DELETE_TASK_MAP.put(lockKey, new ConcurrentHashSet<>());
                        }
                        for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
                            deleteRecord(type, deleteTask);
                        }
                    });
        }, 1, 60, TimeUnit.MINUTES);
    }

    public void deleteRecord(ComponentRecord.Type type, JSONObject deleteTask) {
        String bucketName = deleteTask.getString("bucket");
        String taskMarker = deleteTask.getString("marker");
        String lockKey = bucketName + taskMarker;
        synchronized (SCANNING_DELETE_TASK_MAP.get(lockKey)) {
            if (SCANNING_DELETE_TASK_MAP.get(lockKey).contains(type)) {
                log.debug("deleted task is already scanning,bucket is:{},taskMarker is:{}", bucketName, taskMarker);
                return;
            }
            SCANNING_DELETE_TASK_MAP.get(lockKey).add(type);
        }
        log.info("start scan delete task set,bucket is:{},taskMarker is:{}", bucketName, taskMarker);
        try {
            UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String bucketVnode = metaPool.getBucketVnodeId(bucketName);
            TypeReference<Tuple2<String, ComponentRecord>[]> reference = new TypeReference<Tuple2<String, ComponentRecord>[]>() {
            };

            listController.publishOn(COMP_SCHEDULER)
                    .doFinally(v -> {
                        SCANNING_DELETE_TASK_MAP.get(lockKey).remove(type);
                    })
                    .doOnNext(b -> {
                        if (StringUtils.isEmpty(taskMarker)) {
                            listController.onComplete();
                        }
                    })
                    .flatMap(marker -> Mono.just(marker).zipWith(metaPool.mapToNodeInfo(bucketVnode)))
                    .subscribe(tuple2 -> {
                        if (!KEEP_DELETE_SCANING.get()) {
                            listController.onComplete();
                            return;
                        }
                        String marker = tuple2.getT1();
                        List<Tuple3<String, String, String>> nodeList = tuple2.getT2();

                        List<SocketReqMsg> msgs = nodeList.stream().map(info ->
                                new SocketReqMsg("", 0)
                                        .put("bucket", bucketName)
                                        .put("maxKeys", String.valueOf(ListMultiMediaTaskHandler.DELETE_MAX_KEY))
                                        .put("type", type.name())
                                        .put("marker", marker)
                                        .put("lun", MSRocksDB.getComponentRecordLun(info.var2))
                                        .put("taskMarker", taskMarker)
                        ).collect(Collectors.toList());

                        ClientTemplate.ResponseInfo<Tuple2<String, ComponentRecord>[]> responseInfo =
                                ClientTemplate.oneResponse(msgs, LIST_COMPONENT_RECORD, reference, nodeList);
                        ListMultiMediaTaskHandler clientHandler = new ListMultiMediaTaskHandler(responseInfo, nodeList, bucketName);
                        Disposable subscribe = responseInfo.responses.publishOn(COMP_SCHEDULER)
                                .subscribe(clientHandler::handleResponse, listController::onError, clientHandler::deleteHandleComplete);
                        UnicastProcessor<Integer> recordIndexSignal = UnicastProcessor.create();
                        AtomicLong count = new AtomicLong(0L);
                        clientHandler.listProcessor
                                .subscribe(recordList -> {
                                    //使用index进行流量控制，防止删除时并发过高
                                    recordIndexSignal.publishOn(COMP_SCHEDULER)
                                            .subscribe(index -> {
                                                if (index >= recordList.size()) {
                                                    recordIndexSignal.onComplete();
                                                    if (recordList.size() == 0) {
                                                        listController.onComplete();
                                                    }
                                                    return;
                                                }
                                                Mono.just(recordList.get(index))
                                                        .doFinally(b -> {
                                                            recordIndexSignal.onNext(index + 1);
                                                        })
                                                        .flatMap(record -> ComponentUtils.deleteComponentRecord(record))
                                                        .subscribe(t2 -> {
                                                            long curCount = count.addAndGet(1);
                                                            if (curCount == clientHandler.getCount()) {
                                                                if (clientHandler.getCount() < ListMultiMediaTaskHandler.DELETE_MAX_KEY) {
                                                                    listController.onComplete();
                                                                } else {
                                                                    listController.onNext(clientHandler.getNextMarker());
                                                                }
                                                            }
                                                        });

                                            });
                                    recordIndexSignal.onNext(0);
                                });
                    }, log::error, () -> {
                        Mono.just(true).publishOn(COMP_SCHEDULER).subscribe(b -> {
                            POOL.getShortMasterCommand(REDIS_POOL_INDEX).srem(COMPONENT_TASK_DELETE_SET, deleteTask.toJSONString());
                            listController.onComplete();
                        });
                    });
            listController.onNext("");
        } catch (MsException e) {
            SCANNING_DELETE_TASK_MAP.get(lockKey).remove(type);
        }
    }

}
