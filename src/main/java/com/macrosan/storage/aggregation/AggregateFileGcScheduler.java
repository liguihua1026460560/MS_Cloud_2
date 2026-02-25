package com.macrosan.storage.aggregation;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.aggregator.CompactionAggregateContainerWrapper;
import com.macrosan.storage.aggregation.manager.CompactionContainerManager;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import com.macrosan.storage.aggregation.transaction.UndoLogHandler;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ColumnFamilyHandle;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

@Log4j2
public class AggregateFileGcScheduler implements Runnable {

    public static final Scheduler GC_SCHEDULER;
    private final static ThreadFactory GC_THREAD_FACTORY = new MsThreadFactory("aggregate-gc");

    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(16, 1, GC_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        GC_SCHEDULER = scheduler;
    }

    public static final ConcurrentHashMap<String, AggregateFileGcScheduler> gcSchedulerMap = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, UndoLogHandler> undoLogHandlerMap = new ConcurrentHashMap<>();

    public static synchronized void start(NameSpace nameSpace) {
        if (!gcSchedulerMap.containsKey(nameSpace.getNameSpaceIdentifier())) {
            AggregateFileGcScheduler gcScheduler = new AggregateFileGcScheduler(nameSpace);
            GC_SCHEDULER.schedule(gcScheduler, 30, TimeUnit.SECONDS);
            gcSchedulerMap.put(nameSpace.getNameSpaceIdentifier(), gcScheduler);
            log.info("The initialization of the namespace {} GC task was successful!", nameSpace.getNameSpaceIdentifier());
        } else {
            AggregateFileGcScheduler gcScheduler = gcSchedulerMap.get(nameSpace.getNameSpaceIdentifier());
            if (gcScheduler.getNameSpace() != nameSpace) {
                gcScheduler.setNameSpace(nameSpace);
                log.info("The initialization of the namespace {} GC task was successful!", nameSpace.getNameSpaceIdentifier());
            }
        }
        if (!undoLogHandlerMap.containsKey(nameSpace.getNameSpaceIdentifier())) {
            UndoLogHandler logHandler = new UndoLogHandler(nameSpace);
            GC_SCHEDULER.schedule(logHandler, 30, TimeUnit.SECONDS);
            undoLogHandlerMap.put(nameSpace.getNameSpaceIdentifier(), logHandler);
            log.info("The initialization of the namespace {} undo log handler was successful!", nameSpace.getNameSpaceIdentifier());
        } else {
            UndoLogHandler logHandler = undoLogHandlerMap.get(nameSpace.getNameSpaceIdentifier());
            if (logHandler.getNameSpace() != nameSpace) {
                logHandler.setNameSpace(nameSpace);
                log.info("The initialization of the namespace {} undo log handler was successful!", nameSpace.getNameSpaceIdentifier());
            }
        }
    }

    private final static RedisConnPool redisConnPool = RedisConnPool.getInstance();

    @Setter
    @Getter
    private NameSpace nameSpace;
    public static final int MAX_QUEUE_SIZE = 1000;
    public static final int MIN_QUEUE_SIZE = 1;
    String node = ServerConfig.getInstance().getHostUuid();
    AtomicBoolean locked = new AtomicBoolean();
    String lockKey;
    String runKey;
    CompactionContainerManager activeAggregateContainerManager;

    AggregateFileGcScheduler(NameSpace nameSpace) {
        this.nameSpace = nameSpace;
        this.lockKey = nameSpace.getNameSpaceIdentifier() + "_compact";
        this.runKey = nameSpace.getNameSpaceIdentifier() + "_compact_run";
        this.activeAggregateContainerManager = CompactionContainerManager.getInstance();
        initProcessor();
    }

    UnicastProcessor<Tuple2<String, byte[]>> checkProcessor; // 检查聚合文件的空洞率，过滤聚合文件中不存在的小文件
    UnicastProcessor<Tuple2<String, AggregateFileMetadata>> deleteProcessor; // 删除聚合文件及其数据块
    UnicastProcessor<Tuple2<String, AggregateFileMetadata>> aggregationProcessor; // 对聚合文件中的小文件进行二次聚合
    UnicastProcessor<String> ackProcessor; // 用于删除队列中的消息

    Map<String, String> taskQueue = new ConcurrentHashMap<>(); // 保存待处理聚合文件的key
    Map<String, Set<String>> aggregationMap = new ConcurrentHashMap<>(); // 保存待聚合文件的key
    Map<String, AggregateFileMetadata> waitDeleteMap = new ConcurrentHashMap<>(); // 保存待删除的聚合文件
    Set<CompactionAggregateContainerWrapper> waitingAggregationContainer = Collections.newSetFromMap(new ConcurrentHashMap<>());

    Map<String, String> keyLunMap = new ConcurrentHashMap<>();

    @Override
    public void run() {
        if (canRun() && tryGetLock()) {
            try {
                long start = System.nanoTime();
                List<Tuple2<String, byte[]>> list = scan();
                int n = list.size();
                long time = (System.nanoTime() - start) / 1000_000L;
                if (n < MIN_QUEUE_SIZE) {
                    log.debug("【scan】 Scanning {} data from namespace {} takes {} millis. Procedure", n, nameSpace.getNameSpaceIdentifier(), time);
                    return;
                }
                log.debug("【scan】Scanning {} data from namespace {} takes {} millis. Procedure", n, nameSpace.getNameSpaceIdentifier(), time);
                for (Tuple2<String, byte[]> t : list) {
                    taskQueue.put(t.var1, "");
                    checkProcessor.onNext(t);
                }
                // 等待任务结束
                while (!taskQueue.isEmpty()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }

                // 尝试将所有容器进行下刷处理
                // todo 此处应当判断其聚合度，决定没满得聚合容器是选择继续下刷还是选择回写到缓存池中
                for (CompactionAggregateContainerWrapper wrapper : waitingAggregationContainer) {
                    wrapper.flush(true).doOnError(e -> {
                        if (!(e instanceof IllegalStateException)) {
                            log.info("", e);
                            wrapper.clear();
                        }
                    }).doOnSuccess(v -> wrapper.clear()).subscribe();
                }

                // 等待容器全部下刷完毕
                while (!waitingAggregationContainer.isEmpty()) {
                    try {
                        // 移除空容器，以及不满足下刷条件的容器
                        Iterator<CompactionAggregateContainerWrapper> iterator = waitingAggregationContainer.iterator();
                        while (iterator.hasNext()) {
                            CompactionAggregateContainerWrapper next = iterator.next();
                            if (!next.isFlushing() && (next.isEmpty() || !next.isImmutable())) {
                                next.clear(); // 清空容器
                                iterator.remove();
                            }
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                // 根据下刷的结果处理旧的聚合文件
                if (!aggregationMap.isEmpty()) {
                    Iterator<Map.Entry<String, Set<String>>> iterator = aggregationMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, Set<String>> entry = iterator.next();
                        String k = entry.getKey();
                        Set<String> v = entry.getValue();
                        if (v == null || v.isEmpty()) {
                            AggregateFileMetadata remove = waitDeleteMap.remove(k);
                            if (v != null && remove != null) {
                                taskQueue.put(k, "");
                                deleteProcessor.onNext(new Tuple2<>(k, remove));
                            }
                            iterator.remove(); // 安全地移除当前元素
                        } else {
                            // 如果 v 不为空，不需要清空 v，因为已经移除了该条目
                            iterator.remove();
                        }
                    }
                }
                // 等待任务结束
                while (!taskQueue.isEmpty() || !ackProcessor.isEmpty()) {
                    synchronized (Thread.currentThread()) {
                        try {
                            Thread.currentThread().wait(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                keyLunMap .clear();
                long total = (System.nanoTime() - start) / 1000_000L;
                log.debug("【scan】The reclamation of {} aggregate files in namespace {} has been completed, which takes {} seconds. Procedure", n, nameSpace.getNameSpaceIdentifier(), total);
            } finally {
                try {
                    redisConnPool.getShortMasterCommand(0).del(lockKey); // release lock
                } finally {
                    locked.set(false);
                    GC_SCHEDULER.schedule(this, 30, TimeUnit.SECONDS);
                }
            }
        } else {
            GC_SCHEDULER.schedule(this, 30, TimeUnit.SECONDS);
        }
    }

    public void initProcessor() {
        checkProcessor = UnicastProcessor.create(Queues.<Tuple2<String, byte[]>>unboundedMultiproducer().get());
        checkProcessor.publishOn(DISK_SCHEDULER)
                .flatMap(tuple2 -> {
                    // 开始检查聚合文件的元数据
                    log.debug("【check】Start checking the metadata of the aggregate file:{}", tuple2.var1);
                    String key = tuple2.var1;
                    String value = new String(tuple2.var2);
                    BitSet bitSet = AggregationUtils.deserialize(value);
                    String vnode = Utils.getVnode(key);
                    String aggregationId = key.split("/")[2];
                    return StoragePoolFactory.getMetaStoragePool(aggregationId)
                            .mapToNodeInfo(vnode)
                            .flatMap(nodeList -> AggregateFileClient.getAggregationMeta(nameSpace.getNameSpaceIdentifier(), aggregationId, nodeList))
                            .zipWith(Mono.just(bitSet))
                            .map(t -> new Tuple3<>(t.getT1(), t.getT2(), key))
                            .doOnError(e -> taskQueue.remove(key))
                            .onErrorMap(e -> new MsException(ErrorNo.UNKNOWN_ERROR, e.getMessage()));
                }, 1)
                .flatMap(tuple3 -> {
                    AggregateFileMetadata meta = tuple3.var1;
                    BitSet bitSet = tuple3.var2;
                    String key = tuple3.var3;
                    if (meta.equals(AggregateFileMetadata.NOT_FOUND_AGGREGATION_META)
                            || meta.equals(AggregateFileMetadata.ERROR_AGGREGATION_META)) {
                        taskQueue.remove(key);
                        if (meta.equals(AggregateFileMetadata.NOT_FOUND_AGGREGATION_META)) {
                            ackProcessor.onNext(key);
                        }
                        log.debug("【check】The key:{} metadata detection result is:{}", key, meta);
                        return Mono.just(true);
                    }
                    if (meta.deleteMark) {
                        log.debug("【check】The key:{} metadata detection result is deleteMark", key);
                        deleteProcessor.onNext(new Tuple2<>(key, meta));
                        return Mono.just(true);
                    }
                    long holeSize = 0;
                    ArrayList<String> keys = new ArrayList<>(Arrays.asList(meta.getSegmentKeys()));
                    for (int i = 0; i < meta.getDeltaOffsets().length; i++) {
                        if (!bitSet.get(i)) {
                            long len = i == meta.getDeltaOffsets().length - 1 ? meta.getFileSize() - meta.getDeltaOffsets()[i] : meta.getDeltaOffsets()[i + 1] - meta.getDeltaOffsets()[i];
                            holeSize += len;
                            keys.remove(meta.getSegmentKeys()[i]);
                        }
                    }
                    String[] newKeys = keys.toArray(new String[0]);
                    meta.setSegmentKeys(newKeys);
                    double holeRate = (double) holeSize / meta.getFileSize();
                    if (holeRate >= 1.0) {
                        // 聚合文件的空洞超超过100%
                        log.debug("【check】The key:{} hole of the aggregate file exceeds 100%", key);
                        deleteProcessor.onNext(new Tuple2<>(key, meta));
                    } else {
                        log.debug("【check】The key:{} hole rate of the aggregate file is:{}", key, holeRate);
                        aggregationProcessor.onNext(new Tuple2<>(key, meta));
                    }
                    return Mono.just(true);
                })
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false)
                .subscribe();

        deleteProcessor = UnicastProcessor.create(Queues.<Tuple2<String, AggregateFileMetadata>>unboundedMultiproducer().get());
        deleteProcessor.publishOn(DISK_SCHEDULER)
                .flatMap(tuple2 -> {
                    log.debug("【delete】Start deleting the aggregate file:{}", tuple2.var1);
                    String key = tuple2.var1;
                    AggregateFileMetadata meta = tuple2.var2;
                    meta.setDeleteMark(true);
                    meta.setVersionNum(VersionUtil.getVersionNum());
                    meta.setSegmentKeys(null);
                    meta.setDeltaOffsets(null);
                    String vnode = Utils.getVnode(key);
                    String aggregationKey = AggregationUtils.getAggregationKey(vnode, nameSpace.getNameSpaceIdentifier(), meta.aggregationId);
                    return StoragePoolFactory.getMetaStoragePool(meta.aggregationId)
                            .mapToNodeInfo(vnode)
                            .flatMap(nodeList -> AggregateFileClient.putAggregationMeta(aggregationKey, meta, nodeList, null))
                            .flatMap(b -> {
                                if (b) {
                                    return ErasureClient.deleteObjectFile(StoragePoolFactory.getStoragePool(meta), new String[]{meta.fileName}, null);
                                }
                                return Mono.just(false);
                            })
                            .doOnNext(b -> log.debug("【delete】 key:{} {} res:{}", key, meta.fileName, b))
                            .onErrorReturn(false)
                            .doOnNext(b -> {
                                if (b) {
                                    ackProcessor.onNext(key);
                                }
                            })
                            .doFinally(s -> taskQueue.remove(key));
                }, 1)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false)
                .subscribe();

        aggregationProcessor = UnicastProcessor.create(Queues.<Tuple2<String, AggregateFileMetadata>>unboundedMultiproducer().get());
        aggregationProcessor.publishOn(DISK_SCHEDULER)
                .flatMap(tuple2 -> {
                    log.debug("【aggregation】Start aggregation file:{}", tuple2.var1);
                    String key = tuple2.var1;
                    AggregateFileMetadata metadata = tuple2.var2;
                    String[] k2 = metadata.getSegmentKeys();
                    return Flux.fromArray(k2)
                            .flatMap(k -> {
                                // bucket-1/e/file-2838/null
                                int startIndex = k.indexOf("/");
                                int endIndex = k.lastIndexOf("/");
                                String bucket = k.substring(0, startIndex);
                                String object = k.substring(startIndex + 1, endIndex);
                                String version = k.substring(endIndex + 1);
                                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                                String bucketVnodeId = storagePool.getBucketVnodeId(bucket, object);
                                return storagePool.mapToNodeInfo(bucketVnodeId)
                                        .flatMap(nodeList -> ErasureClient.getObjectMetaVersionRes(bucket, object, version, nodeList, null, null, null))
                                        .flatMap(res -> {
                                            // 若对象元数据查询失败，跳过处理，不删除原始聚合文件和数据块
                                            if (res.var1.equals(MetaData.ERROR_META)) {
                                                return Mono.just(0);
                                            }
                                            // 若对象元数据查询成功，但对象元数据与原始聚合文件元数据不一致，说明小对象已被删除或者覆盖，跳过处理，可删除原始聚合文件和数据块
                                            if (res.var1.equals(MetaData.NOT_FOUND_META) || res.var1.deleteMark
                                                    || res.var1.fileName == null ||!res.var1.fileName.equals(metadata.fileName)) {
                                                return Mono.just(1);
                                            }
                                            // 获取当前命名空间活跃的聚合容器
                                            CompactionAggregateContainerWrapper activeContainer = activeAggregateContainerManager.getActiveContainer(nameSpace);
                                            if (activeContainer == null) {
                                                return Mono.just(0); // 聚合容器不存在，跳过处理，不删除原始聚合文件和数据块
                                            }
                                            waitingAggregationContainer.add(activeContainer);
                                            // 小文件聚合成功之后执行回调
                                            Runnable runnable = () -> aggregationMap.computeIfAbsent(key, k1 -> new ConcurrentHashSet<>()).remove(k);
                                            // 追加到聚合容器中进行二次聚合
                                            return activeContainer.append(k, new Tuple3<>(res.var1, res.var2, runnable))
                                                    .doOnNext(b -> {
                                                        if (b) {
                                                            // 等待聚合
                                                            aggregationMap.computeIfAbsent(key, e -> new ConcurrentHashSet<>()).add(k);
                                                        }
                                                    })
                                                    .flatMap(b -> b ? Mono.just(2) : Mono.just(0))
                                                    .onErrorReturn(0);
                                        });

                            }, 4)
                            .distinct()
                            .collectList()
                            .flatMap(list -> {
                                if (list.contains(0)) {
                                    taskQueue.remove(key); // skip;
                                    aggregationMap.remove(key); // skip;
                                    waitDeleteMap.remove(key);
                                } else if (list.size() == 1 && list.get(0) == 1) {
                                    // 删除聚合文件，并删除数据块
                                    deleteProcessor.onNext(new Tuple2<>(key, metadata));
                                } else {
                                    // 等待聚合完毕之后删除聚合文件，并删除数据块
                                    waitDeleteMap.put(key, metadata);
                                    taskQueue.remove(key);
                                }
                                return Mono.just(true);
                            })
                            .doOnError(e -> {
                                log.error("aggregation error {}", key, e);
                                taskQueue.remove(key);
                            })
                            .onErrorReturn(false);
                }, 1)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false)
                .subscribe();


        ackProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        ackProcessor
                .publishOn(DISK_SCHEDULER)
                .flatMap(key -> {
                    log.debug("【ACK】key:{}", key);
                    String lun = keyLunMap.remove(key);
                    String vnode = Utils.getVnode(key);
                    List<Tuple3<String, String, String>> list = StoragePoolFactory.getMetaStoragePool("").mapToNodeInfo(vnode).block();
                    boolean contains = StringUtils.isEmpty(lun);
                    for (Tuple3<String, String, String> tuple3 : list) {
                        if (tuple3.var2.equals(lun)) {
                            contains = true;
                            break;
                        }
                    }
                    BatchRocksDB.RequestConsumer consumer;
                    if (!contains) {
                        consumer = (db, w, r) -> {
                            try {
                                w.delete(MSRocksDB.getColumnFamily(lun, MSRocksDB.ColumnFamilyEnum.AGGREGATION_GC_FAMILY.getName()), key.getBytes());
                            } catch (Exception ignored) {
                            }
                        };
                    } else {
                        consumer = null;
                    }
                    return Mono.just(list)
                            .flatMap(nodeList -> ErasureClient.deleteRocksKey(key, MSRocksDB.IndexDBEnum.AGGREGATE_DB, MSRocksDB.ColumnFamilyEnum.AGGREGATION_GC_FAMILY.getName(), nodeList))
                            .flatMap(b -> {
                                if (consumer != null) {
                                    return BatchRocksDB.customizeOperateData(lun, consumer)
                                            .map(b1 -> true)
                                            .onErrorReturn(false);
                                }
                                return Mono.just(true);
                            });
                })
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false)
                .subscribe();
    }

    public List<Tuple2<String, byte[]>> scan() {
        String prefix = ROCKS_OBJ_META_DELETE_MARKER + AggregationUtils.convertNameSpace(nameSpace.getNameSpaceIdentifier()) + "/";
        String[] disks = getCurNodePoolDisk();
        List<Tuple2<String, byte[]>> list = new ArrayList<>();
        for (String disk : disks) {
            int n = 0;
            String lun = MSRocksDB.getAggregateLun(disk);
            ColumnFamilyHandle columnFamilyHandle = MSRocksDB.getColumnFamily(lun, MSRocksDB.ColumnFamilyEnum.AGGREGATION_GC_FAMILY.getName());
            try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(columnFamilyHandle)) {
                iterator.seek(prefix.getBytes());
                while (iterator.isValid() && n < MAX_QUEUE_SIZE) {
                    String key = new String(iterator.key());
                    if (!key.startsWith(prefix)) {
                        break;
                    }
                    list.add(new Tuple2<>(key, iterator.value()));
                    keyLunMap .put(key, lun);
                    n++;
                    iterator.next();
                }

            }
        }
        return list;
    }

    protected String[] getCurNodePoolDisk() {
        List<String> list = new LinkedList<>();
        StoragePool pool = StoragePoolFactory.getMetaStoragePool("");
        for (String lun : pool.getCache().lunSet) {
            String node = lun.split("@")[0];
            if (this.node.equalsIgnoreCase(node)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    list.add(lun.split("@")[1]);
                }
            }
        }

        return list.toArray(new String[0]);
    }

    protected boolean canRun() {
        try {
            String lastRunNode = redisConnPool.getShortMasterCommand(0).get(this.runKey);
            if (StringUtils.isBlank(lastRunNode)) {
                return true;
            }

            List<String> totalNodes = redisConnPool.getCommand(REDIS_NODEINFO_INDEX).keys("*");
            totalNodes.sort(String::compareTo);
            int index = -1;
            int lastRun = -1;
            int i = 0;
            for (String node : totalNodes) {
                if (node.equalsIgnoreCase(this.node)) {
                    index = i;
                }

                if (node.equalsIgnoreCase(lastRunNode)) {
                    lastRun = i;
                }

                i++;
            }

            i = (lastRun + 1) >= totalNodes.size() ? 0 : lastRun + 1;

            while (i != index) {
                String serverState = redisConnPool.getCommand(REDIS_NODEINFO_INDEX).hget(totalNodes.get(i), NODE_SERVER_STATE);
                if ("0".equalsIgnoreCase(serverState) || notExistIndexLun(totalNodes.get(i))) {//节点不在线或节点不存在使用的缓存盘
                    i++;
                    if (i >= totalNodes.size()) {
                        i = 0;
                    }
                } else {
                    return false;
                }
            }
            return !notExistIndexLun(this.node);
        } catch (Exception e) {
            return false;
        }
    }

    protected boolean tryGetLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx().ex(30);
            String setKey = redisConnPool.getShortMasterCommand(0).set(this.lockKey, this.node, setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);
            if (res) {
                locked.set(true);
                keepLock();
                redisConnPool.getShortMasterCommand(0).set(this.runKey, this.node);
            }

            return res;
        } catch (Exception e) {
            return false;
        }
    }

    private void keepLock() {
        if (locked.get()) {
            SetArgs setArgs = SetArgs.Builder.xx().ex(30);
            try (StatefulRedisConnection<String, String> tmpConnection =
                         redisConnPool.getSharedConnection(0).newMaster()) {
                RedisCommands<String, String> target = tmpConnection.sync();
                target.watch(lockKey);
                String lockNode = target.get(lockKey);
                if (node.equalsIgnoreCase(lockNode)) {
                    target.multi();
                    target.set(lockKey, this.node, setArgs);
                    target.exec();
                    DISK_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
                } else {
                    locked.set(false);
                }
            } catch (Exception e) {
                DISK_SCHEDULER.schedule(this::keepLock, 10, TimeUnit.SECONDS);
            }
        }
    }

    protected boolean notExistIndexLun(String node) {
        int count = 0;
        for (String lun : StoragePoolFactory.getMetaStoragePool("").getCache().lunSet) {
            String diskNode = lun.split("@")[0];
            if (node.equalsIgnoreCase(diskNode)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    count++;
                }
            }
        }
        if (count == 0) {
            return true;
        }
        return false;
    }
}
