package com.macrosan.ec.error.overwrite;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.quota.QuotaRecorder;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.*;
import reactor.core.publisher.Flux;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;
import static com.macrosan.ec.server.RequestResponseServerHandler.updateCapacityInfo;

@Log4j2
public class OverWriteHandler {
    //是否启用
    public static boolean ASYNC_OVER_WRITE;
    //控制实际删除覆盖对象的并发数
    private static int THREAD_NUM;

    public static volatile boolean OVER_WRITE_HANDLE;

    static {
        try {
            boolean async_over_write = true;
            boolean over_write_handle = true;
            int threadNum = 4;
            Map<String, String> config = RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).hgetall("overwrite_config");
            if (!config.isEmpty()) {

                if ("0".equals(config.getOrDefault("async_over_write", "0"))) {
                    async_over_write = false;
                }

                if (config.containsKey("over_write_handle")) {
                    if ("0".equals(config.get("over_write_handle"))) {
                        over_write_handle = false;
                    }
                }

                if (config.containsKey("threadNum")) {
                    try {
                        threadNum = Integer.parseInt(config.get("threadNum"));
                    } catch (Exception e) {
                    }
                }

                ASYNC_OVER_WRITE = async_over_write;
                // 若启动async_over_write则必须启动over_write_handler
                OVER_WRITE_HANDLE = async_over_write || over_write_handle;
                THREAD_NUM = threadNum;

                if (ASYNC_OVER_WRITE) {
                    log.info("enable async over write");
                }

                if (OVER_WRITE_HANDLE) {
                    log.info("enable over write handler with {} threads", threadNum);
                }

            } else {
                loadPropertyConfig();
            }
        } catch (Exception e) {
            loadPropertyConfig();
        }
        log.info("asyncOverwrite={}, asyncOverHandler={}, threadNum={}", ASYNC_OVER_WRITE, OVER_WRITE_HANDLE, THREAD_NUM);
    }


    private static final Map<String, OverWriteHandler> MAP = new ConcurrentHashMap<>();
    //扫描rocksdb的线程
    private static final MsExecutor SCAN_EXECUTOR = new MsExecutor(1, 1, new MsThreadFactory("overwrite-scan"));
    //处理元数据的线程
    private static final MsExecutor RUN_EXECUTOR;

    static {
        RUN_EXECUTOR = new MsExecutor(THREAD_NUM, 1,
                new MsThreadFactory("overwrite-run"));
    }

    private static void loadPropertyConfig() {
        boolean enable;
        String s = System.getProperty("com.macrosan.overwrite.async", "0");
        if ("0".equals(s)) {
            enable = false;
        } else {
            enable = true;
        }
        ASYNC_OVER_WRITE = enable;

        s = System.getProperty("com.macrosan.overwrite.thread.num");
        int threadNum = 4;
        if (s != null) {
            try {
                threadNum = Integer.parseInt(s);
            } catch (Exception ignored) {
            }
        }
        THREAD_NUM = threadNum;

        s = System.getProperty("com.macrosan.overwrite.handle");
        if ("0".equals(s)) {
            enable = false;
        } else {
            enable = true;
        }

        // 若启动async_over_write则必须启动over_write_handler
        OVER_WRITE_HANDLE = ASYNC_OVER_WRITE || enable;

        if (ASYNC_OVER_WRITE) {
            log.info("enable async over write");
        }

        if (OVER_WRITE_HANDLE) {
            log.info("enable over write handler with {} threads", threadNum);
        }
    }

    private final Map<String, String> runMap = new ConcurrentHashMap<>();
    String lun;
    RocksDB db;
    Options options;
    AtomicBoolean end = new AtomicBoolean(false);
    ConcurrentHashMap<String, List<Tuple3<String, MetaData, MetaData>>> waitMap = new ConcurrentHashMap<>();

    private OverWriteHandler(String lun, RocksDB db, Options options) {
        this.lun = lun;
        this.db = db;
        this.options = new Options(options);
        SCAN_EXECUTOR.schedule(this::scan, 30, TimeUnit.SECONDS);
        for (int i = 0; i < THREAD_NUM; i++) {
            RUN_EXECUTOR.schedule(this::run, 30, TimeUnit.SECONDS);
        }
    }

    public static void start(String lun, Options options) {
        RocksDB db = MSRocksDB.overWriteMap.get(lun);
        OverWriteHandler handler = new OverWriteHandler(lun, db, options);
        MAP.put(lun, handler);
    }

    public static void end(String lun) {
        OverWriteHandler handler = MAP.remove(lun);
        if (null != handler) {
            handler.end();
        }
    }

    private static Tuple2<MetaData, MetaData> getMeta(byte[] value) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == 1) {
                byte[] a = Arrays.copyOfRange(value, 0, i);
                byte[] b = Arrays.copyOfRange(value, i + 1, value.length);

                try {
                    MetaData m1 = Json.decodeValue(new String(a), MetaData.class);
                    MetaData m2 = Json.decodeValue(new String(b), MetaData.class);
                    return new Tuple2<>(m1, m2);
                } catch (Exception e) {

                }
            }
        }

        return null;
    }

    public void end() {
        end.set(true);
        while (!waitMap.isEmpty()) {
            try {
                synchronized (Thread.currentThread()) {
                    Thread.currentThread().wait(10);
                }
            } catch (InterruptedException e) {

            }
        }

        while (!runMap.isEmpty()) {
            try {
                synchronized (Thread.currentThread()) {
                    Thread.currentThread().wait(10);
                }
            } catch (InterruptedException e) {

            }
        }
    }

    public void scan() {
        if (end.get()) {
            return;
        }

        try {
            List<LiveFileMetaData> list = db.getLiveFilesMetaData();
            list.sort(Comparator.comparing(m -> {
                String fileName = m.fileName();
                return Long.parseLong(fileName.substring(1, fileName.indexOf(".")));
            }));

            if (list.size() == 0) {
                //如果存在.log文件就执行flush生成.sst
                long logFileSize = db.getSortedWalFiles().stream().mapToLong(LogFile::sizeFileBytes).sum();
                if (logFileSize > 0) {
                    try (FlushOptions flushOptions = new FlushOptions()) {
                        flushOptions.setWaitForFlush(true)
                                .setAllowWriteStall(false);
                        db.flush(flushOptions);
                    }
                }
            } else {
                for (LiveFileMetaData meta : list) {
                    String path = meta.path() + File.separator + meta.fileName();
                    if (waitMap.containsKey(meta.fileName())) {
                        continue;
                    }

                    if (waitMap.size() > THREAD_NUM * 2) {
                        break;
                    }

                    try (SstFileReader reader = new SstFileReader(options)) {
                        reader.open(path);
                        try (SstFileReaderIterator iterator = reader.newIterator(READ_OPTIONS)) {
                            iterator.seekToFirst();

                            LinkedList<Tuple3<String, MetaData, MetaData>> tuples = new LinkedList<>();
                            while (iterator.isValid()) {
                                Tuple2<MetaData, MetaData> tuple = getMeta(iterator.value());
                                if (null != tuple) {
                                    String key = new String(iterator.key());
                                    String vnode = key.split(File.separator)[1];
                                    tuples.add(new Tuple3<>(vnode, tuple.var1, tuple.var2));
                                }

                                iterator.next();
                            }

                            waitMap.put(meta.fileName(), tuples);
                        }

                    }
                }
            }
        } catch (Exception e) {
            log.error("over write handler error", e);
        } finally {
            SCAN_EXECUTOR.schedule(this::scan, 30, TimeUnit.SECONDS);
        }
    }

    private void run() {
        if (end.get()) {
            waitMap.clear();
            runMap.clear();
        }
        List<String> keys = waitMap.keySet().stream().sorted().collect(Collectors.toList());
        for (String key : keys) {
            if (!runMap.containsKey(key)) {
                synchronized (runMap) {
                    if (runMap.containsKey(key)) {
                        continue;
                    } else {
                        runMap.put(key, "");
                    }
                }

                List<Tuple3<String, MetaData, MetaData>> list = waitMap.get(key);
                try {
                    handle(list);
                    deleteFile(key);
                } catch (Exception e) {
                    log.error("", e);
                    runMap.remove(key);
                }

                break;
            }
        }

        RUN_EXECUTOR.schedule(this::run, 30, TimeUnit.SECONDS);
    }

    private void deleteFile(String key) {
        if (end.get()) {
            waitMap.remove(key);
            runMap.remove(key);
            return;
        }

        try {
            db.deleteFile(key);
        } catch (Exception e) {

        }

        List<String> fileNames = db.getLiveFilesMetaData().stream()
                .map(SstFileMetaData::fileName).collect(Collectors.toList());
        Set<String> keys = new HashSet<>(fileNames);
        if (keys.contains(key)) {
            RUN_EXECUTOR.schedule(() -> deleteFile(key), 30, TimeUnit.SECONDS);
        } else {
            waitMap.remove(key);
            runMap.remove(key);
        }
    }

    private void handle(List<Tuple3<String, MetaData, MetaData>> list) {
        for (Tuple3<String, MetaData, MetaData> tuple : list) {
            MetaData oldMeta = tuple.var2;
            MetaData[] newMeta = new MetaData[]{tuple.var3};
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(newMeta[0].bucket);
            // 获取当前同名对象最新元数据，判断新的元数据是否真正覆盖成功
            if (newMeta[0].inode == 0 && metaStoragePool.getBucketShardCache().contains(newMeta[0].bucket)) {
                String bucketVnodeId = metaStoragePool.getBucketVnodeId(newMeta[0].bucket, newMeta[0].key);
                List<Tuple3<String, String, String>> bucketNodeList = metaStoragePool.mapToNodeInfo(bucketVnodeId).block();
                newMeta[0] = ErasureClient.getObjectMetaVersion(newMeta[0].bucket, newMeta[0].key, newMeta[0].versionId, bucketNodeList, null, newMeta[0].snapshotMark, null, true).timeout(Duration.ofSeconds(30)).block();
                if (newMeta[0] == null || MetaData.ERROR_META.equals(newMeta[0])) {
                    throw new RuntimeException("get bucket " + oldMeta.bucket + " object " + oldMeta.key + " newest metadata error!");
                }
            }
            if (oldMeta.isAvailable()) {
                boolean equals;

                if (oldMeta.fileName != null) {
                    equals = oldMeta.fileName.equals(newMeta[0].fileName) && StringUtils.isEmpty(newMeta[0].duplicateKey);
                } else if (oldMeta.partInfos != null) {
                    if (newMeta[0].partInfos == null) {
                        equals = false;
                    } else {
                        equals = Arrays.equals(oldMeta.partInfos, newMeta[0].partInfos);
                    }
                } else {
                    equals = (newMeta[0].fileName == null) && (newMeta[0].partInfos == null);
                    if (equals && oldMeta.inode > 0 && newMeta[0].inode > 0 && !oldMeta.stamp.equals(newMeta[0].stamp)) {
                        equals = false;
                    }
                }

                if (!equals) {
                    String vnode = tuple.var1;
                    String oldMetaKey = Utils.getMetaDataKey(vnode, oldMeta.bucket, oldMeta.key, oldMeta.versionId, oldMeta.stamp, oldMeta.snapshotMark);
                    String oldLifeKey = Utils.getLifeCycleMetaKey(vnode, oldMeta.bucket, oldMeta.key, oldMeta.versionId, oldMeta.stamp, oldMeta.snapshotMark);
                    List<BatchRocksDB.RequestConsumer> consumers = new LinkedList<>();
                    long newMetaInodeId = newMeta[0].inode;
                    if (!oldMeta.deleteMark) {
                        long capacity;
                        if (oldMeta.snapshotMark != null && oldMeta.partUploadId != null) {
                            long tempCap = 0L;
                            for (PartInfo partInfo : oldMeta.partInfos) {
                                if (oldMeta.snapshotMark.equals(partInfo.snapshotMark)) {
                                    tempCap += partInfo.partSize;
                                }
                            }
                            capacity = tempCap;
                        } else {
                            capacity = oldMeta.deleteMarker ? 0 : Utils.getObjectSize(oldMeta);
                        }
                        long objNum = -1;

                        consumers.add((db, writeBatch, request) -> {
                            // 如果旧的对象versionNum比创建桶时的versionNum要小，说明对象为历史同名桶中的对象，不对现有桶进行容量更新操作，防止对象数以及容量出现负数的情况
                            Long exists = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).exists(oldMeta.bucket);
                            String versionNum = RedisConnPool.getInstance().getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hget(oldMeta.bucket, "version_num");
                            if (exists == 0 || (StringUtils.isNotEmpty(versionNum) && oldMeta.versionNum != null && oldMeta.versionNum.compareTo(versionNum) < 0)) {
                                return;
                            }
                            long oldCapacity = -1;
                            int uid = 0;
                            int gid = 0;
                            if (oldMeta.getInode() > 0) {
                                String oldKey = Inode.getKey(vnode, oldMeta.bucket, oldMeta.getInode());
                                byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, oldKey.getBytes());
                                if (oldValue != null) {
                                    Inode oldInode = Json.decodeValue(new String(oldValue), Inode.class);
                                    if (oldInode.getLinkN() <= 1) {
                                        oldCapacity = oldInode.getSize();
                                        uid = oldInode.getUid();
                                        gid = oldInode.getGid();
                                    }
                                }
                            }
                            updateCapacityInfo(writeBatch, oldMeta.getBucket(), oldMeta.getKey(),vnode, objNum, oldCapacity == -1 ? -capacity : -oldCapacity);
                            String quotaKeys = FSQuotaUtils.getQuotaKeys(oldMeta.bucket, oldMeta.key, System.currentTimeMillis(), uid, gid);
                            FSQuotaUtils.updateAllKeyCap(quotaKeys, writeBatch, oldMeta.bucket, vnode, objNum, oldCapacity == -1 ? -capacity : -oldCapacity);
                            QuotaRecorder.addCheckBucket(oldMeta.bucket);
                        });
                    }

                    Inode[] oldInode = new Inode[1];
                    String[] oldInodeKey = new String[1];
                    if (oldMeta.getInode() > 0) {
                        oldInodeKey[0] = Inode.getKey(vnode, oldMeta.bucket, oldMeta.getInode());
                        consumers.add((db, writeBatch, request) -> {
                            byte[] oldInodeValue = writeBatch.getFromBatchAndDB(db, oldInodeKey[0].getBytes());
                            if (null != oldInodeValue) {
                                oldInode[0] = Json.decodeValue(new String(oldInodeValue), Inode.class);
                                if (StringUtils.isEmpty(oldMeta.fileName)) {
                                    Inode.mergeMeta(oldMeta, oldInode[0]);
                                }
                                if (newMetaInodeId > 0) {
                                    String newInodeKey = Inode.getKey(vnode, oldMeta.bucket, newMetaInodeId);
                                    byte[] newInodeValue = writeBatch.getFromBatchAndDB(db, newInodeKey.getBytes());
                                    if (newInodeValue != null) {
                                        Inode newMetaInode = Json.decodeValue(new String(newInodeValue), Inode.class);
                                        Inode.mergeMeta(newMeta[0], newMetaInode);
                                    }
                                }
                            }
                        });
                    }

//                    MetaData delMeta = oldMeta;
                    // 根据moss.cc，syncStamp大的会被放在newMeta。syncStamp为老格式则仍由versionNum判断
                    if (!oldMeta.stamp.equals(newMeta[0].stamp)) {
                        consumers.add((db, writeBatch, request) -> {
                            writeBatch.delete(oldLifeKey.getBytes());
                            writeBatch.delete(oldMetaKey.getBytes());
                            if (null != oldInode[0] && oldInode[0].getNodeId() > 0) {
                                // 删inode，仅当无硬链接时可删
                                if (oldInode[0].getLinkN() <= 1 && oldInode[0].getNodeId() != newMeta[0].inode) {
                                    writeBatch.delete(oldInodeKey[0].getBytes());
                                }
                                // 删cookie，每删掉一个metaData，删一次cookie
                                if (oldMeta.cookie > 0) {
                                    String oldCookieKey = Inode.getCookieKey(vnode, oldMeta.bucket, oldMeta.cookie);
                                    writeBatch.delete(oldCookieKey.getBytes());
                                }
                            }
                        });
                    }

                    if (!consumers.isEmpty()) {
                        String latestKey = Utils.getLatestMetaKey(vnode, oldMeta.bucket, oldMeta.key, oldMeta.snapshotMark);
                        BatchRocksDB.customizeOperateMeta(lun, latestKey.hashCode(), (db, writeBatch, request) -> {
                            // 判断覆盖操作是否已经执行 TODO oldMeta.stamp和newMeta.stamp相同时结果不准确
                            // 不判断lifekey是否存在会有一次对象覆盖减少多次容量的问题，导致容量统计不准
                            byte[] value = writeBatch.getFromBatchAndDB(db, oldLifeKey.getBytes());
                            boolean isUpdate = value != null;

                            if (isUpdate) {
                                for (BatchRocksDB.RequestConsumer consumer : consumers) {
                                    consumer.accept(db, writeBatch, request);
                                }
                            }
                        }).block();
                    }

                    StoragePool pool = StoragePoolFactory.getStoragePool(oldMeta);
                    List<String> overWriteFiles = new LinkedList<>();
                    List<String> overwriteAggregateFiles = new LinkedList<>();
                    List<String> overWriteDedupFiles = new LinkedList<>();

                    boolean isInodeDataNotAdd = true;
                    if (oldMeta.fileName != null) {
                        if (StringUtils.isNotEmpty(oldMeta.aggregationKey)) {
                            overwriteAggregateFiles.add(oldMeta.aggregationKey);
                        } else if (StringUtils.isEmpty(oldMeta.duplicateKey)) {
                            // 判断当前被覆盖数据是否含有inode，如果没有按s3情况删，如果有则按inodeData删，并且不再按照partInfos删
                            if (null != oldInode[0] && oldInode[0].getNodeId() > 0) {
                                List<Inode.InodeData> inodeData = oldInode[0].getInodeData();
                                if (null != inodeData && inodeData.size() > 0 && oldInode[0].getLinkN() <= 1) {
                                    for (Inode.InodeData inodeDatum : inodeData) {
                                        overWriteFiles.add(inodeDatum.getFileName());
                                        isInodeDataNotAdd = false;
                                    }
                                }
                            } else {
                                overWriteFiles.add(oldMeta.fileName);
                            }
                        } else {
                            overWriteDedupFiles.add(oldMeta.duplicateKey);
                        }

                    }
                    //硬链接被同名覆盖，不删除
                    if (null != oldInode[0] && oldInode[0].getNodeId() > 0 && oldInode[0].getLinkN() > 1) {
                        isInodeDataNotAdd = false;
                    }
                    if (oldMeta.partInfos != null && isInodeDataNotAdd) {
                        for (PartInfo partInfo : oldMeta.getPartInfos()) {
                            if (newMeta[0].partInfos != null) {
                                List<String> newMetaFileNames = Arrays.stream(newMeta[0].getPartInfos()).map(partInfo1 -> partInfo1.fileName).collect(Collectors.toList());
                                if (newMetaFileNames.contains(partInfo.fileName)) {
                                    continue;
                                }
                            }
                            if (StringUtils.isEmpty(partInfo.deduplicateKey)) {
                                overWriteFiles.add(partInfo.fileName);
                            } else {
                                overWriteDedupFiles.add(partInfo.deduplicateKey);
                            }
                        }
                    }

                    if (!overwriteAggregateFiles.isEmpty()) {
                        Flux.fromIterable(overwriteAggregateFiles)
                                .flatMap(file -> {
                                    String v = Utils.getVnode(file);
                                    return metaStoragePool.mapToNodeInfo(v)
                                            .flatMap(aNodeList -> AggregateFileClient.freeAggregationSpace(file, aNodeList));
                                })
                                .collectList()
                                .block();
                    } else if (overWriteDedupFiles.isEmpty()) {
                        String[] fileArray = overWriteFiles.toArray(new String[0]);
                        // 如果inode已被删，fileArray=[""]，则直接返回避免报错
                        if (oldMeta.getInode() > 0 && fileArray.length == 1 && fileArray[0].equals("")) {
                            return;
                        }
                        if (pool != null) {
                            ErasureClient.deleteObjectFile(pool, fileArray, null).block();
                        }
                    } else {
                        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(oldMeta.bucket);
                        String[] fileArray = overWriteDedupFiles.toArray(new String[0]);

                        ErasureClient.deleteDedupObjectFile(metaPool, fileArray, null, false).block();
                        if (!overWriteFiles.isEmpty()) {
                            String[] fileArrays = overWriteFiles.toArray(new String[0]);
                            if (pool != null) {
                                ErasureClient.deleteObjectFile(pool, fileArrays, null).block();
                            }
                        }
                    }
                }
            }
        }
    }

}
