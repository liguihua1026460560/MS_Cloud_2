package com.macrosan.database.rocksdb;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.ec.error.overwrite.OverWriteHandler;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.ec.rebuild.RebuildCheckpointUtil;
import com.macrosan.ec.server.LocalMigrateServer;
import com.macrosan.ec.server.MigrateServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsException;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;
import reactor.core.publisher.Mono;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.IndexDBEnum.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.fs.BlockDevice.ROCKS_FILE_SYSTEM_PREFIX_OFFSET;
import static com.macrosan.storage.PoolHealth.recoverLun;
import static org.rocksdb.Priority.HIGH;
import static org.rocksdb.Priority.LOW;
import static org.rocksdb.RateLimiter.DEFAULT_REFILL_PERIOD_MICROS;
import static org.rocksdb.ThreadType.HIGH_PRIORITY;
import static org.rocksdb.ThreadType.LOW_PRIORITY;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class MSRocksDB {
    public static Map<String, MSRocksDB> dbMap = new ConcurrentHashMap<>();
    public static Map<String, RocksDB> overWriteMap = new ConcurrentHashMap<>();
    public static String UnsyncRecordDir = "rocks_db_sync";
    public static String MultiMediaRecordDir = "rocks_db_media";
    public static String RabbitmqRecordDir = "rocks_db_rabbitmq";
    public static String STSTokenDBDir = "rocks_db_STS";

    private static final Map<String, Map<String, ColumnFamilyHandle>> cfHandleMap = new ConcurrentHashMap<>();
    public static final WriteOptions WRITE_OPTIONS;
    public static final ReadOptions READ_OPTIONS;

    private AtomicBoolean close = new AtomicBoolean(false);
    @Getter
    private RocksDB rocksDB;
    private RocksDB checkPointDB;

    protected MSRocksDB(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    protected MSRocksDB(RocksDB checkPointDB, boolean check) {
        if (check) {
            this.checkPointDB = checkPointDB;
        } else {
            this.rocksDB = checkPointDB;
        }
    }

    static {
        RocksDB.loadLibrary();
        WRITE_OPTIONS = new WriteOptions().setSync(true);
        READ_OPTIONS = new ReadOptions();
    }

    public static void init() {
        String node = ServerConfig.getInstance().getHostUuid();
        List<String> lunList = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).keys(node + "@fs*");
        lunList = lunList.stream().map(key -> key.split("@")[1]).collect(Collectors.toList());

        lunList.forEach(lun -> {
            try {
                if (DiskStatusChecker.isRebuildWaiter(lun)) {
                    return;
                }
                RocksDB transactionDB = openRocks(lun, "/" + lun + "/rocks_db");

                dbMap.put(lun, new MSRocksDB(transactionDB));
                BatchRocksDB.addBatch(lun, transactionDB);

                if (lun.contains("index")) {
                    RocksDB syncRecordDB = openRocks(getSyncRecordLun(lun), "/" + getSyncRecordLun(lun));
                    dbMap.put(getSyncRecordLun(lun), new MSRocksDB(syncRecordDB));
                    BatchRocksDB.addRecordBatch(getSyncRecordLun(lun), syncRecordDB);

                    // 多媒体处理的rocksDB
                    RocksDB mediaRecordDB = openRocks(getComponentRecordLun(lun), "/" + getComponentRecordLun(lun));
                    dbMap.put(getComponentRecordLun(lun), new MSRocksDB(mediaRecordDB));
                    BatchRocksDB.addRecordBatch(getComponentRecordLun(lun), mediaRecordDB);

                    //代替rabbitmq处理的rocksDB
                    RocksDB rabbitmqRecordDB = openRocks(getRabbitmqRecordLun(lun), "/" + getRabbitmqRecordLun(lun));
                    dbMap.put(getRabbitmqRecordLun(lun), new MSRocksDB(rabbitmqRecordDB));
                    BatchRocksDB.addRecordBatch(getRabbitmqRecordLun(lun), rabbitmqRecordDB);

                    //存储STS凭证信息的rocksDB
                    RocksDB STSTokenDB = openRocks(getSTSTokenLun(lun), "/" + getSTSTokenLun(lun));
                    dbMap.put(getSTSTokenLun(lun), new MSRocksDB(STSTokenDB));
                    BatchRocksDB.addRecordBatch(getSTSTokenLun(lun), STSTokenDB);

                    // 存储聚合文件元数据信息的rocksDB
                    RocksDB aggregateDB = openRocks(getAggregateLun(lun), "/" + getAggregateLun(lun));
                    dbMap.put(getAggregateLun(lun), new MSRocksDB(aggregateDB));
                    BatchRocksDB.addRecordBatch(getAggregateLun(lun), aggregateDB);
                }
            } catch (Exception e) {
                log.error("load rocks db with " + lun + " fail", e);
            }
        });
        MsRocksMonitor.init();
    }

    private static Set<String> failOpened = new ConcurrentSkipListSet<>();

    private static Map<String, AbstractCompactionFilter<Slice>> compactionFilterMap = new ConcurrentHashMap<>();

    private static RocksDB openRocks(String lun, String path) {
        if (failOpened.contains(path)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "open " + lun + " rocks db fail");
        }
        boolean isRecordDb = false;
        if (path.contains(UnsyncRecordDir) || path.contains(MultiMediaRecordDir) || path.contains(RabbitmqRecordDir)) {
            isRecordDb = true;
        }
        boolean isSTSDb = false;
        if (path.contains(STSTokenDBDir)) {
            isSTSDb = true;
        }
        boolean isAggregateDb = false;
        if (path.contains(IndexDBEnum.AGGREGATE_DB.dir)) {
            isAggregateDb = true;
        }

        long targetFileSize = 1024 * 1024 * 256L;

        Options options = new Options()
                .setCreateIfMissing(true)
                .setKeepLogFileNum(10)
                .setMaxLogFileSize(1L << 30)
                .setMaxBackgroundJobs(8)
                .setMaxSubcompactions(8)
                .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
                .setLevel0FileNumCompactionTrigger(1)
                .setDisableAutoCompactions(false)
                .setTargetFileSizeBase(targetFileSize)
                .setMaxBytesForLevelBase(targetFileSize)
                .setMaxWriteBufferNumber(4)
                .setCreateMissingColumnFamilies(true);

        if (isRecordDb) {
            options.setNumLevels(1);
        }
        if (isSTSDb) {//只在存放凭证信息的rocksdb中生效
            //启用动态压缩
            options.setLevelCompactionDynamicLevelBytes(true);
            RemoveExpiredDataCompactionFilter abstractCompactionFilter = new RemoveExpiredDataCompactionFilter();
            compactionFilterMap.put(lun, abstractCompactionFilter);
            options.setCompactionFilter(abstractCompactionFilter);
            options.setMaxWriteBufferNumber(2);
            try {
                //配置定时压缩
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                Class<?> clazz = loader.loadClass(options.getClass().getSuperclass().getName());
                Field filed = clazz.getDeclaredField("nativeHandle_");
                filed.setAccessible(true);
                long nativeHandle_ = (long) filed.get(options);
                CustomOptions.setPeriodicCompactionSeconds0(nativeHandle_, 3600);//设置每隔一小时compact一次;
            } catch (Exception e) {
                log.error("", e);
            }

        }

//        TransactionDBOptions transactionOptions = new TransactionDBOptions().setDefaultLockTimeout(5000);
        BlockBasedTableConfig config = new BlockBasedTableConfig()
                .setIndexType(IndexType.kTwoLevelIndexSearch)
                .setFilterPolicy(new BloomFilter(10, false));
        options.setTableFormatConfig(config);
        options.setRateLimiter(new RateLimiter(1L << 32, DEFAULT_REFILL_PERIOD_MICROS, 8,
                RateLimiterMode.ALL_IO, true));

        if (path.contains("index")) {
            options.setMaxOpenFiles(4096);
            options.setMaxWriteBufferNumber(8);
        } else {
            options.setMaxOpenFiles(512);
        }

        try {
            File f = new File(path);
            long totalSize = f.getTotalSpace();

            SstFileManager manager = new SstFileManager(options.getEnv());
            long allowed = Math.max(totalSize - (10L << 30), totalSize * 95 / 100);
            manager.setMaxAllowedSpaceUsage(allowed);

            options.setSstFileManager(manager);
            options.setSoftPendingCompactionBytesLimit(-1);
            options.setHardPendingCompactionBytesLimit(-1);

            MossMergeOperator mergeOperator = new MossMergeOperator();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions(options)
                    .setMergeOperator(mergeOperator);

            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>(
                    Arrays.asList(
                            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                            new ColumnFamilyDescriptor(ROCKS_FILE_SYSTEM_PREFIX_OFFSET.getBytes(), columnFamilyOptions),
                            new ColumnFamilyDescriptor(ColumnFamilyEnum.MIGRATE_COLUMN_FAMILY.name.getBytes(), columnFamilyOptions)
                    ));

            if (isAggregateDb) {
                descriptors.add(new ColumnFamilyDescriptor(ColumnFamilyEnum.AGGREGATION_GC_FAMILY.name.getBytes(), columnFamilyOptions));
            }

            List<ColumnFamilyHandle> handles = new ArrayList<>();

            if (path.contains("index") && OverWriteHandler.OVER_WRITE_HANDLE && !isRecordDb && !isSTSDb && !isAggregateDb) {
                try {
                    Options overWriteOptions = new Options(options)
                            .setMaxOpenFiles(512)
                            .setMaxWriteBufferNumber(Integer.MAX_VALUE)
                            .setDisableAutoCompactions(true);
                    if (!f.exists()) {
                        f.mkdir();
                    }
                    RocksDB overWriteDB = RocksDB.open(overWriteOptions, path + "/over_write");
                    Field field = RocksObject.class.getDeclaredField("nativeHandle_");
                    field.setAccessible(true);
                    long db = (long) field.get(overWriteDB);
                    long op = (long) field.get(mergeOperator);
                    long cf = (long) field.get(overWriteDB.getDefaultColumnFamily());
                    overWriteMap.put(lun, overWriteDB);
                    MossMergeOperator.setAvg(op, db, cf);
                    OverWriteHandler.start(lun, overWriteOptions);
                } catch (Exception e) {
                    log.error("", e);
                }
            }

            RocksDB rocksDB = RocksDB.open(new DBOptions(options), path, descriptors, handles);
            Map<String, ColumnFamilyHandle> handleMap = cfHandleMap.getOrDefault(lun, new HashMap<>());
            cfHandleMap.put(lun, handleMap);
            handles.forEach(x -> {
                try {
                    handleMap.put(new String(x.getName()), x);
                } catch (RocksDBException e) {
                    log.info("rocksdb handle:{}", e.getMessage());
                }
            });

            if (isRecordDb || isSTSDb || isAggregateDb) {
                return rocksDB;
            }

            Compaction.startCompact(path.replace("rocks_db", ""), rocksDB, manager, allowed);
            if (path.contains("index")) {
                MsRocksMonitor.addMonitoredPath(lun, manager);
            }
            int high = (int) Env.getDefault().getThreadList().stream()
                    .filter(t -> t.getThreadType() == HIGH_PRIORITY).count();
            int low = (int) Env.getDefault().getThreadList().stream()
                    .filter(t -> t.getThreadType() == LOW_PRIORITY).count();
            if (path.contains("index")) {
                Env.getDefault().setBackgroundThreads(high + 2, HIGH);
                Env.getDefault().setBackgroundThreads(low + 6, LOW);
            } else {
                Env.getDefault().setBackgroundThreads(high + 1, HIGH);
                Env.getDefault().setBackgroundThreads(low + 1, LOW);
            }


            return rocksDB;
        } catch (Exception e) {
            log.error("open rocks db in {} fail ", path, e);
            failOpened.add(path);
            Mono.delay(Duration.ofSeconds(10))
                    .doOnNext(l -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("error_rocksdb_lun", ServerConfig.getInstance().getHostUuid() + "@" + lun))
                    .subscribeOn(DISK_SCHEDULER)
                    .subscribe(l -> failOpened.remove(path));
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "open rocks db fail");
        }
    }

    private static RocksDB openCheckPointRocks(String path) {
        try (MossMergeOperator mergeOperator = new MossMergeOperator();
             Options options = new Options().setMergeOperator(mergeOperator).setCreateIfMissing(true)
                     .setCreateMissingColumnFamilies(true).setMaxOpenFiles(1024);) {

            return RocksDB.openReadOnly(options, path);
        } catch (RocksDBException e) {
            log.error("open check point rocks db in {} fail ", path, e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "open check point rocks db fail");
        }
    }

    public static Set<String> errorLun = new ConcurrentSkipListSet<>();

    void getRocksDBException(RocksDBException e) {
        //Lock TimeOut 异常不放入error lun
        if (e.getMessage().contains("LockTimeout")) {
            return;
        }

        MSRocksDB.dbMap.entrySet().forEach(entry -> {
            if (entry.getValue().equals(this)) {
                errorLun.add(entry.getKey());
            }
        });
    }

    /**
     * 根据盘名获取rocksDB实例。
     */
    public static MSRocksDB getRocksDB(String lun) {
        return getRocksDB(lun, true);
    }

    /**
     * 根据盘名获取rocksDB实例。
     */
    public static MSRocksDB getRocksDB(String lun, boolean migrate) {
        if ((errorLun.contains(lun) && !recoverLun.contains(lun)) || DiskStatusChecker.isRebuildWaiter(lun)) {
            return null;
        }

        MSRocksDB resDB = dbMap.get(lun);
        if (resDB == null) {
            synchronized (MSRocksDB.class) {
                resDB = dbMap.get(lun);
                if (resDB == null) {
                    if (lun.contains(RebuildCheckpointUtil.REBUILD_CHECK_POINT_SUFFIX)) {
                        RocksDB db = openCheckPointRocks("/" + lun);
                        resDB = new MSRocksDB(db, true);
                        dbMap.put(lun, resDB);
                    } else if (lun.contains(UnsyncRecordDir) || lun.contains(MultiMediaRecordDir) || lun.contains(RabbitmqRecordDir)
                            || lun.contains(STSTokenDBDir) || lun.contains(IndexDBEnum.AGGREGATE_DB.dir)) {
                        RocksDB transactionDB = openRocks(lun, "/" + lun);
                        resDB = new MSRocksDB(transactionDB);
                        dbMap.put(lun, resDB);
                        BatchRocksDB.addRecordBatch(lun, transactionDB);
                    } else {
                        RocksDB transactionDB = openRocks(lun, "/" + lun + "/rocks_db");
                        resDB = new MSRocksDB(transactionDB);
                        dbMap.put(lun, resDB);
                        BatchRocksDB.addBatch(lun, transactionDB);
                    }
                }
            }
        }

        if (!migrate) {
            return resDB;
        }

        if (MigrateServer.getInstance().start > 0) {
            if (resDB.rocksDB != null) {
                return new MigrateRocksDB(resDB.rocksDB, lun, false);
            } else {
                return new MigrateRocksDB(resDB.checkPointDB, lun, true);
            }
        }

        if (LocalMigrateServer.getInstance().start > 0) {
            if (resDB.rocksDB != null) {
                return new LocalMigrateRocksDB(lun, resDB.rocksDB, dbMap, false);
            } else {
                return new LocalMigrateRocksDB(lun, resDB.checkPointDB, dbMap, true);
            }

        }

        return resDB;
    }

    /**
     * 移除盘时将rocksDB实例移除。
     */
    public static void remove(String lun) {
        MSRocksDB rocksDB = dbMap.remove(lun);
        if (rocksDB != null && rocksDB.checkPointDB != null) {
            try {
                rocksDB.checkPointDB.closeE();
                log.info("real close {} rocks db", lun);
            } catch (RocksDBException e) {
                log.error("close {} rocks db fail", lun, e);
            }
            return;
        }
        compactionFilterMap.remove(lun);
        BatchRocksDB.remove(lun);
        if (OverWriteHandler.OVER_WRITE_HANDLE) {
            OverWriteHandler.end(lun);
        }
        MsRocksMonitor.remove(lun);
        Compaction.endCompact(lun);

        if (null != rocksDB) {
            Mono.delay(Duration.ofSeconds(10)).publishOn(DISK_SCHEDULER).flatMap(l -> {
                log.info("set {} rocks db close", lun);
                rocksDB.close.set(true);
                return Mono.delay(Duration.ofSeconds(10)).publishOn(DISK_SCHEDULER);
            }).subscribe(l -> {
                try {
                    if (OverWriteHandler.OVER_WRITE_HANDLE && !lun.contains(UnsyncRecordDir) && !lun.contains(STSTokenDBDir)
                            && !lun.contains(IndexDBEnum.AGGREGATE_DB.dir)) {
                        RocksDB overWritedb = overWriteMap.remove(lun);
                        if (overWritedb != null) {
                            try {
                                overWritedb.closeE();
                            } catch (Exception e) {
                                log.error("close {} rocks db fail", lun, e);
                            }
                        }
                    }
                    rocksDB.rocksDB.closeE();
                    log.info("real close {} rocks db", lun);
                } catch (Exception e) {
                    log.error("close {} rocks db fail", lun, e);
                }
            });
        }
    }


    void checkClose() {
        if (this.close.get()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "db is close");
        }
    }

    /**
     * 删除key-value
     */
    public void delete(final byte[] key) throws RocksDBException {
        checkClose();
        try {
            if (key[0] >= '0' && key[0] <= '9') {
                byte[] deleteMarker = Utils.getDeleteMarkKey(key);
                delete(deleteMarker);
            }

            rocksDB.delete(key);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    /**
     * @param range 传入要删除的数据范围
     * @throws RocksDBException
     */
    public void deleteFilesInRangesDefaultColumnFamily(List<byte[]> range, boolean includeEnd) throws RocksDBException {
        checkClose();
        try {
            rocksDB.deleteFilesInRanges(rocksDB.getDefaultColumnFamily(), range, includeEnd);
        } catch (RocksDBException e) {
            log.error("", e);
            getRocksDBException(e);
            throw e;
        }
    }

    /**
     * @param range 传入要删除的数据范围
     * @throws RocksDBException
     */
    public void deleteFilesInRangesMigrateColumnFamily(ColumnFamilyHandle columnFamilyHandle, List<byte[]> range, boolean includeEnd) throws RocksDBException {
        checkClose();
        try {
            rocksDB.deleteFilesInRanges(columnFamilyHandle, range, includeEnd);
        } catch (RocksDBException e) {
            log.error("", e);
            getRocksDBException(e);
            throw e;
        }
    }

    public Snapshot getSnapshot() {
        checkClose();
        return rocksDB.getSnapshot();
    }

    public MSRocksIterator newIterator() {
        checkClose();
        if (checkPointDB != null) {
            return new MSRocksIterator(this, checkPointDB.newIterator());
        }
        return new MSRocksIterator(this, rocksDB.newIterator());
    }

    public MSRocksIterator newIterator(ColumnFamilyHandle columnFamilyHandle) {
        checkClose();
        return new MSRocksIterator(this, rocksDB.newIterator(columnFamilyHandle));
    }

    public MSRocksIterator newIterator(ColumnFamilyHandle columnFamilyHandle, ReadOptions readOptions) {
        checkClose();
        return new MSRocksIterator(this, rocksDB.newIterator(columnFamilyHandle, readOptions));
    }

    public MSRocksIterator newIterator(final ReadOptions readOptions) {
        checkClose();
        return new MSRocksIterator(this, rocksDB.newIterator(readOptions));
    }

    public List<LiveFileMetaData> getLiveFilesMetaData() {
        checkClose();
        return rocksDB.getLiveFilesMetaData();
    }

    public void releaseSnapshot(final Snapshot snapshot) {
        checkClose();
        rocksDB.releaseSnapshot(snapshot);
        snapshot.close();
    }

    /**
     * @param isTailor 是否去除多余的元数据
     * @throws RocksDBException
     */
    public void put(boolean isTailor, byte[] key, byte[] value) throws RocksDBException {
        if (isTailor) {
            put(key, Utils.simplifyMetaJson(value));
        } else {
            put(key, value);
        }
    }

    public void put(byte[] key, final byte[] value) throws RocksDBException {
        checkClose();
        try {
            if (key[0] >= '0' && key[0] <= '9') {
                delete(key);
                if (Utils.isDeleteMarker(value)) {
                    key = Utils.getDeleteMarkKey(key);
                }
            }
            rocksDB.put(WRITE_OPTIONS, key, value);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    public void put(ColumnFamilyHandle handle, byte[] key, final byte[] value) throws RocksDBException {
        checkClose();
        try {
            if (key[0] >= '0' && key[0] <= '9') {
                delete(key);
                if (Utils.isDeleteMarker(value)) {
                    key = Utils.getDeleteMarkKey(key);
                }
            }
            rocksDB.put(handle, WRITE_OPTIONS, key, value);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }


    public boolean keyMayExist(final byte[] key, final Holder<byte[]> valueHolder) {
        checkClose();
        return rocksDB.keyMayExist(key, valueHolder);
    }

    public byte[] get(final byte[] key) throws RocksDBException {
        checkClose();
        try {
            if (key[0] >= '0' && key[0] <= '9') {
                byte[] res = rocksDB.get(key);
                if (null == res) {
                    byte[] deleteMarker = Utils.getDeleteMarkKey(key);
                    res = rocksDB.get(deleteMarker);
                }

                return res;
            }

            return rocksDB.get(key);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    public byte[] get(ColumnFamilyHandle handle, final byte[] key) throws RocksDBException {
        checkClose();
        try {
            if (key[0] >= '0' && key[0] <= '9') {
                byte[] res = rocksDB.get(key);
                if (null == res) {
                    byte[] deleteMarker = Utils.getDeleteMarkKey(key);
                    res = rocksDB.get(deleteMarker);
                }

                return res;
            }

            return rocksDB.get(handle, key);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    public void merge(byte[] key, byte[] value) throws RocksDBException {
        checkClose();
        try {
            rocksDB.merge(key, value);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    public void delete(ColumnFamilyHandle handle, byte[] key) throws RocksDBException {
        checkClose();
        try {
            rocksDB.delete(handle, key);
        } catch (RocksDBException e) {
            getRocksDBException(e);
            throw e;
        }
    }

    public static ColumnFamilyHandle getColumnFamily(String lun, String cfName) {
        return cfHandleMap.getOrDefault(lun, new HashMap<>()).get(cfName);
    }

    public static ColumnFamilyHandle getColumnFamily(String lun) {
        return cfHandleMap.getOrDefault(lun, new HashMap<>()).get(ROCKS_FILE_SYSTEM_PREFIX_OFFSET);
    }

    public static Map<String, ColumnFamilyHandle> getColumnFamilies(String lun) {
        return cfHandleMap.get(lun);
    }

    public static ColumnFamilyHandle getMigrateColumnFamilyHandle(String lun) {
        return cfHandleMap.getOrDefault(lun, new HashMap<>()).get(ColumnFamilyEnum.MIGRATE_COLUMN_FAMILY.name);
    }

    public void ingestExternalFile(List<String> filePathList, IngestExternalFileOptions ingestExternalFileOptions) {
        checkClose();
        try {
            rocksDB.ingestExternalFile(filePathList, ingestExternalFileOptions);
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
    }

    public static String getSyncRecordLun(String lun) {
        return lun + File.separator + UnsyncRecordDir;
    }

    public static String getComponentRecordLun(String lun) {
        return lun + File.separator + MultiMediaRecordDir;
    }

    public static String getRabbitmqRecordLun(String lun) {
        return lun + File.separator + RabbitmqRecordDir;
    }

    public static String getSTSTokenLun(String lun) {
        return lun + File.separator + STSTokenDBDir;
    }

    public static String getAggregateLun(String lun) {
        return lun + File.separator + IndexDBEnum.AGGREGATE_DB.dir;
    }

    public static String getIndexRocksDBLun(String lun, IndexDBEnum indexDBEnum) {
        return lun + File.separator + indexDBEnum.getDir();
    }

    /**
     * 根据IndexDBEnum获取对应的rocksdb实例
     *
     * @param lun         原始lun名称
     * @param indexDBEnum 索引数据库类型
     * @return 对应的rocksdb实例，如果是ROCKS_DB则返回原始lun
     */
    public static MSRocksDB getRocksDB(String lun, IndexDBEnum indexDBEnum) {
        return getRocksDB(getLunByIndexDBEnum(lun, indexDBEnum));
    }

    /**
     * 根据IndexDBEnum获取对应的rocksdb lun名称
     *
     * @param lun         原始lun名称
     * @param indexDBEnum 索引数据库类型
     * @return 对应的rocksdb lun名称，如果是ROCKS_DB则返回原始lun
     */
    public static String getLunByIndexDBEnum(String lun, IndexDBEnum indexDBEnum) {
        switch (indexDBEnum) {
            case UNSYNC_RECORD_DB:
                return getSyncRecordLun(lun);
            case COMPONENT_RECORD_DB:
                return getComponentRecordLun(lun);
            case STS_TOKEN_DB:
                return getSTSTokenLun(lun);
            case RABBITMQ_RECORD_DB:
                return getRabbitmqRecordLun(lun);
            case AGGREGATE_DB:
                return getAggregateLun(lun);
            case ROCKS_DB:
            default:
                return lun;
        }
    }

    public static Set<IndexDBEnum> NEED_REBUILD_INDEX_DB = new LinkedHashSet<>(Arrays.asList(ROCKS_DB, UNSYNC_RECORD_DB, COMPONENT_RECORD_DB, STS_TOKEN_DB, AGGREGATE_DB));

    public enum IndexDBEnum {
        ROCKS_DB("rocks_db"),
        UNSYNC_RECORD_DB("rocks_db_sync"),
        COMPONENT_RECORD_DB("rocks_db_media"),
        RABBITMQ_RECORD_DB("rocks_db_rabbitmq"),
        STS_TOKEN_DB("rocks_db_STS"),
        AGGREGATE_DB("rocks_db_aggregate");
        private final String dir;

        IndexDBEnum(String dir) {
            this.dir = dir;
        }

        public String getDir() {
            return this.dir;
        }
    }

    public enum ColumnFamilyEnum {
        MIGRATE_COLUMN_FAMILY("migrate"),
        AGGREGATION_GC_FAMILY("aggregation_gc");

        private final String name;

        ColumnFamilyEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
