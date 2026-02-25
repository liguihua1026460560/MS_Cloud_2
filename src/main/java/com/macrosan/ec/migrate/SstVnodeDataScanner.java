package com.macrosan.ec.migrate;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.rabbitmq.RequeueMQException;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.ec.migrate.SstVnodeDataScannerUtils.*;

/**
 * @author wanhao
 * @description: 使用扫描sst文件的方式遍历数据
 * @date 2022/6/6 下午 4:03
 */
@Log4j2
public class SstVnodeDataScanner extends VnodeDataScanner {

    private MSRocksDB db;
    private String lun;
    private String vnode;
    private final String objVnode;

    private MSRocksIterator iterator;
    private String key;
    private int state;

    private int sstState;
    private String sstKey;
    private SstFileReaderIterator sstIterator;
    private SstFileReader sstFileReader;

    /**
     * 遍历migrateCF时使用的ReadOptions
     */
    private ReadOptions migrateNewReadOptions;


    /**
     * 需要扫描的所有sst文件
     */
    private List<LiveFileMetaData> migrateSstList = new CopyOnWriteArrayList<>();

    /**
     * 标识扩容前源盘已经存在的数据是否全部扫描
     */
    private final AtomicBoolean oldScanEnd = new AtomicBoolean(false);

    /**
     * 标识扩容过程中新写入的数据是否全部扫描
     */
    private final AtomicBoolean scanEnd = new AtomicBoolean(false);

    public boolean isOldScanEnd() {
        return oldScanEnd.get();
    }

    public boolean isScanEnd() {
        return scanEnd.get();
    }

    public void setOldScanEnd(boolean b) {
        oldScanEnd.set(b);
    }

    @Override
    public void start() {
        try {
            migrateSstList = getSstFileList(objVnode, vnode, lun);
            refreshDeleteSstFile();
        } catch (Exception e) {
            log.error(e);
        }
    }

    public SstVnodeDataScanner(String objVnode, String srcDisk, String vnode) {
        this.objVnode = objVnode;
        db = MSRocksDB.getRocksDB(srcDisk);
        if (db == null) {
            scanEnd.set(true);
        }
        lun = srcDisk;
        this.vnode = vnode;
    }

    public SstVnodeDataScanner(String objVnode, LiveFileMetaData metaData) {
        this.objVnode = objVnode;
        String filePath = metaData.path() + metaData.fileName();
        this.sstFileName = metaData.fileName().substring(1);
        sstFileReader = new SstFileReader(OPTIONS);

        try {
            sstFileReader.open(filePath);
            sstIterator = sstFileReader.newIterator(READ_OPTIONS);
            sstState = 0;
            updatetSstKey();
            sstState++;
            sstIterator.seek(sstKey.getBytes());
        } catch (RocksDBException e) {
            log.error(e);
            if (e instanceof RocksDBException && !e.getMessage().contains("LockTimeout")) {
                log.error("srcDisk create sst reader error, requeue", e);
                throw new RequeueMQException("srcDisk create sst reader error, requeue");
            }
        }
    }

    @Override
    public Tuple3<Type, byte[], byte[]> next() {
        if (!oldScanEnd.get()) {
            return nextDeleteOld();
        }
        if (scanEnd.get() || iterator == null) {
            return null;
        }
        try {
            if (iterator.isValid() && new String(iterator.key()).startsWith(key)) {
                Tuple3<Type, byte[], byte[]> res;
                byte[] resKey = iterator.key();
                byte[] resValue = iterator.value();
                if (new String(resKey).startsWith(SysConstants.ROCKS_AGGREGATION_RATE_PREFIX)) {
                    resValue = Base64.getEncoder().encodeToString(resValue).getBytes();
                }
                res = new Tuple3<>(Type.META, resKey, resValue);
                iterator.next();
                return res;
            }
        } catch (Exception e) {
            log.error(e);
        }
        boolean b = updateKey();
        if (!b) {
            scanEnd.set(true);
            db.releaseSnapshot(snapshot);
            migrateNewReadOptions.close();
            iterator.close();
            return null;
        }
        state++;
        try {
            iterator.seek(key.getBytes());
        } catch (Exception e) {
            log.error(e);
        }
        return next();
    }

    /**
     * 从sst文件中获取下一个源盘需要删除的元数据
     */
    public Tuple3<Type, byte[], byte[]> nextDeleteOld() {
        if (sstIterator == null) {
            oldScanEnd.set(true);
            return null;
        }
        if (sstIterator.isValid() && new String(sstIterator.key()).startsWith(sstKey)) {
            Tuple3<Type, byte[], byte[]> res;
            res = new Tuple3<>(Type.META, sstIterator.key(), sstIterator.value());
            sstIterator.next();
            return res;
        }
        boolean b = updatetSstKey();
        if (!b) {
            if (refreshDeleteSstFile()) {
                return nextDeleteOld();
            }
            return null;
        }
        sstState++;
        sstIterator.seek(sstKey.getBytes());
        return nextDeleteOld();
    }

    Snapshot snapshot;

    /**
     * 源盘迁移前存在的所有元数据扫描完成，开始扫描 迁移源盘元数据过程中新写入的数据，此部分数据记录在源盘rocksdb的migrateColumnFamily中
     */
    public void refreshRocksIterator() {
        if (db != null) {
            try {
                snapshot = db.getSnapshot();
                migrateNewReadOptions = new ReadOptions().setSnapshot(snapshot);
                iterator = db.newIterator(MSRocksDB.getMigrateColumnFamilyHandle(lun), migrateNewReadOptions);
                state = 0;
                updateKey();
                state++;
                iterator.seek(key.getBytes());
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    /**
     * 加节点删除源盘数据流程中，用于一个sst文件扫描结束后，切换为其他的sst文件
     */
    public boolean refreshDeleteSstFile() {
        if (!migrateSstList.isEmpty()) {
            try {
                closeSstIterator();
                sstFileReader = new SstFileReader(OPTIONS);
                LiveFileMetaData metaData = migrateSstList.remove(0);
                String usedSstFileName = metaData.path() + metaData.fileName();
                sstFileReader.open(usedSstFileName);
                sstIterator = sstFileReader.newIterator(READ_OPTIONS);
                sstState = 0;
                updatetSstKey();
                sstState++;
                sstIterator.seek(sstKey.getBytes());
                return true;
            } catch (Exception e) {
                log.error("", e);
                if (e instanceof RocksDBException && !e.getMessage().contains("LockTimeout")) {
                    log.error("srcDisk refresh delete sst file error, requeue", e);
                    throw new RequeueMQException("srcDisk refresh delete sst file error, requeue");
                }
            }
        }
        closeSstIterator();
        oldScanEnd.set(true);
        return false;
    }

    private boolean updatetSstKey() {
        if (sstState < PREFIX_LIST.size()) {
            sstKey = PREFIX_LIST.get(sstState) + objVnode + getSeparator(PREFIX_LIST.get(sstState));
            return true;
        }
        return false;
    }

    private boolean updateKey() {
        if (state < PREFIX_LIST.size()) {
            key = PREFIX_LIST.get(state) + objVnode + getSeparator(PREFIX_LIST.get(state));
            return true;
        }
        return false;
    }

    private void closeSstIterator() {
        if (sstIterator != null) {
            sstIterator.close();
            sstIterator = null;
        }
    }


    public String sstFileName;


    /**
     * 从sst文件获取下一条需要迁移的元数据
     */
    public Tuple3<Type, byte[], byte[]> nextMigrateOld() {

        //此时元数据已经扫描结束，sstIterator已关闭，开始扫描删除标记
        if (sstIterator == null) {
            return null;
        }
        if (sstIterator.isValid() && new String(sstIterator.key()).startsWith(sstKey)) {
            byte[] resKey = sstIterator.key();
            byte[] resValue = sstIterator.value();
            if (new String(resKey).startsWith(SysConstants.ROCKS_AGGREGATION_RATE_PREFIX)) {
                resValue = Base64.getEncoder().encodeToString(resValue).getBytes();
            }
            sstIterator.next();
            return new Tuple3<>(Type.META, resKey, resValue);
        }
        boolean b = updatetSstKey();
        if (!b) {
            closeSstIterator();
            initDeletionIterator();
            return null;
        }
        sstState++;
        sstIterator.seek(sstKey.getBytes());
        return nextMigrateOld();
    }


    /**
     * 当前sst文件的数据扫描完成，从未扫描的sst文件中选择一个新的文件
     */
    public boolean refreshSstFile(List<LiveFileMetaData> list) {
        if (!list.isEmpty()) {
            try {
                sstFileReader = new SstFileReader(OPTIONS);
                LiveFileMetaData metaData = list.remove(0);
                sstFileName = metaData.fileName().substring(1);
                sstFileReader.open(metaData.path() + metaData.fileName());
                sstIterator = sstFileReader.newIterator(READ_OPTIONS);
                sstState = 0;
                updatetSstKey();
                sstState++;
                sstIterator.seek(sstKey.getBytes());
                return true;
            } catch (Exception e) {
                log.error("", e);
                if (e instanceof RocksDBException && !e.getMessage().contains("LockTimeout")) {
                    log.error("srcDisk refresh sst file error, requeue", e);
                    throw new RequeueMQException("srcDisk refresh sst file error, requeue");
                }
            }
        }
        return false;
    }

    private SstFileReaderIterator deletionIterator;
    private final AtomicBoolean deletionScanEnd = new AtomicBoolean();

    //删除标记会扫描多次，记录当前sst文件是否为第一次扫描删除标记
    public final AtomicBoolean firstScanDeletion = new AtomicBoolean(true);

    private String deletionKey;
    private int deletionState;


    private boolean updateDeletionKey() {
        if (deletionState < PREFIX_LIST.size()) {
            deletionKey = PREFIX_LIST.get(deletionState) + objVnode + getSeparator(PREFIX_LIST.get(deletionState));
            return true;
        }
        return false;
    }

    /**
     * 初始化扫描删除标记的iterator
     */
    private void initDeletionIterator() {
        try {
            firstScanDeletion.set(true);
            deletionScanEnd.set(false);
            TableProperties properties = sstFileReader.getTableProperties();
            if (properties.getNumDeletions() == 0) {
                deletionScanEnd.set(true);
                sstFileReader.close();
                return;
            }
            //通过ReadOptions设置setIterStartSeqnum>0，读取sst的内部key
            deletionIterator = sstFileReader.newIterator(INTERNAL_READ_OPTIONS);
            deletionState = 0;
            updateDeletionKey();
            deletionState++;
            deletionIterator.seek(deletionKey.getBytes());
        } catch (Exception e) {
            log.error("", e);
            if (e instanceof RocksDBException && !e.getMessage().contains("LockTimeout")) {
                log.error("srcDisk init Deletion iterator error, requeue", e);
                throw new RequeueMQException("srcDisk init Deletion iterator error, requeue");
            }
        }
    }

    /**
     * 从sst文件中获取指定数量的rocksdb底层删除标记
     */
    public List<byte[]> nextDeletion(int num) {
        List<byte[]> deletionList = new ArrayList<>(num);
        if (deletionScanEnd.get()) {
            return deletionList;
        }
        while (deletionIterator.isValid()) {
            if (new String(deletionIterator.key()).startsWith(deletionKey)) {
                //internalKey为删除标记时的value为[]
                if (deletionIterator.value().length == 0) {
                    //扫描到的为internalKey，需转为userKey，去除后8个byte
                    byte[] userKey = Arrays.copyOfRange(deletionIterator.key(), 0, deletionIterator.key().length - 8);
                    deletionList.add(userKey);
                }
                deletionIterator.next();
                if (deletionList.size() >= num) {
                    return deletionList;
                }
            } else {
                if (!updateDeletionKey()) {
                    break;
                }
                deletionState++;
                deletionIterator.seek(deletionKey.getBytes());
            }
        }
        deletionScanEnd.set(true);
        deletionIterator.close();
        sstFileReader.close();
        return deletionList;
    }
}

