package com.macrosan.database.rocksdb;

import com.macrosan.ec.server.LocalMigrateServer;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;

import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;

@Log4j2
public class LocalMigrateRocksDB extends MSRocksDB {
    protected String lun;
    private Map<String, MSRocksDB> dbMap;

    @Override
    public void delete(byte[] key) throws RocksDBException {
        String keyStr = new String(key);
        String vnode = LocalMigrateServer.getVnode(keyStr);
        String dstLun = LocalMigrateServer.getInstance().getDstLun(lun, vnode);
        if (null != dstLun) {
            if (keyStr.startsWith(ROCKS_FILE_META_PREFIX)) {
                String fileName = keyStr.substring(ROCKS_FILE_META_PREFIX.length());
                dbMap.get(dstLun).delete(key);
                String path = File.separator + dstLun + File.separator + fileName;
                try {
                    Files.deleteIfExists(FileSystems.getDefault().getPath(path));
                } catch (Exception e) {
                    log.error(e);
                }
            } else {
                //只有删除对象的FileMeta不使用事务直接删除，其他删除使用事务
                throw new UnsupportedOperationException("");
            }
        }

        super.delete(key);
    }

    @Override
    public void put(final byte[] key, final byte[] value)
            throws RocksDBException {
        String keyStr = new String(key);
        String vnode = LocalMigrateServer.getVnode(keyStr);
        String dstLun = LocalMigrateServer.getInstance().getDstLun(lun, vnode);
        if (null != dstLun) {
            dbMap.get(dstLun).put(key, value);
        }
        super.put(key, value);
    }

    public LocalMigrateRocksDB(String lun, TransactionDB db, Map<String, MSRocksDB> dbMap) {
        super(db);
        this.lun = lun;
        this.dbMap = dbMap;
    }
    public LocalMigrateRocksDB(String lun, RocksDB db, Map<String, MSRocksDB> dbMap, boolean check) {
        super(db, check);
        this.lun = lun;
        this.dbMap = dbMap;
    }
}
