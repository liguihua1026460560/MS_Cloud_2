package com.macrosan.ec.migrate;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.utils.functional.Tuple3;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;

import java.io.File;

import static com.macrosan.constants.SysConstants.*;

/**
 * 扫描源磁盘内和vnode相关的元数据和数据
 *
 * @author gaozhiyuan
 */
public class VnodeDataScanner {
    public enum Type {
        /**
         * 扫描到数据的类型
         */
        META,
        FILE
    }

    private MSRocksDB rocksDB;
    private MSRocksIterator iterator;
    private Snapshot snapshot;
    private String key;
    private int state;
    private String objVnode;

    public VnodeDataScanner(String objVnode, String srcDisk) {
        this.objVnode = objVnode;
        rocksDB = MSRocksDB.getRocksDB(srcDisk);
    }

    public VnodeDataScanner() {
    }

    public void start() {
        snapshot = rocksDB.getSnapshot();
        ReadOptions readOptions = new ReadOptions()
                .setSnapshot(snapshot);
        iterator = rocksDB.newIterator(readOptions);
        key = objVnode + File.separator;
        state = 0;
        iterator.seek(key.getBytes());
    }

    public Tuple3<Type, byte[], byte[]> next() {
        if (iterator.isValid() && new String(iterator.key()).startsWith(key)) {
            Tuple3<Type, byte[], byte[]> res;
            if (state == 3) {
                res = new Tuple3<>(Type.FILE, iterator.key(), iterator.value());
            } else {
                res = new Tuple3<>(Type.META, iterator.key(), iterator.value());
            }

            iterator.next();
            return res;
        }

        switch (state) {
            case 0:
                key = ROCKS_PART_PREFIX + objVnode + File.separator;
                break;
            case 1:
                key = ROCKS_PART_META_PREFIX + objVnode + File.separator;
                break;
            case 2:
                key = ROCKS_FILE_META_PREFIX + objVnode + "_";
                break;
            case 3:
                key = ROCKS_BUCKET_META_PREFIX + objVnode + File.separator;
                break;
            case 4:
                key = ROCKS_VERSION_PREFIX + objVnode + File.separator;
                break;
            case 5:
                key = ROCKS_LIFE_CYCLE_PREFIX + objVnode + File.separator;
                break;
            case 6:
                key = ROCKS_LATEST_KEY + objVnode + File.separator;
                break;
            case 7:
                key = ROCKS_UNSYNCHRONIZED_KEY + objVnode + File.separator;
                break;
            case 8:
                key = ROCKS_COMPONENT_VIDEO_KEY + File.separator + objVnode + File.separator;
                break;
            case 9:
                key = ROCKS_COMPONENT_IMAGE_KEY + File.separator + objVnode + File.separator;
                break;
            case 10:
                key = ROCKS_INODE_PREFIX + objVnode + File.separator;
                break;
            case 11:
                key = ROCKS_CHUNK_FILE_KEY + objVnode + "_";
                break;
            case 12:
                key = ROCKS_COOKIE_KEY + objVnode + File.separator;
                break;
            case 13:
                key = ROCKS_STS_TOKEN_KEY + objVnode + File.separator;
                break;
            case 14:
                key = ROCKS_AGGREGATION_META_PREFIX + objVnode + File.separator;
                break;
            case 15:
                key = ROCKS_AGGREGATION_RATE_PREFIX + objVnode + File.separator;
                break;
            case 16:
                key = ROCKS_AGGREGATION_UNDO_LOG_PREFIX + objVnode + File.separator;
                break;
            default:
                rocksDB.releaseSnapshot(snapshot);
                iterator.close();
                return null;
        }
        state++;
        iterator.seek(key.getBytes());
        return next();
    }
}
