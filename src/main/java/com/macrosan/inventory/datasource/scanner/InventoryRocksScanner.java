package com.macrosan.inventory.datasource.scanner;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_LATEST_KEY;
import static com.macrosan.constants.SysConstants.ROCKS_VERSION_PREFIX;

@Log4j2
public class InventoryRocksScanner implements Scanner<Tuple2<byte[], byte[]>> {

    private MSRocksDB rocksDB;
    private MSRocksIterator iterator;
    private Snapshot snapshot;
    private String topSeekKey;
    private String vnode;
    private volatile boolean closed = true;

    public InventoryRocksScanner(String disk, String bucketVnode, String bucketName, String prefix, String includeVersionObjects) {
        this.rocksDB = MSRocksDB.getRocksDB(disk);
        this.vnode = bucketVnode;
        this.topSeekKey = bucketVnode + File.separator + bucketName + File.separator + prefix;
        if ("Current".equals(includeVersionObjects)) {
            this.topSeekKey = ROCKS_LATEST_KEY + topSeekKey;
        } else if ("All".equals(includeVersionObjects)) {
            this.topSeekKey = ROCKS_VERSION_PREFIX + topSeekKey;
        }
    }

    @Override
    public void start(String startKey) {
        if (closed) {
            snapshot = rocksDB.getSnapshot();
            ReadOptions readOptions = new ReadOptions()
                    .setSnapshot(this.snapshot);
            iterator = rocksDB.newIterator(readOptions);
            if (StringUtils.isNotEmpty(startKey)) {
                iterator.seek(startKey.getBytes());
            } else {
                iterator.seek(topSeekKey.getBytes());
            }
            closed = false;
        }
    }

    @Override
    public void seek(byte[] point) {
        if (iterator != null) {
            iterator.seek(point);
        }
    }

    @Override
    public Tuple2<byte[], byte[]>  next() {
        Tuple2<byte[], byte[]> res = null;
        try {
            if (!closed) {
                synchronized (this) {
                    if (!closed) {
                        if (iterator.isValid() && new String(iterator.key()).startsWith(topSeekKey)) {
                            res = new Tuple2<>(iterator.key(), iterator.value());
                            iterator.next();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("rocks next error!");
        }
        return res;
    }

    @Override
    public void release() {
        try {
            if (!closed) {
                synchronized (this) {
                    if (!closed) {
                        iterator.close();
                        this.rocksDB.releaseSnapshot(snapshot);
                        iterator = null;
                        snapshot = null;
                        closed = true;

                        log.info("release rocks scanner." + topSeekKey);
                    }
                }
            }
        } catch (Exception e) {
            log.error("release rocks scanner error!", e);
        }
    }

    @Override
    public String getCurrentSnapshotMark() {
        return null;
    }

}
