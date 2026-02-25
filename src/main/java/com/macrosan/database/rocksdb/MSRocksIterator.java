package com.macrosan.database.rocksdb;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksIterator;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class MSRocksIterator implements AutoCloseable {
    private RocksIterator iterator;
    private MSRocksDB rocksDB;
    private boolean closed = false;
    private Throwable throwable;

    MSRocksIterator(MSRocksDB rocksDB, RocksIterator iterator) {
        if (log.isDebugEnabled()) {
            throwable = new Throwable();
        }

        this.rocksDB = rocksDB;
        this.iterator = iterator;
    }

    @Override
    public void close() {
        iterator.close();
        closed = true;
    }

    public byte[] key() {
        rocksDB.checkClose();
        return iterator.key();
    }

    public byte[] value() {
        rocksDB.checkClose();
        return iterator.value();
    }

    public boolean isValid() {
        rocksDB.checkClose();
        return iterator.isValid();
    }

    public void seek(byte[] target) {
        rocksDB.checkClose();
        iterator.seek(target);
    }

    public void next() {
        rocksDB.checkClose();
        iterator.next();
    }

    public void prev() {
        rocksDB.checkClose();
        iterator.prev();
    }

    public void seekForPrev(byte[] target) {
        rocksDB.checkClose();
        iterator.seekForPrev(target);
    }

    public void seekToLast() {
        rocksDB.checkClose();
        iterator.seekToLast();
    }

    @Override
    @Deprecated
    protected void finalize() throws Throwable {
        if (!closed) {
            if (log.isDebugEnabled()) {
                log.debug("rocks iterator not close, created by ", throwable);
            } else {
                log.error("rocks iterator not close");
            }
        }
        super.finalize();
    }
}
