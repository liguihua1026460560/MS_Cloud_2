package com.macrosan.database.rocksdb;

import lombok.extern.log4j.Log4j2;
import org.rocksdb.MergeOperator;

/**
 * 自定义rocksdb merge操作
 *
 * @author gaozhiyuan
 */
@Log4j2
public class MossMergeOperator extends MergeOperator {
    public static final long SPACE_SIZE = 1L << 24; //16MB
    public static final int SPACE_LEN = (int) SPACE_SIZE / 4096 / 8;

    public MossMergeOperator() {
        super(newSharedMossOperator());
    }

    private native static long newSharedMossOperator();

    public native static void setAvg(long op, long db, long cf);

    @Override
    protected final native void disposeInternal(final long handle);
}
