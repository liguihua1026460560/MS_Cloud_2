package com.macrosan.fs;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class Allocator {
    static {
        try {
            System.load("/moss/jni/liballoc.so");
        } catch (Throwable e) {
            log.error("load alloc library fail", e);
            System.exit(0);
        }
    }

    /**
     * 代表一个逻辑空间块。jni接口生成的Result和之后用于落盘数据时使用的Result是不一样的。
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Result {
        /**
         * allocator.allocate生成的offset：申请到的逻辑空间块是整个存储空间的第几个块
         * <br>
         * 落盘时BlockDevice.alloc换算后：该逻辑空间块起始第一个字节在整个逻辑空间的位置
         */
        public long offset;

        /**
         * allocator.allocate生成的size：申请到的逻辑空间块的个数
         * <br>
         * 落盘时BlockDevice.alloc换算后：该逻辑空间块的大小
         */
        public long size;
    }

    private long addr;

    protected Allocator() {

    }

    /**
     * @param diskSize  磁盘有多少个逻辑空间块
     * @param blockSize 一个磁盘有多少个Block
     */
    public Allocator(long diskSize, long blockSize) {
        addr = init(diskSize, blockSize);
        initFree(addr, 0, diskSize);
    }

    /**
     * 申请逻辑空间
     *
     * @param want 需要多少个大小为MIN_ALLOC_SIZE的逻辑空间块
     */
    public Result[] allocate(long want) {
        long[] res = alloc(addr, want);
        Result[] mems = new Result[res.length / 2];
        for (int i = 0; i < mems.length; i++) {
            mems[i] = new Result();
            mems[i].offset = res[2 * i];
            mems[i].size = res[2 * i + 1];
        }
        return mems;
    }

    public void release(Result[] mems) {
        long[] res = new long[mems.length * 2];
        for (int i = 0; i < mems.length; i++) {
            res[2 * i] = mems[i].offset;
            res[2 * i + 1] = mems[i].size;
        }

        release(addr, res, res.length);
    }

    /**
     * 项目开始时，初始化已有文件所占用的逻辑空间
     *
     * @param offset 该文件所在的是第几个逻辑空间块
     * @param size   该文件需要几块逻辑空间块
     */
    public void initAllocated(long offset, long size) {
        initAllocated(addr, offset, size);
    }

    private native long init(long diskSize, long blockSize);

    private native void initFree(long addr, long offset, long size);

    private native void initAllocated(long addr, long offset, long size);

    private native long[] alloc(long addr, long wantSize);

    private native void release(long addr, long[] res, int len);
}
