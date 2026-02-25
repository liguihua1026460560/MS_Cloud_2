package com.macrosan.storage.coder;

/**
 * @author gaozhiyuan
 */
public interface LimitEncoder extends Encoder {
    /**
     * 下游增加需要的请求数据个数
     * 在k个下游的数据块都需要数据块的时候
     * 实际增加等待的数据块数量
     *
     * @param index 数据块索引
     * @param n     请求N个
     */
    void request(int index, long n);

    default void completeN(int index, long n) {
    }
}
