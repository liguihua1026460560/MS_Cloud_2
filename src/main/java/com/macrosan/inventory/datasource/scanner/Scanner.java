package com.macrosan.inventory.datasource.scanner;

public interface Scanner<T> {

    /**
     * 初始化扫描器
     * @param startKey 指定起始值
     */
    void start(String startKey);

    /**
     * 为扫描器重启指定扫描位置
     * @param point 新位置
     */
    void seek(byte[] point);

    /**
     * 扫描数据
     * @return 扫描出的数据
     */
    T next() throws InterruptedException, Exception;

    /**
     * 释放扫描
     */
    void release();

    /**
     * 获取scan时的最新快照标记
     * @return 当前快照标记
     */
    String getCurrentSnapshotMark();
}
