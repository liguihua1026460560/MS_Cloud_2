package com.macrosan.inventory.datasource;

import com.macrosan.inventory.datasource.scanner.Scanner;
import com.macrosan.utils.functional.Tuple2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DataSource {

    /**
     * 初始化数据源
     */
    Mono<Boolean> start();

    /**
     * 从数据源拉取数据流
     */
    void fetch(long n);

    /**
     * 返回数据源中的数据
     * @return 数据流
     */
    Flux<Tuple2<byte[], byte[]>> data();

    /**
     * 停止拉取数据
     */
    void complete();

    /**
     * 重置数据源头
     */
    void reset();

    /**
     * 关闭数据源
     */
    void release();

    /**
     * 数据是否已读完
     * @return
     */
    boolean hasRemaining();

    /**
     * 返回上一次数据流中断的位置，并且设置新的位置，若为null则不设置
     */
    byte[] cursor(byte[] cursor);

    /**
     * 获取数据源的扫描器
     * @return
     */
    Scanner scanner();

    Flux<Long> keepalive();

}
