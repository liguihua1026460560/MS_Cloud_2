package com.macrosan.storage.coder;

import io.vertx.core.buffer.Buffer;
import reactor.core.publisher.UnicastProcessor;

/**
 * @author gaozhiyuan
 */
public interface Encoder {
    /**
     * 输入原始数据
     *
     * @param bytes 原始数据
     */
    void put(byte[] bytes);

    default void put(Buffer buffer) {
        put(buffer.getBytes());
    }

    /**
     * 完成输入
     */
    void complete();

    /**
     * 编码后的数据流
     *
     * @return 编码后的数据流
     */
    UnicastProcessor<byte[]>[] data();

    long size();

    void flush();
}
