package com.macrosan.storage.client.channel;

import com.macrosan.message.socketmsg.SocketReqMsg;

public interface Channel<T> {

    /**
     * 最大并发数
     */
    int MAX_CONCURRENT_SHARD_AMOUNT = 32;

    /**
     * 默认最大数量
     */
    int DEFAULT_MAX_KEYS = 1000;

    /**
     * 将数据写入channel中
     * @param e list出的数据
     */
    void write(T e);

    /**
     * 向通道发起一次socket请求
     * @param msg 请求信息
     */
    void request(SocketReqMsg msg);

    /**
     * 从通道中返回的合并后数据
     * @return 通道加工后的数据
     */
    Object response();
}
