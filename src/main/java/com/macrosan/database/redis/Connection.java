package com.macrosan.database.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Connection
 * 抽象一个redis连接
 *
 * @author liyixin
 * @date 2019/4/22
 */
public interface Connection {

    /**
     * 将主连接状态设置成closed
     */
    void setMasterClosed();

    /**
     * 获得一个新的连接到主的连接，使用完后需要手动关闭
     *
     * @return 主连接
     */
    StatefulRedisConnection<String, String> newMaster();


    /**
     * 获得连接到主的连接
     *
     * @return 主连接
     */
    StatefulRedisConnection<String, String> master();

    /**
     * 获得连接到从的连接
     *
     * @return 从连接
     */
    StatefulRedisConnection<String, String> slave();

    /**
     * 返回同步API集合
     *
     * @return Lettuce提供的连接
     */
    RedisCommands<String, String> sync();

    /**
     * 返回异步API集合
     *
     * @return Lettuce提供的连接
     */
    RedisAsyncCommands<String, String> async();

    /**
     * 返回响应式API集合
     *
     * @return Lettuce提供的连接
     */
    RedisReactiveCommands<String, String> reactive();
}
