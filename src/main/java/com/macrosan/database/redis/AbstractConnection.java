package com.macrosan.database.redis;

import com.macrosan.database.StatusEnum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.reactivex.annotations.Nullable;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;

/**
 * AbstractConnection
 * 连接的抽象类
 *
 * @author liyixin
 * @date 2018/11/23
 */
public abstract class AbstractConnection implements Connection {

    private Logger logger = LogManager.getLogger(AbstractConnection.class.getName());

    static RedisClient client = null;

    final FastList<RedisURI> nodes;

    StatefulRedisPubSubConnection<String, String> subConnection;

    @Getter
    StatusEnum status;

    AbstractConnection(RedisClient client, FastList<RedisURI> nodes) {
        AbstractConnection.client = client;
        this.nodes = nodes;
    }

    /**
     * 处理状态变化
     */
    abstract void dealStatus();

    /**
     * getConnection
     * <p>
     * 根据静态拓扑创建一个主从连接,初始化以及重新连接时使用
     */
    abstract void createConnection();

    void reconnect() {
        createConnection();
        status = StatusEnum.ALIVE;
    }

    /**
     * 返回同步API集合
     *
     * @return Lettuce提供的连接
     */
    @Override
    @Nullable
    public RedisCommands<String, String> sync() {
        dealStatus();
        try {
            return slave().sync();
        } catch (Exception e) {
            logger.error("connection closed", e);
            return null;
        }
    }

    /**
     * 返回异步API集合
     *
     * @return Lettuce提供的连接
     */
    @Override
    @Nullable
    public RedisAsyncCommands<String, String> async() {
        dealStatus();
        try {
            return slave().async();
        } catch (Exception e) {
            logger.error("connection closed", e);
            return null;
        }
    }

    /**
     * 返回响应式API集合
     *
     * @return Lettuce提供的连接
     */
    @Override
    @Nullable
    public RedisReactiveCommands<String, String> reactive() {
        dealStatus();
        try {
            return slave().reactive();
        } catch (Exception e) {
            logger.error("connection closed", e);
            return null;
        }
    }

    /**
     * 缓存redis的内容，按固定时间更新。
     * 读取redis时直接读缓存
     *
     * @return commands
     */
    public RedisReactiveCommands<String, String> cacheReactive() {
        return cacheCommands;
    }

    private RedisReactiveCommands<String, String> cacheCommands;

    /**
     * 获取本地的redis内容放入缓存。
     *
     * @param database
     */
    protected void initCache(int database) {
        RedisReactiveCommands<String, String> commands = slave().reactive();
        RedisCache cache = new RedisCache(database, subConnection, commands);

        cacheCommands = (RedisReactiveCommands<String, String>) Proxy.newProxyInstance(
                RedisReactiveCommands.class.getClassLoader(),
                new Class<?>[]{RedisReactiveCommands.class}
                , (proxy, method, args) -> {
                    if (method.getName().contains("scan")) {
                        return method.invoke(commands, args);
                    }

                    if (method.getReturnType() == Mono.class) {
                        return cache.invokeMono(method, args);
                    }

                    if (method.getReturnType() == Flux.class) {
                        return cache.invokeFlux(method, args);
                    }

                    return method.invoke(commands, args);
                });


    }
}
