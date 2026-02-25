package com.macrosan.database.redis;

import com.macrosan.httpserver.ServerConfig;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.EventLoopGroupProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * VertxEventLoopProvider
 * 给Lettuce提供EventLoopGroup，将Vertx中的EventLoopGroup复用到Lettuce中
 *
 * @author liyixin
 * @date 2019/4/26
 */
public class VertxEventLoopProvider implements EventLoopGroupProvider {

    public static final ClientResources resources = ClientResources.builder()
            .computationThreadPoolSize(3)
            .ioThreadPoolSize(3)
            .build();

    private static final Logger logger = LogManager.getLogger(VertxEventLoopProvider.class);

    @Override
    @SuppressWarnings("unchecked")
    public <T extends EventLoopGroup> T allocate(Class<T> type) {
        return (T) ServerConfig.getInstance().getVertx().nettyEventLoopGroup();
    }

    @Override
    public int threadPoolSize() {
        return 3;
    }

    @Override
    public Future<Boolean> release(EventExecutorGroup eventLoopGroup, long quietPeriod, long timeout, TimeUnit unit) {
        logger.error("unsupported method, release the eventLoopGroup outside");
        return null;
    }

    @Override
    public Future<Boolean> shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
        logger.error("unsupported method, shutdown the eventLoopGroup outside");
        return null;
    }
}
