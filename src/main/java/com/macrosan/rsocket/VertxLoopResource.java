package com.macrosan.rsocket;

import com.macrosan.httpserver.ServerConfig;
import io.netty.channel.EventLoopGroup;
import io.vertx.core.impl.VertxImpl;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import reactor.netty.resources.LoopResources;
import reactor.util.annotation.NonNull;

/**
 * VertxLoopResource
 *
 * @author liyixin
 * @date 2019/8/29
 */
@Log4j2
public class VertxLoopResource implements LoopResources {

    public static final Vertx VERTX = ServerConfig.getInstance().getVertx();

    public static final LoopResources VERTX_LOOP_RESOURCE = new VertxLoopResource();

    private VertxLoopResource() {
    }

    @Override
    @NonNull
    public EventLoopGroup onServer(boolean useNative) {
        log.debug("reuse Vert.x child EventLoopGroup : {}", VERTX::nettyEventLoopGroup);
        return VERTX.nettyEventLoopGroup();
    }

    @Override
    @NonNull
    public EventLoopGroup onServerSelect(boolean useNative) {
        log.debug("reuse Vert.x parent EventLoopGroup : {}", () -> ((VertxImpl) VERTX.getDelegate()).getAcceptorEventLoopGroup());
        return ((VertxImpl) VERTX.getDelegate()).getAcceptorEventLoopGroup();
    }
}
