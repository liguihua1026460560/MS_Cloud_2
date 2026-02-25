package com.macrosan.filesystem.cifs.rpc;

import com.macrosan.constants.ServerConstants;
import com.macrosan.filesystem.cifs.rpc.handler.RPCHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetServer;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class DCERPC {
    public static final AtomicInteger AssocGroupID = new AtomicInteger(0);
    public static boolean rpcDebug = true;
    public static int dcerpcPort = 135;

    static Vertx vertx;

    public static void start() {
        DeploymentOptions options = new DeploymentOptions()
                .setInstances(ServerConstants.PROC_NUM);
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                .setPreferNativeTransport(true));

        vertx.rxDeployVerticle(DCERPCVerticle.class.getName(), options).subscribe();
        log.info("start dcerpc service in {}", dcerpcPort);
    }

    public static class DCERPCVerticle extends AbstractVerticle {
        @Override
        public void start() {
            NetServerOptions serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setTcpKeepAlive(true)          // 启用 Keepalive 检测死连接
                    .setTcpNoDelay(true);           // 禁用 Nagle 算法（降低延迟）

            NetServer dcerpc = DCERPC.vertx.createNetServer(serverOptions);

            dcerpc.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        RPCHandler handler = new RPCHandler(socket);
                        socket.handler(b -> {
                            try {
                                handler.handler(b);
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });

                        socket.resume();
                    });

            dcerpc.listen(dcerpcPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen dcerpcPort failed, port : {}\ncause : {}", dcerpcPort, res.cause());
                }
            });
        }
    }
}
