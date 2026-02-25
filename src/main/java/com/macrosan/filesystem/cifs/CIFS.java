package com.macrosan.filesystem.cifs;

import com.macrosan.constants.ServerConstants;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.lease.LeaseServer;
import com.macrosan.filesystem.cifs.lock.CIFSLockServer;
import com.macrosan.filesystem.cifs.notify.NotifyServer;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessServer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetServer;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class CIFS {
    public static boolean cifsDebug = false;
    static int cifsPort = 445;

    static Vertx vertx;

    public static void start() {
        NotifyServer.register();
        LeaseServer.register();
        ShareAccessServer.register();
        CIFSLockServer.register();
        SMB1.initProc();
        SMB2.initProc();

        DeploymentOptions options = new DeploymentOptions()
                .setInstances(ServerConstants.PROC_NUM);
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                .setPreferNativeTransport(true));

        vertx.rxDeployVerticle(CIFSVerticle.class.getName(), options).subscribe();
        log.info("start cifs service in {}", cifsPort);
    }

    public static AtomicLong printTimeout = new AtomicLong();

    public static class CIFSVerticle extends AbstractVerticle {
        @Override
        public void start() {
            NetServerOptions serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setTcpKeepAlive(true);

            NetServer cifs = CIFS.vertx.createNetServer(serverOptions);

            cifs.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        SMBHandler handler = new SMBHandler(socket);
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });

                        socket.exceptionHandler(e -> {
                            if (e.getMessage() != null && e.getMessage().contains("Connection reset by peer")) {
                                long cur = System.nanoTime();
                                long print = printTimeout.updateAndGet(l -> {
                                    if (cur - l > 300_000_000_000L) {
                                        return cur;
                                    } else {
                                        return l;
                                    }
                                });

                                if (print == cur) {
                                    log.error("", e);
                                }
                            } else {
                                log.error("", e);
                            }
                        });

                        socket.resume();
                    });

            cifs.listen(cifsPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen cifsPort failed, port : {}\ncause : {}", cifsPort, res.cause());
                }
            });
        }
    }
}
