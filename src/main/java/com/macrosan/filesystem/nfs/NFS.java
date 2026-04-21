package com.macrosan.filesystem.nfs;

import com.macrosan.constants.ServerConstants;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.lock.redlock.RedLockServer;
import com.macrosan.filesystem.nfs.delegate.DelegateServer;
import com.macrosan.filesystem.nfs.handler.*;
import com.macrosan.filesystem.nfs.lock.NFS4LockServer;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.impl.NetSocketImpl;
import io.vertx.core.net.impl.transport.Transport;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetServer;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Field;

@Log4j2
public class NFS {
    public static boolean nfsDebug = false;
    static int portMapPort = 112;
    public static int mountPort = 20048;
    public static int nfsPort = 2049;
    public static int nlmPort = 4045;
    public static int nsmPort = 32767;
    public static int nfsRQuotaPort = 875;
    public static long bootNanoTime;
    public static final int IDLE_TIMEOUT = 15;

    static Vertx vertx;
    static DeploymentOptions options;

    static {
        try {
            nfsPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_PORT, nfsPort);
            mountPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_MOUNT_PORT, mountPort);
            nlmPort = FsUtils.getFsPort(FsConstants.FSConfig.NLM_PORT, nlmPort);
            nsmPort = FsUtils.getFsPort(FsConstants.FSConfig.NSM_PORT, nsmPort);
            nfsRQuotaPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_QUOTA_PORT, nfsRQuotaPort);
        } catch (Exception e) {
            log.error("Failed to obtain NFS settings, default settings applied.", e);
        }
    }

    public static void start() {
        RedLockServer.register();
        NFS4LockServer.register();
        ShareAccessServer.register();
        DelegateServer.register();
        options = new DeploymentOptions()
                .setInstances(ServerConstants.PROC_NUM);
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                .setPreferNativeTransport(true));

        VertxImpl vertxImpl = (VertxImpl) vertx.getDelegate();
        Transport transport = vertxImpl.transport();
        try {
            Field field = vertxImpl.getClass().getDeclaredField("transport");
            field.setAccessible(true);
            NFSTransport nfsTransport = new NFSTransport(transport);
            field.set(vertxImpl, nfsTransport);
        } catch (Exception e) {
            log.error("", e);
            System.exit(-1);
        }

        vertx.rxDeployVerticle(NFSVerticle.class.getName(), options).subscribe();
        NFSV3.initProc();
        NSM.initProc(vertx);
        NLM4.initProc(vertx);
        NFSV4.initProc();
        bootNanoTime = System.nanoTime();
        log.info("start nfs service in {}", nfsPort);
    }

    public static void restart() {
        vertx.close(ar -> {
            if (ar.succeeded()) {
                vertx = Vertx.vertx(new VertxOptions()
                        .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                        .setPreferNativeTransport(true));
                VertxImpl vertxImpl = (VertxImpl) vertx.getDelegate();
                Transport transport = vertxImpl.transport();
                try {
                    Field field = vertxImpl.getClass().getDeclaredField("transport");
                    field.setAccessible(true);
                    NFSTransport nfsTransport = new NFSTransport(transport);
                    field.set(vertxImpl, nfsTransport);
                } catch (Exception e) {
                    log.error("", e);
                    System.exit(-1);
                }

                vertx.rxDeployVerticle(NFSVerticle.class.getName(), options).subscribe();
                // NFSV4.initProc();
                bootNanoTime = System.nanoTime();
                log.info("restart nfs: {}, mount: {}, nlm: {}, nsm: {}, quota: {} service", nfsPort, mountPort, nlmPort, nsmPort, nfsRQuotaPort);
            } else {
                log.error("restart nfs: {}, mount: {}, nlm: {}, nsm: {}, quota: {} service failed", nfsPort, mountPort, nlmPort, nsmPort, nfsRQuotaPort);
            }
        });
    }

    public static class NFSVerticle extends AbstractVerticle {
        @Override
        public void start() {
            NetServerOptions serverOptions = new NetServerOptions()
                    .setSoLinger(0)
                    .setReuseAddress(true)
                    .setTcpKeepAlive(true)
                    .setTcpQuickAck(true)
                    .setReusePort(true);

            NetServer portMap = NFS.vertx.createNetServer(serverOptions);
            NetServer mount = NFS.vertx.createNetServer(serverOptions);
            NetServer nfs = NFS.vertx.createNetServer(serverOptions);
            NetServer nlm = NFS.vertx.createNetServer(serverOptions);
            NetServer nsm = NFS.vertx.createNetServer(serverOptions);
            NetServer rQuota = NFS.vertx.createNetServer(serverOptions);
            //监听udp 112端口，linux客户端showmount 命令会通过udp端口进行通信
            DatagramSocketOptions udpServerOptions = new DatagramSocketOptions()
                    .setReusePort(true)
                    .setReuseAddress(true)
                    .setReceiveBufferSize(2 << 20);
            DatagramSocket udpPortMap = NFS.vertx.createDatagramSocket(udpServerOptions);
            DatagramSocket udpMount = NFS.vertx.createDatagramSocket(udpServerOptions);
            DatagramSocket udpNfs = NFS.vertx.createDatagramSocket(udpServerOptions);
            DatagramSocket udpNlm = NFS.vertx.createDatagramSocket(udpServerOptions);
            DatagramSocket udpNsm = NFS.vertx.createDatagramSocket(udpServerOptions);

            DatagramSocket udpRQuota = NFS.vertx.createDatagramSocket(udpServerOptions);

            udpPortMap.toFlowable()
                    .subscribe(socket -> {
                        PortMapHandler udpPortMapHandler = new PortMapHandler(udpPortMap, socket.sender());
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpPortMapHandler.isUdp = true;
                        udpPortMapHandler.handle(buf);
                    });

            udpMount.toFlowable()
                    .subscribe(socket -> {
                        MountHandler udpMountHandler = new MountHandler(udpMount, socket.sender());
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpMountHandler.isUdp = true;
                        udpMountHandler.handle(buf);
                    });

            udpNfs.toFlowable()
                    .subscribe(socket -> {
                        NFSHandler udpNfsHandler = new NFSHandler(udpNfs, socket.sender());
                        udpNfsHandler.nfsHandler = udpNfsHandler;
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpNfsHandler.isUdp = true;
                        udpNfsHandler.handle(buf);
                    });

            udpNlm.toFlowable()
                    .subscribe(socket -> {
                        NLMHandler udpNlmHandler = new NLMHandler(udpNlm, socket);
                        udpNlmHandler.nlmHandler = udpNlmHandler;
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpNlmHandler.isUdp = true;
                        udpNlmHandler.handle(buf);
                    });

            udpNsm.toFlowable()
                    .subscribe(socket -> {
                        NSMHandler udpNsmHandler = new NSMHandler(udpNsm, socket);
                        udpNsmHandler.nsmHandler = udpNsmHandler;
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpNsmHandler.isUdp = true;
                        udpNsmHandler.handle(buf);
                    });
            udpRQuota.toFlowable()
                    .subscribe(socket -> {
                        NFSHandler udpNfsHandler = new NFSHandler(udpRQuota, socket.sender());
                        udpNfsHandler.nfsHandler = udpNfsHandler;
                        Buffer data = socket.data();
                        ByteBuf buf = Unpooled.wrappedBuffer(data.getBytes());
                        udpNfsHandler.isUdp = true;
                        udpNfsHandler.handle(buf);
                    });

            portMap.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        PortMapHandler handler = new PortMapHandler(socket);
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });
                    });

            mount.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        MountHandler handler = new MountHandler(socket);
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });
                    });

            nfs.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        NFSHandler handler = new NFSHandler(socket);
                        handler.nfsHandler = handler;

                        NetSocketImpl impl = (NetSocketImpl) socket.getDelegate();
                        impl.messageHandler(event -> {
                            if (event instanceof ByteBuf) {
                                ByteBuf byteBuf = (ByteBuf) event;

                                try {
                                    handler.handle(byteBuf);
                                } catch (Exception e) {
                                    log.error("", e);
                                    socket.close();
                                } finally {
                                    byteBuf.release();
                                }
                            }
                        });
                    });

            nlm.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        NLMHandler handler = new NLMHandler(socket);
                        handler.nlmHandler = handler;
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });
                    });

            nsm.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        NSMHandler handler = new NSMHandler(socket);
                        handler.nsmHandler = handler;
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });
                    });
            rQuota.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        NFSHandler handler = new NFSHandler(socket);
                        handler.nfsHandler = handler;
                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                            } catch (Exception e) {
                                log.error("", e);
                                socket.close();
                            }
                        });
                    });

            portMap.listen(portMapPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen portmap failed, port : {}\ncause : {}", portMapPort, res.cause());
                }
            });

            udpPortMap.listen(portMapPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen portmap failed, port : {}\ncause : {}", portMapPort, res.cause());
                }
            });

            mount.listen(mountPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfs mount failed, port : {}\ncause : {}", mountPort, res.cause());
                }
            });

            udpMount.listen(mountPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfs mount failed, port : {}\ncause : {}", mountPort, res.cause());
                }
            });

            nfs.listen(nfsPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfs failed, port : {}\ncause : {}", nfsPort, res.cause());
                }
            });

            udpNfs.listen(nfsPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfs failed, port : {}\ncause : {}", nfsPort, res.cause());
                }
            });

            nlm.listen(nlmPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nlm failed, port : {}\ncause : {}", nlmPort, res.cause());
                }
            });

            udpNlm.listen(nlmPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nlm failed, port : {}\ncause : {}", nlmPort, res.cause());
                }
            });

            nsm.listen(nsmPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nsm failed, port : {}\ncause : {}", nsmPort, res.cause());
                }
            });

            udpNsm.listen(nsmPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nsm failed, port : {}\ncause : {}", nsmPort, res.cause());
                }
            });

            rQuota.listen(nfsRQuotaPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfsRQuota failed, port : {}\ncause : {}", nfsRQuotaPort, res.cause());
                }
            });
            udpRQuota.listen(nfsRQuotaPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen nfsRQuota failed, port : {}\ncause : {}", nfsRQuotaPort, res.cause());
                }
            });
        }
    }

}
