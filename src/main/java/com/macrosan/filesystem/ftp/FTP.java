package com.macrosan.filesystem.ftp;

import com.macrosan.constants.ServerConstants;
import com.macrosan.filesystem.ftp.handler.DataTransferHandler;
import com.macrosan.filesystem.ftp.handler.ControlHandler;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function2;
import com.macrosan.utils.functional.Function3;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.CERT_CRT;
import static com.macrosan.constants.SysConstants.PRIVATE_PEM;
import static com.macrosan.filesystem.ftp.FTPPort.FTP_DATA_MAX_PORT;
import static com.macrosan.filesystem.ftp.FTPPort.FTP_DATA_MIN_PORT;

@Log4j2
public class FTP {
    public static boolean ftpDebug = false;
    static int ftpControllerPort = 21;
    static int ftpsControllerPort = 990;
    static Vertx vertx;


    public static void start() {
        initProc();
        initDataTransfer();

        DeploymentOptions options = new DeploymentOptions()
                .setInstances(ServerConstants.PROC_NUM);
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                .setPreferNativeTransport(true));

        vertx.rxDeployVerticle(FTPVerticle.class.getName(), options).subscribe();
        log.info("start ftp control service in {}", ftpControllerPort);
        log.info("start ftps control service in {}", ftpsControllerPort);
        log.info("start ftp data service in {}->{}", FTP_DATA_MIN_PORT, FTP_DATA_MAX_PORT);
    }


    public static class FTPVerticle extends AbstractVerticle {
        @Override
        public void start() {
            NetServerOptions serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setSsl(false)
                    .setPemKeyCertOptions(new PemKeyCertOptions().setKeyPath(PRIVATE_PEM)
                            .setCertPath(CERT_CRT));


            NetServer ftpControlSSL = FTP.vertx.createNetServer(serverOptions);

            ftpControlSSL.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        ControlHandler handler = new ControlHandler();
                        handler.handle(socket, context);
                    });

            ftpControlSSL.listen(ftpControllerPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen ftpControllerPort SSL failed, port : {}\ncause : {}", ftpControllerPort, res.cause());
                }
            });

            // 增加 ftp隐式 ssl连接
            serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setSsl(true)
                    .setPemKeyCertOptions(new PemKeyCertOptions().setKeyPath(PRIVATE_PEM)
                            .setCertPath(CERT_CRT))
                    .addEnabledCipherSuite("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
                    .addEnabledCipherSuite("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
                    .addEnabledCipherSuite("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
                    .addEnabledCipherSuite("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");


            NetServer ftpsControlSSL = FTP.vertx.createNetServer(serverOptions);

            ftpsControlSSL.connectStream()
                    .toFlowable()
                    .subscribe(socket -> {
                        ControlHandler handler = new ControlHandler();
                        handler.handle(socket, context);
                    });

            ftpsControlSSL.listen(ftpsControllerPort, "0.0.0.0", res -> {
                if (!res.succeeded()) {
                    log.error("Listen ftpsControllerPort SSL failed, port : {}\ncause : {}", ftpsControllerPort, res.cause());
                }
            });

            serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setIdleTimeout(10);

            for (int port = FTP_DATA_MIN_PORT; port <= FTP_DATA_MAX_PORT; port++) {
                NetServer ftpData = FTP.vertx.createNetServer(serverOptions);
                int finalPort = port;
                ftpData.connectStream()
                        .toFlowable()
                        .subscribe(socket -> {
                            DataTransferHandler handler = new DataTransferHandler();
                            handler.handle(finalPort, socket);
                        });

                ftpData.listen(port, "0.0.0.0", res -> {
                    if (!res.succeeded()) {
                        log.error("Listen ftpControllerPort failed, port : {}\ncause : {}", finalPort, res.cause());
                    }
                });
            }
        }
    }

    public static class DataTransferOptInfo {
        public Function3<FTPRequest, Session, NetSocket, Mono<String>> function;

        public Mono<String> run(FTPRequest ftpRequest, Session session, NetSocket socket) {
            return function.apply(ftpRequest, session, socket);
        }
    }

    // 存储 dataTransfer 方法
    public static Map<String, DataTransferOptInfo> dataTransferOpt = new HashMap<>();

    /**
     * 反射获取api包下的所有类，过滤出满足条件的 DataTransfer 方法，并存储在 dataTransferOpt map中，用于请求到来时对处理方法的映射
     */
    public static void initDataTransfer() {
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.ftp.api", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(FTPRequest.FTPOpt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if (param[0] == FTPRequest.class && param[1] == Session.class && param[2] == NetSocket.class) {
                            return true;
                        }
                    }
                    return false;
                }).subscribe(m -> {
                    try {
                        FTPRequest.FTPOpt opt = m.getAnnotation(FTPRequest.FTPOpt.class);
                        DataTransferOptInfo info = new DataTransferOptInfo();
                        info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        dataTransferOpt.put(opt.value().name(), info);
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init ftp data transfer opt num {}", succ.get());
    }

    public static class OptInfo {
        public Function2<FTPRequest, Session, Mono<String>> function;

        public Mono<String> run(FTPRequest ftpRequest, Session session) {
            return function.apply(ftpRequest, session);
        }
    }

    // 存储 ftpProc 方法
    public static Map<String, OptInfo> ftpOpt = new HashMap<>();

    /**
     * 反射获取api包下的所有类，过滤出满足条件的 control(对应FTProc类) 方法，并存储在 ftpOpt map中，用于请求到来时对处理方法的映射
     */
    public static void initProc() {
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.ftp.api", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(FTPRequest.FTPOpt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 2) {
                        if (param[0] == FTPRequest.class && param[1] == Session.class) {
                            return true;
                        }
                    }
                    return false;
                }).subscribe(m -> {
                    try {
                        FTPRequest.FTPOpt opt = m.getAnnotation(FTPRequest.FTPOpt.class);
                        OptInfo info = new OptInfo();
                        info.function = ClassUtils.generateFunction2(m.getDeclaringClass(), m);
                        ftpOpt.put(opt.value().name(), info);
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init ftp opt num {}", succ.get());
    }
}
