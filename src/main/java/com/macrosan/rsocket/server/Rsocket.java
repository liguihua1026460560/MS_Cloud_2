package com.macrosan.rsocket.server;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rsocket.client.RSocketClient;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.rsocket.RSocketFactory;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.transport.netty.TcpDuplexConnection;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpServer;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.rsocket.VertxLoopResource.VERTX_LOOP_RESOURCE;

/**
 * RSocketServer
 *
 * @author liyixin
 * @date 2019/8/29
 */
@Log4j2
public class Rsocket {

    public static final int BACK_END_PORT = 11115;
    public static final int MERGE_PUT_PORT = 11116;
    public static Map<String, Long> notPrintErr = new ConcurrentHashMap<>();
    private static CloseableChannel takeOverServer = null;

    private static TcpServer createTcpServer(String ip) {
        return TcpServer.create()
//                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .runOn(VERTX_LOOP_RESOURCE, true)
                .addressSupplier(() -> new InetSocketAddress(ip, BACK_END_PORT));
    }

    static Field connectionField;

    static {
        try {
            connectionField = TcpDuplexConnection.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
        } catch (Exception e) {
            log.error("", e);
            System.exit(-1);
        }
    }

    public static void init(String ip, boolean takeOver) {
        final CloseableChannel channel = RSocketFactory.receive()
                .frameDecoder(MsPayloadDecoder.DEFAULT)
                .errorConsumer(e -> {
                    if (null != e && null != e.toString() && (e.toString().contains("Connection reset by peer") || e.toString().contains("No keep-alive acks for"))) {
                        String message = e.toString();
                        if (!notPrintErr.containsKey(message)) {
                            notPrintErr.compute(message, (k ,v) -> {
                               if (null == v) {
                                   log.error("", e);
                                   v = System.currentTimeMillis();
                               }

                               return v;
                            });
                        }
                    } else {
                        log.error("", e);
                    }
                })
                .addConnectionPlugin((type, conn) -> {
                    if (type == DuplexConnectionInterceptor.Type.SOURCE) {
                        if (conn instanceof TcpDuplexConnection) {
                            try {
                                Connection connection = (Connection) connectionField.get(conn);
                                String srcIP = connection.address().getHostString();

                                int key = (srcIP + BACK_END_PORT).hashCode();
                                RSocketClient.clearErrorRSocket(key);
                            } catch (Exception e) {
                                log.error("", e);
                            }
                        }
                    }
                    return conn;
                })
                .acceptor(((setup, sendingSocket) -> {
                    return Mono.just(new ErasureServer());
                }))
                .transport(() -> {
                    log.info("rsocket server bind ip : {}", ServerConfig.getInstance().getHeartIp1());
                    return TcpServerTransport.create(createTcpServer(ip));
                })
                .start()
                .block();

        if (takeOver) {
            if (takeOverServer != null) {
                log.error("take over server has been started");
            } else {
                takeOverServer = channel;
            }
        }
    }

    public static void init() {
        init(ServerConfig.getInstance().getHeartIp1(), false);
    }

    public static void stop() {
        if (takeOverServer != null) {
            log.info("close take over server");
            takeOverServer.dispose();
        }
    }
}
