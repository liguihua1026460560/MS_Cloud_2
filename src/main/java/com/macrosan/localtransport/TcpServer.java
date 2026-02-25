package com.macrosan.localtransport;

import com.macrosan.message.jsonmsg.CliCommand;
import com.macrosan.utils.serialize.JsonUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.InternetProtocolFamily;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.net.ClientOptionsBase;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.impl.transport.Transport;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.net.NetServer;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import io.vertx.reactivex.core.parsetools.RecordParser;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;

/**
 * TcpServer
 *
 * @author liyixin
 * @date 2019/10/14
 */
@Log4j2
public class TcpServer extends AbstractVerticle {

    private static class UnixTransport extends Transport {
        Transport transport;

        public UnixTransport(Transport transport) {
            this.transport = transport;
        }

        public boolean isAvailable() {
            return transport.isAvailable();
        }

        public Throwable unavailabilityCause() {
            return transport.unavailabilityCause();
        }

        public java.net.SocketAddress convert(io.vertx.core.net.SocketAddress address, boolean resolved) {
            return transport.convert(address, resolved);
        }

        public io.vertx.core.net.SocketAddress convert(java.net.SocketAddress address) {
            return transport.convert(address);
        }

        public EventLoopGroup eventLoopGroup(int nThreads, ThreadFactory threadFactory, int ioRatio) {
            return transport.eventLoopGroup(nThreads, threadFactory, ioRatio);
        }

        public DatagramChannel datagramChannel() {
            return transport.datagramChannel();
        }

        public DatagramChannel datagramChannel(InternetProtocolFamily family) {
            return transport.datagramChannel(family);
        }

        public ChannelFactory<? extends Channel> channelFactory(boolean domainSocket) {
            return transport.channelFactory(domainSocket);
        }

        public ChannelFactory<? extends ServerChannel> serverChannelFactory(boolean domainSocket) {
            return transport.serverChannelFactory(domainSocket);
        }

        public void configure(DatagramChannel channel, DatagramSocketOptions options) {
            transport.configure(channel, options);
        }

        public void configure(ClientOptionsBase options, boolean domainSocket, Bootstrap bootstrap) {
            transport.configure(options, domainSocket, bootstrap);
        }

        public void configure(NetServerOptions options, boolean domainSocket, ServerBootstrap bootstrap) {
            if (!domainSocket) {
                transport.configure(options, domainSocket, bootstrap);
            } else {
                if (options.isTcpFastOpen()) {
                    bootstrap.option(EpollChannelOption.TCP_FASTOPEN, options.isTcpFastOpen() ? 256 : 0);
                }

                super.configure(options, domainSocket, bootstrap);
            }
        }
    }

    private static final NetServerOptions options = new NetServerOptions()
            .setUsePooledBuffers(true);

    @Override
    public void start() throws Exception {
        VertxImpl vertxImpl = (VertxImpl) vertx.getDelegate();
        Transport transport = vertxImpl.transport();

        if (!(transport instanceof UnixTransport)) {
            try {
                Field field = vertxImpl.getClass().getDeclaredField("transport");
                field.setAccessible(true);
                UnixTransport nfsTransport = new UnixTransport(transport);
                field.set(vertxImpl, nfsTransport);
            } catch (Exception e) {
                log.error("", e);
                System.exit(-1);
            }
        }

        final NetServer localServer = vertx.createNetServer(options);
        localServer.connectHandler(socket ->
                        RecordParser
                                .newDelimited("\n", socket)
                                .handler(buf -> {
                                    log.info("receive : {}", buf.toString());
                                    final CliCommand command = JsonUtils.toObject(CliCommand.class, buf.getBytes());
                                    if (command == null) {
                                        defaultResponse(socket);
                                        return;
                                    }
                                    socket.write(CommandExecutor.execute(command) + '\n');
                                }))
                .listen(SocketAddress.domainSocketAddress("/tmp/local.sock"), res -> {
                    if (res.failed()) {
                        log.error("start local tcp server failed : {}", res.cause());
                    }
                });
    }

    private static void defaultResponse(NetSocket socket) {

    }
}
