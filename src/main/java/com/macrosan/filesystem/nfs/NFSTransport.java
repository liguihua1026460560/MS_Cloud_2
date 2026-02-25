package com.macrosan.filesystem.nfs;

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
import io.vertx.core.net.ClientOptionsBase;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.impl.transport.Transport;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

public class NFSTransport extends Transport {
    Transport transport;

    public NFSTransport(Transport transport) {
        this.transport = transport;
    }

    public boolean isAvailable() {
        return transport.isAvailable();
    }

    public Throwable unavailabilityCause() {
        return transport.unavailabilityCause();
    }

    public SocketAddress convert(io.vertx.core.net.SocketAddress address, boolean resolved) {
        return transport.convert(address, resolved);
    }

    public io.vertx.core.net.SocketAddress convert(SocketAddress address) {
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
        channel.config().setOption(EpollChannelOption.IP_RECVORIGDSTADDR, true);
    }

    public void configure(ClientOptionsBase options, boolean domainSocket, Bootstrap bootstrap) {
        transport.configure(options, domainSocket, bootstrap);
    }

    public void configure(NetServerOptions options, boolean domainSocket, ServerBootstrap bootstrap) {
        transport.configure(options, domainSocket, bootstrap);
    }

}
