package com.macrosan.rsocket.data;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rsocket.VertxLoopResource;
import com.macrosan.utils.msutils.EscapeException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

@Log4j2
public class DataClient extends ChannelDuplexHandler {
    private final static ConcurrentHashMap<Integer, MonoProcessor<ClientHandler>>[] HOLDERS = new ConcurrentHashMap[Math.min(PROC_NUM, 48)];

    static {
        for (int i = 0; i < HOLDERS.length; i++) {
            HOLDERS[i] = new ConcurrentHashMap<>();
        }
    }

    public static LongIntHashMap loopThreads = new LongIntHashMap();
    public static EventLoop[] loops;

    static {
        List<EventLoop> loopList = new LinkedList<>();
        AtomicInteger i = new AtomicInteger();
        VertxLoopResource.VERTX.nettyEventLoopGroup().iterator().forEachRemaining(e -> {
            SingleThreadEventExecutor loop = (SingleThreadEventExecutor) e;
            loopList.add((EventLoop) loop);
            loopThreads.put(loop.threadProperties().id(), i.getAndIncrement());
        });

        loops = loopList.toArray(new EventLoop[loopList.size()]);
    }

    private static void connect(int i, String ip, MonoProcessor<ClientHandler> processor) {
        try {
            Vertx vertx = ServerConfig.getInstance().getVertx();
            Bootstrap bootstrap = new Bootstrap();
            NettyClientHandler handler = new NettyClientHandler();
            bootstrap.group(vertx.nettyEventLoopGroup())
                    .channelFactory(EpollSocketChannel::new)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 0));
                            handler.setChannel(ch);
                            pipeline.addLast(handler);
                        }
                    });

            bootstrap.connect(ip, DataServer.DATA_PORT)
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            log.error("connect failed, ip: {}", ip, f.cause());
                            processor.onNext(ERROR_CLIENT_HANDLER);
                        } else {
                            processor.onNext(handler);
                        }
                    });

        } catch (Exception e) {
            log.error("", e);
            processor.onNext(ERROR_CLIENT_HANDLER);
        }
    }

    private static Mono<ClientHandler> connect(int i, int key, String ip) {
        MonoProcessor<ClientHandler> processor;
        synchronized (HOLDERS[i]) {
            if (HOLDERS[i].get(key) == null) {
                processor = MonoProcessor.create();
                HOLDERS[i].put(key, processor);
            } else {
                return HOLDERS[i].get(key);
            }
        }

        connect(i, ip, processor);

        return processor;
    }

    private static int index = 0;

    public static Mono<ClientHandler> getRSocket(String ip) {
        if (CURRENT_IP.equalsIgnoreCase(ip)) {
            return Mono.just(new LocalClientHandler());
        }

        int i;
        int key = ip.hashCode();
        int loopIndex = loopThreads.getIfAbsent(Thread.currentThread().getId(), -1);
        if (loops.length == HOLDERS.length && loopIndex != -1) {
            i = loopIndex;
        } else {
            i = Math.abs(index++ % HOLDERS.length);
        }

        Mono<ClientHandler> res = HOLDERS[i].get(key);
        if (null == res) {
            res = connect(i, key, ip);
        }

        return res;
    }

    public interface ClientHandler {
        Mono<ByteBuf> handle(ByteBuf buf);
    }

    private static class NettyClientHandler extends ChannelDuplexHandler implements ClientHandler {
        AtomicLong id = new AtomicLong();
        ConcurrentHashMap<Long, MonoProcessor<ByteBuf>> processors = new ConcurrentHashMap<>();
        SocketChannel channel;

        NettyClientHandler() {
        }

        public void setChannel(SocketChannel channel) {
            this.channel = channel;
            channel.closeFuture().addListener(f -> {
                ClosedChannelException closedChannelException = new ClosedChannelException();
                processors.values().forEach(p -> p.onError(closedChannelException));
            });
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = ((ByteBuf) msg);
                    buf.skipBytes(4);
                    long id = buf.readLong();
                    MonoProcessor<ByteBuf> processor = processors.remove(id);
                    if (processor != null) {
                        processor.onNext(buf);
                    } else {
                        log.error("processor is null, id: {}", id);
                        buf.release();
                    }
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        public void send(ByteBuf... buf) {
            for (int i = 0; i < buf.length - 1; i++) {
                channel.write(buf[i]);
            }
            channel.writeAndFlush(buf[buf.length - 1])
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            log.info("send {}:{}", f.isSuccess(), f.cause());
                        }
                    });
        }

        @Override
        public Mono<ByteBuf> handle(ByteBuf buf) {
            long id = this.id.getAndIncrement();
            MonoProcessor<ByteBuf> res = MonoProcessor.create();
            processors.put(id, res);
            int length = buf.readableBytes() + 8;
            ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(12);
            header.writeInt(length);
            header.writeLong(id);

            if (channel.eventLoop().inEventLoop()) {
                send(header, buf);
            } else {
                channel.eventLoop().submit(() -> send(header, buf));
            }

            return res;
        }
    }

    public static class LocalClientHandler implements ClientHandler {
        @Override
        public Mono<ByteBuf> handle(ByteBuf buf) {
            return DataServer.handle(buf);
        }
    }

    public static class ErrorClientHandler implements ClientHandler {
        @Override
        public Mono<ByteBuf> handle(ByteBuf buf) {
            buf.release();
            return Mono.error(new EscapeException("ErrorClientHandler"));
        }
    }

    public static ErrorClientHandler ERROR_CLIENT_HANDLER = new ErrorClientHandler();

}
