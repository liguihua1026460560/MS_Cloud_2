package com.macrosan.rsocket.data;

import com.macrosan.ec.server.OneUploadServerHandler;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.rsocket.Payload;
import io.vertx.core.impl.VertxImpl;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

@Log4j2
public class DataServer {
    public static final int DATA_PORT = 11116;

    public static void start() {
        Vertx vertx = ServerConfig.getInstance().getVertx();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            VertxImpl impl = (VertxImpl) vertx.getDelegate();
            bootstrap.group(impl.getAcceptorEventLoopGroup(), vertx.nettyEventLoopGroup())
                    .channelFactory(EpollServerSocketChannel::new)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            DataDecoder dataDecoder = new DataDecoder();
//                            ch.config().setAllocator(new MsDataByteBufAllocator(dataDecoder));
                            pipeline.addLast(dataDecoder);
                            pipeline.addLast(new NettyDataHandler(ch, dataDecoder));
                        }
                    });

            ChannelFuture future = bootstrap.bind("0.0.0.0", DATA_PORT).sync();
            log.info("DataServer started in {}", DATA_PORT);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static ByteBuf SUCCESS;
    public static ByteBuf ERROR;

    static {
        SUCCESS = UnpooledByteBufAllocator.DEFAULT.directBuffer(1, 1);
        SUCCESS.writeByte(0);

        ERROR = UnpooledByteBufAllocator.DEFAULT.directBuffer(1, 1);
        ERROR.writeByte(1);
    }


    public static class NettyDataHandler extends ChannelDuplexHandler {
        SocketChannel channel;
        DataDecoder dataDecoder;

        NettyDataHandler(SocketChannel channel, DataDecoder dataDecoder) {
            this.channel = channel;
            this.dataDecoder = dataDecoder;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = ((ByteBuf) msg);
                try {
                    exec(buf);
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }

        public void exec(ByteBuf buf) {
            buf.skipBytes(4);
            long id = buf.readLong();
            handle(buf)
                    .subscribe(b -> {
                        ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(12, 12);
                        header.writeInt(b.readableBytes() + 8);
                        header.writeLong(id);
                        send(header, b);
                    }, e -> {
                        log.error("", e);
                        ByteBuf header = PooledByteBufAllocator.DEFAULT.directBuffer(13, 13);
                        header.writeInt(9);
                        header.writeLong(id);
                        header.writeInt(1);
                        send(header);
                    });
        }

        public void send(ByteBuf... buf) {
            if (channel.eventLoop().inEventLoop()) {
                send0(buf);
            } else {
                channel.eventLoop().submit(() -> send0(buf));
            }
        }

        public void send0(ByteBuf... buf) {
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
    }

    public static Mono<ByteBuf> handle(ByteBuf buf) {
        MonoProcessor<ByteBuf> processor = MonoProcessor.create();

        buf.readerIndex();
        SocketReqMsg msg = SocketReqMsg.toSocketReqMsg(buf);


        OneUploadServerHandler handler = new OneUploadServerHandler(empty, false);
        //TODO before put
        handler.start(msg);
        handler.complete(buf, processor);

        return processor.doFinally(s -> {
            buf.release();
        });
    }

    public static UnicastProcessor<Payload> empty = UnicastProcessor.create();

    static {
        empty.onComplete();
    }

    public static class DataDecoder extends ChannelInboundHandlerAdapter {
        List<ByteBuf> list = new LinkedList<>();
        int size = 0;
        int readSize = -1;

        public DataDecoder() {
        }

        private void decode(ChannelHandlerContext ctx) throws Exception {
            if (readSize == -1 && size >= 4) {
                ByteBuf tmp = Unpooled.wrappedBuffer(list.toArray(new ByteBuf[0]));
                readSize = tmp.getInt(tmp.readerIndex());
            }

            if (readSize >= 0 && size >= readSize + 4) {
                ByteBuf buf = Unpooled.wrappedBuffer(list.toArray(new ByteBuf[0])).slice(0, readSize + 4);
                int clearSize = readSize + 4;
                ListIterator<ByteBuf> listIterator = list.listIterator();
                while (clearSize > 0) {
                    ByteBuf tmp = listIterator.next();
                    int len = Math.min(tmp.readableBytes(), clearSize);
                    tmp.skipBytes(len);
                    clearSize -= len;
                    if (tmp.readableBytes() == 0) {
                        listIterator.remove();
                    } else {
                        //buf.release()会释放一次tmp
                        tmp.retain();
                    }
                }
                size -= readSize + 4;
                readSize = -1;
                super.channelRead(ctx, buf);
            }

            if (size >= 4 && readSize == -1) {
                decode(ctx);
            }
        }

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf data = (ByteBuf) msg;
                size += data.readableBytes();
                list.add(data);
                decode(ctx);
            }
        }
    }
}
