package com.macrosan.utils.socket;

import com.macrosan.utils.msutils.BytesWrapperBuffer;

import com.macrosan.utils.msutils.MsException;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;


import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;

import io.vertx.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

import static com.macrosan.constants.ErrorNo.GET_OBJECT_CANCELED;

/**
 * @author zhaoyang
 * @date 2026/02/09
 * @description 基于 Vert.x NetClient 的 TCP 客户端
 **/
@Log4j2
public class VertxTcpClient {
    private final NetClient client;

    public VertxTcpClient(Vertx vertx) {
        NetClientOptions options = new NetClientOptions()
                .setConnectTimeout(10_000)
                .setTcpNoDelay(true)
                .setTcpKeepAlive(true)
                .setIdleTimeout(300)
                .setReuseAddress(true)
                .setReusePort(true);

        this.client = vertx.createNetClient(options);
    }

    /**
     * 发送请求并返回响应流
     *
     * @param dataflux         数据源 Flux
     * @param streamController 流量控制器，调用 onNext 触发拉取
     * @param host             目标主机
     * @param port             目标端口
     * @return 响应数据的 Flux
     */
    public Flux<Buffer> sendRequest(byte[] header, Flux<byte[]> dataflux, UnicastProcessor<Long> streamController, String host, int port) {

        return Flux.create(sink -> {
            // 1. 异步建立连接
            client.connect(port, host, res -> {
                if (res.failed()) {
                    sink.error(res.cause());
                    // 通知上游取消数据拉取
                    streamController.onError(new MsException(GET_OBJECT_CANCELED, "socket connect failed"));
                    return;
                }
                NetSocket socket = res.result();
                // 2. 读控制
                socket.pause();
                // 3. 处理接收响应 (Read)
                socket.handler(sink::next)
                        .exceptionHandler(sink::error)
                        .endHandler(v -> sink.complete());

                // 4. 下游背压信号
                sink.onRequest(socket::fetch);

                sink.onCancel(socket::close);
                // 5. 处理发送请求 (Write)
                writeDataFlux(header, dataflux, streamController, sink, socket);
            });
        });
    }

    private void writeDataFlux(byte[] header, Flux<byte[]> dataflux, UnicastProcessor<Long> streamController, FluxSink<Buffer> sink, NetSocket socket) {
        socket.write(new BytesWrapperBuffer(header), ar -> {
            if (ar.failed()) {
                log.error("Write failed:", ar.cause());
                socket.close();
                // 通知上游取消数据拉取
                streamController.onError(new MsException(GET_OBJECT_CANCELED, "socket write failed"));
                sink.error(ar.cause());
                return;
            }
            dataflux.subscribe(dataChunk -> {
                        socket.write(new BytesWrapperBuffer(dataChunk), ar0 -> {
                            if (ar0.failed()) {
                                log.error("Write failed:", ar0.cause());
                                socket.close();
                                // 通知上游取消数据拉取
                                streamController.onError(new MsException(GET_OBJECT_CANCELED, "socket write failed"));
                                sink.error(ar0.cause());
                            } else {
                                // 写入成功，拉取下一个元素
                                if (streamController != null) {
                                    // 写入成功，拉取下一个元素
                                    streamController.onNext(1L);
                                }
                            }
                        });
                    },
                    error -> {
                        // isTerminated,则说明出现异常 主动取消了dataflux流，此处不需要再处理
                        if (!streamController.isTerminated()) {
                            socket.close();
                            sink.error(error);
                        }
                    });

        });

    }
}
