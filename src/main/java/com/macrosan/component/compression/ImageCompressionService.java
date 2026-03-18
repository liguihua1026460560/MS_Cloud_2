package com.macrosan.component.compression;

import com.macrosan.utils.socket.LengthFieldDecoder;
import com.macrosan.utils.socket.VertxTcpClient;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.nio.ByteBuffer;

/**
 * @author zhaoyang
 * @date 2026/02/05
 * @description 压缩服务，处理压缩解压协议，发送socket请求到压缩服务器
 **/
@Log4j2
public class ImageCompressionService {
    private static final byte COMPRESS = 'C';
    private static final byte DECOMPRESS = 'U';

    private final VertxTcpClient tcpClient;


    public ImageCompressionService(Vertx vertx) {
        tcpClient = new VertxTcpClient(vertx);
    }

    public Flux<Buffer> compressImage(Flux<byte[]> data, long dataSize, UnicastProcessor<Long> streamController, CompressionServerManager.ServerInfo serverInfo) {
        byte[] header = buildHeader(COMPRESS, dataSize);
        Flux<Buffer> rawResponse = tcpClient.sendRequest(header, data, streamController, serverInfo.getIp(), serverInfo.getPort());
        return new LengthFieldDecoder(rawResponse).decode();
    }

    public Flux<Buffer> decompressImage(Flux<byte[]> data, long dataSize, UnicastProcessor<Long> streamController, Publisher<Long> readController, CompressionServerManager.ServerInfo serverInfo) {
        byte[] header = buildHeader(DECOMPRESS, dataSize);
        Flux<Buffer> rawResponse = tcpClient.sendRequest(header, data, streamController, serverInfo.getIp(), serverInfo.getPort());
        return new LengthFieldDecoder(rawResponse, readController).decode();
    }

    private byte[] buildHeader(byte mode, long size) {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(mode);
        buffer.putLong(size);
        return buffer.array();
    }
}
