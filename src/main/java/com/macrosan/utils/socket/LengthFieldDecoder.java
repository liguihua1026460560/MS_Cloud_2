package com.macrosan.utils.socket;

import io.vertx.core.buffer.Buffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

/**
 * @author zhaoyang
 * @date 2026/02/11
 * @description 对socket返回的flux流进行协议解析，[8字节长度][数据Body] -> 去除头，只返回Body，同时通过readController 控制读取的速度
 **/
public class LengthFieldDecoder extends BaseSubscriber<Buffer> {

    private final UnicastProcessor<Buffer> output;
    private final Publisher<Long> readController;
    private long totalLength = -1;
    private long receivedLength = 0;
    private final Buffer headBuffer = Buffer.buffer(8);

    public LengthFieldDecoder(Publisher<Buffer> rawStream, Publisher<Long> readController) {
        this.output = UnicastProcessor.create();
        this.readController = readController;
        rawStream.subscribe(this);
    }

    public LengthFieldDecoder(Publisher<Buffer> rawStream) {
        this(rawStream, null);
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        if (readController != null) {
            // 订阅读取控制器
            readController.subscribe(new BaseSubscriber<Long>() {
                @Override
                protected void hookOnNext(Long value) {
                    subscription.request(value);
                }
            });
            // 请求第一个数据
            subscription.request(1);
        } else {
            // 如果没有读取控制器，则一次性请求所有数据
            subscription.request(Long.MAX_VALUE);
        }
    }

    @Override
    protected void hookOnNext(Buffer buffer) {
        if (totalLength != -1) {
            // 头部已解析，直接转发数据并统计
            receivedLength += buffer.length();
            output.onNext(buffer);
            return;
        }
        // 解析长度头
        headBuffer.appendBuffer(buffer);
        if (headBuffer.length() >= 8) {
            // 计算总长度
            totalLength = headBuffer.getLong(0);
            if (totalLength == 0) {
                output.onError(new RuntimeException("Total length is zero"));
                return;
            }
            Buffer dataPart = headBuffer.slice(8, headBuffer.length());
            int dataSize = dataPart.length();
            if (dataSize > 0) {
                // 返回剩余字节
                receivedLength += dataSize;
                output.onNext(dataPart);
            }
        } else {
            // 头部未解析完毕，则不会向下流发送数据，因此需要主动请求下一个数据
            upstream().request(1);
        }
    }

    @Override
    protected void hookOnComplete() {
        if (totalLength == -1) {
            output.onError(new RuntimeException("No data received"));
        } else if (totalLength != receivedLength) {
            output.onError(new RuntimeException("Incomplete data: expected " + totalLength + ", received " + receivedLength));
        } else {
            output.onComplete();
        }
    }

    @Override
    protected void hookOnError(Throwable throwable) {
        output.onError(throwable);
    }

    public Flux<Buffer> decode() {
        // 收到下游cancel信号，则向上游发送cancel信号
        return output.doOnCancel(this::cancel);
    }

}
