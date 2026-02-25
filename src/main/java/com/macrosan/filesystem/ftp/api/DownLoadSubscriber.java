package com.macrosan.filesystem.ftp.api;

import io.vertx.core.Context;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 控制数据下载，顺序写入，异常处理
 * 参考 com.macrosan.action.datastream.StreamService.DownLoadSubscriber 实现
 */
@Slf4j
public class DownLoadSubscriber implements CoreSubscriber<byte[]> {

    private NetSocket socket;
    //上下文
    private Context context;
    private Subscription s;
    // 控制数据流顺序
    private final UnicastProcessor<Long> streamController;
    private boolean dataStreamStart = false;

    private MonoProcessor<Boolean> suc;
    // 避免socket未写完数据就关闭
    private AtomicLong write = new AtomicLong(1L);

    public DownLoadSubscriber(NetSocket socket, Context context, UnicastProcessor<Long> streamController, MonoProcessor<Boolean> suc) {
        this.socket = socket;
        this.context = context;
        this.streamController = streamController;
        this.suc = suc;
        socket.exceptionHandler(e -> {
            suc.onNext(false);
        });
        socket.closeHandler(e -> {
            suc.onNext(false);
        });
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.s = s;
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onError(Throwable t) {
        if (!dataStreamStart) {
            if ("not match".equals(t.getMessage())) {
                socket.end();
                suc.onNext(false);
                return;
            }
            // TODO 处理异常

        } else {
            socket.end();
            suc.onNext(false);
            socket.close();
        }
        s.cancel();
    }

    @Override
    public void onNext(byte[] bytes) {
        try {
            write.incrementAndGet();
            context.runOnContext(v -> {
                try {
                    socket.write(Buffer.buffer(bytes), unit -> {
                        streamController.onNext(-1L);
                        dataStreamStart = true;
                        checkAndCloseSocket();
                    });
                } catch (Exception e) {
                    log.error("error={}", e);
                }
            });
        } catch (Exception e) {
            log.error("exception in write : {}", e);
            s.cancel();
        }
    }

    @Override
    public void onComplete() {
        context.runOnContext(v -> {
            checkAndCloseSocket();
        });
    }

    private void checkAndCloseSocket() {
        if (write.decrementAndGet() == 0) {
            try {
                socket.end(v0 -> {
                    suc.onNext(true);
                });
            } catch (Exception e) {
                log.error("error={}", e);
            }
        }
    }
}