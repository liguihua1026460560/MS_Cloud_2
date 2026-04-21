package com.macrosan.storage.coder.direct;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.MonoProcessor;

/**
 * 直接使用堆外内存的Encoder
 */
public interface DirectEncoder {
    void putAndComplete(ByteBuf buf);

    MonoProcessor<ByteBuf[]>[] data();
}
