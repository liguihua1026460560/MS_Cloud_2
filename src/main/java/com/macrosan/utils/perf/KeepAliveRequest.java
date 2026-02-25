package com.macrosan.utils.perf;

import com.macrosan.httpserver.MsHttpRequest;
import io.rsocket.Payload;
import io.vertx.core.buffer.Buffer;
import lombok.Data;
import lombok.experimental.Accessors;
import reactor.core.publisher.FluxSink;

/**
 * Copyright 2019 MacroSAN, Co.Ltd. All rights reserved.
 * 类名称  ：KeepAliveRequest
 * 描述    ：
 * 作者    ：yiguangzheng
 * 创建时间：2019/10/15
 * *******************************************************
 * 修改时间        修改人             修改原因
 * *******************************************************
 * 2019/10/15         yiguangzheng      初始版本
 */
@Data
@Accessors(chain = true)
public class KeepAliveRequest {
    MsHttpRequest request;
    Buffer httpBodyChunk;
    FluxSink<Payload> sink;
    int position;
    long waitedTime;
    long totalWaitTime;
    public static final long KEEP_SOCKET_PERIOD = 20 * 1000;

    public KeepAliveRequest add(long waitedTime) {
        this.position++;
        this.waitedTime += waitedTime;
        return this;
    }
}
