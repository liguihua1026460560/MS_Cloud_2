package com.macrosan.message.codec;

import com.macrosan.message.mqmessage.RequestMsg;
import com.macrosan.utils.serialize.JsonUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

/**
 * HandlerCodec
 * 负责Handler中eventbus的编解码
 *
 * @author liyixin
 * @date 2018/11/11
 */
public class HandlerCodec implements MessageCodec<RequestMsg, RequestMsg> {
    @Override
    public void encodeToWire(Buffer buffer, RequestMsg requestMsg) {
        JsonUtils.toBuffer(requestMsg, RequestMsg.class, buffer);
    }

    @Override
    public RequestMsg decodeFromWire(int pos, Buffer buffer) {
        int length = buffer.getInt(pos);
        return JsonUtils.toObject(RequestMsg.class, buffer.getBytes(pos + 4, pos + 4 + length));
    }

    @Override
    public RequestMsg transform(RequestMsg requestMsg) {
        return requestMsg;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
