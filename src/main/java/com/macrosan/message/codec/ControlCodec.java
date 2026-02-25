package com.macrosan.message.codec;

import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.serialize.JsonUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

/**
 * ControlCodec
 * * 控制器端的编解码器
 * *
 * * TODO 尝试用protobuf编码，速度慢一点点但是能减轻GC压力，说不定综合性能有提升
 *
 * @author liyixin
 * @date 2018/11/14
 */
public class ControlCodec implements MessageCodec<ResponseMsg, ResponseMsg> {

    @Override
    public void encodeToWire(Buffer buffer, ResponseMsg responseMsg) {
        JsonUtils.toBuffer(responseMsg, ResponseMsg.class, buffer);
    }

    @Override
    public ResponseMsg decodeFromWire(int pos, Buffer buffer) {
        int length = buffer.getInt(pos);
        return JsonUtils.toObject(ResponseMsg.class, buffer.getBytes(pos + 4, pos + 4 + length));
    }

    @Override
    public ResponseMsg transform(ResponseMsg responseMsg) {
        return responseMsg;
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
