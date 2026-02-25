package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class DestroySessionCall extends CompoundCall {
    public byte[] sessionId;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        sessionId = new byte[16];
        buf.getBytes(offset, sessionId);
        return 16;
    }
}
