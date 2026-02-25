package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class RenewCall extends CompoundCall {
    public long clientId;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        clientId = buf.getLong(offset);
        return 8;
    }
}
