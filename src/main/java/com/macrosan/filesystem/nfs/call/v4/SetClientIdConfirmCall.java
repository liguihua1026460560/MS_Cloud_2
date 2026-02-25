package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetClientIdConfirmCall extends CompoundCall {
    public long clientId;
    public byte[] verifier = new byte[8];

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        clientId = buf.getLong(offset);
        buf.getBytes(offset + 8, verifier);
        return 16;
    }
}
