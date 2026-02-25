package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SequenceCall extends CompoundCall {
    public byte[] sessionId;
    public int seqId;
    public int slotId;
    public int highSlotId;
    public boolean isCache;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        sessionId = new byte[16];
        buf.getBytes(offset,sessionId);
        seqId = buf.getInt(offset + 16);
        slotId = buf.getInt(offset + 20);
        highSlotId = buf.getInt(offset + 24);
        isCache = buf.getBoolean(offset + 28);
        return 32;
    }
}
