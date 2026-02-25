package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CBSequenceCall extends CompoundCall {
    public int opt;
    public byte[] sessionId = new byte[16];
    public int seqId;
    public int slotId;
    public int highSlotId;
    public boolean isCache;
    //todo
    public int referringCallList;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        //11
        buf.setInt(offset, opt);
        offset += 4;
        buf.setBytes(offset, sessionId);
        buf.setInt(offset + 16, seqId);
        buf.setInt(offset + 20, slotId);
        buf.setInt(offset + 24, highSlotId);
        buf.setBoolean(offset + 28, isCache);
        buf.setInt(offset + 32, referringCallList);
        return 40;
    }
}
