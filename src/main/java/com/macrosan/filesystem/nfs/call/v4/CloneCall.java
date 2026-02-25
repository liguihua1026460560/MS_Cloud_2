package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CloneCall extends CompoundCall {
    public StateId srcStateId = new StateId();
    public StateId dstStateId = new StateId();
    public long srcOffset;
    public long dstOffset;
    public long count;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        srcStateId.readStruct(buf, offset);
        dstStateId.readStruct(buf, offset + 16);
        srcOffset = buf.getLong(offset + 32);
        dstOffset = buf.getLong(offset + 40);
        count = buf.getLong(offset + 48);
        return 56;
    }
}