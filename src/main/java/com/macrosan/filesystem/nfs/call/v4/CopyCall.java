package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CopyCall extends CompoundCall {
    public StateId srcStateId = new StateId();
    public StateId dstStateId = new StateId();
    public long srcOffset;
    public long dstOffset;
    public long count;
    public boolean consecutive;
    public boolean synchronous;
    public int[] sourceServer;//c not support

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        srcStateId.readStruct(buf, offset);
        dstStateId.readStruct(buf, offset + 16);
        srcOffset = buf.getLong(offset + 32);
        dstOffset = buf.getLong(offset + 40);
        count = buf.getLong(offset + 48);
        consecutive = buf.getBoolean(offset + 56);
        synchronous = buf.getBoolean(offset + 60);
        sourceServer = new int[1];
        sourceServer[1] = buf.getInt(offset + 64);
        return 68;
    }
}
