package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LockUV4Call extends CompoundCall {
    public int lockType;
    public int seqId;
    public StateId stateId = new StateId();
    public long offset;
    public long length;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        lockType = buf.getInt(offset);
        seqId = buf.getInt(offset + 4);
        stateId.readStruct(buf, offset + 8);
        this.offset = buf.getLong(offset + 24);
        length = buf.getLong(offset + 32);
        return 40;
    }
}

