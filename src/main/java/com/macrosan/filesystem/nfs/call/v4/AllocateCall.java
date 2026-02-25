package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class AllocateCall extends CompoundCall {
    public StateId stateId = new StateId();
    public long offset;
    public long length;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        stateId.readStruct(buf, offset);
        this.offset = buf.getLong(offset + 16);
        length = buf.getLong(offset + 24);
        return 32;
    }
}

