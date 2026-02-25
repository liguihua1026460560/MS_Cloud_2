package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadV4Call extends CompoundCall {
    public StateId stateId = new StateId();
    public long offset;
    public int count;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        stateId.readStruct(buf, offset);
        this.offset = buf.getLong(offset + 16);
        count = buf.getInt(offset + 24);
        offset += 28;
        return offset - start;
    }
}
