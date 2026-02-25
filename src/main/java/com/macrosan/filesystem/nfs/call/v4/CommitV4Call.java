package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CommitV4Call extends CompoundCall {
    public long offset;
    public int count;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        this.offset = buf.getLong(offset);
        count = buf.getInt(offset + 8);
        return 12;
    }
}
