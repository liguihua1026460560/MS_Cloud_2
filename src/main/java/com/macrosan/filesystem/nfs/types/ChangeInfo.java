package com.macrosan.filesystem.nfs.types;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ChangeInfo {
    public int atomic = 1;
    public long beforeChangeId = 0;
    public long afterChangeId = 0;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, atomic);
        buf.setLong(offset + 4, beforeChangeId);
        buf.setLong(offset + 12, afterChangeId);
        return 20;
    }
}
