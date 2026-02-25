package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LockTV4Call extends CompoundCall {
    public int lockType;
    public long offset;
    public long length;
    public int ownerLen;
    public byte[] owner;
    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        lockType = buf.getInt(offset);
        this.offset = buf.getLong(offset + 4);
        length = buf.getLong(offset + 12);
        offset += 20;
        ownerLen = buf.getInt(offset);
        owner=new byte[ownerLen];
        buf.getBytes(offset + 4, owner);
        offset += (ownerLen + 3) / 4 * 4;
        return offset - start;
    }
}

