package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReleaseLockOwnerCall extends CompoundCall {
    public long clientId;
    public int ownerLen;
    public byte[] owner;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        clientId = buf.getLong(offset);
        ownerLen = buf.getInt(offset + 8);
        offset += 12;
        owner = new byte[ownerLen];
        buf.getBytes(offset, owner);
        offset += (ownerLen + 3) / 4 * 4;
        return offset - start;
    }
}

