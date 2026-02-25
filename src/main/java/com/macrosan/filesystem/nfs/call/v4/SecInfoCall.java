package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SecInfoCall extends CompoundCall {
    public byte[] name;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int nameLen = buf.getInt(offset);
        offset += 4;
        name = new byte[nameLen];
        buf.getBytes(offset, name);
        offset += (nameLen + 3) / 4 * 4;
        return offset - start;
    }
}
