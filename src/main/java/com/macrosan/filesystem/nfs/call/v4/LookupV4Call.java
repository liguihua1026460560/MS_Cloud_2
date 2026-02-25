package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LookupV4Call extends CompoundCall {
    public byte[] name;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int len = buf.getInt(offset);
        name = new byte[len];
        buf.getBytes(offset + 4, name);
        return (len + 3) / 4 * 4 + 4;
    }
}
