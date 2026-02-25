package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class RemoveV4Call extends CompoundCall {
    public int nameLen;
    public byte[] name;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        nameLen = buf.getInt(offset);
        name = new byte[nameLen];
        buf.getBytes(offset + 4, name);
        return 4 + (nameLen + 3) / 4 * 4;
    }
}
