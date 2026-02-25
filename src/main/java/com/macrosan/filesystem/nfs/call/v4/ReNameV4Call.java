package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReNameV4Call extends CompoundCall {
    public int sourceNameLen;
    public byte[] sourceName;
    public int targetNameLen;
    public byte[] targetName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        sourceNameLen = buf.getInt(offset);
        sourceName = new byte[sourceNameLen];
        offset += 4;
        buf.getBytes(offset, sourceName);
        int len = (sourceNameLen + 3) / 4 * 4;
        offset += len;
        targetNameLen = buf.getInt(offset);
        targetName = new byte[targetNameLen];
        buf.getBytes(offset + 4, targetName);
        int len1 = (targetNameLen + 3) / 4 * 4;
        offset = offset + 4 + len1;

        return offset - head;
    }
}
