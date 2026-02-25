package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class EchoCall extends SMB1Body {
    public short echoCount;
    public byte echoData;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        int start = offset + 1;
        echoCount = buf.getShortLE(start);
        start = offset + 3 + wordCount * 2;
        echoData = buf.getByte(start);
        return size;
    }
}
