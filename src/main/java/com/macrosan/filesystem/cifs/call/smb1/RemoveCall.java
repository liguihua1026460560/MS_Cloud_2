package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class RemoveCall extends SMB1Body {

    public short sAttr;
    public byte bufFormat = 0x04;
    public char[] fileName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        sAttr = buf.getShortLE(offset + 1);

        int len = super.readStruct(buf, offset);
        int fileNameLen = (byteCount - 1) / 2 - 2;
        fileName = new char[fileNameLen];
        for (int i = 0; i < fileNameLen; i++) {
            fileName[i] = (char) buf.getShortLE(offset + 8 + i * 2);
        }
        return len;
    }
}
