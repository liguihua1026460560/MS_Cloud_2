package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class MkDirCall extends SMB1Body {

    public byte bufferFormat = (byte) 0x04;

    public char[] dirName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        super.readStruct(buf, offset);
        offset += 6;
        int len = (byteCount - 1) / 2 - 2;
        dirName = new char[len];
        for (int i = 0; i < len; i++) {
            dirName[i] = (char) buf.getShortLE(offset + 2 * i);
        }
        return 3 + byteCount;
    }
}
