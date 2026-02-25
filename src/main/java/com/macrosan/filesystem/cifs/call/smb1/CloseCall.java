package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class CloseCall extends SMB1Body {
    public short fsid;
    public int lastWrite;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int len = super.readStruct(buf, offset);
        offset += 1;
        fsid = buf.getShortLE(offset);
        offset += 2;
        lastWrite = buf.getIntLE(offset);
        return len;
    }
}
