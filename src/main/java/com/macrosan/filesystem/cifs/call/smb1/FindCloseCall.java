package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class FindCloseCall extends SMB1Body {
    public short searchId;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        super.readStruct(buf, offset);
        offset += 1;
        searchId = buf.getShortLE(offset);
        return 2 + byteCount;
    }
}
