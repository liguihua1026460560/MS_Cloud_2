package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;

public class TreeDisconnectCall extends SMB2Body {
    public short reserved;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return super.readStruct(buf, offset) + 2;
    }
}
