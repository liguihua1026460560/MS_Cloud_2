package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;

public class TreeDisConnectReply extends SMB2Body {
    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 4;
        return super.writeStruct(buf, offset) + 2;
    }
}
