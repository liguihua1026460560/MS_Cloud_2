package com.macrosan.filesystem.cifs.reply.smb1;

import io.netty.buffer.ByteBuf;

public class SetPathInfoReply extends Trans2Reply {
    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return super.writeStruct(buf, offset);
    }
}
