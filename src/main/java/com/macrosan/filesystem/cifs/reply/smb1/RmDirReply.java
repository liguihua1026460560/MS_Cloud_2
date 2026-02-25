package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class RmDirReply extends SMB1Body {
    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return super.writeStruct(buf, offset);
    }
}
