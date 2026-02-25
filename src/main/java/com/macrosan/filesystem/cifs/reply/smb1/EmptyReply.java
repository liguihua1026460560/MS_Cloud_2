package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class EmptyReply extends SMB1Body {

    private EmptyReply() {
        wordCount = 0;
        byteCount = 0;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return super.writeStruct(buf, offset);
    }

    public static EmptyReply DEFAULT = new EmptyReply();
}
