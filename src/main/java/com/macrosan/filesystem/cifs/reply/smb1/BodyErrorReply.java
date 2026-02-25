package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class BodyErrorReply extends SMB1Body {

    private BodyErrorReply() {

    }

    public short status = -1;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setShortLE(offset + 1, status);
        return super.writeStruct(buf, offset);
    }

    public static BodyErrorReply errorReply(int status) {
        BodyErrorReply errorReply = new BodyErrorReply();
        errorReply.status = (short) status;
        errorReply.byteCount = 0;
        errorReply.wordCount = 1;
        return errorReply;
    }
}
