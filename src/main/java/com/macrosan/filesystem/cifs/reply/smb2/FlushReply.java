package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class FlushReply extends SMB2Body {

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 4;
        return super.writeStruct(buf, offset) + 2;
    }
}
