package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReNameReply extends SMB1Body {

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return super.writeStruct(buf, offset);
    }

}
