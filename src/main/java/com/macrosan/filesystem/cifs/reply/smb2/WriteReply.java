package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;

public class WriteReply extends SMB2Body {

    public int writeCount;
    public int writeRemaining;
    public short channelInfoOffset;
    public short channelInfoLength;

    public static final int SIZE = 16;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        offset += super.writeStruct(buf, offset);
        offset += 2;//reserved
        buf.setIntLE(offset, writeCount);
        buf.setIntLE(offset + 4, writeRemaining);
        buf.setShortLE(offset + 8, channelInfoOffset);
        buf.setShortLE(offset + 10, channelInfoLength);
        return SIZE;
    }
}

