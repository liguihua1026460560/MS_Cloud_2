package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/7539feb4-6fbb-4996-81ac-06863bb1a89e
 */
public class LogOffReply extends SMB2Body {

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 4;
        return super.writeStruct(buf, offset) + 2;
    }
}