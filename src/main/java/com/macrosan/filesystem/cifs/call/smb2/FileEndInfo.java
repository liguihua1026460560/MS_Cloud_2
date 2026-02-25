package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;

public class FileEndInfo extends GetInfoReply.Info {

    public long endOfFile;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        endOfFile = buf.getLongLE(offset);
        return 8;
    }

    @Override
    public int size() {
        return 0;
    }
}
