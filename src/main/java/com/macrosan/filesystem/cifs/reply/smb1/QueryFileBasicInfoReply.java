package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/3da7df75-43ba-4498-a6b3-a68ba57ec922
public class QueryFileBasicInfoReply extends Trans2Reply {
    public long creationTime;
    public long lastAccessTime;
    public long lastWriteTime;
    public long lastChangeTime;
    public int mode;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 10;

        paramCount = totalParamCount = 2;
        dataCount = totalDataCount = 36;
        paramOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;
        dataOffset = paramOffset + paramCount * 2;

        byteCount = paramCount * 2 + dataCount + 1;

        int start = dataOffset + 4;

        buf.setLongLE(start, creationTime);
        buf.setLongLE(start + 8, lastAccessTime);
        buf.setLongLE(start + 16, lastWriteTime);
        buf.setLongLE(start + 24, lastChangeTime);
        buf.setIntLE(start + 32, mode);

        return super.writeStruct(buf, offset);
    }
}
