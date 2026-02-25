package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/3bdd080c-f8a4-4a09-acf1-0f8bd00152e4
public class QueryFileStandInfoReply extends Trans2Reply {
    public long allocationSize;
    public long endOfFile;
    public int nlinks;
    public byte deletePending;
    public byte directory;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 10;

        paramCount = totalParamCount = 2;
        dataCount = totalDataCount = 24;
        paramOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;
        dataOffset = paramOffset + paramCount * 2;

        byteCount = paramCount * 2 + dataCount + 1;

        int start = dataOffset + 4;

        buf.setLongLE(start, allocationSize);
        buf.setLongLE(start + 8, endOfFile);
        buf.setIntLE(start + 16, nlinks);
        buf.setByte(start + 20, deletePending);
        buf.setByte(start + 21, directory);

        return super.writeStruct(buf, offset);
    }
}
