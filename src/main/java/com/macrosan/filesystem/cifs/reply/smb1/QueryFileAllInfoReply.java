package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/162baf45-4201-4b07-a397-060e868599d7
@ToString(callSuper = true)
public class QueryFileAllInfoReply extends Trans2Reply {
    public long creationTime;
    public long lastAccessTime;
    public long lastWriteTime;
    public long lastChangeTime;
    public int mode;
    public long allocationSize;
    public long endOfFile;
    public int nlinks;
    byte deletePending;
    byte directory;
    int eaSize;
    byte[] fileName = new byte[]{0x5c, 0};

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        deletePending = 0;
        directory = (byte) ((mode & FsConstants.FILE_ATTRIBUTE_DIRECTORY) != 0 ? 1 : 0);
        eaSize = 0;

        wordCount = 10;

        paramCount = totalParamCount = 2;
        dataCount = totalDataCount = 72 + fileName.length;
        paramOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;
        dataOffset = paramOffset + paramCount * 2;

        byteCount = paramCount * 2 + dataCount + 1;

        int start = dataOffset + 4;
        buf.setLongLE(start, creationTime);
        buf.setLongLE(start + 8, lastAccessTime);
        buf.setLongLE(start + 16, lastWriteTime);
        buf.setLongLE(start + 24, lastChangeTime);
        buf.setIntLE(start + 32, mode);
        buf.setLongLE(start + 40, allocationSize);
        buf.setLongLE(start + 48, endOfFile);
        buf.setIntLE(start + 56, nlinks);
        buf.setByte(start + 60, deletePending);
        buf.setByte(start + 61, directory);
        buf.setIntLE(start + 64, eaSize);
        buf.setIntLE(start + 68, 2);
        buf.setBytes(start + 72, fileName);

        return super.writeStruct(buf, offset);
    }
}
