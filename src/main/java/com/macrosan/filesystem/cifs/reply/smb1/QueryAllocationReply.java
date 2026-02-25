package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;

public class QueryAllocationReply extends Trans2Reply {
    public int fsid;
    // This field contains the number of sectors per allocation unit.
    public int sectorsOfBlock = 8;

    public int totalBlock = Integer.MAX_VALUE;

    public int availableBlock= Integer.MAX_VALUE;

    public short sizeOfSector = 512;

    public static final int SIZE = 18;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        setParam(12 , SIZE);
        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setIntLE(start, fsid);

        buf.setIntLE(start + 4, sectorsOfBlock);
        buf.setIntLE(start + 8, totalBlock);
        buf.setIntLE(start + 12, availableBlock);
        buf.setShortLE(start + 16, sizeOfSector);
        return super.writeStruct(buf, offset);
    }
}
