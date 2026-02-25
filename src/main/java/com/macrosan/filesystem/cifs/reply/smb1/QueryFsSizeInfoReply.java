package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;

public class QueryFsSizeInfoReply extends Trans2Reply {
    public long totalBlock = Long.MAX_VALUE;
    public long freeBlock = Long.MAX_VALUE;
    public int sectorsOfBlock = 8;
    public int sizeOfSector = 512;

    public static final int SIZE = 24;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        byte[] total = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        setParam(12, SIZE);

        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setBytes(start, total);
        buf.setBytes(start + 8, total);
        buf.setIntLE(start + 16, sectorsOfBlock);
        buf.setIntLE(start + 20, sizeOfSector);

        return super.writeStruct(buf, offset);
    }
}
