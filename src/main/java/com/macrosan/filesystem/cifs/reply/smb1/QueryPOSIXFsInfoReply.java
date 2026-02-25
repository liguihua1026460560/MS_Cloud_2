package com.macrosan.filesystem.cifs.reply.smb1;

import io.netty.buffer.ByteBuf;

public class QueryPOSIXFsInfoReply extends Trans2Reply {

    public int transferSize = 1 << 20;
    public int block = 4096;
    public long totalBlocks;
    public long availableBlocks;
    public long userBlocksAvail;
    public long totalFileNodes;
    public long freeFileNodes;
    public long fsid;

    public static final int SIZE = 56;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        setParam(12 , SIZE);
        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setIntLE(start, transferSize);
        buf.setIntLE(start + 4, block);
        byte[] total = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        buf.setBytes(start + 8, total);
        buf.setBytes(start + 16, total);
        buf.setBytes(start + 24, total);
        buf.setBytes(start + 32, total);
        buf.setBytes(start + 40, total);
        buf.setLongLE(start + 48, fsid);
        return super.writeStruct(buf, offset);
    }
}
