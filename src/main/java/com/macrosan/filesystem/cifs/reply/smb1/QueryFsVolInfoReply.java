package com.macrosan.filesystem.cifs.reply.smb1;

import io.netty.buffer.ByteBuf;

public class QueryFsVolInfoReply extends Trans2Reply {

    public long VolumeCreationTime;
    public int SerialNumber = 0xbc3ac512;
    public int VolumeLabelSize;
    public short Reserved;
    public char[] VolumeLabel ;

    public final static int SIZE = 21;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        setParam(12 , SIZE);
        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setLongLE(start, VolumeCreationTime);
        buf.setIntLE(start + 8, SerialNumber);
        buf.setIntLE(start + 12, VolumeLabelSize);
        buf.setShortLE(start + 16, Reserved);
        for (int i = 0; i < VolumeLabel.length; i++) {
            buf.setShortLE(start + 18 + i * 2, VolumeLabel[i]);
        }
        return super.writeStruct(buf, offset);
    }
}
