package com.macrosan.filesystem.cifs.reply.smb1;

import io.netty.buffer.ByteBuf;

public class QueryVolInfoReply extends Trans2Reply {

    public int SerialNumber = 0xbc3ac512;
    public byte VolumeLabelSize;
    public char[] VolumeLabel;

    public final static int SIZE = 9;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        setParam(12 , SIZE);
        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        VolumeLabelSize = (byte) (VolumeLabel.length * 2);
        buf.setLongLE(start, SerialNumber);
        buf.setIntLE(start + 4, VolumeLabelSize);
        for (int i = 0; i < VolumeLabel.length; i++) {
            buf.setShortLE(start + 5 + i * 2, VolumeLabel[i]);
        }
        return super.writeStruct(buf, offset);
    }
}
