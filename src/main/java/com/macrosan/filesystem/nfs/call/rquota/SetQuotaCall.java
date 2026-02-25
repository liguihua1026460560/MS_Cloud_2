package com.macrosan.filesystem.nfs.call.rquota;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetQuotaCall implements ReadStruct {
    public short cmd;
    public short cmdType;
    public int pathLength;
    public byte[] path;
    public int _id;
    public int quotaType;
    public int blockHardLimit;
    public int blockSoftLimit;
    public int blocks;
    public int filesHardLimit;
    public int filesSoftLimit;
    public int files;
    public int blockTimeLeft;
    public int filesTimeLeft;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        cmd = buf.getShort(offset);
        cmdType = buf.getShort(offset + 2);
        offset += 4;
        pathLength = buf.getInt(offset);
        offset += 4;
        path = new byte[pathLength];
        buf.getBytes(offset, path);
        offset += ((pathLength + 3) / 4) * 4;
        _id = buf.getInt(offset);
        quotaType = buf.getInt(offset + 4);
        blockHardLimit = buf.getInt(offset + 8);
        blockSoftLimit = buf.getInt(offset + 12);
        blocks = buf.getInt(offset + 16);
        filesHardLimit = buf.getInt(offset + 20);
        filesSoftLimit = buf.getInt(offset + 24);
        files = buf.getInt(offset + 28);
        blockTimeLeft = buf.getInt(offset + 32);
        filesTimeLeft = buf.getInt(offset + 36);
        offset += 40;
        return offset - start;
    }

    public int getSize() {
        return 48 + ((pathLength + 3) / 4) * 4;
    }
}
