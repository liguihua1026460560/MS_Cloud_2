package com.macrosan.filesystem.nfs.call.rquota;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;

public class GetQuotaCall implements ReadStruct {

    public int dirLength;
    public byte[] mountPath;
    public int type;//0：user, 1：group，2：project
    public int id;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        dirLength = buf.getInt(offset);
        offset += 4;
        mountPath = new byte[dirLength];
        buf.getBytes(offset, mountPath);
        offset += ((dirLength + 3) / 4) * 4;
        type = buf.getInt(offset);
        offset += 4;
        id = buf.getInt(offset);
        offset += 4;

        return offset - start;
    }

    public String toString() {
        return "GetQuotaCall{" +
                "dirLength=" + dirLength +
                ", dirName=" + new String(mountPath) +
                ", type=" + type +
                ", id=" + id +
                '}';
    }

    public int getSize() {
        return 12 + ((dirLength + 3) / 4) * 4;
    }
}
