package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadDirV4Call extends CompoundCall {
    public long cookie;
    public long cookieVerf;
    //目录的信息的最大字节数
    public int dirCount;
    //响应的最大字节数
    public int maxCount;
    public int maskLen;
    public int[] mask;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        cookie = buf.getLong(offset);
        cookieVerf = buf.getLong(offset + 8);
        dirCount = buf.getInt(offset + 16);
        maxCount = buf.getInt(offset + 20);
        maskLen = buf.getInt(offset + 24);
        offset += 28;
        mask = new int[maskLen];
        for (int i = 0; i < maskLen; i++) {
            mask[i] = buf.getInt(offset + 4 * i);
        }
        offset += 4 * maskLen;
        return offset - start;
    }
}
