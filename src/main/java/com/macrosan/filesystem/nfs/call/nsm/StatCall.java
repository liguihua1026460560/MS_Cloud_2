package com.macrosan.filesystem.nfs.call.nsm;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class StatCall implements ReadStruct {
    public byte[] monName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int monNameLen = buf.getInt(offset);
        offset += 4;
        monName = new byte[monNameLen];
        buf.getBytes(offset, monName);
        offset += ((monNameLen + 3) / 4) * 4;
        return offset -start;
    }
}
