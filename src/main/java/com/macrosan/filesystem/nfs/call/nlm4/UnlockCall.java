package com.macrosan.filesystem.nfs.call.nlm4;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.NLM4Lock;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class UnlockCall implements ReadStruct {
    public byte[] cookie;
    public NLM4Lock NLM4Lock = new NLM4Lock();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int cookieLen = buf.getInt(offset);
        offset += 4;
        cookie = new byte[cookieLen];
        buf.getBytes(offset, cookie);
        offset += ((cookieLen + 3) / 4) * 4;
        offset += NLM4Lock.readStruct(buf, offset);
        return offset - start;
    }
}
