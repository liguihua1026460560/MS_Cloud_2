package com.macrosan.filesystem.nfs.call.nlm4;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.NLM4Share;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class UnshareCall implements ReadStruct {
    public byte[] cookie;
    public NLM4Share NLM4Share = new NLM4Share();
    public boolean reclaim;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int cookieLen = buf.getInt(offset);
        offset += 4;
        cookie = new byte[cookieLen];
        buf.getBytes(offset, cookie);
        offset += ((cookieLen + 3) / 4) * 4;
        offset += NLM4Share.readStruct(buf, offset);
        reclaim = (buf.getInt(offset) != 0);
        offset += 4;
        return offset - start;
    }
}
