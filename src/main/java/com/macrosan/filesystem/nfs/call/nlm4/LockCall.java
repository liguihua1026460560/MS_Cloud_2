package com.macrosan.filesystem.nfs.call.nlm4;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.NLM4Lock;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LockCall implements ReadStruct {
    public byte[] cookie;
    public boolean block;
    public boolean exclusive; // 独占锁还是共享锁
    public NLM4Lock NLM4Lock = new NLM4Lock();
    public boolean reclaim;
    public int state;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int cookieLen = buf.getInt(offset);
        offset += 4;
        cookie = new byte[cookieLen];
        buf.getBytes(offset, cookie);
        offset += ((cookieLen + 3) / 4) * 4;
        block = (buf.getInt(offset) != 0);
        offset += 4;
        exclusive = (buf.getInt(offset) != 0);
        offset += 4;
        offset += NLM4Lock.readStruct(buf, offset);
        reclaim = (buf.getInt(offset) != 0);
        offset += 4;
        state = buf.getInt(offset);
        offset += 4;
        return offset - start;
    }
}
