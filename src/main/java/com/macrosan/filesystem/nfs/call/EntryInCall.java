package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class EntryInCall implements ReadStruct {
    public FH2 fh = new FH2();

    public int len;
    public byte[] name;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = fh.readStruct(buf, offset);
        offset += head;
        len = buf.getInt(offset);
        name = new byte[len];
        buf.getBytes(offset + 4, name);
        return head + len + 4;
    }
}
