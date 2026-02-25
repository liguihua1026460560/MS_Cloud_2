package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadCall implements ReadStruct {
    public FH2 fh = new FH2();
    public long offset;
    public int size;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += fh.readStruct(buf, offset);
        this.offset = buf.getLong(offset);
        size = buf.getInt(offset + 8);
        return offset + 12 - head;
    }
}
