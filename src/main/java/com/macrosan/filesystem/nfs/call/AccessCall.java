package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class AccessCall implements ReadStruct {

    public FH2 fh = new FH2();

    public int access;

    @Override
    public int readStruct(ByteBuf buf, int offset) {

        int head = fh.readStruct(buf, offset);

        access = buf.getInt(offset + head);

        return head + 4;
    }
}
