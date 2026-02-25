package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadDirCall implements ReadStruct {

    public FH2 fh = new FH2();

    public long cookie;

    public long verifier;

    public int count;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = fh.readStruct(buf, offset);

        cookie = buf.getLong(offset + head);
        verifier = buf.getLong(offset + head + 8);

        count = buf.getInt(offset + head + 16);

        return head + 20;
    }
}
