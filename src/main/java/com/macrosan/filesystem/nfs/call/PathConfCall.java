package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;

public class PathConfCall implements ReadStruct {

    public FH2 fh = new FH2();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return fh.readStruct(buf, offset);

    }
}
