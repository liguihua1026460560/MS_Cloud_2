package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GetAttrCall implements ReadStruct {
    public FH2 fh = new FH2();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return fh.readStruct(buf, offset);
    }
}
