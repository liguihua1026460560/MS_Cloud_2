package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReclaimCompleteCall extends CompoundCall {
    public boolean reclaimOneFs;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        reclaimOneFs = buf.getBoolean(offset);
        return 4;
    }
}
