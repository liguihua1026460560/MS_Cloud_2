package com.macrosan.filesystem.nfs.call.nsm;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class NullCall implements ReadStruct {
    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }
}
