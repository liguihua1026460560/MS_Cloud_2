package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CommonCall extends CompoundCall {

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }
}
