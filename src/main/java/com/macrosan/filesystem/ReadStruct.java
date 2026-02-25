package com.macrosan.filesystem;

import io.netty.buffer.ByteBuf;

public interface ReadStruct {
    int readStruct(ByteBuf buf, int offset);
}
