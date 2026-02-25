package com.macrosan.filesystem.cifs;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;

public abstract class SMBBody implements ReadStruct {
    public abstract int writeStruct(ByteBuf buf, int offset);
}
