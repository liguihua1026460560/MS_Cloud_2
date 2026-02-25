package com.macrosan.filesystem.cifs;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class SMB2Body extends SMBBody {
    public short structSize;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setShortLE(offset, structSize);
        return 2;
    }

    public int readStruct(ByteBuf buf, int offset) {
        structSize = buf.getShortLE(offset);
        return 2;
    }
}
