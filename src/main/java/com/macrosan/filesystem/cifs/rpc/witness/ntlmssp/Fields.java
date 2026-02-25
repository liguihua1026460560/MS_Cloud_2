package com.macrosan.filesystem.cifs.rpc.witness.ntlmssp;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Fields {
    public short len;
    public short maxLen;
    public int bufferOffset;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setShortLE(offset, len);
        buf.setShortLE(offset + 2, maxLen);
        buf.setIntLE(offset + 4, bufferOffset);
        return 8;
    }

    public int readStruct(ByteBuf buf, int offset) {
        len = buf.getShortLE(offset);
        maxLen = buf.getShortLE(offset + 2);
        bufferOffset = buf.getIntLE(offset + 4);
        return 8;
    }
}
