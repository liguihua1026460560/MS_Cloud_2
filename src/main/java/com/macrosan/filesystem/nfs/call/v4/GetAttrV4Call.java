package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GetAttrV4Call extends CompoundCall {
    public int maskLen;
    public int[] mask;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        maskLen = buf.getInt(offset);
        mask = new int[maskLen];
        for (int i = 0; i < maskLen; i++) {
            mask[i] = buf.getInt(offset + 4 * (i + 1));
        }
        offset += 4 + 4 * maskLen;
        return offset - start;
    }
}
