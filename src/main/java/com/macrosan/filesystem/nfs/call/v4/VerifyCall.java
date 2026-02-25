package com.macrosan.filesystem.nfs.call.v4;


import com.macrosan.filesystem.nfs.types.FAttr4;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class VerifyCall extends CompoundCall {
    public int maskLen;
    public int[] mask;
    public int maskTotalLen;
    public FAttr4 fAttr4;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        maskLen = buf.getInt(offset);
        mask = new int[maskLen];
        for (int i = 0; i < maskLen; i++) {
            mask[i] = buf.getInt(offset + 4 * (i + 1));
        }
        offset += 4 + 4 * maskLen;
        maskTotalLen = buf.getInt(offset);
        offset += 4;
        fAttr4 = new FAttr4(mask, this.context.minorVersion);
        fAttr4.readStruct(buf, offset);
        offset += maskTotalLen;
        return offset - start;
    }
}
