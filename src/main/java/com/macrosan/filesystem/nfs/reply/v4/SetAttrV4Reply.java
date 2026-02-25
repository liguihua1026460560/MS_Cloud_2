package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetAttrV4Reply extends CompoundReply {
    public int[] mask;

    public SetAttrV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {

        int start = offset;
        buf.setInt(offset, opt);
        offset += 4;
        buf.setInt(offset, status);
        offset += 4;
        if (status != 0) {
            mask = new int[]{0, 0, 0};
        }
        int maskLen = mask.length;
        buf.setInt(offset, maskLen);
        for (int i = 0; i < maskLen; i++) {
            buf.setInt(offset + 4 * (i + 1), mask[i]);
        }
        offset += 4 + 4 * maskLen;
        return offset - start;
    }
}
