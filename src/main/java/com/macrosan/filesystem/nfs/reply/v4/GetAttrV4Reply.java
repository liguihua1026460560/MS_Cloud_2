package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr4;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GetAttrV4Reply extends CompoundReply {
    public FAttr4 fAttr4;

    public GetAttrV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {

        int start = offset;
        buf.setInt(offset, opt);
        offset += 4;
        buf.setInt(offset, status);
        offset += 4;
        offset += fAttr4.writeStruct(buf, offset);
        return offset - start;
    }
}
