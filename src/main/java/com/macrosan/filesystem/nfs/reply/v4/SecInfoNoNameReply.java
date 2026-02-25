package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SecInfoNoNameReply extends CompoundReply {
    public int num;
    public int flavor;


    public SecInfoNoNameReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, num);
        buf.setInt(offset + 12, flavor);
        offset += 16;
        return offset - start;
    }
}
