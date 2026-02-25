package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString(callSuper = true)
public class EmptyReply extends CompoundReply {

    public EmptyReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        return 8;
    }

    public int readStruct(ByteBuf buf, int offset) {
        status = buf.getInt(offset);
        return 4;
    }
}
