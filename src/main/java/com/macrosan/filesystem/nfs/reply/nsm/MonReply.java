package com.macrosan.filesystem.nfs.reply.nsm;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class MonReply extends RpcReply {
    public int stat;
    public int state;

    public MonReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, stat);
        offset += 4;
        buf.setInt(offset, state);
        offset += 4;
        return offset - start;
    }
}
