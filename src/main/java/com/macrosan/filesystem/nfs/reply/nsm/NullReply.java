package com.macrosan.filesystem.nfs.reply.nsm;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class NullReply extends RpcReply implements ReadStruct {

    public NullReply() {
        super(null);
    }
    public NullReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return super.writeStruct(buf, offset);
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }
}
