package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class BindConnToSessionReply extends CompoundReply {
    public long sessionId;
    public int bctsaDir;
    public boolean bctsaUseConnInRdmaMode;

    public BindConnToSessionReply(SunRpcHeader header) {
        super(header);
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLong(offset, sessionId);
        buf.setInt(offset + 16, bctsaDir);
        buf.setBoolean(offset + 20, bctsaUseConnInRdmaMode);
        return 24;
    }
}
