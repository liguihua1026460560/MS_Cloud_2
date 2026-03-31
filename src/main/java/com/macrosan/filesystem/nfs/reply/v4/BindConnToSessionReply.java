package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class BindConnToSessionReply extends CompoundReply {
    public byte[] sessionId = new byte[16];
    public int bctsaDir;
    public boolean bctsaUseConnInRdmaMode;

    public BindConnToSessionReply(SunRpcHeader header) {
        super(header);
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setBytes(offset + 8, sessionId);
        buf.setInt(offset + 24, bctsaDir);
        buf.setBoolean(offset + 28, bctsaUseConnInRdmaMode);
        return 32;
    }
}
