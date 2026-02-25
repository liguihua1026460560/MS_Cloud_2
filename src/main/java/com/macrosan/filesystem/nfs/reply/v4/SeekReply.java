package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(callSuper = true)
public class SeekReply extends CompoundReply {
    public int eof;
    public long offset;
    public SeekReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, eof);
        buf.setLong(offset + 12, this.offset);
        return 20;
    }
}
