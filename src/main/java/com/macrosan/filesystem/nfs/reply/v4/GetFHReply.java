package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GetFHReply extends CompoundReply {
    public FH2 fh = new FH2();


    public GetFHReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        offset += 8;
        offset += fh.writeStruct(buf, offset);
        return offset - start;
    }
}
