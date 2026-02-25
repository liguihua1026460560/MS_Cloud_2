package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadLinkV4Reply extends CompoundReply {
    public byte[] link;


    public ReadLinkV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, link.length);
        offset += 12;
        buf.setBytes(offset, link);
        offset += (link.length + 3) / 4 * 4;
        return offset - start;
    }
}
