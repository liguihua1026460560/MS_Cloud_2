package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.ChangeInfo;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LinkV4Reply extends CompoundReply {
    public ChangeInfo changeInfo = new ChangeInfo();


    public LinkV4Reply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        offset += 8;
        offset += changeInfo.writeStruct(buf, offset);
        return offset - start;
    }
}
