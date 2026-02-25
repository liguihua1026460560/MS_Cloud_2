package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetAclReply extends RpcReply {

    public int status;
    public int attrFollows = 1;
    public FAttr3 attr = new FAttr3();

    public SetAclReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, attrFollows);
        offset += 8;
        if (attrFollows == 1) {
            offset += attr.writeStruct(buf, offset);
        }
        return offset - start;
    }
}
