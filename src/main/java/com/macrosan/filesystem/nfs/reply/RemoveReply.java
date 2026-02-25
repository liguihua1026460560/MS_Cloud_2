package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class RemoveReply extends RpcReply {

    public int status;

    public int before = 0;

    public int after = 0;

    public RemoveReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset +=super.writeStruct(buf, offset);
        buf.setInt(offset,status);
        buf.setInt(offset+4,before);
        buf.setInt(offset+8,after);

        return offset-start+12;
    }
}
