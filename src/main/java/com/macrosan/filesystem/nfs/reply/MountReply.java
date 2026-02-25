package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class MountReply extends RpcReply {
    public MountReply(SunRpcHeader header) {
        super(header);
    }

    public int status;
    public FH2 rootFH;
    public long verf;

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        offset += 4;
        offset += rootFH.writeStruct(buf, offset);
        buf.setLong(offset, verf);

        return offset + 8 - start;
    }
}
