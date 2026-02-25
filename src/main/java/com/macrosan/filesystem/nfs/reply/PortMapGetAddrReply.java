package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class PortMapGetAddrReply extends RpcReply {
    public int len;
    public byte[] address;

    public PortMapGetAddrReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, len);
        buf.setBytes(offset + 4, address);

        return offset + 4 + (len + 4) / 4 * 4 - start;
    }

}
