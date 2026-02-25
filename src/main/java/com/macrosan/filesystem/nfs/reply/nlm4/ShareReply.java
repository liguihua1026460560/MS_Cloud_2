package com.macrosan.filesystem.nfs.reply.nlm4;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ShareReply extends RpcReply {
    public byte[] cookie;
    public int stat;
    public int sequence;

    public ShareReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, cookie.length);
        offset += 4;
        buf.setBytes(offset, cookie);
        offset += ((cookie.length + 3) / 4) * 4;
        buf.setInt(offset, stat);
        offset += 4;
        buf.setInt(offset, sequence);
        offset += 4;
        return offset - start;
    }
}
