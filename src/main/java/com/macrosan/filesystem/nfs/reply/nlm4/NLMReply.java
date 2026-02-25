package com.macrosan.filesystem.nfs.reply.nlm4;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class NLMReply extends RpcReply implements ReadStruct{
    public byte[] cookie;
    public int stat;

    public NLMReply() {
        super(null);
    }
    public NLMReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, cookie.length);
        offset += 4;
        buf.setBytes(offset, cookie);
        offset += ((cookie.length + 3) / 4) * 4;
        buf.setInt(offset, stat);
        offset += 4;
        return offset - start;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int cookieLen = buf.getInt(offset);
        offset += 4;
        cookie = new byte[cookieLen];
        buf.getBytes(offset, cookie);
        offset += ((cookieLen + 3) / 4) * 4;
        stat = buf.getInt(offset);
        offset += 4;
        return offset - start;
    }
}
