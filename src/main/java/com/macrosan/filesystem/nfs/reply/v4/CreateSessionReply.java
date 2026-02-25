package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.call.v4.CreateSessionCall;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CreateSessionReply extends CompoundReply {
    public byte[] sessionId;
    public int seqId;
    public int csrFlags;
    public CreateSessionCall.CsaAttrs csrForceChanAttrs;
    public CreateSessionCall.CsaAttrs csrBackChanAttrs;

    public CreateSessionReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        // sessionId占据16字节
        buf.setBytes(offset + 8, sessionId);
        buf.setInt(offset + 24, seqId);
        buf.setInt(offset + 28, csrFlags);
        offset += 32;
        offset += csrForceChanAttrs.writeStruct(buf, offset);
        offset += csrBackChanAttrs.writeStruct(buf, offset);
        return offset - start;
    }
}
