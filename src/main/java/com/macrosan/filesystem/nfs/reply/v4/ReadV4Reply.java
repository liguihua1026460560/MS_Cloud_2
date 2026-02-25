package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(exclude = "contents")
public class ReadV4Reply extends CompoundReply {
    public int eof;
    public int readLen;
    public byte[] contents;


    public ReadV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, eof);
        buf.setInt(offset + 12, readLen);
        buf.setBytes(offset + 16, contents);
        offset += (16 + (readLen + 3) / 4 * 4);
        return offset - start;
    }
}
