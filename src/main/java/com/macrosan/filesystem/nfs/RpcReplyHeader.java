package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RpcReplyHeader implements ReadStruct {
    SunRpcHeader header;

    public RpcReplyHeader(SunRpcHeader header) {
        this.header = header;
    }

    public int replyState;
    public long padding;
    public int acceptState;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        replyState = buf.getInt(offset);
        padding = buf.getLong(offset + 4);
        acceptState = buf.getInt(offset + 12);
        return 16;
    }

    public SunRpcHeader getHeader() {
        return header;
    }
}
