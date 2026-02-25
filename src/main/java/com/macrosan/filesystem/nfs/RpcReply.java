package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.nfs.auth.GssVerifier;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RpcReply {
    SunRpcHeader header;

    public RpcReply(SunRpcHeader header) {
        this.header = header;
    }

    public int replyState = 0;
    public int execState = 0;
    public GssVerifier verifier = GssVerifier.NULL_VERIFIER;

    public static final int SIZE = 16;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, replyState);
        //auth
        int verifyLen = verifier.writeStruct(buf, offset + 4);
        buf.setInt(offset + 4 + verifyLen, execState);

        return verifyLen + 8;
    }
}
