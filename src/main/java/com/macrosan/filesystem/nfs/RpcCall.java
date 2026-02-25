package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.nfs.auth.Auth;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RpcCall {
    SunRpcHeader header;

    public RpcCall(SunRpcHeader header) {
        this.header = header;
    }

    public int rpcVersion;
    public int rpcProgram;
    public int rpcProgramVersion;
    public int rpcOpt;
    public Auth auth;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, rpcVersion);
        buf.setInt(offset + 4, rpcProgram);
        buf.setInt(offset + 8, rpcProgramVersion);
        buf.setInt(offset + 12, rpcOpt);
        int authWrite = Auth.writeAuth(buf, offset + 16, auth);
        return authWrite + 16;
    }
}
