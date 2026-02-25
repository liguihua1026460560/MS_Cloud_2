package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.auth.Auth;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
@ToString
public class RpcCallHeader implements ReadStruct {
    public SunRpcHeader header;

    public RpcCallHeader(SunRpcHeader header) {
        this.header = header;
    }

    //2
    public int rpcVersion;
    //nfs program is 100003
    public int program;
    //2==nfsv2 3==nfsv3 4==nfsv4
    public int programVersion;
    public int opt;
    public Auth auth;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        rpcVersion = buf.getInt(offset);
        program = buf.getInt(offset + 4);
        programVersion = buf.getInt(offset + 8);
        opt = buf.getInt(offset + 12);

        AtomicReference<Auth> reference = new AtomicReference<>();
        int len = Auth.readAuth(buf, offset + 16, reference);
        auth = reference.get();
        return len + 16;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, rpcVersion);
        buf.setInt(offset + 4, program);
        buf.setInt(offset + 8, programVersion);
        buf.setInt(offset + 12, opt);
        int authWrite = Auth.writeAuth(buf, offset + 16, auth);
        return authWrite + 16;
    }
}
