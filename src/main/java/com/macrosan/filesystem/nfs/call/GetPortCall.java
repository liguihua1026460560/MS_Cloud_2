package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.nfs.RpcCall;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GetPortCall extends RpcCall {
    public int program;
    public int programVersion;
    public int proto;
    public int port;

    public GetPortCall(SunRpcHeader header) {
        super(header);
        this.rpcVersion = 2;
        this.rpcProgram = 100000;
        this.rpcProgramVersion = 2;
        this.rpcOpt = 3;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, program);
        buf.setInt(offset + 4, programVersion);
        buf.setInt(offset + 8, proto);
        buf.setInt(offset + 12, port);
        return offset + 16 - start;
    }

}
