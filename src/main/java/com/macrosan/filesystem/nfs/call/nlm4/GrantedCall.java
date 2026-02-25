package com.macrosan.filesystem.nfs.call.nlm4;

import com.macrosan.filesystem.nfs.RpcCall;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.NLM4Lock;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class GrantedCall extends RpcCall {
    public byte[] cookie;
    public boolean exclusive;
    public NLM4Lock NLM4Lock = new NLM4Lock();

    public GrantedCall(SunRpcHeader header) {
        super(header);
        this.rpcVersion = 2;
        this.rpcProgram = 100021;
        this.rpcProgramVersion = 4;
        this.rpcOpt = 5;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        int cookieLen = cookie.length;
        buf.setInt(offset, cookieLen);
        offset += 4;
        buf.setBytes(offset, cookie);
        offset += ((cookieLen + 3) / 4) * 4;
        buf.setInt(offset, exclusive ? 1 : 0);
        offset += 4;
        offset += NLM4Lock.writeStruct(buf, offset);
        return offset - start;
    }
}
