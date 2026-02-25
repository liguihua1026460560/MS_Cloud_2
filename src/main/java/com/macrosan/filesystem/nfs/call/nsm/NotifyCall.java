package com.macrosan.filesystem.nfs.call.nsm;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcCall;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class NotifyCall extends RpcCall implements ReadStruct {
    public byte[] monName;
    public int state;

    public NotifyCall() {
        super(null);
    }
    public NotifyCall(SunRpcHeader header) {
        super(header);
        this.rpcVersion = 2;
        this.rpcProgram = 100024;
        this.rpcProgramVersion = 1;
        this.rpcOpt = 6;
    }


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int monNameLen = buf.getInt(offset);
        offset += 4;
        monName = new byte[monNameLen];
        buf.getBytes(offset, monName);
        offset += ((monNameLen + 3) / 4) * 4;
        state = buf.getInt(offset);
        offset += 4;
        return offset - start;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        int monNameLen = monName.length;
        buf.setInt(offset, monNameLen);
        offset += 4;
        buf.setBytes(offset, monName);
        offset += ((monNameLen + 3) / 4) * 4;
        buf.setInt(offset, state);
        offset += 4;
        return offset - start;
    }
}
