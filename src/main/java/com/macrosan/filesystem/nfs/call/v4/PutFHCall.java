package com.macrosan.filesystem.nfs.call.v4;


import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class PutFHCall extends CompoundCall {
    public FH2 fh = new FH2();


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return fh.readStruct(buf, offset);
    }
}
