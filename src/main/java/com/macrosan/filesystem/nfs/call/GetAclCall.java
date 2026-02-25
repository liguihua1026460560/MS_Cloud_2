package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


//NFSACL
@ToString
public class GetAclCall implements ReadStruct {

    public FH2 fh = new FH2();

    public int mask;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += fh.readStruct(buf, offset);
        mask = buf.getInt(offset);
        offset += 4;
        return offset - head;
    }
}
