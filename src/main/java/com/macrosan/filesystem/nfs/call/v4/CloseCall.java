package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CloseCall extends CompoundCall {
    public int seqId;
    public StateId stateId = new StateId();


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        seqId = buf.getInt(offset);
        stateId.readStruct(buf, offset + 4);
        return 20;
    }
}
