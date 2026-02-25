package com.macrosan.filesystem.nfs.call.v4;


import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class OpenConfirmCall extends CompoundCall {
    public StateId stateId = new StateId();
    public int seqId;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        stateId.readStruct(buf, offset);
        seqId = buf.getInt(offset + 16);
        return 20;
    }
}
