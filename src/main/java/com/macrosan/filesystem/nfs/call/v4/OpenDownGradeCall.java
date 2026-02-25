package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class OpenDownGradeCall extends CompoundCall {
    public StateId stateId = new StateId();
    public int seqId;
    public int shareAccess;
    public int shareDeny;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        stateId.readStruct(buf,offset);
        seqId = buf.getInt(offset + 16);
        shareAccess = buf.getInt(offset + 20);
        shareDeny = buf.getInt(offset + 24);
        return 28;
    }

}
