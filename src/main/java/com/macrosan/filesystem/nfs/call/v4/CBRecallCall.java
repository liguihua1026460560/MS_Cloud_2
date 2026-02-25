package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CBRecallCall extends CompoundCall {
    public int opt;
    public StateId stateId = new StateId();
    public boolean truncated;
    public FH2 fh2 = new FH2();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        offset += 4;
        offset += stateId.writeStruct(buf, offset);
        buf.setBoolean(offset, truncated);
        offset += 4;
        offset += fh2.writeStruct(buf, offset);
        return offset - start;
    }
}
