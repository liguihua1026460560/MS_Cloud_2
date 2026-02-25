package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SeekCall extends CompoundCall {
    public StateId stateId = new StateId();
    public long offset;
    public int what;
//NFS4_CONTENT_DATA = 0,NFS4_CONTENT_HOLE = 1

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        stateId.readStruct(buf, offset);
        this.offset = buf.getLong(offset + 16);
        what = buf.getInt(offset + 24);
        return 28;
    }
}
