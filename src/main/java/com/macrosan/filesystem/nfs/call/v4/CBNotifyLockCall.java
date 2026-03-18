package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.StateOwner;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CBNotifyLockCall extends CompoundCall {
    public int opt;
    public FH2 fh2 = new FH2();
    public StateOwner.Owner owner = new StateOwner.Owner();


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        offset += 4;
        offset += fh2.writeStruct(buf, offset);
        buf.setLong(offset, owner.clientId);
        buf.setInt(offset + 8, owner.owner.length);
        buf.setBytes(offset + 12, owner.owner);
        offset += 12 + (owner.owner.length + 3) / 4 * 4;
        return offset - start;
    }
}
