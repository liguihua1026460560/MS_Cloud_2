package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CommitCall implements ReadStruct {
    public FH2 fh = new FH2();
    public long offset;
    public int count;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int fhRead = fh.readStruct(buf, offset);
        if (fhRead < 0) {
            return -1;
        }

        offset += fhRead;

        this.offset = buf.getLong(offset);
        count = buf.getInt(offset + 8);

        return fhRead + 12;
    }
}
