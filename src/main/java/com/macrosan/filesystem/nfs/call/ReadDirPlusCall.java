package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadDirPlusCall implements ReadStruct {
    public FH2 fh = new FH2();
    public long cookie;
    public long verf;
    public int dirCount;
    public int count;

    public int readStruct(ByteBuf buf, int offset) {
        int fhRead = fh.readStruct(buf, offset);
        if (fhRead < 0) {
            return -1;
        }

        offset += fhRead;

        cookie = buf.getLong(offset);
        verf = buf.getLong(offset + 8);
        dirCount = buf.getInt(offset + 16);
        count = buf.getInt(offset + 20);

        return fhRead + 24;
    }

}
