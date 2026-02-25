package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReNameCall implements ReadStruct {
    public FH2 fromDirFh = new FH2();
    public int fromNameLen;
    public byte[] fromName;
    public FH2 toDirFh = new FH2();
    public int toNameLen;
    public byte[] toName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += fromDirFh.readStruct(buf, offset);
        fromNameLen = buf.getInt(offset);
        fromName = new byte[fromNameLen];
        offset += 4;
        buf.getBytes(offset, fromName);
        int len = (fromNameLen + 3) / 4 * 4;
        offset += len;
        offset += toDirFh.readStruct(buf, offset);
        toNameLen = buf.getInt(offset);
        toName = new byte[toNameLen];
        buf.getBytes(offset + 4, toName);

        offset = offset + 4 + toNameLen;

        return offset - head;
    }

    public static final String RENAME_NOT_FOUND = "rename_not_found";
}
