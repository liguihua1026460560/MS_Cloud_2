package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class LinkCall implements ReadStruct {

    public FH2 srcFh = new FH2();

    public FH2 srcParentDirFh = new FH2();

    public int nameLen;

    public byte[] name;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += srcFh.readStruct(buf, offset);
        offset += srcParentDirFh.readStruct(buf, offset);
        nameLen = buf.getInt(offset);
        offset += 4;
        name = new byte[nameLen];
        buf.getBytes(offset, name);


        return offset + nameLen - head;
    }
}
