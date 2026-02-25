package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import io.netty.buffer.ByteBuf;

public class MkdirCall implements ReadStruct {

    public FH2 dirFh = new FH2();

    public int nameLen;

    public byte[] name;

    public ObjAttr attr = new ObjAttr();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;

        offset += dirFh.readStruct(buf, offset);
        nameLen = buf.getInt(offset);
        name = new byte[nameLen];
        buf.getBytes(offset + 4, name);
        int fillLen = (nameLen + 3) / 4 * 4;
        offset = offset + fillLen + 4;
        offset += attr.readStruct(buf, offset);

        return offset - head;
    }
}
