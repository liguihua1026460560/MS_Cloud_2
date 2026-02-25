package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import io.netty.buffer.ByteBuf;

public class SymLinkCall implements ReadStruct {
    public FH2 dirFh = new FH2();
    public int linkNameLen;
    public byte[] linkName;
    public ObjAttr linkAttr = new ObjAttr();
    public int referenceNameLen;
    public byte[] referenceName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += dirFh.readStruct(buf, offset);
        linkNameLen = buf.getInt(offset);
        linkName = new byte[linkNameLen];
        buf.getBytes(offset + 4, linkName);
        int len = (linkNameLen + 3) / 4 * 4;
        offset = offset + 4 + len;
        offset += linkAttr.readStruct(buf, offset);
        referenceNameLen = buf.getInt(offset);
        referenceName = new byte[referenceNameLen];
        buf.getBytes(offset + 4, referenceName);
        offset = offset + 4 + referenceNameLen;

        return offset - head;
    }
}
