package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CreateCall implements ReadStruct {

    public FH2 dirFh = new FH2();

    public int nameLen;

    public byte[] name;

    public int createMode;

    public ObjAttr attr = new ObjAttr();

    public long verf;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;

        offset += dirFh.readStruct(buf, offset);
        nameLen = buf.getInt(offset);
        offset += 4;
        name = new byte[nameLen];
        buf.getBytes(offset, name);
        int len = (nameLen + 3) / 4 * 4;
        offset += len;
        createMode = buf.getInt(offset);

        offset = offset + 4;
        if (createMode == 0 || createMode == 1) {
            offset += attr.readStruct(buf, offset);
        } else if (createMode == 2) {
            verf = buf.getLong(offset);
            offset += 8;
        } else {
            return -1;
        }

        return offset - head;
    }
}
