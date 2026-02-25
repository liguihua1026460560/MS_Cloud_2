package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class MkNodCall implements ReadStruct {

    public FH2 dirFh = new FH2();
    public int nameLen;
    public byte[] name;
    public int type;
    public ObjAttr attr = new ObjAttr();
    public int specData1;
    public int specData2;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        offset += dirFh.readStruct(buf, offset);
        nameLen = buf.getInt(offset);
        offset += 4;
        name = new byte[nameLen];

        buf.getBytes(offset, name);
        //name长度会补齐为4的倍数
        int fillLen = (nameLen + 3) / 4 * 4;
        offset += fillLen;
        type = buf.getInt(offset);
        offset += 4;
        offset += attr.readStruct(buf, offset);
        if (type != FsConstants.FileType.NF_FIFO) {
            specData1 = buf.getInt(offset);
            specData2 = buf.getInt(offset + 4);
            offset += 8;
        }
        return offset - head;
    }
}
