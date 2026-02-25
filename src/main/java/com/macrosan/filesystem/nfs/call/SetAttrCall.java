package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class SetAttrCall implements ReadStruct {

    public FH2 fh = new FH2();

    public ObjAttr attr = new ObjAttr();

    public int checkGuard;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;

        offset += fh.readStruct(buf, offset);

        offset += attr.readStruct(buf, offset);
        checkGuard = buf.getInt(offset);

        return offset + 4 - start;
    }


}
