package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SecInfoNoNameCall extends CompoundCall {
    public int secInfoStyle;

    //SECINFO_NO_NAME
    public static final int SECINFO_STYLE4_CURRENT_FH = 0;
    public static final int SECINFO_STYLE4_PARENT = 1;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        secInfoStyle = buf.getInt(offset);
        offset += 4;
        return offset - start;
    }
}
