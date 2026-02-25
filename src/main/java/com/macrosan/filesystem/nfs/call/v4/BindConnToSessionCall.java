package com.macrosan.filesystem.nfs.call.v4;


import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class BindConnToSessionCall extends CompoundCall {
    public long sessionId;
    public int bctsaDir;
    //not support
    public boolean bctsaUseConnInRdmaMode;

    //bind to conn session
    public static final int CDFC4_FORE = 0x1;
    public static final int CDFC4_BACK = 0x2;
    public static final int CDFC4_FORE_OR_BOTH = 0x3;
    public static final int CDFC4_BACK_OR_BOTH = 0x7;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        sessionId = buf.getLong(offset);
        bctsaDir = buf.getInt(offset + 16);
        bctsaUseConnInRdmaMode = buf.getBoolean(offset + 20);
        return 24;
    }
}

