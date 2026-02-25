package com.macrosan.filesystem.nfs.call.v4;


import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class AccessV4Call extends CompoundCall {
    public int access;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        access = buf.getInt(offset);
        return 4;
    }

    public static final int ACCESS_READ = 0x00000001;
    public static final int ACCESS_LOOKUP = 0x00000002;
    public static final int ACCESS_MODIFY = 0x00000004;
    public static final int ACCESS_EXTEND = 0x00000008;
    public static final int ACCESS_DELETE = 0x00000010;
    public static final int ACCESS_EXECUTE = 0x00000020;
}
