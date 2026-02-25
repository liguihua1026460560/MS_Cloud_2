package com.macrosan.filesystem.cifs.types.smb1;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SimpleFileAttr implements ReadStruct {
    public long size;
    public long allocSize;
    public long ctime;
    public long atime;
    public long mtime;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }
}
