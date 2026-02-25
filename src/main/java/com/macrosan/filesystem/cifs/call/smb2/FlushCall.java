package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;

public class FlushCall extends SMB2Body {

    public SMB2FileId fileId = new SMB2FileId();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        offset += super.readStruct(buf, offset);
        offset += 6;//reserved
        fileId.readStruct(buf, offset);
        return 24;
    }
}
