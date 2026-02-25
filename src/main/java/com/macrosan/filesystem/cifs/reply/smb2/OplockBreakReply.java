package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class OplockBreakReply extends SMB2Body {
    byte oplockLevel;
    SMB2FileId fileId;

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 24;
        super.writeStruct(buf, offset);
        buf.setByte(offset + 2, oplockLevel);
        fileId.writeStruct(buf, offset + 8);
        return 24;
    }
}
