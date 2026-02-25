package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/abdc4ea9-52df-480e-9a36-34f104797d2c
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class LogOffCall extends SMB2Body {
    //reserved 2

    public int readStruct(ByteBuf buf, int offset) {
        structSize =buf.getShortLE(offset);
        return 4;
    }
}
