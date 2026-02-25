package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/91913fc6-4ec9-4a83-961b-370070067e63
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CancelCall extends SMB2Body {
    @Override
    public int readStruct(ByteBuf buf, int offset) {
        super.readStruct(buf, offset);
        return 4;
    }
}
