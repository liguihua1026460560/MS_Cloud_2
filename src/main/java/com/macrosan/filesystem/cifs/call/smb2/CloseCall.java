package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/f84053b0-bcb2-4f85-9717-536dae2b02bd
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CloseCall extends SMB2Body {
    short flags;
    SMB2FileId fileId;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);
        flags = buf.getShortLE(start);
        fileId = new SMB2FileId();
        fileId.readStruct(buf, start + 6);

        return 24;
    }
}
