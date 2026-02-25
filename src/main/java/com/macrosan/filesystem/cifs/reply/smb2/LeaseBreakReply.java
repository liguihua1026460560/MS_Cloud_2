package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/9c814bbb-43a2-46fe-94d8-d713a58cd702
 */

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class LeaseBreakReply extends SMB2Body {
    short reserved;
    int leaseFlags;
    byte[] leaseKey = new byte[16];
    int leaseState;
    long leaseDuration;

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 36;
        offset += super.writeStruct(buf, offset);
        buf.setShortLE(offset, reserved);
        buf.setIntLE(offset + 2, leaseFlags);
        buf.setBytes(offset + 6, leaseKey);
        buf.setIntLE(offset + 22, leaseState);
        buf.setLongLE(offset + 26, leaseDuration);
        return 36;
    }
}
