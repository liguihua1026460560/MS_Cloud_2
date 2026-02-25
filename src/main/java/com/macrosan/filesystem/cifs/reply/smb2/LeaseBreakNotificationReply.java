package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/9abe6f73-f32f-4a23-998d-ee9da2b90e2e
 */

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class LeaseBreakNotificationReply extends SMB2Body {
    short epoch;
    int leaseFlags;
    byte[] leaseKey = new byte[16];
    int currentLeaseState;
    int newLeaseState;
    int breakReason;
    int accessMaskHint;
    int shareMaskHint;

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 44;
        offset += super.writeStruct(buf, offset);
        buf.setShortLE(offset, epoch);
        buf.setIntLE(offset + 2, leaseFlags);
        buf.setBytes(offset + 6, leaseKey);
        buf.setIntLE(offset + 22, currentLeaseState);
        buf.setIntLE(offset + 26, newLeaseState);
        buf.setIntLE(offset + 30, breakReason);
        buf.setIntLE(offset + 34, accessMaskHint);
        buf.setIntLE(offset + 38,shareMaskHint);
        return 44;
    }
}
