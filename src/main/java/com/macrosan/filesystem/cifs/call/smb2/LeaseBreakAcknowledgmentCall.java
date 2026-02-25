package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import javax.xml.bind.DatatypeConverter;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/048aae06-3421-418b-85b3-0f7605749596
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
public class LeaseBreakAcknowledgmentCall extends SMB2Body {
    short reserved;
    int leaseFlags;
    byte[] leaseKey = new byte[16];
    int leaseState;
    long leaseDuration;

    public int readStruct(ByteBuf buf, int offset) {
        offset += super.readStruct(buf, offset);
        reserved = buf.getShortLE(offset);
        leaseFlags = buf.getIntLE(offset + 2);
        buf.getBytes(offset + 6, leaseKey);
        leaseState = buf.getIntLE(offset + 22);
        leaseDuration = buf.getLongLE(offset + 26);
        return 36;
    }

    public String getLeaseKeyStr() {
        return DatatypeConverter.printHexBinary(leaseKey).toLowerCase();
    }
}
