package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5a3c2c28-d6b0-48ed-b917-a86b2ca4575f
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class NTLMSSPSetupXCall extends SMB2Body {
    byte flags;
    byte securityMode;
    int capabilities;
    int channel;
    short tokenOff;
    short tokenLen;
    long previousSessionId;
    public NTLMSSP.NTLMSSPMessage message;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = super.readStruct(buf, offset) + offset;
        flags = buf.getByte(start);
        securityMode = buf.getByte(start + 1);
        capabilities = buf.getIntLE(start + 2);
        channel = buf.getIntLE(start + 6);
        tokenOff = buf.getShortLE(start + 10);
        tokenLen = buf.getShortLE(start + 12);

        previousSessionId = buf.getLongLE(start + 14);

        message = NTLMSSP.readStruct(buf, tokenOff + 4);

        if (message == null) {
            return -1;
        }

        return tokenLen + 24;
    }
}
