package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class NTLMSSPSetupXReply extends SMB2Body {
    public short flags;
    public short tokenOff;
    public short tokenLen;
    public NTLMSSP.NTLMSSPMessage message;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 9;
        int start = super.writeStruct(buf, offset) + offset;
        buf.setShortLE(start, flags);

        if (message != null) {
            int size = message.writeStruct(buf, start + 6);

            tokenOff = (short) (start + 6 - 4);
            tokenLen = (short) size;
            buf.setShortLE(start + 2, tokenOff);
            buf.setShortLE(start + 4, tokenLen);

            return 8 + size;
        } else {
            tokenOff = 0;
            tokenLen = 0;
            buf.setShortLE(start + 2, tokenOff);
            buf.setShortLE(start + 4, tokenLen);

            return 8;
        }
    }
}
