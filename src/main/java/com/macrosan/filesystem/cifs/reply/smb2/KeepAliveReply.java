package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/2abe9b3c-c5ab-417f-bcc3-9ab51f2fce35
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class KeepAliveReply extends SMB2Body {

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 4;
        super.writeStruct(buf, offset);
        return 4;
    }

    public static final KeepAliveReply DEFAULT = new KeepAliveReply();
}
