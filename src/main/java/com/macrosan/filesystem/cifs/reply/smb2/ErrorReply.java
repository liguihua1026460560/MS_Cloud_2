package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d4da8b67-c180-47e3-ba7a-d24214ac4aaa
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ErrorReply extends SMB2Body {
    short errorContextCount = 0;
    int byteCount = 0;
    byte[] bytes = new byte[1];
    //win7，win8查看权限时使用，用于代替bytes
    int requiredBufSize = 0;

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 9;
        super.writeStruct(buf, offset);
        buf.setShortLE(offset + 2, errorContextCount);
        buf.setIntLE(offset + 4, byteCount);
        if (requiredBufSize == 0) {
            buf.setBytes(offset + 8, bytes);
            return 9;
        } else {
            buf.setIntLE(offset + 8, requiredBufSize);
            return 12;
        }
    }

    public static ErrorReply EMPTY_ERROR = new ErrorReply();
}
