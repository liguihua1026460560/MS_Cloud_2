package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e215700a-102c-450a-a598-7ec2a99cd82c
 */
public class LockReply extends SMB2Body {

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 4;
        return super.writeStruct(buf, offset) + 2;
    }
}
