package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;

/**
 * @Author: WANG CHENXING
 * @Date: 2024/8/1
 * @Description: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/c4318eb4-bdab-49b7-9352-abd7005c7f19
 */
public class SetInfoReply extends SMB2Body {
    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 2;
        super.writeStruct(buf, offset);
        return 2;
    }
}
