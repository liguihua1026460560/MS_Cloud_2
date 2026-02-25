package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileAccessInfo extends GetInfoReply.Info {
    public int accessFlags;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, accessFlags);
        return 4;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 4;
    }

}
