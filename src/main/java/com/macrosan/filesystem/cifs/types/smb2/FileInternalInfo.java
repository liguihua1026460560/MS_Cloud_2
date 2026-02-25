package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class FileInternalInfo extends GetInfoReply.Info {
    public long nodeId;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, nodeId);
        return 8;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 8;
    }


}
