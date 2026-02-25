package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FilePositionInfo extends GetInfoReply.Info {
    public int currentByteOffset;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, currentByteOffset);
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

    public static final int SSINFO_FLAGS_ALIGNED_DEVICE = 0x00000001;
    public static final int SSINFO_FLAGS_PARTITION_ALIGNED_ON_DEVICE = 0x00000002;
    public static final int SSINFO_FLAGS_NO_SEEK_PENALTY = 0x00000004;
    public static final int SSINFO_FLAGS_TRIM_ENABLED = 0x00000008;

}
