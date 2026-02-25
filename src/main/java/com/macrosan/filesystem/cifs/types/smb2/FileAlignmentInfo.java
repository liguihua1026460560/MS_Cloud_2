package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileAlignmentInfo extends GetInfoReply.Info {
    public int alignmentRequirement;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, alignmentRequirement);
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
    //无对其要求
    public static final int FILE_BYTE_ALIGNMENT = 0x00000000;
    //指定数据必须在 2 字节上对齐 边界，依次 4 8 16 32 64 256
    public static final int FILE_WORD_ALIGNMENT = 0x00000001;
    public static final int FILE_LONG_ALIGNMENT = 0x00000003;
    public static final int FILE_QUAD_ALIGNMENT = 0x00000007;
    public static final int FILE_OCTA_ALIGNMENT = 0X0000000F;
    public static final int FILE_32_BYTE_ALIGNMENT = 0X0000001F;
    public static final int FILE_64_BYTE_ALIGNMENT = 0X0000003F;
    public static final int FILE_128_BYTE_ALIGNMENT = 0X0000007F;
    public static final int FILE_256_BYTE_ALIGNMENT = 0X000000FF;
    public static final int FILE_512_BYTE_ALIGNMENT = 0X000001FF;

}
