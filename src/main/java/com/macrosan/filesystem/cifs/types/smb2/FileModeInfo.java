package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileModeInfo extends GetInfoReply.Info {
    public int mode;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, mode);
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

    public static final int FILE_WRITE_THROUGH = 0x00000002;
    public static final int FILE_SEQUENTIAL_ONLY = 0x00000004;
    public static final int FILE_NO_INTERMEDIATE_BUFFERING = 0x00000008;
    public static final int FILE_SYNCHRONOUS_IO_ALERT = 0x00000010;
    public static final int FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020;
    public static final int FILE_DELETE_ON_CLOSE = 0x00001000;

}
