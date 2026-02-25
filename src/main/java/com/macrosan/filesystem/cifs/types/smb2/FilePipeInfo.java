package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FilePipeInfo extends GetInfoReply.Info {
    public int readMode;
    public int completionMode;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, readMode);
        buf.setIntLE(offset + 4, completionMode);
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
    //readMode
    public static final int FILE_PIPE_BYTE_STREAM_MODE = 0x00000000;
    public static final int FILE_PIPE_MESSAGE_MODE = 0x00000001;

    //completionMode
    public static final int FILE_PIPE_QUEUE_OPERATION = 0x00000000;
    public static final int FILE_PIPE_COMPLETE_OPERATION = 0x00000001;
}
