package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileCompressionInfo extends GetInfoReply.Info {
    public long compressedFileSize;
    public short compressionFormat;
    public byte compressionUnitShift;
    public byte chunkShift = 12;
    public byte clusterShift;
    //reserved  3


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, compressedFileSize);
        buf.setShortLE(offset + 8, compressionFormat);
        buf.setByte(offset + 10, compressionUnitShift);
        buf.setByte(offset + 11, chunkShift);
        buf.setByte(offset + 12, clusterShift);
        buf.setBytes(offset + 13, new byte[3]);
        return 16;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 16;
    }

    //compressionFormat
    public static final int COMPRESSION_FORMAT_NONE = 0x0000;
    public static final int COMPRESSION_FORMAT_LZNT1 = 0x0002;

}
