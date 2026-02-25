package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FsSizeInfo extends GetInfoReply.Info {
    //   =totalSize/分配单元大小
    //分配单元总数
    public long totalAllocate;
    public long actualAllocate ;
    public int sectorsPerAllocate = 1;
    public int bytesPerSector = 4096;
    //sectorsPerAllocate * bytesPerSector == 分配单元大小(一般4k)
    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, totalAllocate);
        buf.setLongLE(offset + 8, actualAllocate);
        buf.setIntLE(offset + 16, sectorsPerAllocate);
        buf.setIntLE(offset + 20, bytesPerSector);
        return 24;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 24;
    }


}
