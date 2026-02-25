package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/63768db7-9012-4209-8cca-00781e7322f5
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FsFullSizeInfo extends GetInfoReply.Info {
    long units ;
    long availableUnits ;
    long actualAvailableUnits ;
    int sectorsPerUnit = 1;
    int bytesPerSector = 4096;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, units);
        buf.setLongLE(offset + 8, availableUnits);
        buf.setLongLE(offset + 16, actualAvailableUnits);
        buf.setIntLE(offset + 24, sectorsPerUnit);
        buf.setIntLE(offset + 28, bytesPerSector);
        return 32;
    }

    @Override
    public int size() {
        return 32;
    }
}
