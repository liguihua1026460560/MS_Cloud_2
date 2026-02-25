package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FsSectorSizeInfo extends GetInfoReply.Info {
    public int logicalBytesPerSector;
    public int physicalBytesPerSectorForAtomicity;
    public int physicalBytesPerSectorForPerformance;
    public int fsEffectivePhysicalBytesPerSectorForAtomicity;
    public int flag;
    public int byteOffsetForSectorAlignment;
    public int byteOffsetForPartitionAlignment;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, logicalBytesPerSector);
        buf.setIntLE(offset + 4, physicalBytesPerSectorForAtomicity);
        buf.setIntLE(offset + 8, physicalBytesPerSectorForPerformance);
        buf.setIntLE(offset + 12, fsEffectivePhysicalBytesPerSectorForAtomicity);
        buf.setIntLE(offset + 14, flag);
        buf.setIntLE(offset + 16, byteOffsetForSectorAlignment);
        buf.setIntLE(offset + 20, byteOffsetForPartitionAlignment);
        return 28;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 28;
    }

    public static final int SSINFO_FLAGS_ALIGNED_DEVICE = 0x00000001;
    public static final int SSINFO_FLAGS_PARTITION_ALIGNED_ON_DEVICE = 0x00000002;
    public static final int SSINFO_FLAGS_NO_SEEK_PENALTY = 0x00000004;
    public static final int SSINFO_FLAGS_TRIM_ENABLED = 0x00000008;

}
