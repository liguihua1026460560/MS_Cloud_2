package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/616b66d5-b335-4e1c-8f87-b4a55e8d3e4a
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FsDeviceInfo extends GetInfoReply.Info {
    int deviceType = FILE_DEVICE_DISK;
    int characteristics = FILE_DEVICE_IS_MOUNTED;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, deviceType);
        buf.setIntLE(offset + 4, characteristics);
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

    //deviceType
    public static final int FILE_DEVICE_CD_ROM = 0x00000002;
    public static final int FILE_DEVICE_DISK = 0x00000007;

    //characteristics
    public static final int FILE_REMOVABLE_MEDIA = 0x001;
    public static final int FILE_READ_ONLY_DEVICE = 0x002;
    public static final int FILE_FLOPPY_DISKETTE = 0x004;
    public static final int FILE_WRITE_ONCE_MEDIA = 0x008;
    public static final int FILE_REMOTE_DEVICE = 0x010;
    public static final int FILE_DEVICE_IS_MOUNTED = 0x020;
    public static final int FILE_VIRTUAL_VOLUME = 0x040;
    public static final int FILE_DEVICE_SECURE_OPEN = 0x100;

}
