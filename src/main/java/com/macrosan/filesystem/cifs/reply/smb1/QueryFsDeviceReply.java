package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/d7ea6e1a-6526-4230-b566-e9588c7498f1
@ToString(callSuper = true)
public class QueryFsDeviceReply extends Trans2Reply {
    int deviceType = FILE_DEVICE_DISK;
    int deviceCharacteristics = FILE_DEVICE_IS_MOUNTED;


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        byteCount = 9;
        wordCount = 10;

        paramCount = totalParamCount = 0;
        dataCount = totalDataCount = 8;
        dataOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;

        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setIntLE(start, deviceType);
        buf.setIntLE(start + 4, deviceCharacteristics);

        return super.writeStruct(buf, offset);
    }

    public static final int FILE_DEVICE_DISK = 0x7;

    public static final int FILE_REMOVABLE_MEDIA = 0x001;
    public static final int FILE_READ_ONLY_DEVICE = 0x002;
    public static final int FILE_FLOPPY_DISKETTE = 0x004;
    public static final int FILE_WRITE_ONCE_MEDIA = 0x008;
    public static final int FILE_REMOTE_DEVICE = 0x010;
    public static final int FILE_DEVICE_IS_MOUNTED = 0x020;
    public static final int FILE_VIRTUAL_VOLUME = 0x040;
    public static final int FILE_DEVICE_SECURE_OPEN = 0x100;
}
