package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/598f395a-e7a2-4cc8-afb3-ccb30dd2df7c
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class NotifyCall extends SMB2Body {
    public short flags;
    public int maxResponseLen;
    public SMB2FileId fileId;
    public int filter;

    //flags 是否包含子目录
    public static final short SMB2_WATCH_TREE = 1;

    //filter
    public static final int FILE_NOTIFY_CHANGE_FILE_NAME = 0x00000001;
    public static final int FILE_NOTIFY_CHANGE_DIR_NAME = 0x00000002;

    public static final int FILE_NOTIFY_CHANGE_ATTRIBUTES = 0x00000004;
    public static final int FILE_NOTIFY_CHANGE_SIZE = 0x00000008;
    public static final int FILE_NOTIFY_CHANGE_LAST_WRITE = 0x00000010;
    public static final int FILE_NOTIFY_CHANGE_LAST_ACCESS = 0x00000020;
    public static final int FILE_NOTIFY_CHANGE_CREATION = 0x00000040;

    public static final int FILE_NOTIFY_CHANGE_EA = 0x00000080;
    public static final int FILE_NOTIFY_CHANGE_SECURITY = 0x00000100;
    public static final int FILE_NOTIFY_CHANGE_STREAM_NAME = 0x00000200;
    public static final int FILE_NOTIFY_CHANGE_STREAM_SIZE = 0x00000400;
    public static final int FILE_NOTIFY_CHANGE_STREAM_WRITE = 0x00000800;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = super.readStruct(buf, offset) + offset;
        flags = buf.getShortLE(start);
        maxResponseLen = buf.getIntLE(start + 2);
        fileId = new SMB2FileId();
        //16
        fileId.readStruct(buf, start + 6);
        filter = buf.getIntLE(start + 22);

        return 32;
    }
}
