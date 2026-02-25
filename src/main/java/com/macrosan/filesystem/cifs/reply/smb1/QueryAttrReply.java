package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/1011206a-55c5-4dbf-aff0-119514136940
@ToString(callSuper = true)
public class QueryAttrReply extends Trans2Reply {
    public int flags;
    int maxFileNameLen = 255;
    char[] fileSystem = "NTFS".toCharArray();

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        byteCount = 12 + fileSystem.length * 2 + 1;
        wordCount = 10;

        paramCount = totalParamCount = 0;
        dataCount = totalDataCount = byteCount - 1;
        dataOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;

        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setIntLE(start, flags);
        buf.setIntLE(start + 4, maxFileNameLen);
        buf.setIntLE(start + 8, fileSystem.length * 2);
        int off = start + 12;
        for (int i = 0; i < fileSystem.length; i++) {
            buf.setShortLE(off, fileSystem[i]);
            off += 2;
        }

        return super.writeStruct(buf, offset);
    }


    //https://learn.microsoft.com/zh-cn/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_fs_attribute_information
    public static final int FILE_CASE_SENSITIVE_SEARCH = 0x00000001;
    public static final int FILE_CASE_PRESERVED_NAMES = 0x00000002;
    public static final int FILE_UNICODE_ON_DISK = 0x00000004;
    public static final int FILE_PERSISTENT_ACLS = 0x00000008;
    public static final int FILE_FILE_COMPRESSION = 0x00000010;
    public static final int FILE_VOLUME_QUOTAS = 0x00000020;
    public static final int FILE_SUPPORTS_SPARSE_FILES = 0x00000040;
    public static final int FILE_SUPPORTS_REPARSE_POINTS = 0x00000080;
    public static final int FILE_SUPPORTS_REMOTE_STORAGE = 0x00000100;
    public static final int FS_LFN_APIS = 0x00004000;
    public static final int FILE_VOLUME_IS_COMPRESSED = 0x00008000;
    public static final int FILE_SUPPORTS_OBJECT_IDS = 0x00010000;
    public static final int FILE_SUPPORTS_ENCRYPTION = 0x00020000;
    public static final int FILE_NAMED_STREAMS = 0x00040000;
    public static final int FILE_READ_ONLY_VOLUME = 0x00080000;
    public static final int FILE_SUPPORTS_BLOCK_REFCOUNTING = 0x08000000;
}
