package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.charset.StandardCharsets;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/ebc7e6e5-4650-4e54-b17c-cf60f6fbeeaa
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FsAttrInfo extends GetInfoReply.Info {
    int fsAttr;
    int maxNameLen = 255;
    byte[] fsName = "NTFS".getBytes(StandardCharsets.UTF_16LE);

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, fsAttr);
        buf.setIntLE(offset + 4, maxNameLen);
        buf.setIntLE(offset + 8, fsName.length);
        buf.setBytes(offset + 12, fsName);
        return 20;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 20;
    }

    public static final int FILE_SUPPORTS_USN_JOURNAL = 0x02000000;
    public static final int FILE_SUPPORTS_OPEN_BY_FILE_ID = 0x01000000;
    public static final int FILE_SUPPORTS_EXTENDED_ATTRIBUTES = 0x00800000;
    public static final int FILE_SUPPORTS_HARD_LINKS = 0x00400000;
    public static final int FILE_SUPPORTS_TRANSACTIONS = 0x00200000;
    public static final int FILE_SEQUENTIAL_WRITE_ONCE = 0x00100000;
    public static final int FILE_READ_ONLY_VOLUME = 0x00080000;
    public static final int FILE_NAMED_STREAMS = 0x00040000;
    public static final int FILE_SUPPORTS_ENCRYPTION = 0x00020000;
    public static final int FILE_SUPPORTS_OBJECT_IDS = 0x00010000;
    public static final int FILE_VOLUME_IS_COMPRESSED = 0x00008000;
    public static final int FILE_SUPPORTS_REMOTE_STORAGE = 0x00000100;
    public static final int FILE_SUPPORTS_REPARSE_POINTS = 0x00000080;
    public static final int FILE_SUPPORTS_SPARSE_FILES = 0x00000040;
    public static final int FILE_VOLUME_QUOTAS = 0x00000020;
    public static final int FILE_FILE_COMPRESSION = 0x00000010;
    public static final int FILE_PERSISTENT_ACLS = 0x00000008;
    public static final int FILE_UNICODE_ON_DISK = 0x00000004;
    public static final int FILE_CASE_PRESERVED_NAMES = 0x00000002;
    public static final int FILE_CASE_SENSITIVE_SEARCH = 0x00000001;
    public static final int FILE_SUPPORT_INTEGRITY_STREAMS = 0x04000000;
    public static final int FILE_SUPPORTS_BLOCK_REFCOUNTING = 0x08000000;
    public static final int FILE_SUPPORTS_SPARSE_VDL = 0x10000000;

}
