package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileAttrTagInfo extends GetInfoReply.Info {
    public int fileAttr;
    //重解析标志:硬/软连接，快捷方式
    public int reparseTag = IO_REPARSE_TAG_RESERVED_ZERO;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, fileAttr);
        buf.setIntLE(offset + 4, reparseTag);
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

    //fileAttr
    public static final int FILE_ATTRIBUTE_READONLY = 0x00000001;
    public static final int FILE_ATTRIBUTE_HIDDEN = 0x00000002;
    public static final int FILE_ATTRIBUTE_SYSTEM = 0x00000004;
    public static final int FILE_ATTRIBUTE_DIRECTORY = 0x00000010;
    public static final int FILE_ATTRIBUTE_ARCHIVE = 0x00000020;
    public static final int FILE_ATTRIBUTE_NORMAL = 0x00000080;
    public static final int FILE_ATTRIBUTE_TEMPORARY = 0x00000100;
    public static final int FILE_ATTRIBUTE_SPARSE_FILE = 0x00000200;
    public static final int FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400;
    public static final int FILE_ATTRIBUTE_COMPRESSED = 0x00000800;
    public static final int FILE_ATTRIBUTE_OFFLINE = 0x00001000;
    public static final int FILE_ATTRIBUTE_NOT_CONTENT_INDEXED = 0x00002000;
    public static final int FILE_ATTRIBUTE_ENCRYPTED = 0x00004000;
    public static final int FILE_ATTRIBUTE_INTEGRITY_STREAM = 0x00008000;
    public static final int FILE_ATTRIBUTE_NO_SCRUB_DATA = 0x00020000;
    public static final int FILE_ATTRIBUTE_RECALL_ON_OPEN = 0x00040000;
    public static final int FILE_ATTRIBUTE_PINNED = 0x00080000;
    public static final int FILE_ATTRIBUTE_UNPINNED = 0x00100000;
    public static final int FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000;

    //reparseTag
    public static final int IO_REPARSE_TAG_RESERVED_ZERO = 0x00000000;
    public static final int IO_REPARSE_TAG_RESERVED_ONE = 0x00000001;
    public static final int IO_REPARSE_TAG_RESERVED_TWO = 0x00000002;
    public static final int IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003;
    public static final int IO_REPARSE_TAG_HSM = 0xC0000004;
    public static final int IO_REPARSE_TAG_DRIVE_EXTENDER = 0x80000005;
    public static final int IO_REPARSE_TAG_HSM2 = 0x80000006;
    public static final int IO_REPARSE_TAG_SIS = 0x80000007;
    public static final int IO_REPARSE_TAG_WIM = 0x80000008;
    public static final int IO_REPARSE_TAG_CSV = 0x80000009;
    public static final int IO_REPARSE_TAG_DFS = 0x8000000A;
    public static final int IO_REPARSE_TAG_FILTER_MANAGER = 0x8000000B;
    //软连接
    public static final int IO_REPARSE_TAG_SYMLINK = 0xA000000C;
    public static final int IO_REPARSE_TAG_IIS_CACHE = 0xA0000010;
    public static final int IO_REPARSE_TAG_DFSR = 0x80000012;
    public static final int IO_REPARSE_TAG_DEDUP = 0x80000013;
    public static final int IO_REPARSE_TAG_APPXSTRM = 0xC0000014;
    public static final int IO_REPARSE_TAG_NFS = 0x80000014;
    public static final int IO_REPARSE_TAG_FILE_PLACEHOLDER = 0x80000015;
    public static final int IO_REPARSE_TAG_DFM = 0x80000016;
    public static final int IO_REPARSE_TAG_WOF = 0x80000017;
    public static final int IO_REPARSE_TAG_WCI = 0x80000018;
    public static final int IO_REPARSE_TAG_WCI_1 = 0x90001018;
    public static final int IO_REPARSE_TAG_GLOBAL_REPARSE = 0xA0000019;
    public static final int IO_REPARSE_TAG_CLOUD = 0x9000001A;
    public static final int IO_REPARSE_TAG_CLOUD_1 = 0x9000101A;
    public static final int IO_REPARSE_TAG_CLOUD_2 = 0x9000201A;
    public static final int IO_REPARSE_TAG_CLOUD_3 = 0x9000301A;
    public static final int IO_REPARSE_TAG_CLOUD_4 = 0x9000401A;
    public static final int IO_REPARSE_TAG_CLOUD_5 = 0x9000501A;
    public static final int IO_REPARSE_TAG_CLOUD_6 = 0x9000601A;
    public static final int IO_REPARSE_TAG_CLOUD_7 = 0x9000701A;
    public static final int IO_REPARSE_TAG_CLOUD_8 = 0x9000801A;
    public static final int IO_REPARSE_TAG_CLOUD_9 = 0x9000901A;
    public static final int IO_REPARSE_TAG_CLOUD_A = 0x9000A01A;
    public static final int IO_REPARSE_TAG_CLOUD_B = 0x9000B01A;
    public static final int IO_REPARSE_TAG_CLOUD_C = 0x9000C01A;
    public static final int IO_REPARSE_TAG_CLOUD_D = 0x9000D01A;
    public static final int IO_REPARSE_TAG_CLOUD_E = 0x9000E01A;
    public static final int IO_REPARSE_TAG_CLOUD_F = 0x9000F01A;
    public static final int IO_REPARSE_TAG_APPEXECLINK = 0x8000001B;
    public static final int IO_REPARSE_TAG_PROJFS = 0x9000001C;
    //unix 软连接
    public static final int IO_REPARSE_TAG_LX_SYMLINK = 0xA000001D;
    public static final int IO_REPARSE_TAG_STORAGE_SYNC = 0x8000001E;
    public static final int IO_REPARSE_TAG_WCI_TOMBSTONE = 0xA000001F;
    public static final int IO_REPARSE_TAG_UNHANDLED = 0x80000020;
    public static final int IO_REPARSE_TAG_ONEDRIVE = 0x80000021;
    public static final int IO_REPARSE_TAG_PROJFS_TOMBSTONE = 0xA0000022;
    public static final int IO_REPARSE_TAG_AF_UNIX = 0x80000023;
    //unix
    public static final int IO_REPARSE_TAG_LX_FIFO = 0x80000024;
    public static final int IO_REPARSE_TAG_LX_CHR = 0x80000025;
    public static final int IO_REPARSE_TAG_LX_BLK = 0x80000026;

    public static final int IO_REPARSE_TAG_WCI_LINK = 0xA0000027;
    public static final int IO_REPARSE_TAG_WCI_LINK_1 = 0xA0001027;

}
