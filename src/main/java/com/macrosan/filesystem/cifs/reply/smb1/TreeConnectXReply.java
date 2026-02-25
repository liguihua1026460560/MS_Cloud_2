package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@Data
@EqualsAndHashCode(callSuper = true)
public class TreeConnectXReply extends SMB1Body {
    public byte xOpcode;
    public short xOffset;
    public short optionalSupport;
    public int accessRights;
    public int guestAccessRights;

    public byte[] service;
    public char[] fileSystem;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setByte(offset + 1, xOpcode);
        buf.setShortLE(offset + 3, xOffset);
        buf.setShortLE(offset + 5, optionalSupport);

        if (wordCount == 7) {
            buf.setIntLE(offset + 7, accessRights);
            buf.setIntLE(offset + 11, guestAccessRights);
        }

        int byteStart = offset + 3 + wordCount * 2;


        buf.setBytes(byteStart, service);
        buf.setByte(byteStart + service.length, 0);
        int off = byteStart + service.length + 1;

        CifsUtils.writeChars(buf, off, fileSystem);

        off += 2 + fileSystem.length * 2;

        byteCount = off - byteStart;
        return super.writeStruct(buf, offset);
    }

    public static final short SMB_SUPPORT_SEARCH_BITS = 0x0001;
    public static final short SMB_SHARE_IN_DFS = 0x0002;
    public static final short SMB_CSC_MASK = 0x000C;
    public static final short SMB_CSC_POLICY_SHIFT = 2;
    public static final short SMB_UNIQUE_FILE_NAME = 0x0010;
    public static final short SMB_EXTENDED_SIGNATURES = 0x0020;


    public static final int FILE_ALL_ACCESS = 0x000001ff;
    public static final int STANDARD_RIGHTS_ALL_ACCESS = 0x001F0000;
    public static final int FILE_LIST_DIRECTORY = 0x00000001;
    public static final int FILE_ADD_FILE = 0x00000002;
    public static final int FILE_ADD_SUBDIRECTORY = 0x00000004;
    public static final int FILE_READ_EA = 0x00000008;
    public static final int FILE_WRITE_EA = 0x00000010;
    public static final int FILE_TRAVERSE = 0x00000020;
    public static final int FILE_DELETE_CHILD = 0x00000040;
    public static final int FILE_READ_ATTRIBUTES = 0x00000080;
    public static final int FILE_WRITE_ATTRIBUTES = 0x00000100;
    public static final int DELETE = 0x00010000;
    public static final int READ_CONTROL = 0x00020000;
    public static final int WRITE_DAC = 0x00040000;
    public static final int WRITE_OWNER = 0x00080000;
    public static final int SYNCHRONIZE = 0x00100000;
    public static final int ACCESS_SYSTEM_SECURITY = 0x01000000;
    public static final int MAXIMUM_ALLOWED = 0x02000000;
    public static final int GENERIC_ALL = 0x10000000;
    public static final int GENERIC_EXECUTE = 0x20000000;
    public static final int GENERIC_WRITE = 0x40000000;
    public static final int GENERIC_READ = 0x80000000;
}
