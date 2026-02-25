package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.CreateContext;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e8fb45c1-a03d-44ca-b7ae-47385cfd7997
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class CreateCall extends SMB2Body {
    byte securityFlags;
    byte oplockLevel;
    int impersonationLevel;
    //reserved
    long createFlags;

    //表示当前create请求希望获取的权限类型，若不满足则予以拒绝；无符号4byte，java中用long表示
    long access;
    int mode;
    int shareAccess;

    int createDisposition;
    int createOptions;
    char[] fileName;
    CreateContext[] context;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        try {
            int start = super.readStruct(buf, offset) + offset;
            securityFlags = buf.getByte(start);
            oplockLevel = buf.getByte(start + 1);
            impersonationLevel = buf.getIntLE(start + 2);
            createFlags = buf.getLongLE(start + 6);
            access = buf.getUnsignedIntLE(start + 22);
            mode = buf.getIntLE(start + 26);
            shareAccess = buf.getIntLE(start + 30);
            createDisposition = buf.getIntLE(start + 34);
            createOptions = buf.getIntLE(start + 38);

            int fileNameOff = buf.getShortLE(start + 42);
            int fileNameLen = buf.getShortLE(start + 44);
            fileName = new char[fileNameLen / 2];
            for (int i = 0; i < fileName.length; i++) {
                fileName[i] = (char) buf.getShortLE(4 + fileNameOff + i * 2);
            }

            int createContextOff = buf.getIntLE(start + 46);
            int createContextLen = buf.getIntLE(start + 50);
            if (createContextLen > 0) {
                context = CreateContext.readCreateContexts(buf, createContextOff + 4, createContextLen);
                return createContextOff + createContextLen - offset;
            } else {
                context = new CreateContext[0];
                return 56 + fileNameLen;
            }
        } catch (Exception e) {
            log.error("Invalid SMB packet......");
            return READ_STRUCT_INVALID_PARAMETER;
        }
    }

    public static final int FILE_SUPERSEDE = 0;       /* File exists overwrite/supersede. File not exist create. */
    public static final int FILE_OPEN = 1;            /* File exists open. File not exist fail. */
    public static final int FILE_CREATE = 2;          /* File exists fail. File not exist create. */
    public static final int FILE_OPEN_IF = 3;         /* File exists open. File not exist create. */
    public static final int FILE_OVERWRITE = 4;       /* File exists overwrite. File not exist fail. */
    public static final int FILE_OVERWRITE_IF = 5;    /* File exists overwrite. File not exist create. */

    // createOptions, https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e8fb45c1-a03d-44ca-b7ae-47385cfd7997
    public static final int FILE_DIRECTORY_FILE = 0x00000001;
    public static final int FILE_WRITE_THROUGH = 0x00000002;
    public static final int FILE_SEQUENTIAL_ONLY = 0x00000004;
    public static final int FILE_NO_INTERMEDIATE_BUFFERING = 0x00000008;
    public static final int FILE_SYNCHRONOUS_IO_ALERT = 0x00000010;
    public static final int FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020;
    public static final int FILE_NON_DIRECTORY_FILE = 0x00000040;
    public static final int FILE_COMPLETE_IF_OPLOCKED = 0x00000100;
    public static final int FILE_NO_EA_KNOWLEDGE = 0x00000200;
    public static final int FILE_RANDOM_ACCESS = 0x00000800;
    public static final int FILE_DELETE_ON_CLOSE = 0x00001000;
    public static final int FILE_OPEN_BY_FILE_ID = 0x00002000;
    public static final int FILE_OPEN_FOR_BACKUP_INTENT = 0x00004000;
    public static final int FILE_NO_COMPRESSION = 0x00008000;
    public static final int FILE_OPEN_REMOTE_INSTANCE = 0x00000400;
    public static final int FILE_OPEN_REQUIRING_OPLOCK = 0x00010000;
    public static final int FILE_DISALLOW_EXCLUSIVE = 0x00020000;
    public static final int FILE_RESERVE_OPFILTER = 0x00100000;
    public static final int FILE_OPEN_REPARSE_POINT = 0x00200000;
    public static final int FILE_OPEN_NO_RECALL = 0x00400000;
    public static final int FILE_OPEN_FOR_FREE_SPACE_QUERY = 0x00800000;

    public static final int READ_STRUCT_INVALID_PARAMETER = -2;


    public static final byte SMB2_OPLOCK_LEVEL_NONE = 0x00;
    public static final byte SMB2_OPLOCK_LEVEL_II = 0x01;
    public static final byte SMB2_OPLOCK_LEVEL_EXCLUSIVE = 0x08;
    public static final byte SMB2_OPLOCK_LEVEL_BATCH = 0x09;
    public static final byte SMB2_OPLOCK_LEVEL_LEASE = (byte) 0xFF;


    public static final int FILE_READ_DATA = 0x00000001;
    public static final int FILE_WRITE_DATA = 0x00000002;
    public static final int FILE_APPEND_DATA = 0x00000004;
    public static final int FILE_READ_EA = 0x00000008;
    public static final int FILE_WRITE_EA = 0x00000010;
    public static final int FILE_EXECUTE = 0x00000020;
    public static final int FILE_DELETE_CHILD = 0x00000040;
    public static final int FILE_READ_ATTRIBUTES = 0x00000080;
    public static final int FILE_WRITE_ATTRIBUTES = 0x00000100;
    public static final int FILE_DELETE = 0x00010000;
    public static final int FILE_READ_CONTROL = 0x00020000;
    public static final int FILE_WRITE_DAC = 0x00040000;
    public static final int FILE_WRITE_OWNER = 0x00080000;
    public static final int FILE_SYNCHRONIZE = 0x00100000;
    public static final int FILE_ACCESS_SYSTEM_SECURITY = 0x01000000;
    public static final int FILE_MAXIMUM_ALLOWED = 0x02000000;
    public static final int FILE_GENERIC_ALL = 0x10000000;
    public static final int FILE_GENERIC_EXECUTE = 0x20000000;
    public static final int FILE_GENERIC_READ = 0x80000000;
    public static final int FILE_GENERIC_WRITE = 0x40000000;

    public static final int FILE_WRITE_ACCESS = FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_WRITE_DAC | FILE_GENERIC_WRITE;
}
