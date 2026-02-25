package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class QueryCifsUnixReply extends Trans2Reply {
    short major = 1;
    short minor = 0;
    public long capabilities;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        byteCount = 13;
        wordCount = 10;

        paramCount = totalParamCount = 0;
        dataCount = totalDataCount = 12;
        dataOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;

        int start = offset + 3 + wordCount * 2 + 1;//dataOffset + 4
        buf.setShortLE(start, major);
        buf.setShortLE(start + 2, minor);
        buf.setLongLE(start + 4, capabilities);

        return super.writeStruct(buf, offset);
    }

    public static final int CIFS_UNIX_FCNTL_LOCKS_CAP = 0x1;
    public static final int CIFS_UNIX_POSIX_ACLS_CAP = 0x2;
    public static final int CIFS_UNIX_XATTTR_CAP = 0x4;
    public static final int CIFS_UNIX_EXTATTR_CAP = 0x8;
    public static final int CIFS_UNIX_POSIX_PATHNAMES_CAP = 0x10;
    public static final int CIFS_UNIX_POSIX_PATH_OPERATIONS_CAP = 0x20;
    public static final int CIFS_UNIX_LARGE_READ_CAP = 0x40;
    public static final int CIFS_UNIX_LARGE_WRITE_CAP = 0x80;
}
