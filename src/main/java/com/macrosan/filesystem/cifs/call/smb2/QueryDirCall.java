package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static java.nio.charset.StandardCharsets.UTF_16LE;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/10906442-294c-46d3-8515-c277efe1f752
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryDirCall extends SMB2Body {
    byte fileInfoClass;
    byte flags;
    int fileIndex;
    SMB2FileId fileId;
    String pattern;
    int responseLen;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);
        fileInfoClass = buf.getByte(start);
        flags = buf.getByte(start + 1);
        fileIndex = buf.getIntLE(start + 2);
        fileId = new SMB2FileId();
        fileId.readStruct(buf, start + 6);

        int patternOff = buf.getShortLE(start + 22);
        int patternLen = buf.getShortLE(start + 24);

        responseLen = buf.getIntLE(start + 26);

        byte[] patternBytes = new byte[patternLen];
        buf.getBytes(patternOff + offset - SMB2Header.SIZE, patternBytes);
        pattern = new String(patternBytes, UTF_16LE);

        return 32 + patternLen;
    }

    //samba-4.17.5实现的类型
    public static final byte SMB2_FIND_DIRECTORY_INFO = 0x01;
    public static final byte SMB2_FIND_FULL_DIRECTORY_INFO = 0x02;
    public static final byte SMB2_FIND_BOTH_DIRECTORY_INFO = 0x03;
    public static final byte SMB2_FIND_NAME_INFO = 0x0C;
    public static final byte SMB2_FIND_ID_BOTH_DIRECTORY_INFO = 0x25;
    public static final byte SMB2_FIND_ID_FULL_DIRECTORY_INFO = 0x26;

    //flags
    public static final int SMB2_CONTINUE_FLAG_RESTART = 0x01;
    public static final int SMB2_CONTINUE_FLAG_SINGLE = 0x02;
    public static final int SMB2_CONTINUE_FLAG_INDEX = 0x04;
    public static final int SMB2_CONTINUE_FLAG_REOPEN = 0x10;
}
