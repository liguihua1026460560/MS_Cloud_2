package com.macrosan.filesystem.cifs.types.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;

public class Trans2 extends SMB1Body {
    //整个trans的数量
    int totalParamCount;
    int totalDataCount;
    //reply时最大的count
    int maxParamCount;
    int maxDataCount;
    int maxSetupCount;
    int flags;
    long timeout;
    //当前trans的数量
    int paramCount;
    public int paramOffset;
    int dataCount;
    public int dataOffset;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);

        totalParamCount = buf.getShortLE(offset) & 0xffff;
        totalDataCount = buf.getShortLE(offset + 2) & 0xffff;
        maxParamCount = buf.getShortLE(offset + 4) & 0xffff;
        maxDataCount = buf.getShortLE(offset + 6) & 0xffff;
        maxSetupCount = buf.getByte(offset + 8) & 0xff;
        flags = buf.getShortLE(offset + 10) & 0xffff;
        timeout = buf.getIntLE(offset + 12);
        paramCount = buf.getShortLE(offset + 18) & 0xffff;
        paramOffset = buf.getShortLE(offset + 20) & 0xffff;
        dataCount = buf.getShortLE(offset + 22) & 0xffff;
        dataOffset = buf.getShortLE(offset + 24) & 0xffff;

        return size;
    }

    public static final byte TRANSACT2_OPEN = 0;
    public static final byte TRANSACT2_FINDFIRST = 1;
    public static final byte TRANSACT2_FINDNEXT = 2;
    public static final byte TRANSACT2_QFSINFO = 3;
    public static final byte TRANSACT2_SETFSINFO = 4;
    public static final byte TRANSACT2_QPATHINFO = 5;
    public static final byte TRANSACT2_SETPATHINFO = 6;
    public static final byte TRANSACT2_QFILEINFO = 7;
    public static final byte TRANSACT2_SETFILEINFO = 8;
    public static final byte TRANSACT2_FSCTL = 9;
    public static final byte TRANSACT2_IOCTL = 0xA;
    public static final byte TRANSACT2_FINDNOTIFYFIRST = 0xB;
    public static final byte TRANSACT2_FINDNOTIFYNEXT = 0xC;
    public static final byte TRANSACT2_MKDIR = 0xD;
    public static final byte TRANSACT2_SESSION_SETUP = 0xE;
    public static final byte TRANSACT2_GET_DFS_REFERRAL = 0x10;
    public static final byte TRANSACT2_REPORT_DFS_INCONSISTANCY = 0x11;
}
