package com.macrosan.filesystem.cifs;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODES;


@EqualsAndHashCode(callSuper = true)
@Data
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/69a29f73-de0c-45a6-a1aa-8ceeea42217f
 */
public class SMB1Header extends SMBHeader {
    public static final int SIZE = 32;
    public static final int MAGIC = 0x424D53FF;

    public int magic;
    public byte opcode;
    public int status;
    public byte flags;
    public short flags2;
    public long sign;
    public short tid;
    public int pid;
    public short uid;
    public short mid;


    public int size() {
        return SIZE;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        magic = buf.getIntLE(offset);
        opcode = buf.getByte(offset + 4);
        status = buf.getIntLE(offset + 5);
        flags = buf.getByte(offset + 9);
        flags2 = buf.getShortLE(offset + 10);
        short pidHigh = buf.getShortLE(offset + 12);
        sign = buf.getLongLE(offset + 14);
        tid = buf.getShortLE(offset + 24);
        short pidLow = buf.getShortLE(offset + 26);
        pid = (pidHigh << 16) | (pidLow & 0xffff);

        uid = buf.getShortLE(offset + 28);
        mid = buf.getShortLE(offset + 30);
        return SIZE;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, magic);
        buf.setByte(offset + 4, opcode);
        buf.setIntLE(offset + 5, status);
        buf.setByte(offset + 9, flags);
        buf.setShortLE(offset + 10, flags2);
        buf.setShortLE(offset + 12, pid >> 16);
        buf.setLongLE(offset + 14, sign);
        buf.setShortLE(offset + 24, tid);
        buf.setShortLE(offset + 26, pid & 0xffff);
        buf.setShortLE(offset + 28, uid);
        buf.setShortLE(offset + 30, mid);
        return SIZE;
    }

    @Override
    public String toString() {
        return "SMBHeader{" +
                "magic=0x" + Integer.toHexString(magic) +
                ", opcode=" + SMB1_OPCODES[opcode & 0xff] +
                ", status=" + status +
                ", flags=" + flags +
                ", flags2=" + flags2 +
                ", sign=" + Long.toHexString(sign) +
                ", tid=" + tid +
                ", pid=" + pid +
                ", uid=" + uid +
                ", mid=" + mid +
                '}';
    }

    public static final byte FLAG_SUPPORT_LOCKREAD = 0x01;
    public static final byte FLAG_CLIENT_BUF_AVAIL = 0x02;
    public static final byte FLAG_RESERVED = 0x04;
    public static final byte FLAG_CASELESS_PATHNAMES = 0x08;
    public static final byte FLAG_CANONICAL_PATHNAMES = 0x10;
    public static final byte FLAG_REQUEST_OPLOCK = 0x20;
    public static final byte FLAG_REQUEST_BATCH_OPLOCK = 0x40;
    public static final byte FLAG_REPLY = (byte) 0x80;

    public static final short FLAGS2_LONG_PATH_COMPONENTS = 0x0001;
    public static final short FLAGS2_EXTENDED_ATTRIBUTES = 0x0002;
    public static final short FLAGS2_SMB_SECURITY_SIGNATURES = 0x0004;
    public static final short FLAGS2_COMPRESSED = 0x0008; /* MS-SMB */
    public static final short FLAGS2_SMB_SECURITY_SIGNATURES_REQUIRED = 0x0010;
    public static final short FLAGS2_IS_LONG_NAME = 0x0040;
    public static final short FLAGS2_REPARSE_PATH = 0x0400; /* MS-SMB @GMT- path. */
    public static final short FLAGS2_EXTENDED_SECURITY = 0x0800;
    public static final short FLAGS2_DFS_PATHNAMES = 0x1000;
    public static final short FLAGS2_READ_PERMIT_EXECUTE = 0x2000;
    public static final short FLAGS2_32_BIT_ERROR_CODES = 0x4000;
    public static final short FLAGS2_UNICODE_STRINGS = (short) 0x8000;


    public static SMB1Header newReplyHeader(SMB1Header reqHeader) {
        SMB1Header replyHeader = new SMB1Header();
        replyHeader.magic = reqHeader.magic;
        replyHeader.flags2 = FLAGS2_UNICODE_STRINGS | FLAGS2_32_BIT_ERROR_CODES |
                FLAGS2_EXTENDED_SECURITY | FLAGS2_EXTENDED_ATTRIBUTES | FLAGS2_LONG_PATH_COMPONENTS
                | FLAGS2_SMB_SECURITY_SIGNATURES;
        replyHeader.flags = FLAG_REPLY;
        replyHeader.sign = reqHeader.sign;
        replyHeader.opcode = reqHeader.opcode;
        replyHeader.tid = reqHeader.tid;
        replyHeader.pid = reqHeader.pid;
        replyHeader.uid = reqHeader.uid;
        replyHeader.mid = reqHeader.mid;

        return replyHeader;
    }
}
