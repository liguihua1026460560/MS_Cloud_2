package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/90bf689a-8536-4f03-9f1b-683ee4bdd67c
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString
public class TreeConnectXCall extends SMB1Body {
    byte xOpcode;
    short xOffset;
    public short flags;
    byte[] passwd;
    public char[] path;
    public byte[] service;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        xOpcode = buf.getByte(offset + 1);
        xOffset = buf.getShortLE(offset + 3);
        flags = buf.getShortLE(offset + 5);
        int passLen = buf.getShortLE(offset + 7) & 0xffff;
        int start = offset + 11;
        passwd = new byte[passLen];
        buf.getBytes(start, passwd);

        int off = start + passwd.length;
        path = CifsUtils.readChars(buf, off);

        off += path.length * 2 + 2;

        int end = off;
        while (buf.getByte(end) != 0) {
            end++;
        }

        service = new byte[end - off];
        buf.getBytes(off, service);

        return super.readStruct(buf, offset);
    }


    public static final short TCONX_FLAG_DISCONNECT_TID = 0x0001;
    public static final short TCONX_FLAG_EXTENDED_SIGNATURES = 0x0004;
    public static final short TCONX_FLAG_EXTENDED_RESPONSE = 0x0008;

    //Service String
    public static byte[] DISK = "A:".getBytes();
    public static byte[] PRINTER = "LPT1:".getBytes();
    public static byte[] NAME = "IPC".getBytes();
    public static byte[] SERIAL = "COMM".getBytes();
    public static byte[] ANY = "?????".getBytes();
}
