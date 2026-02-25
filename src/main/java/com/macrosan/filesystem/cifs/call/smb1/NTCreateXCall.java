package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/f2a0f032-7545-41c9-9ceb-aab39852c11a
@EqualsAndHashCode(callSuper = true)
@ToString
@Data
public class NTCreateXCall extends SMB1Body {
    public byte xOpcode;
    public short xOffset;
    public short nameLen;
    public int flags;
    public int rootFid;
    public int access;
    public long allocSize;
    public int mode;
    public int share;
    public int createDisposition;
    public int createOptions;
    public int impersonationLevel;
    public byte securityFlags;
    public char[] fileName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        int start = offset + 1;


        xOpcode = buf.getByte(start);
        xOffset = buf.getShortLE(start + 2);
        nameLen = buf.getShortLE(start + 5);
        flags = buf.getIntLE(start + 7);
        rootFid = buf.getIntLE(start + 11);
        access = buf.getIntLE(start + 15);
        allocSize = buf.getLongLE(start + 19);
        mode = buf.getIntLE(start + 27);
        share = buf.getIntLE(start + 31);
        createDisposition = buf.getIntLE(start + 35);
        createOptions = buf.getIntLE(start + 39);
        impersonationLevel = buf.getIntLE(start + 43);
        securityFlags = buf.getByte(start + 47);

        int off = offset + 3 + wordCount * 2;
        //pad
        if ((off & 1) == 1) {
            off++;
        }

        fileName = CifsUtils.readChars(buf, off);
        return size;
    }
}
