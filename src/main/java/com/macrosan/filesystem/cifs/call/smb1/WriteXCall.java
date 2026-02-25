package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString(exclude = "fileData")
@Slf4j
public class WriteXCall extends SMB1Body {

    public byte xOpcode;
    public short xOffset;
    public short fsid;
    public int offset;
    public short writeMode;
    public short remaining;
    public short dateLenHigh;
    public short dateLenLow;
    public short dataOffset;
    public int highOffset;

    public byte[] fileData;

    public static final int HIGH_BLOCK_SIZE = 64 * 1024;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + 1;
        xOpcode = buf.getByte(start);
        start += 2;
        xOffset = buf.getShortLE(start);
        fsid = buf.getShortLE(start + 2);
        this.offset = buf.getIntLE(start + 4);
        // reserved 4
        start += 4;
        writeMode = buf.getShortLE(start + 8);
        remaining = buf.getShortLE(start + 10);
        dateLenHigh = buf.getShortLE(start + 12);
        dateLenLow = buf.getShortLE(start + 14);
        dataOffset = buf.getShortLE(start + 16);
        highOffset = buf.getIntLE(start + 18);
        if (dateLenHigh != 0) {
            fileData = new byte[dateLenHigh * HIGH_BLOCK_SIZE];
        } else {
            if (dateLenLow < 0) {
                fileData = new byte[dateLenLow + HIGH_BLOCK_SIZE];
            } else {
                fileData = new byte[dateLenLow];
            }
        }
        buf.getBytes(start + 25, fileData);
        return super.readStruct(buf, offset);
    }
}
