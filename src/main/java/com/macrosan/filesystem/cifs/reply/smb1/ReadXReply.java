package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString(exclude = "data")
@Slf4j
public class ReadXReply extends SMB1Body {
    public byte xOpcode;
    public byte reserved;
    public short xOffset;
    public long available;
    public short dataCompactionMode;
    public short reserved1;
    public short dataLowLen;
    public short dataOffset;
    public int dataHighLen;
    public byte[] reserved2 = new byte[6];
    public byte[] data;

    public static final int SHORT_MAX_VALUE = 32767;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 12;
        int len = 0;
        if (dataHighLen > 0) {
            byteCount = 1;
            len = super.writeStruct(buf, offset);
        } else {
            if (dataLowLen > Short.MIN_VALUE) {
                byteCount = dataLowLen;
                len = 3 + (byteCount) + wordCount * 2;
            } else {
                byteCount = Short.MIN_VALUE;
                len = SHORT_MAX_VALUE + 1 + wordCount * 2 + 3;
            }
            super.writeStruct(buf, offset);
        }


        dataOffset = (short) 27;
        int start = offset + 1;
        buf.setByte(start, xOpcode);
        buf.setByte(start + 1, reserved);
        buf.setShortLE(start + 2, xOffset);
        if (available > Short.MAX_VALUE) {
            buf.setByte(start + 4, 0xff);
            buf.setByte(start + 5, 0xff);
        } else {
            buf.setShort(start + 4, (short) available);
        }
        buf.setShortLE(start + 6, dataCompactionMode);
        buf.setShortLE(start + 8, reserved1);
        buf.setShortLE(start + 10, dataLowLen);
        buf.setShortLE(start + 12, dataOffset);
        buf.setIntLE(start + 14, dataHighLen);
        start += 6;//reserved2;
        if (dataHighLen > 0) {
            start += 1;//padding
        }
        buf.setBytes(start + 20, data);
        return len;
    }
}
