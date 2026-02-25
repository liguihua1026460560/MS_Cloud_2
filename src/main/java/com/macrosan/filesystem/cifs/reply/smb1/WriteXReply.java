package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class WriteXReply extends SMB1Body {

    public byte xOpcode;
    public byte xReserved;
    public short xOffset;
    public short dataLenLow;
    public short remaining;
    public short dataLenHigh;
    public short reserved;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 6;
        buf.setByte(offset + 1, xOpcode);
        buf.setByte(offset + 2, xReserved);
        buf.setShortLE(offset + 3, xOffset);
        buf.setShortLE(offset + 5, dataLenLow);
        buf.setShortLE(offset + 7, remaining);
        buf.setShortLE(offset + 9, dataLenHigh);
        buf.setShortLE(offset + 11, reserved);
        return super.writeStruct(buf, offset);
    }
}
