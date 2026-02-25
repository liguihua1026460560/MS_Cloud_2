package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadXCall extends SMB1Body {

    public byte xOpcode;
    public byte reserved;
    public short xOffset;
    public short fsid;
    public int readOffset;
    public int maxCountLow;
    public short minCount;
    public int maxCountHigh;
    public short remaining;
    public int highOffset;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + 1;
        xOpcode = buf.getByte(start);
        reserved = buf.getByte(start + 1);
        xOffset = buf.getShortLE(start + 2);
        fsid = buf.getShortLE(start + 4);
        readOffset = buf.getIntLE(start + 6);
        maxCountLow = buf.getShortLE(start + 10);
        if (maxCountLow < 0){
            maxCountLow = Short.MAX_VALUE + 1;
        }
        minCount = buf.getShortLE(start + 12);
        maxCountHigh = buf.getShortLE(start + 14);
        remaining = buf.getShortLE(start + 18);
        highOffset = buf.getIntLE(start + 20);
        return super.readStruct(buf, offset);
    }
}
