package com.macrosan.filesystem.cifs.types.smb2;

import io.netty.buffer.ByteBuf;

public class QueryQuotaInfo {
    public byte returnSingle;
    public byte restartScan;
    public short reserved;
    public int sidListLen;
    public int startSidLen;
    public int startSidOffset;
    public int nextOffset;
    public int sidLength;
    public SID owerSid;

    public int readStruct(ByteBuf buf, int offset) {
        returnSingle = buf.getByte(offset);
        restartScan = buf.getByte(offset + 1);
        reserved = buf.getShortLE(offset + 2);
        sidListLen = buf.getIntLE(offset + 4);
        startSidLen = buf.getIntLE(offset + 8);
        startSidOffset = buf.getIntLE(offset + 12);
        if (sidListLen > 0) {
            nextOffset = buf.getIntLE(offset + 16);
            sidLength = buf.getByte(offset + 20);
            owerSid = new SID();
            return owerSid.readStruct(buf, offset + 24) + 24;
        } else {
            return 16;
        }
    }
}
