package com.macrosan.filesystem.cifs.rpc.witness.pdu;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;

public class AuthInfo implements ReadStruct {
    public byte type;
    public byte level;
    public byte len;
    public byte reserved;
    public int contextID;

    public int versionNum;
    public long checkSum;
    public int seqNum;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        type = buf.getByte(offset);
        level = buf.getByte(offset + 1);
        len = buf.getByte(offset + 2);
        reserved = buf.getByte(offset + 3);
        contextID = buf.getIntLE(offset + 4);

        versionNum = buf.getIntLE(offset + 8);
        checkSum = buf.getLongLE(offset + 12);
        seqNum = buf.getInt(offset + 20);
        return 24;
    }
}
