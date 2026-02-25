package com.macrosan.filesystem.cifs.rpc.witness.pdu;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;

public class Pcontext implements ReadStruct {
    public short command;
    public short length;
    public byte[] abstractSyntax = new byte[16];
    public int aVersion;
    public byte[] transferSyntax = new byte[16];
    public int tVersion;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        command = buf.getShortLE(offset);
        length = buf.getShortLE(offset + 2);
        buf.getBytes(offset + 4, abstractSyntax);
        aVersion = buf.getByte(offset + 20);
        buf.getBytes(offset + 24, transferSyntax);
        tVersion = buf.getIntLE(offset + 40);
        return 44;
    }
}
