package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetClientIdCall extends CompoundCall {
    public byte[] verifier;
    public int idLen;
    public byte[] clientOwner;
    //callback
    //callback cb_program
    public int cbProgram;
    //callback netId
    public int netIdLen;
    public byte[] netId;
    //callback addr
    public int addrLen;
    public byte[] addr;
    public int callbackIdent;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        verifier = new byte[8];
        buf.getBytes(offset, verifier);
        offset += 8;
        idLen = buf.getInt(offset);
        offset += 4;
        clientOwner = new byte[idLen];
        buf.getBytes(offset, clientOwner);
        offset += (idLen + 3) / 4 * 4;
        cbProgram = buf.getInt(offset);
        offset += 4;
        netIdLen = buf.getInt(offset);
        offset += 4;
        netId = new byte[netIdLen];
        buf.getBytes(offset, netId);
        offset += (netIdLen + 3) / 4 * 4;
        addrLen = buf.getInt(offset);
        offset += 4;
        addr = new byte[addrLen];
        buf.getBytes(offset, addr);
        offset += (addrLen + 3) / 4 * 4;
        return offset - head;
    }


}
