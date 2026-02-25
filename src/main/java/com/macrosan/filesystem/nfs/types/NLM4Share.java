package com.macrosan.filesystem.nfs.types;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class NLM4Share {
    public byte[] clientName;
    public FH2 fh2 = new FH2();
    public byte[] owner;
    public int mode;
    public int access;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int clientNameLen = buf.getInt(offset);
        offset += 4;
        clientName = new byte[clientNameLen];
        buf.getBytes(offset, clientName);
        offset += ((clientNameLen + 3) / 4) * 4;
        offset += fh2.readStruct(buf, offset);
        int ownerLen = buf.getInt(offset);
        offset += 4;
        owner = new byte[ownerLen];
        buf.getBytes(offset, owner);
        offset += ((ownerLen + 3) / 4) * 4;
        mode = buf.getInt(offset);
        offset += 4;
        access = buf.getInt(offset);
        offset += 4;
        return offset - start;
    }
}
