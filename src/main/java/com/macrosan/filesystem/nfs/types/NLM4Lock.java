package com.macrosan.filesystem.nfs.types;

import io.netty.buffer.ByteBuf;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class NLM4Lock {
    public byte[] clientName;
    public FH2 fh2 = new FH2();
    public byte[] owner;
    public int svid;
    public long offset;
    public long len;

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
        svid = buf.getInt(offset);
        offset += 4;
        this.offset = buf.getLong(offset);
        offset += 8;
        len = buf.getLong(offset);
        offset += 8;
        return offset - start;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        int clientNameLen = clientName.length;
        buf.setInt(offset, clientNameLen);
        offset += 4;
        buf.setBytes(offset, clientName);
        offset += ((clientNameLen + 3) / 4) * 4;
        offset += fh2.writeStruct(buf, offset);
        int ownerLen = owner.length;
        buf.setInt(offset, ownerLen);
        offset += 4;
        buf.setBytes(offset, owner);
        offset += ((ownerLen + 3) / 4) * 4;
        buf.setInt(offset, svid);
        offset += 4;
        buf.setLong(offset, this.offset);
        offset += 8;
        buf.setLong(offset, len);
        offset += 8;
        return offset - start;
    }
}
