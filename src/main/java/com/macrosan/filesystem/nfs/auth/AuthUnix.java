package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@Data
@EqualsAndHashCode(callSuper = false)
public class AuthUnix extends Auth implements ReadStruct {
    int stamp;
    int nameSize;
    byte[] name;
    int uid;
    int gid;
    int gidN;
    int[] gids;
    long padding;

    @Override
    public int auth(RpcCallHeader header, ByteBuf msg, int offset) {
        return 0;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        this.stamp = buf.getInt(offset);
        this.nameSize = buf.getInt(offset + 4);
        this.name = new byte[this.nameSize];
        offset += 8;
        buf.getBytes(offset, this.name);
        int nameLen = (this.nameSize + 3) / 4 * 4;
        offset += nameLen;
        this.uid = buf.getInt(offset);
        this.gid = buf.getInt(offset + 4);
        this.gidN = buf.getInt(offset + 8);
        offset += 12;
        this.gids = new int[this.gidN];
        for (int i = 0; i < this.gidN; i++) {
            this.gids[i] = buf.getInt(offset);
            offset += 4;
        }
        this.padding = buf.getLong(offset);
        offset += 8;
        return offset - start;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, this.stamp);
        offset += 4;
        buf.setInt(offset, this.nameSize);
        offset += 4;
        buf.setBytes(offset, this.name);
        offset += (this.nameSize + 3) / 4 * 4;
        buf.setInt(offset, this.uid);
        offset += 4;
        buf.setInt(offset, this.gid);
        offset += 4;
        buf.setInt(offset, this.gidN);
        offset += 4;
        for (int i = 0; i < this.gidN; i++) {
            buf.setInt(offset, this.gids[i]);
            offset += 4;
        }
        buf.setLong(offset, this.padding);
        offset += 8;
        return offset - start;
    }

    public int length() {
        return 20 + ((this.name.length + 3) / 4 * 4) + this.gidN * 4;
    }
}
