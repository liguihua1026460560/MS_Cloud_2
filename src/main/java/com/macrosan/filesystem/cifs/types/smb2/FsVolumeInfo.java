package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class FsVolumeInfo extends GetInfoReply.Info {
    long creationTime;
    public int serialNum;
    //    public int labelLen;
    public byte supportsObject;
    //reserve 1

    //bucket
    public byte[] label;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, creationTime);
        buf.setIntLE(offset + 8, serialNum);
        buf.setIntLE(offset + 12, label.length);
        buf.setByte(offset + 16, supportsObject);
        buf.setBytes(offset + 18, label);
        return 18 + label.length;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 18 + label.length;
    }
}
