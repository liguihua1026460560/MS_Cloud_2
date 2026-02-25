package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileFullEAInfo extends GetInfoReply.Info {
    public int nextEntryOffset;
    //0x0 or 0x00000080
    public byte flags;
    public byte eaNameLen;
    public short eaValueLen;
    public byte[] eaName = new byte[0];

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, nextEntryOffset);
        buf.setByte(offset + 4, flags);
        buf.setByte(offset + 5, eaName.length);
        buf.setLongLE(offset + 6, eaValueLen);
        buf.setBytes(offset + 8, eaName);
        return 8 + eaName.length;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 8 + eaName.length;
    }


}
