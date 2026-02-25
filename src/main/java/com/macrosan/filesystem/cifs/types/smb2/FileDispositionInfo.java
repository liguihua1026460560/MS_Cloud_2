package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class FileDispositionInfo extends GetInfoReply.Info{

    public byte needDelete;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        needDelete = buf.getByte(offset);
        return 1;
    }

    @Override
    public int size() {
        return 1;
    }
}
