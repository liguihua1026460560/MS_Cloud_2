package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/3b1b3598-a898-44ca-bfac-2dcae065247f
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GetInfoReply extends SMB2Body {
    Info info;

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 9;
        super.writeStruct(buf, offset);
        int size = 0;
        if (info != null) {
            size = info.size();
            info.writeStruct(buf, offset + 8);
        }
        buf.setShortLE(offset + 2, 72);//64+8
        buf.setIntLE(offset + 4, size);

        return size + 8;
    }

    public abstract static class Info {
        public abstract int writeStruct(ByteBuf buf, int offset);

        public abstract int readStruct(ByteBuf buf, int offset);

        public abstract int size();
    }
}
