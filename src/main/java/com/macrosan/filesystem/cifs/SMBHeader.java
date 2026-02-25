package com.macrosan.filesystem.cifs;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;

import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_BUFFER_OVERFLOW;

@Data
public abstract class SMBHeader implements ReadStruct {
    abstract public int writeStruct(ByteBuf buf, int offset);

    public abstract int size();

    @Data
    public static class SMBReply {
        SMBHeader header;
        SMBBody body;

        public void setFileInfoBody(GetInfoReply body, int maxResponseSize) {
            if (body.getInfo().size() > maxResponseSize) {
                ((SMB2Header) header).status = STATUS_BUFFER_OVERFLOW;
            } else {
                this.body = body;
            }
        }
    }
}
