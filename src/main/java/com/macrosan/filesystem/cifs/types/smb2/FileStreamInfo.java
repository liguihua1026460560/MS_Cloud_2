package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FileStreamInfo extends GetInfoReply.Info {
    public int nextEntryOffset;
    //    public int streamNameLen;
    public long streamSize;
    public long allocationSize;
    public byte[] streamName;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, nextEntryOffset);
        buf.setIntLE(offset + 4, streamName.length);
        buf.setLongLE(offset + 8, streamSize);
        buf.setLongLE(offset + 16, allocationSize);
        buf.setBytes(offset + 24, streamName);
        return 24 + streamName.length;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 24 + streamName.length;
    }


}
