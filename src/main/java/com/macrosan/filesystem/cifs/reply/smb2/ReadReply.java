package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(exclude = "data")
public class ReadReply extends SMB2Body {

    public byte dataOffset;
    public byte reserved;
    public int dataLen;
    public int dataRemaining;
    public int flags;
    public byte[] data;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        structSize = 17;
        dataOffset = 80;
        offset += super.writeStruct(buf, offset);
        buf.setByte(offset, dataOffset);
        offset++;
        buf.setByte(offset, reserved);
        offset++;
        buf.setIntLE(offset, dataLen);
        offset += 4;
        buf.setIntLE(offset, dataRemaining);
        offset += 4;
        buf.setIntLE(offset, flags);
        offset += 4;
        buf.setBytes(offset, data);
        offset += data.length;
        return offset - start;
    }
}
