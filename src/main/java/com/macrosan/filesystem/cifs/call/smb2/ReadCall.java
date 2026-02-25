package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReadCall extends SMB2Body {

    public byte padding;
    public byte flags;
    public int length;
    public long readOffset;
    public SMB2FileId fileId = new SMB2FileId();
    public int minCount;
    public int channel;
    public int remainingBytes;
    public short readChannelInfoOffset;
    public short readChannelInfoLength;
    public byte[] channelInfoData;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.readStruct(buf, offset);
        flags = buf.getByte(offset + 1);
        length = buf.getIntLE(offset + 2);
        offset += 6;
        readOffset = buf.getLongLE(offset);
        offset += 8;

        offset += fileId.readStruct(buf, offset);
        minCount = buf.getIntLE(offset);
        offset += 4;
        channel = buf.getIntLE(offset);
        offset += 4;
        remainingBytes = buf.getIntLE(offset);
        offset += 4;
        readChannelInfoOffset = buf.getShortLE(offset);
        offset += 2;
        readChannelInfoLength = buf.getShortLE(offset);
        offset += 2;
        return offset - start;
    }
}
