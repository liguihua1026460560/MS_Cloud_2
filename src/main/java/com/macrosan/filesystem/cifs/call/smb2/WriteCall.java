package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(exclude = "data")
//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e7046961-3318-4350-be2a-a8d69bb59ce8
public class WriteCall extends SMB2Body {
    public short dataOffset;
    public int dataLength;
    public long writeOffset;
    public SMB2FileId fileId;
    public int channel;
    public int remainingBytes;
    public short writeChannelInfoOffset;
    public short writeChannelInfoLength;
    public int writeFlags;
    public byte[] data;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.readStruct(buf, offset);
        dataOffset = buf.getShortLE(offset);
        offset += 2;
        dataLength = buf.getIntLE(offset);
        offset += 4;
        writeOffset = buf.getLongLE(offset);
        offset += 8;
        fileId = new SMB2FileId();
        offset += fileId.readStruct(buf, offset);
        channel = buf.getIntLE(offset);
        offset += 4;
        remainingBytes = buf.getIntLE(offset);
        offset += 4;
        writeChannelInfoOffset = buf.getShortLE(offset);
        offset += 2;

        writeChannelInfoLength = buf.getShortLE(offset);
        offset += 2;
        writeFlags = buf.getShortLE(offset);
        offset += 4;
        data = new byte[dataLength];
        buf.getBytes(offset, data);
        offset += dataLength;
        return offset - start;
    }

    public static final int SMB2_WRITEFLAG_WRITE_THROUGH = 0x1;
    public static final int SMB2_WRITEFLAG_WRITE_UNBUFFERED = 0x2;
}
