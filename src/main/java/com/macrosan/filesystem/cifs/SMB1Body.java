package com.macrosan.filesystem.cifs;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class SMB1Body extends SMBBody {
    public int wordCount;
    public int byteCount;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        wordCount = buf.getByte(offset) & 0xff;
        byteCount = buf.getShortLE(offset + 1 + 2 * wordCount) & 0xffff;
        //3是byteCount本身占2字节wordCount占1字节。
        return 3 + (byteCount) + wordCount * 2;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setByte(offset, (byte) (wordCount & 0xff));
        buf.setShortLE(offset + 1 + 2 * wordCount, (short) (byteCount & 0xffff));
        //3是byteCount本身占2字节wordCount占1字节。
        return 3 + (byteCount) + wordCount * 2;
    }
}
