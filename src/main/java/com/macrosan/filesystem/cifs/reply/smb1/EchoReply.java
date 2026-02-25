package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class EchoReply extends SMB1Body {
    public short seqNum;
    public byte echoData;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 1;
        byteCount = 1;

        buf.setShortLE(offset + 1, seqNum);
        int start = offset + 3 + wordCount * 2;
        buf.setByte(start, echoData);

        return super.writeStruct(buf, offset);
    }

}
