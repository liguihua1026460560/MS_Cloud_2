package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class ReNameCall extends SMB1Body {
    public short searchAttributes;
    //1
    public byte bufferFormat1;
    //
    public byte[] oldFileName;
    public byte bufferFormat2;
    public byte[] newFileName;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        searchAttributes = buf.getShortLE(offset + 1);
        int start = offset + 3 + wordCount * 2;
        //1
        bufferFormat1 = buf.getByte(start);
        int oldLen = 0, newLen = 0;
        int index = 0;
        //oldLen
        for (int i = 0; i < byteCount - 1; i++) {
            byte aByte = buf.getByte(start + i + 1);
            index = '\0' == aByte ? ++index : 0;
            if (index == 3) {
                oldLen = i + 1;
                break;
            }
        }
        oldFileName = new byte[oldLen];
        buf.getBytes(start + 1, oldFileName);
        //1
        bufferFormat2 = buf.getByte(start + 1 + oldLen);
        //1 \0
        index = 0;
        //newLen
        for (int i = 0; i < byteCount - oldLen - 3; i++) {
            byte aByte = buf.getByte(start + oldLen + i + 3);
            index = '\0' == aByte ? ++index : 0;
            if (index == 3) {
                newLen = i + 1;
                break;
            }
        }
        newFileName = new byte[newLen];
        buf.getBytes(start + oldLen + 3, newFileName);
        return size;
    }
}
