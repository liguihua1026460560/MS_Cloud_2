package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/25c8c3c9-58fc-4bb8-aa8f-0272dede84c5
 */
@ToString
public class NegprotCall extends SMB1Body {
    public String[] dialects;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        int start = offset + 3 + wordCount * 2;

        List<String> list = new LinkedList<>();
        while (start < size + offset) {
            byte bufferFormat = buf.getByte(start); //0x02
            start++;
            int end = start;
            while (true) {
                if (buf.getByte(end) != 0) {
                    end++;
                } else {
                    break;
                }
            }

            byte[] bytes = new byte[end - start];
            buf.getBytes(start, bytes);
            list.add(new String(bytes));
            start = end + 1;
        }

        dialects = list.toArray(new String[0]);
        return size;
    }
}
