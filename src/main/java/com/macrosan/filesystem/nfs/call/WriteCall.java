package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(exclude = "bytes")
public class WriteCall implements ReadStruct {
    public FH2 fh = new FH2();
    public long writeOffset = 0;
    public int count;
    public int sync;
    public int dataLen;
    public byte[] bytes;

    public int readStruct(ByteBuf buf, int offset) {
        int fhRead = fh.readStruct(buf, offset);
        if (fhRead < 0) {
            return -1;
        }

        offset += fhRead;

        writeOffset = buf.getLong(offset);
        count = buf.getInt(offset + 8);
        sync = buf.getInt(offset + 12);
        dataLen = buf.getInt(offset + 16);
        if (count != dataLen) {
            return -1;
        }

        bytes = new byte[dataLen];
        buf.getBytes(offset + 20, bytes);

        return fhRead + 20 + bytes.length;
    }
}
