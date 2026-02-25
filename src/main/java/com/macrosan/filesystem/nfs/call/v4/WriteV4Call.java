package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString(exclude = "contents")
public class WriteV4Call extends CompoundCall {
    public StateId stateId = new StateId();
    public long offset;
    public int stable;
    public int writeLen;
    public byte[] contents;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        stateId.readStruct(buf, offset);
        this.offset = buf.getLong(offset + 16);
        stable = buf.getInt(offset + 24);
        writeLen = buf.getInt(offset + 28);
        offset += 32;
        contents = new byte[writeLen];
        buf.getBytes(offset, contents);
        offset +=(writeLen + 3) / 4 * 4;

        return offset - start;
    }


    //write不同步
    public static final int UNSTABLE = 0;
    //同步数据，不同步元数据
    public static final int DATA_SYNC = 1;
    //同步数据和元数据
    public static final int FILE_SYNC = 2;
}
