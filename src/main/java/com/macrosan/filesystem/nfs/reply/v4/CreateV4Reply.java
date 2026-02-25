package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.ChangeInfo;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class CreateV4Reply extends CompoundReply {
    public ChangeInfo changeInfo = new ChangeInfo();
    public int[] mask;


    public CreateV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        offset += 8;
        offset += changeInfo.writeStruct(buf, offset);
        buf.setInt(offset, mask.length);
        offset += 4;
        for (int i : mask) {
            buf.setInt(offset, i);
            offset += 4;
        }
        return offset - start;
    }
}
