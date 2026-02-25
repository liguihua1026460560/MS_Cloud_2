package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.ChangeInfo;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReNameV4Reply extends CompoundReply {
    public ChangeInfo sourceChangeInfo = new ChangeInfo();
    public ChangeInfo targetChangeInfo = new ChangeInfo();


    public ReNameV4Reply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        offset += 8;
        offset += sourceChangeInfo.writeStruct(buf, offset);
        offset += targetChangeInfo.writeStruct(buf, offset);
        return offset - start;
    }

}
