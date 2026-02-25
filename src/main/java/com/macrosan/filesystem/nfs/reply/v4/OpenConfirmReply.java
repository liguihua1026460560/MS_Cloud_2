package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class OpenConfirmReply extends CompoundReply {
    public StateId stateId = new StateId();

    public OpenConfirmReply(SunRpcHeader header) {
        super(header);
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, stateId.seqId);
        buf.setBytes(offset + 12, stateId.other);
        return 24;
    }
}
