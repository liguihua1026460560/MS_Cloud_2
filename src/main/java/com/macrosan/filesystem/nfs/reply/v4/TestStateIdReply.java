package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString(callSuper = true)
public class TestStateIdReply extends CompoundReply {
    public int num;
    public List<Integer> stateIdStatus = new ArrayList<>();

    public TestStateIdReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, num);
        offset += 12;
        for (Integer status : stateIdStatus) {
            buf.setInt(offset, status);
            offset += 4;
        }
        return offset - start;
    }
}
