package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
public class TestStateIdCall extends CompoundCall {
    public int num;
    public List<StateId> stateIds;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        num = buf.getInt(offset);
        stateIds = new ArrayList<>();
        offset += 4;
        for (int i = 0; i < num; i++) {
            StateId stateId = new StateId();
            stateId.seqId = ((buf.getInt(offset)));
            buf.getBytes(offset + 4, stateId.other);
            stateIds.add(stateId);
            offset += 16;
        }
        return offset - start;
    }
}

