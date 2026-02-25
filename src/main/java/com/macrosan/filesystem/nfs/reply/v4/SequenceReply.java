package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SequenceReply extends CompoundReply {
    //16 byte
    public byte[] sessionId = new byte[0];
    public int seqId;
    public int slotId;
    public int highSlotId;
    public int targetHighSlotId;
    public int statusFlags;


    public SequenceReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setBytes(offset + 8, sessionId);
        buf.setInt(offset + 24, seqId);
        buf.setInt(offset + 28, slotId);
        buf.setInt(offset + 32, highSlotId);
        buf.setInt(offset + 36, targetHighSlotId);
        buf.setInt(offset + 40, statusFlags);
        return 44;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        status = buf.getInt(offset);
        if (status != 0){
            return 4;
        }
        offset += 4;
        buf.getBytes(offset, sessionId);
        seqId = buf.getInt(offset + 16);
        slotId = buf.getInt(offset + 20);
        highSlotId = buf.getInt(offset + 24);
        targetHighSlotId = buf.getInt(offset + 28);
        return 36;
    }
}
