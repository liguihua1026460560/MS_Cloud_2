package com.macrosan.filesystem.nfs.reply.v4;

import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@ToString(callSuper = true)
public class CopyReply extends CompoundReply {
    //    public int stateIdSeqId;
//    public long stateIdOther;
    public long count;
    public int committed;
    public long verifier;
    public int tv_sec = (int) (NFS.bootNanoTime / ONE_SECOND_NANO);
    public int tv_usec = (int) (NFS.bootNanoTime % ONE_SECOND_NANO);
    public boolean consecutive;
    public boolean synchronous;

    public CopyReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setLong(offset + 8, count);
        buf.setInt(offset + 16, committed);
        buf.setInt(offset + 20, tv_sec);
        buf.setInt(offset + 24, tv_usec);
        buf.setBoolean(offset + 28, consecutive);
        buf.setBoolean(offset + 32, synchronous);
        return 36;
    }
}