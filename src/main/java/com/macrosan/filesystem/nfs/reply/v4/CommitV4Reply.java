package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@ToString
public class CommitV4Reply extends CompoundReply {
    public long verifier = 0;
    public int tv_sec = (int) (NFS.bootNanoTime / ONE_SECOND_NANO);
    public int tv_usec = (int) (NFS.bootNanoTime % ONE_SECOND_NANO);


    public CommitV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
//        buf.setLong(offset+8, verifier);
        buf.setInt(offset + 8, tv_sec);
        buf.setInt(offset + 12, tv_usec);
        return 16;
    }
}
