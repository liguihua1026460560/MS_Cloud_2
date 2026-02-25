package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@ToString
public class WriteV4Reply extends CompoundReply {
    public int count;
    public int committed;
    public long verifier = 0;
    public int tv_sec = (int) (NFS.bootNanoTime / ONE_SECOND_NANO);
    public int tv_usec = (int) (NFS.bootNanoTime % ONE_SECOND_NANO);

    public WriteV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, count);
        buf.setInt(offset + 12, committed);
//        buf.setLong(offset + 16, verifier);
        buf.setInt(offset + 16, tv_sec);
        buf.setInt(offset + 20, tv_usec);
        offset += 24;
        return offset - start;
    }
}
