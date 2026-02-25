package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.Attr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;
import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@ToString(callSuper = true)
public class CommitReply extends RpcReply {
    public int status;
    int attrBefore = 0;
    public Attr attr = new Attr();
    public int tv_sec = (int) (NFS.bootNanoTime / ONE_SECOND_NANO);
    public int tv_usec = (int) (NFS.bootNanoTime % ONE_SECOND_NANO);

    public CommitReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, attrBefore);
        offset += 8;
        if (status == NFS3ERR_STALE) {
            buf.setInt(offset, 0);
            return offset + 4 - start;
        }
        offset += attr.writeStruct(buf, offset);

        buf.setInt(offset, tv_sec);
        buf.setInt(offset + 4, tv_usec);


        return offset + 8 - start;
    }
}
