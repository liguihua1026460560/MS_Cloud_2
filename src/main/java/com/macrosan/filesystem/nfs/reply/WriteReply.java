package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.Attr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;
import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@ToString(callSuper = true)
public class WriteReply extends RpcReply {
    public WriteReply(SunRpcHeader header) {
        super(header);
    }

    public int status;
    int attrBefore = 0;
    public Attr attr = new Attr();
    public int count;
    public int sync;
    //nfsd返回nfsd的启动时间，nfs3没有处理.
    public int tv_sec = (int) (NFS.bootNanoTime / ONE_SECOND_NANO);
    public int tv_usec = (int) (NFS.bootNanoTime % ONE_SECOND_NANO);

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
        if (status == NFS3ERR_DQUOT) {
            return offset - start;
        }

        buf.setInt(offset, count);
        buf.setInt(offset + 4, sync);
        buf.setInt(offset + 8, tv_sec);
        buf.setInt(offset + 12, tv_usec);

        return offset + 16 - start;
    }
}
