package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

public class PathConfReply extends RpcReply {

    public int status;

    public int attrBefore = 0;

    public int linkMax;

    public int nameMax;
    public int noTrunc;
    public int chownRestricted;
    public int caseInsensitive;
    public int casePreserving;

    public PathConfReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        offset += 4;
        buf.setInt(offset, attrBefore);
        if (status == NFS3ERR_STALE) {
            return offset + 4 - start;
        }
        buf.setInt(offset + 4, linkMax);
        buf.setInt(offset + 8, nameMax);
        buf.setInt(offset + 12, noTrunc);
        buf.setInt(offset + 16, chownRestricted);
        buf.setInt(offset + 20, caseInsensitive);
        buf.setInt(offset + 24, casePreserving);

        return offset + 28 - start;
    }
}
