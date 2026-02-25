package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class GetAttrReply extends RpcReply {
    public int status;

    public FAttr3 stat = new FAttr3();
    public GetAttrReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        offset += 4;
        if (status == NFS3ERR_STALE) {
            return offset - start;
        }
        offset += stat.writeStruct(buf, offset);

        return offset - start;
    }
}
