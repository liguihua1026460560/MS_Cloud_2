package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3_OK;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_CLID_INUSE;

@ToString(callSuper = true)
public class SetClientIdReply extends CompoundReply {
    public long clientId;
    public byte[] verifier;
    public byte[] netId;
    public byte[] addr;

    public SetClientIdReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        switch (status) {
            case NFS3_OK:
                buf.setLong(offset + 8, clientId);
                buf.setBytes(offset + 16, verifier);
                return 24;
            case NFS4ERR_CLID_INUSE:
                buf.setInt(offset + 8, netId.length);
                buf.setBytes(offset + 12, netId);
                offset += (netId.length + 3) / 4 * 4 + 12;
                buf.setInt(offset, addr.length);
                buf.setBytes(offset + 4, addr);
                offset += (addr.length + 3) / 4 * 4 + 4;
                return offset;
        }
        return -1;
    }
}
