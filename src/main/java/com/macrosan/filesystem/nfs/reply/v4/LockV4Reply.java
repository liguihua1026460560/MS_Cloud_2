package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_DENIED;
import static com.macrosan.filesystem.FsConstants.OK;

@ToString
public class LockV4Reply extends CompoundReply {
    public StateId stateId = new StateId();
    public int lockType;
    public long offset;
    public long length;
    public long clientId;
    public byte[] owner;


    public LockV4Reply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        switch (status) {
            case OK:
                stateId.writeStruct(buf, offset + 8);
                return 24;
            case NFS4ERR_DENIED:
                buf.setLong(offset + 8, this.offset);
                buf.setLong(offset + 16, length);
                buf.setInt(offset + 24, lockType);
                buf.setLong(offset + 28, clientId);
                buf.setInt(offset + 36, owner.length);
                buf.setBytes(offset + 40, owner);
                offset += 40 + (owner.length + 3) / 4 * 4;
                return offset - start;
            default:
                return -1;
        }


    }
}
