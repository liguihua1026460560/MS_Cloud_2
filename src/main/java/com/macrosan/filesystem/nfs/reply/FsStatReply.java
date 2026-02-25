package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.math.BigInteger;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class FsStatReply extends RpcReply {
    public int status;
    //0
    public final int noAttr = 0;

    public long totalBytes = 0xffffffffff000000L;

    public long freeBytes = totalBytes;

    public long availFreeBytes = totalBytes;

    public long totalFileSlots = 0;

    public long freeSlots = 0;

    public long availFreeSlots = 0;

    public int invarsec = 0;

    public FsStatReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);

        buf.setInt(offset, status);
        buf.setInt(offset + 4, noAttr);
        if (status == NFS3ERR_STALE) {
            return offset + 8 - start;
        }
        buf.setLong(offset + 8, totalBytes);
        buf.setLong(offset + 16, freeBytes);
        buf.setLong(offset + 24, availFreeBytes);
        buf.setLong(offset + 32, totalFileSlots);
        buf.setLong(offset + 40, freeSlots);
        buf.setLong(offset + 48, availFreeSlots);
        buf.setInt(offset + 56, invarsec);

        return offset + 60 - start;
    }
}
