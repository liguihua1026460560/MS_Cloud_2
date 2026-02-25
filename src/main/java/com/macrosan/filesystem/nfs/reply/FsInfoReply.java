package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class FsInfoReply extends RpcReply {
    //NFS_MAX_FILE_IO_SIZE 1048576
    //NFS_MAX_READDIR_PAGES 8

    public FsInfoReply(SunRpcHeader header) {
        super(header);
    }

    public int status;
    //0
    public final int noAttr = 0;
    public int rtMax = 1048576;
    public int rtPref = 1048576;
    public int rtMult = 4096;
    public int wtMax = 1048576;
    public int wtPref = 1048576;
    public int wtMult = 4096;
    //每次readdir的buf大小，nfs客户端NFS_MAX_READDIR_PAGES为8，所以最大是32KB
    public int dtPref = 1048576;

    public long maxFileSize = Long.MAX_VALUE;
    public long time = 1L << 32;
    public int properties = 0; //ignore

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, 0);
        if (status == NFS3ERR_STALE) {
            return offset + 8 - start;
        }
        buf.setInt(offset + 8, rtMax);
        buf.setInt(offset + 12, rtPref);
        buf.setInt(offset + 16, rtMult);
        buf.setInt(offset + 20, wtMax);
        buf.setInt(offset + 24, wtPref);
        buf.setInt(offset + 28, wtMult);
        buf.setInt(offset + 32, dtPref);

        buf.setLong(offset + 36, maxFileSize);
        buf.setLong(offset + 44, time);
        buf.setInt(offset + 52, properties);

        offset += 56;
        return offset - start;
    }
}
