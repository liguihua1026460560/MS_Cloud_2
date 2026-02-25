package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.Attr;
import com.macrosan.filesystem.nfs.types.Entry;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class ReadDirPlusReply extends RpcReply {
    public ReadDirPlusReply(SunRpcHeader header) {
        super(header);
    }

    public int status;
    public Attr dir = new Attr();
    public long verf;
    public List<Entry> entryList = new LinkedList<>();
    public int eof;


    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        offset += 4;
        if (status == NFS3ERR_STALE) {
            buf.setInt(offset, 0);
            return offset + 4 - start;
        }
        offset += dir.writeStruct(buf, offset);
        buf.setLong(offset, verf);
        offset += 8;

        for (Entry e : entryList) {
            offset += e.writeStruct(buf, offset);
        }

        eof = entryList.isEmpty() ? 1 : 0;
        buf.setInt(offset, 0);
        buf.setInt(offset + 4, eof);
        return offset + 8 - start;
    }

    public int size() {
        int entrySize = entryList.stream().mapToInt(e -> e.size()).sum();
        return Attr.SIZE + 20 + RpcReply.SIZE + entrySize;
    }

}
