package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.DirEntV4;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

@ToString
public class ReadDirV4Reply extends CompoundReply {
    public long verifier = 0;
    public List<DirEntV4> entryList = new LinkedList<>();
    public int follows = 0;
    public int eof = 0;


    public ReadDirV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setLong(offset + 8, verifier);
        offset += 16;
        for (DirEntV4 entry : entryList) {
            offset += entry.writeStruct(buf, offset);
        }
        buf.setInt(offset, follows);
        buf.setInt(offset + 4, eof);
        offset += 8;
        return offset - start;
    }

    public int size() {
        return 24 + RpcReply.SIZE + entryList.stream().mapToInt(DirEntV4::size).sum();
    }
}
