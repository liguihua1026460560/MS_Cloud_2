package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.DirEnt;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class ReadDirReply extends RpcReply {
    public int status;

    public int attrBefore = 1;
    public FAttr3 attr = new FAttr3();

    public long verifier = 0;

    public int follows;
    public List<DirEnt> entryList = new LinkedList<>();

    public int eof = 0;


    public ReadDirReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, attrBefore);

        offset += 8;
        if (status == NFS3ERR_STALE) {
            return offset - start;
        }
        if (attrBefore != 0) {
            offset += attr.writeStruct(buf, offset);
        }

        buf.setLong(offset, verifier);
        offset += 8;
        buf.setInt(offset, follows);
        offset += 4;
        for (DirEnt entry : entryList) {
            offset += entry.writeStruct(buf, offset);
        }
        buf.setInt(offset, eof);
        return offset + 4 - start;
    }

    public int size() {
        int entrySize = entryList.stream().mapToInt(e -> e.entSize()).sum();
        return FAttr3.SIZE + 24 + RpcReply.SIZE + entrySize;
    }


}
