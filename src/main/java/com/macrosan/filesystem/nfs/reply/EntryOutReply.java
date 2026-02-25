package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class EntryOutReply extends RpcReply {

    public int status;
    public int objFhFollows = 1;
    public FH2 fh = new FH2();
    public int attrFollow = 1;
    public FAttr3 attr = new FAttr3();
    public int before = 0;
    public int after = 0;


    public EntryOutReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);

        buf.setInt(offset, status);
        buf.setInt(offset + 4, objFhFollows);
        offset += 8;
        if (objFhFollows != 0) {
            offset += fh.writeStruct(buf, offset);
        }
        buf.setInt(offset, attrFollow);
        offset += 4;
        if (attrFollow != 0) {
            offset += attr.writeStruct(buf, offset);
        }
        if (status == NFS3ERR_STALE) {
            return offset - start;
        }
        buf.setInt(offset, before);
        buf.setInt(offset + 4, after);
        offset += 8;


        return offset - start;
    }
}
