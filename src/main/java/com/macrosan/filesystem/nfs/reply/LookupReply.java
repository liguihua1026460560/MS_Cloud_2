package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class LookupReply extends RpcReply {

    public int status;


    public FH2 fh = new FH2();

    public int attrBefore = 1;

    public FAttr3 stat = new FAttr3();

    public int attrBefore2 = 1;

    public FAttr3 dirStat = new FAttr3();


    public LookupReply(SunRpcHeader header) {


        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);

        buf.setInt(offset, status);
        offset += 4;
        if (status == NFS3ERR_STALE) {
            buf.setInt(offset, 0);
            return offset + 4 - start;
        }
        if (status == 0) {
            offset += fh.writeStruct(buf, offset);

            buf.setInt(offset, attrBefore);

            offset += 4;

            offset += stat.writeStruct(buf, offset);
        }
        buf.setInt(offset, attrBefore2);

        offset += 4;
        if (attrBefore2 != 0) {
            offset += dirStat.writeStruct(buf, offset);
        }


        return offset - start;

    }

    public static FH2 mapToFH2(Inode inode, int fsid) {
        FH2 fh2 = new FH2();
        if (inode.getNodeId() == 1) {
            fh2.version = 1;
            fh2.fhSize = 8;
            fh2.authType = 0;
            fh2.fsidType = (byte) FH2.NFSD_FSID.FSID_NUM.value;
            fh2.fileidType = 0;
            fh2.fsid = (int) fsid;
            fh2.ino = 0;
            fh2.generation = 0;
        } else {
            fh2.version = 1;
            fh2.fhSize = 20;
            fh2.authType = 0;
            fh2.fsidType = (byte) FH2.NFSD_FSID.FSID_NUM.value;
            fh2.fileidType = (byte) 0x81;
            fh2.fsid = (int) fsid;
            fh2.ino = inode.getNodeId();
            fh2.generation = 0;
        }
        return fh2;
    }

    public static FH2 mapToFH2(Inode inode, int fsid, FH2 dirFh) {
        FH2 fh2 = new FH2();
        fh2.authType = dirFh.authType;
        fh2.fileidType = dirFh.fileidType;
        fh2.version = dirFh.version;
        fh2.fsidType = dirFh.fsidType;
        if (inode.getNodeId() == 1) {
            fh2.fhSize = 8;
            fh2.fsid = (int) fsid;
            fh2.ino = 1;
            fh2.generation = 0;
        } else {
            fh2.fhSize = 20;
            fh2.fsid = (int) fsid;
            fh2.ino = inode.getNodeId();
            fh2.generation = 0;
        }
        return fh2;

    }
}
