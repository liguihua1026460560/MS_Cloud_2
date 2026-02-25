package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

/**
 * FH Version new
 */
@ToString
public class FH2 implements ReadStruct {
    public int fhSize;
    //1
    public byte version;
    //0
    public byte authType;
    //NFSD_FSID
    public byte fsidType;
    //由NFS导出的文件系统决定
    //当前测试的fuse 文件系统为0x81
    //root 为0
    public byte fileidType;
    // /etc/exports 中的fsid
    public int fsid;
    //root的fh没有ino和generation
    //fuse的fuse_fh_to_dentry方法
    public long ino;
    public int generation;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        fhSize = buf.getInt(offset);
        version = buf.getByte(offset + 4);
        if (version != 1) {
            return -1;
        }

        authType = buf.getByte(offset + 5);
        fsidType = buf.getByte(offset + 6);

        if (NFSD_FSID.FSID_NUM.value != fsidType) {
            return -1;
        }

        fileidType = buf.getByte(offset + 7);

        offset += 8;

        //root
        if (fileidType == 0 && fhSize == 8) {
            fsid = buf.getIntLE(offset);
            ino = 1L;
            offset += 4;
        } else if ((fileidType & 0xff) == 0x81 && fhSize == 20) {
            fsid = buf.getIntLE(offset);
            ino = buf.getIntLE(offset + 4);
            long tmp = buf.getIntLE(offset + 8);
            ino = (ino << 32) | (tmp & 0xffffffffL);
            generation = buf.getIntLE(offset + 12);
            offset += 16;
        } else {
            return -1;
        }


        return offset - start;
    }

    public static int NO_ROOT_HEADER = 0x01000181;
    public static int ROOT_HEADER = 0x01000100;

    public int writeStruct(ByteBuf buf, int offset) {
        //root
        if (fileidType == 0) {
            buf.setInt(offset, 8);
            buf.setInt(offset + 4, ROOT_HEADER);
            buf.setIntLE(offset + 8, fsid);
            return 12;
        } else {
            buf.setInt(offset, 20);
            buf.setInt(offset + 4, NO_ROOT_HEADER);
            buf.setIntLE(offset + 8, fsid);
            buf.setIntLE(offset + 12, (int) (ino >>> 32));
            buf.setIntLE(offset + 16, (int) (ino));
            buf.setIntLE(offset + 20, generation);
            return 24;
        }
    }

    public static FH2 mapToFH2(Inode inode, int fsid) {
        FH2 fh2 = new FH2();
        if (inode.getNodeId() == 1) {
            fh2.version = 1;
            fh2.fhSize = 8;
            fh2.authType = 0;
            fh2.fsidType = (byte) NFSD_FSID.FSID_NUM.value;
            fh2.fileidType = 0;
            fh2.fsid = (int) fsid;
            fh2.ino = 0;
            fh2.generation = 0;
        } else {
            fh2.version = 1;
            fh2.fhSize = 20;
            fh2.authType = 0;
            fh2.fsidType = (byte) NFSD_FSID.FSID_NUM.value;
            fh2.fileidType = (byte) 0x81;
            fh2.fsid = (int) fsid;
            fh2.ino = inode.getNodeId();
            fh2.generation = 0;
        }
        return fh2;
    }

    /**
     * fuse导出的NFS没有dev和uuid，都是FSID_NUM类型
     */
    public enum NFSD_FSID {
        FSID_DEV(0),
        FSID_NUM(1),
        FSID_MAJOR_MINOR(2),
        FSID_ENCODE_DEV(3),
        FSID_UUID4_INUM(4),
        FSID_UUID8(5),
        FSID_UUID16(6),
        FSID_UUID16_INUM(7);

        public final int value;

        NFSD_FSID(int value) {
            this.value = value;
        }
    }
}
