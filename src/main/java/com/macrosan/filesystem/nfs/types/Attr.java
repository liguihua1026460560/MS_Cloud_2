package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NFSACLType.NFSACL_GROUP_OBJ;
import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.nfs.types.Attr.NFS3_FS_Type.*;
import static com.macrosan.message.jsonmsg.Inode.getAndSetNFSMask;


@ToString
public class Attr {
    public enum NFS3_FS_Type {
        NF3NON(0),
        NF3REG(1),
        NF3DIR(2),
        NF3BLK(3),
        NF3CHR(4),
        NF3LNK(5),
        NF3SOCK(6),
        NF3FIFO(7),
        NF3BAD(8);

        int value;

        NFS3_FS_Type(int value) {
            this.value = value;
        }
    }

    int mode;
    int linkN;
    int uid;
    int gid;
    long size;
    long blocks;
    long rdev;
    int majorDev;
    int minorDev;
    long fsid;
    long ino;

    int atime;
    int atimeNano;
    int mtime;
    int mtimeNano;
    int ctime;
    int ctimeNano;

    public static int SIZE = 88;

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, 1);

        switch (mode & S_IFMT) {
            case FsConstants.S_IFDIR:
                buf.setInt(offset + 4, NF3DIR.value);
                break;
            case FsConstants.S_IFLNK:
                buf.setInt(offset + 4, NF3LNK.value);
                break;
            case FsConstants.S_IFREG:
                buf.setInt(offset + 4, NF3REG.value);
                break;
            case FsConstants.S_IFBLK:
                buf.setInt(offset + 4, NF3BLK.value);
                break;
            case FsConstants.S_IFFIFO:
                buf.setInt(offset + 4, NF3FIFO.value);
                break;
            case FsConstants.S_IFCHR:
                buf.setInt(offset + 4, NF3CHR.value);
                break;
            default:
                buf.setInt(offset + 4, NF3BAD.value);
                break;
        }

        buf.setInt(offset + 8, mode & 4095);

        buf.setInt(offset + 12, linkN);
        buf.setInt(offset + 16, uid);
        buf.setInt(offset + 20, gid);

        buf.setLong(offset + 24, size);
        buf.setLong(offset + 32, blocks);
//        buf.setLong(offset + 40, rdev);
        buf.setInt(offset + 40, majorDev);
        buf.setInt(offset + 44, minorDev);
        buf.setLong(offset + 48, fsid);
        buf.setLong(offset + 56, ino);
        buf.setInt(offset + 64, atime);
        buf.setInt(offset + 68, atimeNano);
        buf.setInt(offset + 72, mtime);
        buf.setInt(offset + 76, mtimeNano);
        buf.setInt(offset + 80, ctime);
        buf.setInt(offset + 84, ctimeNano);

        return SIZE;
    }

    public static Attr mapToAttr(Inode inode, long fsid) {
        Attr attr = new Attr();
        if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
            if (CIFSACL.isExistNfsACE(inode)) {
                int mask = getAndSetNFSMask(inode.getACEs(), 0, false).var1;
                int newMode = NFSACL.refreshMode(inode.getMode(), NFSACL_GROUP_OBJ) | NFSACL.parsePermissionToMode(mask, NFSACL_GROUP_OBJ);
                attr.mode = newMode;
            } else {
                attr.mode = inode.getMode();
            }
        } else {
            attr.mode = inode.getMode();
        }
        attr.linkN = inode.getLinkN();
        attr.uid = inode.getUid();
        attr.gid = inode.getGid();
        attr.size = inode.getSize();
        if ((inode.getMode() & S_IFMT) == S_IFLNK) {
            attr.blocks = 0;
        } else {
            attr.blocks = inode.getSize() % 4096 == 0 ? inode.getSize() : (inode.getSize() / 4096 + 1) * 4096;
        }
//        attr.rdev = 0;
        attr.majorDev = inode.getMajorDev();
        attr.minorDev = inode.getMinorDev();
        attr.fsid = fsid;
        attr.ino = inode.getNodeId();
        attr.atime = (int) inode.getAtime();
        attr.atimeNano = inode.getAtimensec();

        attr.mtime = (int) inode.getMtime();
        attr.mtimeNano = inode.getMtimensec();
        attr.ctime = (int) inode.getCtime();
        attr.ctimeNano = inode.getCtimensec();

        return attr;
    }
}
