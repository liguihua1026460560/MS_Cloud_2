package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFSACL_GROUP_OBJ;
import static com.macrosan.message.jsonmsg.Inode.getAndSetNFSMask;

//比attr多了type
@ToString
public class FAttr3 {
    enum fType {
        NF_NON(0),
        NF_REG(1),
        NF_DIR(2),
        NF_BLK(3),
        NF_CHR(4),
        NF_LINK(5),
        NF_SOCK(6),
        NF_FIFO(7),
        NF_BAD(8);
        public int type;

        fType(int type) {
            this.type = type;
        }
    }

    int type;
    int mode;
    int linkN;
    int uid;
    int gid;
    public long size;
    long blocks;
    long rdev;
    int majorDev;
    int minorDev;
    long fsid;
    long ino;

    public int atime;
    public int atimeNano;
    public int mtime;
    public int mtimeNano;
    int ctime;
    int ctimeNano;

    public static int SIZE = 84;

    public int writeStruct(ByteBuf buf, int offset) {
//        buf.setInt(offset, 1);
        buf.setInt(offset, type);
//        offset += 4;

//        switch (mode & FuseConstants.S_IFMT) {
//            case FuseConstants.S_IFDIR:
//                buf.setInt(offset + 4, NF3DIR.value);
//                break;
//            case FuseConstants.S_IFLNK:
//                buf.setInt(offset + 4, NF3LNK.value);
//                break;
//            case FuseConstants.S_IFREG:
//                buf.setInt(offset + 4, NF3REG.value);
//                break;
//            default:
//                return -1;
//        }

        buf.setInt(offset + 4, mode & 4095);

        buf.setInt(offset + 8, linkN);
        buf.setInt(offset + 12, uid);
        buf.setInt(offset + 16, gid);

        buf.setLong(offset + 20, size);
        buf.setLong(offset + 28, blocks);
        buf.setInt(offset+36,majorDev);
        buf.setInt(offset+40,minorDev);

        buf.setLong(offset + 44, fsid);
        buf.setLong(offset + 52, ino);
        buf.setInt(offset + 60, atime);
        buf.setInt(offset + 64, atimeNano);
        buf.setInt(offset + 68, mtime);
        buf.setInt(offset + 72, mtimeNano);
        buf.setInt(offset + 76, ctime);
        buf.setInt(offset + 80, ctimeNano);

        return SIZE;
    }

    public static FAttr3 mapToAttr(Inode inode, long fsid) {
        FAttr3 attr = new FAttr3();

        switch (inode.getMode() & FsConstants.S_IFMT) {
            case FsConstants.S_IFDIR:
                attr.type = fType.NF_DIR.type;
                break;
            case FsConstants.S_IFLNK:
                attr.type = fType.NF_LINK.type;
                break;
            case FsConstants.S_IFREG:
                attr.type = fType.NF_REG.type;
                break;
            case FsConstants.S_IFBLK:
                attr.type = fType.NF_BLK.type;
                break;
            case FsConstants.S_IFCHR:
                attr.type = fType.NF_CHR.type;
                break;
            case FsConstants.S_IFFIFO:
                attr.type = fType.NF_FIFO.type;
                break;
            case FsConstants.S_IFSOCK:
                attr.type = fType.NF_SOCK.type;
                break;
            default:
                attr.type = fType.NF_BAD.type;
                break;
        }

        // 存在NFS ACL时，group mode为mask权限
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
        attr.minorDev = inode.getMinorDev();
        attr.majorDev = inode.getMajorDev();
        attr.fsid = fsid;
        attr.ino = inode.getNodeId();
        attr.atime = (int) inode.getAtime();
        attr.atimeNano = inode.getAtimensec();

        attr.mtime = (int) inode.getMtime();
        attr.mtimeNano = inode.getMtimensec();
        attr.ctime = (int) inode.getCtime();
        attr.ctimeNano = inode.getCtimensec();
        changeTimeNano(attr);
        return attr;
    }

    public static void changeTimeNano(FAttr3 attr3) {
        if (attr3.atimeNano > ONE_SECOND_NANO) {
            attr3.atimeNano = 0;
        }
        if (attr3.mtimeNano > ONE_SECOND_NANO) {
            attr3.mtimeNano = 0;
        }
        if (attr3.ctimeNano > ONE_SECOND_NANO) {
            attr3.ctimeNano = 0;
        }
    }
}
