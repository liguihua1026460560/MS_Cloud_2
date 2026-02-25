package com.macrosan.filesystem.cifs.types.smb1;

import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;

@ToString
public class FileInfo {
    public long fileSize;
    public long allocationSize;
    public long lastChangeTime;
    public long lastAccessTime;
    public long lastModifyTime;
    public long uid;
    public long gid;
    public int mode;
    public long devMajor;
    public long devMinor;
    public long nodeId;
    public long rw = 511; //允许所有操作
    public long nlinks;

    public static final int SIZE = 100;

    public int writeStruct(ByteBuf buf, int start) {
        buf.setLongLE(start, fileSize);
        buf.setLongLE(start + 8, allocationSize);
        buf.setLongLE(start + 16, lastChangeTime);
        buf.setLongLE(start + 24, lastAccessTime);
        buf.setLongLE(start + 32, lastModifyTime);
        buf.setLongLE(start + 40, uid);
        buf.setLongLE(start + 48, gid);
        buf.setIntLE(start + 56, mode);
        buf.setLongLE(start + 60, devMajor);
        buf.setLongLE(start + 68, devMinor);
        buf.setLongLE(start + 76, nodeId);
        buf.setLongLE(start + 84, rw);
        buf.setLongLE(start + 92, nlinks);
        return SIZE;
    }

    public static FileInfo mapToFileInfo(Inode inode) {
        FileInfo reply = new FileInfo();
        reply.fileSize = inode.getSize();
        if ((inode.getMode() & S_IFMT) == S_IFLNK) {
            reply.allocationSize = 0;
        } else {
            reply.allocationSize = inode.getSize() % 4096 == 0 ? inode.getSize() : (inode.getSize() / 4096 + 1) * 4096;
        }
        reply.lastAccessTime = CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100;
        reply.lastChangeTime = CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100;
        reply.lastModifyTime =  CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100;
        reply.uid = inode.getUid();
        reply.gid = inode.getGid();
        reply.mode = CifsUtils.transToUnixMode(inode.getMode());
        reply.devMajor = inode.getMajorDev();
        reply.devMinor = inode.getMinorDev();
        reply.nodeId = inode.getNodeId();
        reply.rw = inode.getMode() & 4095;
        reply.nlinks = inode.getLinkN();
        return reply;
    }
}
