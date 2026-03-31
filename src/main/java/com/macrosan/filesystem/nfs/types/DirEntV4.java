package com.macrosan.filesystem.nfs.types;

import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class DirEntV4 {
    public int follows;
    public long cookie;
    public byte[] name;
    public FAttr4 fAttr4;

    private DirEntV4() {

    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, follows);
        buf.setLong(offset + 4, cookie);
        buf.setInt(offset + 12, name.length);
        buf.setBytes(offset + 16, name);
        offset += (16 + (name.length + 3) / 4 * 4);
        offset += fAttr4.writeStruct(buf, offset);
        return offset - start;
    }

    public static DirEntV4 mapDirEnt(Inode inode, int fsid, int[] mask, int follows, int minorVersion, int dirLength) {
        DirEntV4 dirEnt = new DirEntV4();
        dirEnt.fAttr4 = new FAttr4(inode, fsid, mask, minorVersion);
        dirEnt.name = inode.getObjName().substring(dirLength).getBytes();
        if (dirEnt.name[dirEnt.name.length - 1] == '/') {
            byte[] dirName = new byte[dirEnt.name.length - 1];
            System.arraycopy(dirEnt.name, 0, dirName, 0, dirName.length);
            dirEnt.name = dirName;
        }
        dirEnt.follows = follows;
        dirEnt.cookie = inode.getCookie();
        return dirEnt;
    }

    public int size() {
        return 16 + (name.length + 3) / 4 * 4 + fAttr4.getAttrSize();
    }
}
