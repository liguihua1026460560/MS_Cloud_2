package com.macrosan.filesystem.nfs.types;

import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class DirEntV4 {
    public int follows;
    public long cookie;
    public int nameLen;
    public byte[] name;
    public FAttr4 fAttr4;

    private DirEntV4() {

    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, follows);
        buf.setLong(offset + 4, cookie);
        buf.setInt(offset + 12, nameLen);
        buf.setBytes(offset + 16, name);
        int padding = nameLen % 4 == 0 ? 0 : 4 - nameLen % 4;
        offset += (16 + nameLen + padding);
        offset += fAttr4.writeStruct(buf, offset);
        return offset - start;
    }

    public static DirEntV4 mapDirEnt(Inode inode, int fsid, int[] mask, int follows, int minorVersion, int dirLength) {
        DirEntV4 dirEnt = new DirEntV4();
        dirEnt.fAttr4 = new FAttr4(inode, fsid, mask, minorVersion);
        String objName = inode.getObjName();
        objName = objName.substring(dirLength);
        int lastIndex = objName.lastIndexOf("/");
        if (lastIndex == objName.length() - 1) {
            objName = objName.substring(0, lastIndex);
        }
        dirEnt.nameLen = objName.length();
        dirEnt.name = objName.getBytes();
        dirEnt.follows = follows;
        dirEnt.cookie = inode.getCookie();
        return dirEnt;
    }

    public int size() {
        return 16 + (nameLen + 3) / 4 * 4 + fAttr4.getAttrSize();
    }
}
