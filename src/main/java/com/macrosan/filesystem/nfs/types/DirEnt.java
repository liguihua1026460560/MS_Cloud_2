package com.macrosan.filesystem.nfs.types;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.nio.charset.StandardCharsets;

@ToString
public class DirEnt {
    public long fileId;
    public int len;
    public byte[] name;
    public long cookie;
    public int follows;


    public int entSize() {
        return (name.length + 3) / 4 * 4 + 24;
    }

    public static DirEnt mapToDirEnt(long nodeId, String objName, int follows, long cookie, int dirLength) {

        DirEnt dirEnt = new DirEnt();
        dirEnt.fileId = nodeId;
        objName = objName.substring(dirLength);
        int lastIndex = objName.lastIndexOf("/");
        if (lastIndex == objName.length() - 1) {
            objName = objName.substring(0, lastIndex);
        }
        dirEnt.len = objName.getBytes(StandardCharsets.UTF_8).length;
        dirEnt.name = new byte[dirEnt.len];
        dirEnt.name = objName.getBytes();
        dirEnt.follows = follows;
        dirEnt.cookie = cookie;
        return dirEnt;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLong(offset, fileId);
        buf.setInt(offset + 8, len);
        buf.setBytes(offset + 12, name);
        int fillLen = (len + 3) / 4 * 4;
        buf.setLong(offset + fillLen + 12, cookie);
        buf.setInt(offset + fillLen + 20, follows);
        return entSize();
    }

}
