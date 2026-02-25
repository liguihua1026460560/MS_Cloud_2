package com.macrosan.filesystem.nfs.types;

import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;

public class Entry {
    long ino;
    byte[] name;
    long cookie;
    Attr attr;
    FH2 fh;

    @Override
    public String toString() {
        return "Entry(ino=" +
                ino +
                ", name=" +
                new String(name) +
                ", cookie=" +
                cookie +
                ", attr=" +
                attr +
                ", fh=" +
                fh +
                ")";
    }

    public int size() {
        return Attr.SIZE + fh.fhSize + 32 + (name.length + 3) / 4 * 4;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, 1);
        buf.setLong(offset + 4, ino);
        buf.setInt(offset + 12, name.length);
        buf.setBytes(offset + 16, name);
        int len = (name.length + 3) / 4 * 4;
        offset = offset + 16 + len;

        buf.setLong(offset, cookie);
        offset += 8;
        offset += attr.writeStruct(buf, offset);
        buf.setInt(offset, 1);
        offset += 4;
        offset += fh.writeStruct(buf, offset);

        return offset - start;
    }

    public static Entry mapToEntry(int dirLength, Inode inode, long fsid) {
        Entry entry = new Entry();
        entry.attr = Attr.mapToAttr(inode, fsid);
        entry.ino = inode.getNodeId();
        entry.name = inode.getObjName().substring(dirLength).getBytes();
        if (entry.name[entry.name.length - 1] == '/') {
            byte[] dirName = new byte[entry.name.length - 1];
            System.arraycopy(entry.name, 0, dirName, 0, dirName.length);
            entry.name = dirName;
        }

        entry.cookie = inode.getCookie();
        entry.fh = new FH2();
        if (inode.getNodeId() == 1) {
            entry.fh.version = 1;
            entry.fh.fhSize = 8;
            entry.fh.authType = 0;

            entry.fh.fsidType = (byte) FH2.NFSD_FSID.FSID_NUM.value;
            entry.fh.fileidType = 0;
            entry.fh.fsid = (int) fsid;
            entry.fh.ino = 0;
            entry.fh.generation = 0;
        } else {
            entry.fh.version = 1;
            entry.fh.fhSize = 20;
            entry.fh.authType = 0;
            entry.fh.fsidType = (byte) FH2.NFSD_FSID.FSID_NUM.value;
            entry.fh.fileidType = (byte) 0x81;
            entry.fh.fsid = (int) fsid;
            entry.fh.ino = inode.getNodeId();
            entry.fh.generation = 0;
        }


        return entry;
    }
}
