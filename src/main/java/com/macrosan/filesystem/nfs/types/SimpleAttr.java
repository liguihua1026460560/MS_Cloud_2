package com.macrosan.filesystem.nfs.types;

import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SimpleAttr {
    public long size;
    public int atime;
    public int atimeNano;
    public int mtime;
    public int mtimeNano;

    public static final int ATTR_SIZE = 24;
    public int writeStruct(ByteBuf buf, int offset){
        buf.setLong(offset, size);
        offset += 8;
        buf.setInt(offset, atime);
        buf.setInt(offset + 4, atimeNano);
        buf.setInt(offset + 8, mtime);
        buf.setInt(offset + 12, mtimeNano);
        return ATTR_SIZE;
    }

    public static SimpleAttr mapToSimpleAttr(Inode inode){
        SimpleAttr simpleAttr = new SimpleAttr();
        simpleAttr.size = inode.getSize();
        simpleAttr.atime = (int) inode.getAtime();
        simpleAttr.atimeNano = inode.getAtimensec();
        simpleAttr.mtime = (int )inode.getMtime();
        simpleAttr.mtimeNano = inode.getMtimensec();
        return simpleAttr;
    }
}
