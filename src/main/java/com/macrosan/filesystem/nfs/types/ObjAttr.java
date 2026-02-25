package com.macrosan.filesystem.nfs.types;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

/**
 * 文件修改操作时传的属性信息
 */
@ToString
public class ObjAttr {
    public int hasMode;
    public int mode;
    public int hasUid;
    public int uid;
    public int hasGid;
    public int gid;
    public int hasSize;
    public long size;
    public int hasAtime;
    public int atime;
    public int atimeNano;
    public int hasMtime;
    public int mtime;
    public int mtimeNano;
    public int hasCtime;
    public int ctime;
    public int ctimeNano;
    public int hasCreateTime;
    public int createTime;
    public int hasCifsMode;
    public int cifsMode;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        hasMode = buf.getInt(offset);
        offset += 4;
        if (hasMode != 0) {
            mode = buf.getInt(offset);
            offset += 4;
        }
        hasUid = buf.getInt(offset);
        offset += 4;
        if (hasUid != 0) {
            uid = buf.getInt(offset);
            offset += 4;
        }

        hasGid = buf.getInt(offset);
        offset += 4;
        if (hasGid != 0) {
            gid = buf.getInt(offset);
            offset += 4;
        }

        hasSize = buf.getInt(offset);
        offset += 4;
        if (hasSize != 0) {
            size = buf.getLong(offset);
            offset += 8;

        }


        hasAtime = buf.getInt(offset);
        offset += 4;
        if (hasAtime == 2) {
            atime = buf.getInt(offset);
            atimeNano = buf.getInt(offset + 4);
            offset += 8;
        }

        hasMtime = buf.getInt(offset);
        offset += 4;
        if (hasMtime == 2) {
            mtime = buf.getInt(offset);
            mtimeNano = buf.getInt(offset + 4);
            offset += 8;
        }
        return offset - start;
    }
}
