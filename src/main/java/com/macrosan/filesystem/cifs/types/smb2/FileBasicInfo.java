package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/16023025-8a78-492f-8b96-c873b042ac50
public class FileBasicInfo extends GetInfoReply.Info {
    public long creationTime;
    public long lastAccessTime;
    public long lastWriteTime;
    public long lastChangeTime;
    public int mode;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, creationTime);
        buf.setLongLE(offset + 8, lastAccessTime);
        buf.setLongLE(offset + 16, lastWriteTime);
        buf.setLongLE(offset + 24, lastChangeTime);
        buf.setIntLE(offset + 32, mode);
        return 40;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        creationTime = buf.getLongLE(offset);
        lastAccessTime = buf.getLongLE(offset + 8);
        lastWriteTime = buf.getLongLE(offset + 16);
        lastChangeTime = buf.getLongLE(offset + 24);
        mode = buf.getIntLE(offset + 32);
        return 40;
    }

    @Override
    public int size() {
        return 40;
    }

    public static FileBasicInfo mapToFileBasicInfo(Inode inode) {
        FileBasicInfo fileBasicInfo = new FileBasicInfo();
        long createTimeStamp = inode.getCreateTime() == 0 ? System.currentTimeMillis() / 1000 : inode.getCreateTime();
        fileBasicInfo.setCreationTime(CifsUtils.nttime(createTimeStamp * 1000L));
        fileBasicInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
        fileBasicInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
        fileBasicInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
        CifsUtils.setDefaultCifsMode(inode);
        fileBasicInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
        return fileBasicInfo;
    }
}
