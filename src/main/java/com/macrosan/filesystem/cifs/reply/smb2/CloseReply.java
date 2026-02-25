package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.cifs.types.smb2.FileAttrTagInfo.FILE_ATTRIBUTE_DIRECTORY;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/c0c15c57-3f3e-452b-b51c-9cc650a13f7b
 */
public class CloseReply extends SMB2Body {
    short flags;
    long createTime;
    long accessTime;
    long writeTime;
    long changTime;
    long allocSize;
    long fileSize;
    int mode;


    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 60;
        offset += super.writeStruct(buf, offset);
        buf.setShortLE(offset, flags);
        buf.setLongLE(offset + 6, createTime);
        buf.setLongLE(offset + 14, accessTime);
        buf.setLongLE(offset + 22, writeTime);
        buf.setLongLE(offset + 30, changTime);
        buf.setLongLE(offset + 38, allocSize);
        buf.setLongLE(offset + 46, fileSize);
        buf.setIntLE(offset + 54, mode);
        return 60;
    }

    public static CloseReply mapToCloseReply(Inode inode, short flags) {
        CloseReply closeReply = new CloseReply();
        closeReply.flags = flags;
        closeReply.createTime = CifsUtils.nttime(inode.getCreateTime() * 1000L);
        closeReply.accessTime = CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100;
        closeReply.writeTime = CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100;
        closeReply.changTime = CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100;
        closeReply.allocSize = CifsUtils.getAllocationSize(inode.getMode(), inode.getSize());
        closeReply.fileSize = inode.getSize();
        CifsUtils.setDefaultCifsMode(inode);
        closeReply.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
        return closeReply;
    }

    public static CloseReply DEFAULT = new CloseReply();
}
