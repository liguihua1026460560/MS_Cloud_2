package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.Random;

import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.FILE_ALL_ACCESS;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.STANDARD_RIGHTS_ALL_ACCESS;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/32085986-b516-486c-abbb-0abbdf9f1909
@ToString
public class NTCreateXReply extends SMB1Body {
    //5
    public byte xOpcode;
    public short xOffset;
    public byte opLockLevel;
    //6
    public short fid;
    public int createAction;
    //32
    public long createTime;
    public long lastAccessTime;
    public long lastWriteTime;
    public long lastChangeTime;
    //4
    public int mode;
    //16
    public long allocationSize;
    public long end;
    //5
    public short resourceType;
    public short ipcState;
    public byte directory;
    //32
    public byte[] volume = new byte[16];
    public long id;
    public int accessRights;
    public int guestAccessRights;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 50;
        byteCount = 0;

        buf.setByte(offset + 1, xOpcode);
        buf.setShortLE(offset + 3, xOffset);
        buf.setShortLE(offset + 5, opLockLevel);
        buf.setShortLE(offset + 6, fid);
        buf.setIntLE(offset + 8, createAction);
        buf.setLongLE(offset + 12, createTime);
        buf.setLongLE(offset + 20, lastAccessTime);
        buf.setLongLE(offset + 28, lastWriteTime);
        buf.setLongLE(offset + 36, lastChangeTime);
        buf.setIntLE(offset + 44, mode);
        buf.setLongLE(offset + 48, allocationSize);
        buf.setLongLE(offset + 56, end);
        buf.setShortLE(offset + 64, resourceType);
        buf.setShortLE(offset + 66, ipcState);
        buf.setByte(offset + 68, directory);
        buf.setBytes(offset + 69, volume);
        buf.setLongLE(offset + 85, id);
        buf.setIntLE(offset + 93, accessRights);
        buf.setIntLE(offset + 97, guestAccessRights);

        return super.writeStruct(buf, offset);
    }

    public static NTCreateXReply mapToNTCreateXReply(Inode inode) {
        NTCreateXReply reply = new NTCreateXReply();
        reply.createTime = CifsUtils.nttime(inode.getCreateTime() * 1000L);
        reply.lastAccessTime = CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100;
        reply.lastWriteTime = CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100;
        reply.lastChangeTime = CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100;
        reply.mode = inode.getMode() & 4095;
        reply.allocationSize = CifsUtils.getAllocationSize(inode.getMode(), inode.getSize());
        reply.end = inode.getSize();
        reply.resourceType = (short) CifsUtils.transToUnixMode(inode.getMode());
        reply.directory = (byte) ((reply.resourceType == 1 ? 1 : 0) & 0xff);
        reply.id = inode.getNodeId();
        return reply;
    }

    public void setDefaultValue(byte xOpcode) {
        this.xOpcode = xOpcode;
        this.accessRights = FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS;
        this.createAction = 1;
        this.ipcState = 0x6;
    }
}
