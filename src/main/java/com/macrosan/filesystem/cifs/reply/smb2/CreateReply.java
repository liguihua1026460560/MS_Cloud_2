package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.CompoundRequest;
import com.macrosan.filesystem.cifs.types.smb2.CreateContext;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

@EqualsAndHashCode(callSuper = true)
@Data
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d166aa9e-0b53-410e-b35e-3933d8131927
 */
public class CreateReply extends SMB2Body {
    byte oplockLevel;
    byte flags;
    int createAction;

    long createTime;
    long accessTime;
    long writeTime;
    long changTime;

    long allocSize;
    long fileSize;
    int mode;
    SMB2FileId fileId;
    List<CreateContext> contexts = new LinkedList<>();

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 89;
        int start = offset + super.writeStruct(buf, offset);
        buf.setByte(start, oplockLevel);
        buf.setByte(start + 1, flags);
        buf.setIntLE(start + 2, createAction);
        buf.setLongLE(start + 6, createTime);
        buf.setLongLE(start + 14, accessTime);
        buf.setLongLE(start + 22, writeTime);
        buf.setLongLE(start + 30, changTime);
        buf.setLongLE(start + 38, allocSize);
        buf.setLongLE(start + 46, fileSize);
        buf.setIntLE(start + 54, mode);
        fileId.writeStruct(buf, start + 62);
        buf.setIntLE(start + 78, offset + 88 - 4);
        if (contexts.isEmpty()) {
            buf.setIntLE(start + 82, 0);
            return 88;
        } else {
            int size = 0;

            ListIterator<CreateContext> listIterator = contexts.listIterator();
            while (listIterator.hasNext()) {
                CreateContext context = listIterator.next();
                size += context.writeStruct(buf, start + 86 + size, !listIterator.hasNext());
            }

            buf.setIntLE(start + 82, size);
            return 88 + size;
        }
    }

    public static void mapToCreateReply(CreateReply body, Inode inode, int createAction, long dirNodeId, CompoundRequest compoundRequest, int createOptions, List<Inode.ACE> ACEs, long id0, boolean isLink, Inode linkInode) {
        body.setCreateAction(createAction);
        CifsUtils.setDefaultCifsMode(inode);
        body.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
        body.setFileId(SMB2FileId.randomFileId(compoundRequest, inode.getBucket(), inode.getObjName(), inode.getNodeId(), dirNodeId, createOptions, inode, ACEs, id0));
        body.setCreateTime(CifsUtils.nttime(inode.getCreateTime() * 1000));
        body.setAccessTime(CifsUtils.nttime(inode.getAtime() * 1000) + inode.getAtimensec() / 100);
        body.setWriteTime(CifsUtils.nttime(inode.getMtime() * 1000) + inode.getMtimensec() / 100);
        body.setChangTime(CifsUtils.nttime(inode.getCtime() * 1000) + inode.getCtimensec() / 100);
        if (isLink && linkInode != null) {
            body.setFileSize(linkInode.getSize());
        } else {
            body.setFileSize(inode.getSize());
        }
        body.setAllocSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
    }

    public static final int FILE_SUPERSEDED = 0;
    public static final int FILE_OPEND = 1;
    public static final int FILE_CREATED = 2;
    public static final int FILE_OVERWRITTEN = 3;

}
