package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.FILE_ALL_ACCESS;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/95f3056a-ebc1-4f5d-b938-3f68a44677a6
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FileAllInfo extends GetInfoReply.Info {
    FileBasicInfo basicInfo = new FileBasicInfo();
    FileStandardInfo standardInfo = new FileStandardInfo();
    long nodeId;
    int eaSize;
    long accessFlags;
    long positionInfo;
    int modeInfo;
    int alignmentInfo;
    byte[] fileName;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        offset += basicInfo.writeStruct(buf, offset);
        offset += standardInfo.writeStruct(buf, offset);
        buf.setLongLE(offset, nodeId);
        buf.setIntLE(offset + 8, eaSize);
        buf.setIntLE(offset + 12, (int) (accessFlags & 0xFFFFFFFFL));
        buf.setLongLE(offset + 16, positionInfo);
        buf.setIntLE(offset + 24, modeInfo);
        buf.setIntLE(offset + 28, alignmentInfo);
        buf.setIntLE(offset + 32, fileName.length);
        buf.setBytes(offset + 36, fileName);

        return 100 + fileName.length;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        //40+24+36
        return 100 + fileName.length;
    }

    public static FileAllInfo mapToFileAllInfo(Inode inode, String s3Id) {
        FileAllInfo fileInfo = new FileAllInfo();
        fileInfo.basicInfo = FileBasicInfo.mapToFileBasicInfo(inode);
        fileInfo.standardInfo = FileStandardInfo.mapToFileStandardInfo(inode);
        fileInfo.setNodeId(inode.getNodeId());

        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(inode, s3Id);
        long access = CIFSACL.parseSMBACLToRight(inode, identity, 4, false).var2;

        fileInfo.setAccessFlags(access);
        fileInfo.setFileName(CifsUtils.objNameToSMB(inode.getObjName()));
        return fileInfo;
    }
}
