package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.macrosan.filesystem.FsConstants.S_IFDIR;

@EqualsAndHashCode(callSuper = true)
@Data
//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/5afa7f66-619c-48f3-955f-68c4ece704ae
public class FileStandardInfo extends GetInfoReply.Info {
    public long allocationSize;
    public long size;
    public int link;
    public byte deletePending;
    public byte directory;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, allocationSize);
        buf.setLongLE(offset + 8, size);
        buf.setIntLE(offset + 16, link);
        buf.setByte(offset + 20, deletePending);
        buf.setByte(offset + 21, directory);
        return 24;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 24;
    }

    public static FileStandardInfo mapToFileStandardInfo(Inode inode) {
        FileStandardInfo fileStandardInfo = new FileStandardInfo();
        fileStandardInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
        fileStandardInfo.setSize(inode.getSize());
        fileStandardInfo.setLink(inode.getLinkN());
        if ((inode.getMode() & S_IFDIR) == S_IFDIR) {
            fileStandardInfo.setDirectory((byte) 1);
        } else {
            fileStandardInfo.setDirectory((byte) 0);
        }
        return fileStandardInfo;
    }
}
