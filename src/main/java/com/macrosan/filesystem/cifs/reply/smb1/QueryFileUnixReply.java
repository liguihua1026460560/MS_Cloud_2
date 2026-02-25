package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.types.smb1.FileInfo;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;

@ToString(callSuper = true)
public class QueryFileUnixReply extends Trans2Reply {
    public FileInfo fileInfo = new FileInfo();

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 10;

        paramCount = totalParamCount = 2;
        dataCount = totalDataCount = 100;
        paramOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;
        dataOffset = paramOffset + paramCount * 2;

        byteCount = paramCount * 2 + dataCount + 1;

        int start = dataOffset + 4;
        fileInfo.writeStruct(buf, start);

        return super.writeStruct(buf, offset);
    }

    public static QueryFileUnixReply mapToQueryFileUnixReply(Inode inode) {
        QueryFileUnixReply reply = new QueryFileUnixReply();
        reply.fileInfo = FileInfo.mapToFileInfo(inode);
        return reply;
    }

    public static final int UNIX_TYPE_FILE = 0;
    public static final int UNIX_TYPE_DIR = 1;
    public static final int UNIX_TYPE_SYMLINK = 2;
    public static final int UNIX_TYPE_CHARDEV = 3;
    public static final int UNIX_TYPE_BLKDEV = 4;
    public static final int UNIX_TYPE_FIFO = 5;
    public static final int UNIX_TYPE_SOCKET = 6;
    public static final int UNIX_TYPE_UNKNOWN = 0xFFFFFFFF;

}
