package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.types.smb1.FileInfo;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/4e65d94e-09af-4511-a77a-b73adf1c52d6
@ToString
public class FindFirstReply extends Trans2Reply {
    public short searchId;
    public short searchCount;
    public short endOfSearch;
    public short eaErrorOffset;
    public short lastNameOffset;
    public List<INFO> dataList;
    public boolean isFindNext = false;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 10;
        paramCount = totalParamCount = 10;
        paramOffset = 3 + wordCount * 2 + SMB1Header.SIZE;
        paramOffset = (paramOffset + 3) & (~3); //对4取整

        int start = paramOffset + 4;
        if (!isFindNext) {
            buf.setShortLE(start, searchId);
        } else {
            start -= 2;
        }
        buf.setIntLE(start + 2, searchCount);
        buf.setIntLE(start + 4, endOfSearch);
        buf.setIntLE(start + 6, eaErrorOffset);
        buf.setIntLE(start + 8, lastNameOffset);

        dataOffset = paramOffset + paramCount;
        dataOffset = (dataOffset + 3) & (~3); //对4取整

        dataCount = 0;

        start = dataOffset + 4;

        for (INFO info : dataList) {
            dataCount += info.writeStruct(buf, start + dataCount);
        }
        totalDataCount = dataCount;
        byteCount = (start + dataCount - offset - 3 - wordCount * 2);
        return super.writeStruct(buf, offset);
    }

    public int getSize() {
        return 10 + 32 + 3;
    }

    private interface INFO {
        int writeStruct(ByteBuf buf, int offset);

        int getNextEntryOffset();
    }

    @ToString
    public static class FILE_UNIX_DIR_INFO implements INFO {
        @Getter
        public int nextEntryOffset;//size of info
        public int resumeKey = 0;
        public FileInfo fileInfo = new FileInfo();
        public char[] fileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, resumeKey);
            fileInfo.writeStruct(buf, offset + 8);
            for (int i = 0; i < fileName.length; i++) {
                buf.setShortLE(offset + i * 2 + FileInfo.SIZE + 8, fileName[i]);
            }
            return this.getNextEntryOffset();
        }

        public static FILE_UNIX_DIR_INFO mapToInfo(Inode inode, int dirNameLen) {
            FILE_UNIX_DIR_INFO info = new FILE_UNIX_DIR_INFO();
            info.fileInfo = FileInfo.mapToFileInfo(inode);
            if (inode.getObjName().endsWith("/")) {
                info.fileName = inode.getObjName().substring(dirNameLen, inode.getObjName().length() - 1).toCharArray();
            } else {
                info.fileName = inode.getObjName().substring(dirNameLen).toCharArray();
            }
            info.calSizeOfInfo();
            return info;
        }

        public void calSizeOfInfo() {
            int res = 8 + fileInfo.SIZE + fileName.length * 2 + 2;
            res = (res + 3) & (~3);
            this.nextEntryOffset = res;
        }
    }

    public static class FILE_UNIX_BASIC_INFO implements INFO {
        long endOfFile;
        long numOfBytes;
        long lastStatusChange;
        long lastAccessTime;
        long lastModificationTime;
        long uid;
        long gid;
        int type;
        long devMajor;
        long devMinor;
        long uniqueId;
        long permissions;
        long nlinks;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setLongLE(offset, endOfFile);
            buf.setLongLE(offset + 8, numOfBytes);
            buf.setLongLE(offset + 16, lastStatusChange);
            buf.setLongLE(offset + 24, lastAccessTime);
            buf.setLongLE(offset + 32, lastModificationTime);
            buf.setLongLE(offset + 40, uid);
            buf.setLongLE(offset + 48, gid);
            buf.setIntLE(offset + 56, type);
            buf.setLongLE(offset + 60, devMajor);
            buf.setLongLE(offset + 68, devMinor);
            buf.setLongLE(offset + 76, uniqueId);
            buf.setLongLE(offset + 84, permissions);
            buf.setLongLE(offset + 92, nlinks);
            return 100;
        }

        @Override
        public int getNextEntryOffset() {
            return 0;
        }
    }

    public static class FILE_UNIX_INFO extends FILE_UNIX_BASIC_INFO {
        int nextEntryOffset;
        int resumeKey;
        char[] fileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, resumeKey);
            int len = super.writeStruct(buf, offset + 8) + 8 + offset;
            for (int i = 0; i < fileName.length; i++) {
                buf.setShortLE(len, fileName[i]);
                len += 2;
            }

            buf.setShortLE(len, 0);
            len += 2;

            int res = len - offset;
            res = (res + 3) & (~3);
            return res;
        }
    }

}
