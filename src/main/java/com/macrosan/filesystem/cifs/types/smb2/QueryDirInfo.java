package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.charset.StandardCharsets;

import static com.macrosan.filesystem.cifs.call.smb2.QueryDirCall.*;

public abstract class QueryDirInfo {
    public int nextEntryOffset;

    public static int size(byte[] fileName, byte[] prefix, byte queryClass) {
        int size;
        switch (queryClass) {
            case SMB2_FIND_DIRECTORY_INFO:
                size = 64 + fileName.length - prefix.length;
                break;
            case SMB2_FIND_FULL_DIRECTORY_INFO:
                size = 68 + fileName.length - prefix.length;
                break;
            case SMB2_FIND_ID_FULL_DIRECTORY_INFO:
                size = 80 + fileName.length - prefix.length;
                break;
            case SMB2_FIND_BOTH_DIRECTORY_INFO:
                size = 94 + fileName.length - prefix.length;
                break;
            case SMB2_FIND_ID_BOTH_DIRECTORY_INFO:
                size = 104 + fileName.length - prefix.length;
                break;
            case SMB2_FIND_NAME_INFO:
                size = 12 + fileName.length - prefix.length;
                break;
            default:
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get query dir size fail");
        }

        return (size + 7) & (~7);
    }

    public static boolean isNotValidQueryClass(byte queryClass) {
        switch (queryClass) {
            case SMB2_FIND_DIRECTORY_INFO:
            case SMB2_FIND_FULL_DIRECTORY_INFO:
            case SMB2_FIND_ID_FULL_DIRECTORY_INFO:
            case SMB2_FIND_BOTH_DIRECTORY_INFO:
            case SMB2_FIND_ID_BOTH_DIRECTORY_INFO:
            case SMB2_FIND_NAME_INFO:
                return false;
            default:
                return true;
        }
    }

    public abstract int writeStruct(ByteBuf buf, int offset, boolean isNotLast);

    public abstract int size();

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/b38bf518-9057-4c88-9ddd-5e2d3976a64b
    //SMB2_FIND_DIRECTORY_INFO
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class DirInfo extends QueryDirInfo {
        int fileIndex;
        long creationTime;
        long lastAccessTime;
        long lastWriteTime;
        long lastChangeTime;
        long endOfFile;
        long allocationSize;
        int mode;

        byte[] fileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setLongLE(offset + 8, creationTime);
            buf.setLongLE(offset + 16, lastAccessTime);
            buf.setLongLE(offset + 24, lastWriteTime);
            buf.setLongLE(offset + 32, lastChangeTime);

            buf.setLongLE(offset + 40, endOfFile);
            buf.setLongLE(offset + 48, allocationSize);
            buf.setIntLE(offset + 56, mode);
            buf.setIntLE(offset + 60, fileName.length);
            buf.setBytes(offset + 64, fileName);
            int size = 64 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        public static DirInfo mapToDirInfo(int dirNameLen, Inode inode) {
            DirInfo dirInfo = new DirInfo();
            dirInfo.setEndOfFile(inode.getSize());
            dirInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
            dirInfo.setCreationTime(CifsUtils.nttime(inode.getCreateTime() * 1000L));
            dirInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
            dirInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
            dirInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
            dirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
            CifsUtils.setDefaultCifsMode(inode);
            dirInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));

            return dirInfo;
        }

        @Override
        public int size() {
            int size = 64 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/e8d926d1-3a22-4654-be9c-58317a85540b
    //SMB2_FIND_FULL_DIRECTORY_INFO
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class FullDirInfo extends QueryDirInfo {

        int fileIndex;
        long creationTime;
        long lastAccessTime;
        long lastWriteTime;
        long lastChangeTime;
        long endOfFile;
        long allocationSize;
        int mode;
        int eaSize;

        byte[] fileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setLongLE(offset + 8, creationTime);
            buf.setLongLE(offset + 16, lastAccessTime);
            buf.setLongLE(offset + 24, lastWriteTime);
            buf.setLongLE(offset + 32, lastChangeTime);

            buf.setLongLE(offset + 40, endOfFile);
            buf.setLongLE(offset + 48, allocationSize);
            buf.setIntLE(offset + 56, mode);
            buf.setIntLE(offset + 60, fileName.length);
            buf.setIntLE(offset + 64, eaSize);

            buf.setBytes(offset + 68, fileName);
            int size = 68 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        @Override
        public int size() {
            int size = 68 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }

        public static FullDirInfo mapToFullDirInfo(int dirNameLen, Inode inode) {
            FullDirInfo dirInfo = new FullDirInfo();
            dirInfo.setEndOfFile(inode.getSize());
            dirInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
            dirInfo.setCreationTime(CifsUtils.nttime(inode.getCreateTime() * 1000L));
            dirInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
            dirInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
            dirInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
            dirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
            CifsUtils.setDefaultCifsMode(inode);
            dirInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
            return dirInfo;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/270df317-9ba5-4ccb-ba00-8d22be139bc5
    //SMB2_FIND_BOTH_DIRECTORY_INFO
    //比FullDirInfo多一个short fileName,类似别名功能，一般不需要支持
    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/18e63b13-ba43-4f5f-a5b7-11e871b71f14
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class BothFullDirInfo extends QueryDirInfo {
        int fileIndex;
        long creationTime;
        long lastAccessTime;
        long lastWriteTime;
        long lastChangeTime;
        long endOfFile;
        long allocationSize;
        int mode;
        byte[] fileName;
        int eaSize;
        byte[] shortFileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setLongLE(offset + 8, creationTime);
            buf.setLongLE(offset + 16, lastAccessTime);
            buf.setLongLE(offset + 24, lastWriteTime);
            buf.setLongLE(offset + 32, lastChangeTime);

            buf.setLongLE(offset + 40, endOfFile);
            buf.setLongLE(offset + 48, allocationSize);
            buf.setIntLE(offset + 56, mode);
            buf.setIntLE(offset + 60, fileName.length);
            buf.setIntLE(offset + 64, eaSize);
            //shortFileName 26
            buf.setBytes(offset + 94, fileName);
            int size = 94 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        public static BothFullDirInfo mapToFullDirInfo(int dirNameLen, Inode inode) {
            BothFullDirInfo queryDirInfo = new BothFullDirInfo();
            queryDirInfo.setEndOfFile(inode.getSize());
            queryDirInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
            queryDirInfo.setCreationTime(CifsUtils.nttime(inode.getCreateTime() * 1000L));
            queryDirInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
            queryDirInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
            queryDirInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
            queryDirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
            CifsUtils.setDefaultCifsMode(inode);
            queryDirInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
            return queryDirInfo;
        }

        @Override
        public int size() {
            int size = 94 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/a289f7a8-83d2-4927-8c88-b2d328dde5a5
//SMB2_FIND_NAME_INFO
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class FileNamesInfo extends QueryDirInfo {
        int fileIndex;
        byte[] fileName;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setIntLE(offset + 8, fileName.length);
            buf.setBytes(offset + 12, fileName);
            int size = 12 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        @Override
        public int size() {
            int size = 12 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/1e144bff-c056-45aa-bd29-c13d214ee2ba
//SMB2_FIND_ID_BOTH_DIRECTORY_INFO
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class IdBothFullDirInfo extends QueryDirInfo {
        int fileIndex;
        long creationTime;
        long lastAccessTime;
        long lastWriteTime;
        long lastChangeTime;
        long endOfFile;
        long allocationSize;
        int mode;
        byte[] fileName;
        int eaSize;
        byte[] shortFileName;
        long nodeId;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setLongLE(offset + 8, creationTime);
            buf.setLongLE(offset + 16, lastAccessTime);
            buf.setLongLE(offset + 24, lastWriteTime);
            buf.setLongLE(offset + 32, lastChangeTime);

            buf.setLongLE(offset + 40, endOfFile);
            buf.setLongLE(offset + 48, allocationSize);
            buf.setIntLE(offset + 56, mode);
            buf.setIntLE(offset + 60, fileName.length);
            buf.setIntLE(offset + 64, eaSize);
            //shortFileName 28
            buf.setLongLE(offset + 96, nodeId);
            buf.setBytes(offset + 104, fileName);
            int size = 104 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        public static IdBothFullDirInfo mapToIdBothFullDirInfo(int dirNameLen, Inode inode) {
            IdBothFullDirInfo queryDirInfo = new IdBothFullDirInfo();
            queryDirInfo.setEndOfFile(inode.getSize());
            queryDirInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
            queryDirInfo.setCreationTime(CifsUtils.nttime(inode.getCreateTime() * 1000L));
            queryDirInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
            queryDirInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
            queryDirInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
            queryDirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
            queryDirInfo.setNodeId(inode.getNodeId());
            CifsUtils.setDefaultCifsMode(inode);
            queryDirInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
            return queryDirInfo;
        }

        @Override
        public int size() {
            int size = 104 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/ab8e7558-899c-4be1-a7c5-3a9ae8ab76a0
    //SMB2_FIND_ID_FULL_DIRECTORY_INFO
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class IdFullDirInfo extends QueryDirInfo {
        int fileIndex;
        long creationTime;
        long lastAccessTime;
        long lastWriteTime;
        long lastChangeTime;
        long endOfFile;
        long allocationSize;
        int mode;
        byte[] fileName;
        int eaSize;
        long nodeId;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean isNotLast) {
            buf.setIntLE(offset, nextEntryOffset);
            buf.setIntLE(offset + 4, fileIndex);
            buf.setLongLE(offset + 8, creationTime);
            buf.setLongLE(offset + 16, lastAccessTime);
            buf.setLongLE(offset + 24, lastWriteTime);
            buf.setLongLE(offset + 32, lastChangeTime);

            buf.setLongLE(offset + 40, endOfFile);
            buf.setLongLE(offset + 48, allocationSize);
            buf.setIntLE(offset + 56, mode);
            buf.setIntLE(offset + 60, fileName.length);
            buf.setIntLE(offset + 64, eaSize);
            //Reserved
            buf.setLongLE(offset + 72, nodeId);
            buf.setBytes(offset + 80, fileName);
            int size = 80 + fileName.length;
            if (isNotLast) {
                size = (size + 7) & (~7); //对8取整
            }
            return size;
        }

        public static IdFullDirInfo mapToIdFullDirInfo(int dirNameLen, Inode inode) {
            IdFullDirInfo queryDirInfo = new IdFullDirInfo();
            queryDirInfo.setEndOfFile(inode.getSize());
            queryDirInfo.setAllocationSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
            queryDirInfo.setCreationTime(CifsUtils.nttime(inode.getCreateTime() * 1000L));
            queryDirInfo.setLastAccessTime(CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100);
            queryDirInfo.setLastWriteTime(CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100);
            queryDirInfo.setLastChangeTime(CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100);
            queryDirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
            queryDirInfo.setNodeId(inode.getNodeId());
            CifsUtils.setDefaultCifsMode(inode);
            queryDirInfo.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(),inode.getCifsMode(),true));
            return queryDirInfo;
        }

        @Override
        public int size() {
            int size = 80 + fileName.length;
            size = (size + 7) & (~7);
            return size;
        }
    }
}
