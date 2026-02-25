package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.types.smb1.FileInfo;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.macrosan.filesystem.FsConstants.SMBSetInfoLevel.*;
import static com.macrosan.filesystem.cifs.types.smb1.Trans2.*;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/f7d148cd-e3d5-49ae-8b37-9633822bfeac
@EqualsAndHashCode(callSuper = true)
@ToString
@Data
public class Trans2Call extends SMB1Body {
    private static final Logger log = LoggerFactory.getLogger(Trans2Call.class);
    //整个trans的数量
    int totalParamCount;
    int totalDataCount;
    //reply时最大的count
    int maxParamCount;
    int maxDataCount;
    int maxSetupCount;
    int flags;
    long timeout;
    //当前trans的数量
    int paramCount;
    public int paramOffset;
    int dataCount;
    public int dataOffset;

    //当前setup数量一直都是1
    public short setup;
    public Trans2SubCall subCall;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);

        totalParamCount = buf.getShortLE(offset + 1) & 0xffff;
        totalDataCount = buf.getShortLE(offset + 3) & 0xffff;
        maxParamCount = buf.getShortLE(offset + 5) & 0xffff;
        maxDataCount = buf.getShortLE(offset + 7) & 0xffff;
        maxSetupCount = buf.getByte(offset + 9) & 0xff;
        flags = buf.getShortLE(offset + 11) & 0xffff;
        timeout = buf.getIntLE(offset + 13);
        paramCount = buf.getShortLE(offset + 19) & 0xffff;
        paramOffset = buf.getShortLE(offset + 21) & 0xffff;
        dataCount = buf.getShortLE(offset + 23) & 0xffff;
        dataOffset = buf.getShortLE(offset + 25) & 0xffff;

        int setupCount = buf.getByte(offset + 27) & 0xff;
        if (setupCount != 1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "setupCount error");
        }

        setup = buf.getShortLE(offset + 29);
        switch (setup) {
            case TRANSACT2_QFSINFO:
                subCall = new QueryFSInfoCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            case TRANSACT2_SETFSINFO:
                short infoLevel = buf.getShortLE(offset - SMB1Header.SIZE + paramOffset + 2);
                switch (infoLevel) {
                    case QueryFSInfoCall.SMB_QUERY_CIFS_UNIX_INFO:
                        subCall = new CifsUnixSetFsInfoCall();
                        subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                }
                break;
            case TRANSACT2_QPATHINFO:
                subCall = new QueryPathInfoCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            case TRANSACT2_FINDNEXT:
                subCall = new FindNextCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            case TRANSACT2_FINDFIRST:
                subCall = new FindFirstCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            case TRANSACT2_SETPATHINFO:
                subCall = new SetPathInfoCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            case TRANSACT2_SETFILEINFO:
                subCall = new SetFileInfoCall();
                subCall.readParam(buf, offset - SMB1Header.SIZE, paramOffset, dataOffset, totalParamCount);
                break;
            default:
                subCall = null;
        }

        return size;
    }


    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/1cc40e02-aaea-4f33-b7b7-3a6b63906516
    public static abstract class Trans2SubCall {
        public abstract void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount);
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/cfa23a11-0e80-43bd-bbd4-e9cfb99b5dce
    @ToString
    public static class QueryFSInfoCall extends Trans2SubCall {
        public short infoLevel;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            infoLevel = buf.getShortLE(offset + parmOff);
        }

        //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/55217a26-87ef-489f-a159-3ed6cc6412e9
        public static final short SMB_INFO_ALLOCATION = 1;
        public static final short SMB_INFO_VOLUME = 2;
        public static final short SMB_QUERY_FS_VOLUME_INFO = 0x102;
        public static final short SMB_QUERY_FS_SIZE_INFO = 0x103;
        public static final short SMB_QUERY_FS_DEVICE_INFO = 0x104;
        public static final short SMB_QUERY_FS_ATTRIBUTE_INFO = 0x105;
        public static final short SMB_QUERY_CIFS_UNIX_INFO = 0x200;
        public static final short SMB_QUERY_POSIX_FS_INFO = 0x201;
        public static final short SMB_QUERY_POSIX_WHOAMI = 0x202;
        public static final short SMB_MAC_QUERY_FS_INFO = 0x301;

    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/9b171bbf-c6d8-4c88-ac85-23c795cbb5d3
    @ToString(callSuper = true)
    public static class QueryPathInfoCall extends Trans2SubCall {
        public short infoLevel;
        public char[] fileName;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            infoLevel = buf.getShortLE(offset + parmOff);
            //Reserved 4
            int fileLen = (totalParamCount - 6) / 2;
            fileName = new char[fileLen - 1];
            for (int i = 0; i < fileName.length; i++) {
                fileName[i] = (char) buf.getShortLE(offset + parmOff + 6 + 2 * i);
            }
        }

        //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/794afe2e-7c11-4a8c-b909-0a397966f6a9
        public static final short SMB_INFO_STANDARD = 1;
        public static final short SMB_INFO_QUERY_EA_SIZE = 2;
        public static final short SMB_INFO_QUERY_EAS_FROM_LIST = 3;
        public static final short SMB_INFO_QUERY_ALL_EAS = 4;
        public static final short SMB_QUERY_FILE_BASIC_INFO = 0x101;
        public static final short SMB_QUERY_FILE_STANDARD_INFO = 0x102;
        public static final short SMB_QUERY_FILE_EA_INFO = 0x103;
        public static final short SMB_QUERY_FILE_NAME_INFO = 0x104;
        public static final short SMB_QUERY_FILE_ALLOCATION_INFO = 0x105;
        public static final short SMB_QUERY_FILE_END_OF_FILEINFO = 0x106;
        public static final short SMB_QUERY_FILE_ALL_INFO = 0x107;
        public static final short SMB_QUERY_FILE_ALT_NAME_INFO = 0x108;
        public static final short SMB_QUERY_FILE_STREAM_INFO = 0x109;
        public static final short SMB_QUERY_COMPRESSION_INFO = 0x10b;
        //https://wiki.samba.org/index.php/UNIX_Extensions#New_File_Info_(and_Path_Info)_levels
        public static final short QUERY_FILE_UNIX_BASIC = 0x200;
        public static final short QUERY_FILE_UNIX_LINK = 0x201;
        public static final short QUERY_POSIX_ACL = 0x204;
        public static final short QUERY_XATTR = 0x205;
        public static final short QUERY_ATTR_FLAGS = 0x206;
        public static final short QUERY_POSIX_PERMISSION = 0x207;
        public static final short QUERY_POSIX_LOCK = 0x208;
        public static final short SMB_POSIX_PATH_OPEN = 0x209;
        public static final short SMB_POSIX_PATH_UNLINK = 0x20a;
        public static final short SMB_QUERY_FILE_UNIX_INFO2 = 0x20b;

        public static final int SMB_QFILEINFO_BASIC_INFORMATION = 1004;
        public static final int SMB_QFILEINFO_STANDARD_INFORMATION = 1005;
        public static final int SMB_QFILEINFO_INTERNAL_INFORMATION = 1006;
        public static final int SMB_QFILEINFO_EA_INFORMATION = 1007;
        public static final int SMB_QFILEINFO_ACCESS_INFORMATION = 1008;
        public static final int SMB_QFILEINFO_NAME_INFORMATION = 1009;
        public static final int SMB_QFILEINFO_POSITION_INFORMATION = 1014;
        public static final int SMB_QFILEINFO_MODE_INFORMATION = 1016;
        public static final int SMB_QFILEINFO_ALIGNMENT_INFORMATION = 1017;
        public static final int SMB_QFILEINFO_ALL_INFORMATION = 1018;
        public static final int SMB_QFILEINFO_ALT_NAME_INFORMATION = 1021;
        public static final int SMB_QFILEINFO_STREAM_INFORMATION = 1022;
        public static final int SMB_QFILEINFO_COMPRESSION_INFORMATION = 1028;
        public static final int SMB_QFILEINFO_NETWORK_OPEN_INFORMATION = 1034;
        public static final int SMB_QFILEINFO_ATTRIBUTE_TAG_INFORMATION = 1035;
        public static final int SMB_QFILEINFO_NORMALIZED_NAME_INFORMATION = 1048;
    }

    @ToString(callSuper = true)
    public static class SetFsInfoCall extends Trans2SubCall {
        public short infoLevel;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            infoLevel = buf.getShortLE(offset + parmOff + 2);
        }
    }

    @ToString(callSuper = true)
    public static class CifsUnixSetFsInfoCall extends SetFsInfoCall {
        short major;
        short minor;
        long capabilities;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            major = buf.getShortLE(offset + dataOff);
            minor = buf.getShortLE(offset + dataOff + 2);
            capabilities = buf.getLongLE(offset + dataOff + 4);
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/80dc980e-fe03-455c-ada6-7c5dd6c551ba
    @ToString
    public static class FindNextCall extends Trans2SubCall {
        public short sid;
        public short searchCount;
        public short infoLevel;
        public int resumeKey;
        public short flags;
        public char[] fileName;

        @Override

        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            sid = buf.getShortLE(offset + parmOff);
            searchCount = buf.getShortLE(offset + parmOff + 2);
            infoLevel = buf.getShortLE(offset + parmOff + 4);
            resumeKey = buf.getIntLE(offset + parmOff + 6);
            flags = buf.getShortLE(offset + parmOff + 10);

            int fileLen = (totalParamCount - 12) / 2;
            fileName = new char[fileLen - 1];
            for (int i = 0; i < fileName.length; i++) {
                fileName[i] = (char) buf.getShortLE(offset + parmOff + 12 + i * 2);
            }
        }

    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/b2b2a730-9499-4f05-884e-d5bb7b9caf90
    @ToString
    public static class FindFirstCall extends Trans2SubCall {
        public short searchAttributes;
        public short searchCount;
        public short flags;
        public short infoLevel;
        public int searchStorageType;
        public char[] fileName;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            searchAttributes = buf.getShortLE(offset + parmOff);
            searchCount = buf.getShortLE(offset + parmOff + 2);
            flags = buf.getShortLE(offset + parmOff + 4);
            infoLevel = buf.getShortLE(offset + parmOff + 6);
            searchStorageType = buf.getIntLE(offset + parmOff + 8);

            int fileLen = (totalParamCount - 12) / 2;
            fileName = new char[fileLen - 1];
            for (int i = 0; i < fileName.length; i++) {
                fileName[i] = (char) buf.getShortLE(offset + parmOff + 12 + i * 2);
            }
        }

        public static int getInfoSize(int level) {
            int infoSize = 0;
            int addSize = 44;//与NFS readdir保持一致
            switch (level) {
                case SMB_FIND_FILE_BOTH_DIRECTORY_INFO:
                    break;
                case SMB_FIND_FILE_UNIX:
                    infoSize = FileInfo.SIZE + 8 + 1 + addSize;
            }
            return infoSize;
        }

        //SearchAttributes
        public static final short SMB_FIND_CLOSE_AFTER_REQUEST = 1;
        public static final short SMB_FIND_CLOSE_AT_EOS = 2;
        public static final short SMB_FIND_RETURN_RESUME_KEYS = 4;
        public static final short SMB_FIND_CONTINUE_FROM_LAST = 8;
        public static final short SMB_FIND_WITH_BACKUP_INTENT = 16;
        // Find File infolevels
        public static final short SMB_FIND_FILE_INFO_STANDARD = 0x001;
        public static final short SMB_FIND_FILE_QUERY_EA_SIZE = 0x002;
        public static final short SMB_FIND_FILE_QUERY_EAS_FROM_LIST = 0x003;
        public static final short SMB_FIND_FILE_DIRECTORY_INFO = 0x101;
        public static final short SMB_FIND_FILE_FULL_DIRECTORY_INFO = 0x102;
        public static final short SMB_FIND_FILE_NAMES_INFO = 0x103;
        public static final short SMB_FIND_FILE_BOTH_DIRECTORY_INFO = 0x104;
        public static final short SMB_FIND_FILE_ID_FULL_DIR_INFO = 0x105;
        public static final short SMB_FIND_FILE_ID_BOTH_DIR_INFO = 0x106;
        public static final short SMB_FIND_FILE_UNIX = 0x202;
    }

    @ToString
    public static class SetPathInfoCall extends Trans2SubCall {
        public short infoLevel;
        public char[] fileName;
        public FILE_INFO info;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            infoLevel = buf.getShortLE(offset + parmOff);
            //Reserved 4
            int fileLen = (totalParamCount - 6) / 2;
            fileName = new char[fileLen - 1];
            for (int i = 0; i < fileName.length; i++) {
                fileName[i] = (char) buf.getShortLE(offset + parmOff + 6 + 2 * i);
            }
            offset = offset + parmOff + 6 + 2 * fileLen;
            info = FILE_INFO.readSetPathInfo(buf, offset, infoLevel, info);
        }

    }

    @ToString
    public static class SetFileInfoCall extends Trans2SubCall {
        public short fsid;
        public short infoLevel;
        public FILE_INFO info;

        @Override
        public void readParam(ByteBuf buf, int offset, int parmOff, int dataOff, int totalParamCount) {
            fsid = buf.getShortLE(offset + parmOff);
            infoLevel = buf.getShortLE(offset + parmOff + 2);
            offset = offset + parmOff + 6;
            info = FILE_INFO.readSetPathInfo(buf, offset, infoLevel, info);
           
        }

    }

    @ToString
    public static abstract class FILE_INFO {
        public abstract void readStruct(ByteBuf buf, int offset);

        public boolean hasChange(long val) {
            return val != 0 && val != -1;
        }

        public static FILE_INFO readSetPathInfo(ByteBuf buf, int offset, short infoLevel, FILE_INFO info) {
            switch (infoLevel) {
                case SMB_POSIX_OPEN:
                    break;
                case SMB_POSIX_UNLINK:
                    break;
                case SMB_SET_FILE_UNIX_HLINK:
                    break;

                case SMB_SET_FILE_UNIX_LINK:
                    break;
                case SMB_SET_FILE_BASIC_INFO:
                    break;
                case SMB_SET_FILE_BASIC_INFO2:
                    break;
                case SMB_SET_FILE_UNIX_BASIC:
                    info = new UnixBasicInfo();
                    info.readStruct(buf, offset);
                    break;
                case SMB_SET_FILE_EA:
                    break;
                case SMB_SET_POSIX_ACL:
                    break;
                case SMB_SET_FILE_END_OF_FILE_INFO2:
                    break;
                case SMB_SET_FILE_END_OF_FILE_INFO:
                    break;
            }
            return info;
        }
    }

    public static class UnixBasicInfo extends FILE_INFO {
        FileInfo fileInfo = new FileInfo();

        public void readStruct(ByteBuf buf, int offset) {
            int start = 0;
            fileInfo.fileSize = buf.getLongLE(offset + 8 * start++);
            fileInfo.allocationSize = buf.getLongLE(offset + 8 * start++);
            fileInfo.lastChangeTime = buf.getLongLE(offset + 8 * start++);
            fileInfo.lastAccessTime = buf.getLongLE(offset + 8 * start++);
            fileInfo.lastModifyTime = buf.getLongLE(offset + 8 * start++);
            fileInfo.uid = buf.getLongLE(offset + 8 * start++);
            fileInfo.gid = buf.getLongLE(offset + 8 * start++);
            fileInfo.mode = buf.getIntLE(offset + 8 * start++);
            offset -= 4;
            fileInfo.devMajor = buf.getLongLE(offset + 8 * start++);
            fileInfo.devMinor = buf.getLongLE(offset + 8 * start++);
            fileInfo.nodeId = buf.getLongLE(offset + 8 * start++);
            fileInfo.rw = buf.getLongLE(offset + 8 * start++);
            fileInfo.nlinks = buf.getLongLE(offset + 8 * start++);
        }

        public void setObjAttr(ObjAttr attr) {
            if (hasChange(fileInfo.fileSize)) {
                attr.hasSize = 1;
                attr.size = fileInfo.fileSize;
            }
            if (hasChange(fileInfo.lastChangeTime)) {
                attr.hasCtime = 2;
                attr.ctime = (int) CifsUtils.SMBTimeToStamp(fileInfo.lastChangeTime);
            }
            if (hasChange(fileInfo.lastAccessTime)) {
                attr.hasAtime = 2;
                attr.atime = (int) CifsUtils.SMBTimeToStamp(fileInfo.lastAccessTime);
            }
            if (hasChange(fileInfo.lastModifyTime)) {
                attr.hasMtime = 2;
                attr.mtime = (int) CifsUtils.SMBTimeToStamp(fileInfo.lastModifyTime);
            }
            if (hasChange(fileInfo.uid)) {
                attr.hasUid = 2;
                attr.uid = (int) fileInfo.uid;
            }
            if (hasChange(fileInfo.gid)) {
                attr.hasGid = 2;
                attr.gid = (int) fileInfo.uid;
            }
            if (hasChange(fileInfo.rw)) {
                attr.hasMode = 1;
                attr.mode = (int) fileInfo.rw;
            }
        }
    }
}
