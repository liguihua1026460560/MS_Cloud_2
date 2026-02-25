package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.QueryQuotaInfo;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d623b2f7-a5cd-4639-8cc9-71fa7d9f9ba9
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GetInfoCall extends SMB2Body {
    byte infoType;       // 1byte
    byte fileInfoClass;  // 1byte
    int responseLen;
    short bufOff;
    int bufLen;
    int additionalInfo;
    int flags;
    SMB2FileId fileId;
    QueryQuotaInfo quotaInfo = new QueryQuotaInfo();

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);
        infoType = buf.getByte(start);
        fileInfoClass = buf.getByte(start + 1);
        responseLen = buf.getIntLE(start + 2);
        bufOff = buf.getShortLE(start + 6);
        bufLen = buf.getIntLE(start + 10);
        additionalInfo = buf.getIntLE(start + 14);
        flags = buf.getIntLE(start + 18);
        fileId = new SMB2FileId();
        fileId.readStruct(buf, start + 22);
        int quotaInfoLen = 0;
        if (SMB2_0_INFO_QUOTA == infoType) {
            quotaInfoLen = quotaInfo.readStruct(buf, start + 38);
        }
        if (bufLen > 0) {
            return 40 + bufLen + quotaInfoLen;
        } else {
            return 41 + quotaInfoLen;
        }
    }

    //infoType
    public static final byte SMB2_0_INFO_FILE = 0x01;
    public static final byte SMB2_0_INFO_FILESYSTEM = 0x02;
    public static final byte SMB2_0_INFO_SECURITY = 0x03;
    public static final byte SMB2_0_INFO_QUOTA = 0x04;

    //infoType=SMB2_0_INFO_FILE
    public static final byte FSCC_FILE_DIRECTORY_INFORMATION = 1;
    public static final byte FSCC_FILE_FULL_DIRECTORY_INFORMATION = 2;
    public static final byte FSCC_FILE_BOTH_DIRECTORY_INFORMATION = 3;
    public static final byte FSCC_FILE_BASIC_INFORMATION = 4;
    public static final byte FSCC_FILE_STANDARD_INFORMATION = 5;
    public static final byte FSCC_FILE_INTERNAL_INFORMATION = 6;
    public static final byte FSCC_FILE_EA_INFORMATION = 7;
    public static final byte FSCC_FILE_ACCESS_INFORMATION = 8;
    public static final byte FSCC_FILE_NAME_INFORMATION = 9;
    public static final byte FSCC_FILE_RENAME_INFORMATION = 10;
    public static final byte FSCC_FILE_LINK_INFORMATION = 11;
    public static final byte FSCC_FILE_NAMES_INFORMATION = 12;
    public static final byte FSCC_FILE_DISPOSITION_INFORMATION = 13;
    public static final byte FSCC_FILE_POSITION_INFORMATION = 14;
    public static final byte FSCC_FILE_FULL_EA_INFORMATION = 15;
    public static final byte FSCC_FILE_MODE_INFORMATION = 16;
    public static final byte FSCC_FILE_ALIGNMENT_INFORMATION = 17;
    public static final byte FSCC_FILE_ALL_INFORMATION = 18;
    public static final byte FSCC_FILE_ALLOCATION_INFORMATION = 19;
    public static final byte FSCC_FILE_END_OF_FILE_INFORMATION = 20;
    public static final byte FSCC_FILE_ALTERNATE_NAME_INFORMATION = 21;
    public static final byte FSCC_FILE_STREAM_INFORMATION = 22;
    public static final byte FSCC_FILE_PIPE_INFORMATION = 23;
    public static final byte FSCC_FILE_PIPE_LOCAL_INFORMATION = 24;
    public static final byte FSCC_FILE_PIPE_REMOTE_INFORMATION = 25;
    public static final byte FSCC_FILE_MAILSLOT_QUERY_INFORMATION = 26;
    public static final byte FSCC_FILE_MAILSLOT_SET_INFORMATION = 27;
    public static final byte FSCC_FILE_COMPRESSION_INFORMATION = 28;
    public static final byte FSCC_FILE_OBJECTID_INFORMATION = 29;
    public static final byte FSCC_FILE_COMPLETION_INFORMATION = 30;
    public static final byte FSCC_FILE_MOVE_CLUSTER_INFORMATION = 31;
    public static final byte FSCC_FILE_QUOTA_INFORMATION = 32;
    public static final byte FSCC_FILE_REPARSEPOINT_INFORMATION = 33;
    public static final byte FSCC_FILE_NETWORK_OPEN_INFORMATION = 34;
    public static final byte FSCC_FILE_ATTRIBUTE_TAG_INFORMATION = 35;
    public static final byte FSCC_FILE_TRACKING_INFORMATION = 36;
    public static final byte FSCC_FILE_ID_BOTH_DIRECTORY_INFORMATION = 37;
    public static final byte FSCC_FILE_ID_FULL_DIRECTORY_INFORMATION = 38;
    public static final byte FSCC_FILE_VALID_DATA_LENGTH_INFORMATION = 39;
    public static final byte FSCC_FILE_SHORT_NAME_INFORMATION = 40;
    public static final byte FSCC_FILE_SFIO_RESERVE_INFORMATION = 44;
    public static final byte FSCC_FILE_SFIO_VOLUME_INFORMATION = 45;
    public static final byte FSCC_FILE_HARD_LINK_INFORMATION = 46;
    public static final byte FSCC_FILE_NORMALIZED_NAME_INFORMATION = 48;
    public static final byte FSCC_FILE_ID_GLOBAL_TX_DIRECTORY_INFORMATION = 50;
    public static final byte FSCC_FILE_STANDARD_LINK_INFORMATION = 54;
    public static final byte FSCC_FILE_MAXIMUM_INFORMATION = 55;

    //infoType=SMB2_0_INFO_FILESYSTEM
    public static final byte FSCC_FS_VOLUME_INFORMATION = 1;
    //not sup
    public static final byte FSCC_FS_LABEL_INFORMATION = 2;
    public static final byte FSCC_FS_SIZE_INFORMATION = 3;
    public static final byte FSCC_FS_DEVICE_INFORMATION = 4;
    public static final byte FSCC_FS_ATTRIBUTE_INFORMATION = 5;
    public static final byte FSCC_FS_QUOTA_INFORMATION = 6;
    public static final byte FSCC_FS_FULL_SIZE_INFORMATION = 7;
    public static final byte FSCC_FS_OBJECTID_INFORMATION = 8;
    public static final byte FSCC_FS_SECTOR_SIZE_INFORMATION = 11;

    //securityInfo
    public static final int OWNER_SECURITY_INFORMATION = 0x00000001;
    public static final int GROUP_SECURITY_INFORMATION = 0x00000002;
    public static final int DACL_SECURITY_INFORMATION = 0x00000004;
    public static final int SACL_SECURITY_INFORMATION = 0x00000008;
    public static final int LABEL_SECURITY_INFORMATION = 0x00000010;
    public static final int ATTRIBUTE_SECURITY_INFORMATION = 0x00000020;
    public static final int SCOPE_SECURITY_INFORMATION = 0x00000040;
    public static final int BACKUP_SECURITY_INFORMATION = 0x00010000;
    public static final Integer[] SCE_INFO_ARR = {OWNER_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION, DACL_SECURITY_INFORMATION, DACL_SECURITY_INFORMATION, LABEL_SECURITY_INFORMATION, ATTRIBUTE_SECURITY_INFORMATION, SCOPE_SECURITY_INFORMATION, BACKUP_SECURITY_INFORMATION};


}
