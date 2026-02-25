package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.*;
import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.macrosan.filesystem.cifs.call.smb2.GetInfoCall.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2024/8/1
 * @Description: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/ee9614c4-be54-4a3c-98f1-769a7032a0e4
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SetInfoCall extends SMB2Body {
    byte infoType;       // 1byte
    byte fileInfoClass;  // 1byte For setting file information
    int bufferLength;    // 4byte The length, in bytes, of the information to be set.
    short bufferOffset;  // 2byte The offset, in bytes, from the beginning of the SMB2 header to the information to be set
    int additionalInfo;  // 4byte Provides additional information to the server
    SMB2FileId fileId;   // 16byte
    GetInfoReply.Info info;  // A variable-length buffer that contains the information being set for the request

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);
        infoType = buf.getByte(start);
        fileInfoClass = buf.getByte(start + 1);
        bufferLength = buf.getIntLE(start + 2);
        bufferOffset = buf.getShortLE(start + 6);
        additionalInfo = buf.getIntLE(start + 10);
        fileId = new SMB2FileId();
        fileId.readStruct(buf, start + 14);
        if (bufferLength > 0) {
            ByteBuf slice = buf.slice(start + 30, bufferLength);
            switch (fileInfoClass) {
                //SECURITY INFO
                case 0:
                    if (additionalInfo != 0) {
                        info = new SecurityInfo();
                        info.readStruct(slice, 0);
                    }
                    break;
                case FSCC_FILE_BASIC_INFORMATION:
                    info = new FileBasicInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_RENAME_INFORMATION:
                    info = new FileRenameInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_LINK_INFORMATION:
                    info = new FileLinkInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_END_OF_FILE_INFORMATION:
                    info = new FileEndInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_DISPOSITION_INFORMATION:
                    info = new FileDispositionInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_ALLOCATION_INFORMATION:
                    info = new FileAllocationInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FILE_QUOTA_INFORMATION:
                    info = new FileQuotaInfo();
                    info.readStruct(slice, 0);
                    break;
                case FSCC_FS_QUOTA_INFORMATION:
                    info = new FsQuotaInfo();
                    info.readStruct(slice, 0);
                    break;
                default:
                    break;
            }
        }

        return 30 + bufferLength;
    }
}
