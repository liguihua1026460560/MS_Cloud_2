package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/e5a70738-7ee4-46d9-a5f7-6644daa49a51
@EqualsAndHashCode(callSuper = true)
@Data
public class FsQuotaInfo extends GetInfoReply.Info {
    public long defaultQuotaThreshold = 0xFFFFFFFFFFFFFFFFL;
    public long defaultQuotaLimit = 0xFFFFFFFFFFFFFFFFL;
    public int fsControlFlags = FILE_VC_QUOTA_ENFORCE | FILE_VC_QUOTA_TRACK;
    //padding 4

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        offset += 24;
        defaultQuotaThreshold = defaultQuotaThreshold ==0 ? -1 : defaultQuotaThreshold;
        defaultQuotaLimit = defaultQuotaLimit == 0 ? -1 : defaultQuotaLimit;
        buf.setLongLE(offset, defaultQuotaThreshold);
        buf.setLongLE(offset + 8, defaultQuotaLimit);
        buf.setIntLE(offset + 16, fsControlFlags);
        return 48;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        defaultQuotaThreshold = buf.getLongLE(offset + 24);
        defaultQuotaLimit = buf.getLongLE(offset + 32);
        fsControlFlags = buf.getIntLE(offset + 40);
        if (fsControlFlags != 0) {
            fsControlFlags |= FILE_VC_QUOTA_TRACK;
        }
        defaultQuotaThreshold = defaultQuotaThreshold < 0 ? 0 : defaultQuotaThreshold;
        defaultQuotaLimit = defaultQuotaLimit < 0 ? 0 : defaultQuotaLimit;
        return 48;
    }

    @Override
    public int size() {
        return 48;
    }

    //fsControlFlags
    public static final int FILE_VC_CONTENT_INDEX_DISABLED = 0x00000008;
    public static final int FILE_VC_LOG_QUOTA_LIMIT = 0x00000020;
    public static final int FILE_VC_LOG_QUOTA_THRESHOLD = 0x00000010;
    public static final int FILE_VC_LOG_VOLUME_LIMIT = 0x00000080;
    public static final int FILE_VC_LOG_VOLUME_THRESHOLD = 0x00000040;
    public static final int FILE_VC_QUOTA_ENFORCE = 0x00000002;
    public static final int FILE_VC_QUOTA_TRACK = 0x00000001;
    public static final int FILE_VC_QUOTAS_INCOMPLETE = 0x00000100;
    public static final int FILE_VC_QUOTAS_REBUILDING = 0x00000200;
}
