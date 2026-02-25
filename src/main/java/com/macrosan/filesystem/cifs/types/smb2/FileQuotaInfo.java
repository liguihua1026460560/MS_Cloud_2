package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class FileQuotaInfo extends GetInfoReply.Info {

    public int nextOffset;
    public int sidLength;
    public long changeTime;
    public long quotaUsed;
    public long quotaThreshold;
    public long quotaLimit;
    public SID ownerSID;

    public FileQuotaInfo() {
    }

    public FileQuotaInfo(long quotaUsed, long quotaThreshold, long quotaLimit, Inode inode) {
        nextOffset = 0;
        changeTime = 0;
        this.quotaUsed = quotaUsed;
        this.quotaThreshold = quotaThreshold;
        this.quotaLimit = quotaLimit;
        String userSidStr = FSIdentity.getUserSIDByUid(inode.getUid());
        this.ownerSID = SID.generateSID(userSidStr);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, nextOffset);
        if (ownerSID != null) {
            sidLength = ownerSID.size();
        }
        buf.setIntLE(offset + 4, sidLength);
        buf.setLongLE(offset + 8, changeTime);
        buf.setLongLE(offset + 16, quotaUsed);
        buf.setLongLE(offset + 24, quotaThreshold);
        buf.setLongLE(offset + 32, quotaLimit);
        if (ownerSID == null) {
            return 40;
        }
        return ownerSID.writeStruct(buf, offset + 40) + 40;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        nextOffset = buf.getIntLE(offset);
        sidLength = buf.getIntLE(offset + 4);
        changeTime = buf.getLongLE(offset + 8);
        quotaUsed = buf.getLongLE(offset + 16);
        quotaThreshold = buf.getLongLE(offset + 24);
        quotaLimit = buf.getLongLE(offset + 32);
        ownerSID = new SID();
        int sidLen = ownerSID.readStruct(buf, offset + 40);
        if (quotaLimit < 0) {
            quotaLimit = 0;
        }
        if (quotaThreshold < 0) {
            quotaThreshold = 0;
        }
        return 40 + sidLen;
    }

    @Override
    public int size() {
        return 40 + ownerSID.size();
    }
}
