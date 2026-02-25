package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.ChangeInfo;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.ToString;

import static com.macrosan.filesystem.nfs.types.FAttr4.*;

@ToString
public class OpenV4Reply extends CompoundReply {
    public StateId stateId;
    public ChangeInfo changeInfo = new ChangeInfo();
    public int resultFlags;
    public int maskLen;
    public int[] mask;
    public OpenDelegation openDelegation = new OpenDelegation();


    public OpenV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, stateId.seqId);
        offset += 12;
        buf.setBytes(offset, stateId.other);
        offset += 12;
        offset += changeInfo.writeStruct(buf, offset);
        buf.setInt(offset, resultFlags);
        buf.setInt(offset + 4, maskLen);
        offset += 8;
        for (int i = 0; i < maskLen; i++) {
            buf.setInt(offset + 4 * i, mask[i]);
        }
        offset += 4 * maskLen;
        offset += openDelegation.writeStruct(buf, offset);
        return offset - start;
    }

    @Data
    public static class OpenDelegation {
        public int delegateType;
        public ReadDelegation readDelegation = new ReadDelegation();
        public WriteDelegation writeDelegation = new WriteDelegation();
        public NoneDelegation noneDelegation = new NoneDelegation();

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, delegateType);
            offset += 4;
            switch (delegateType) {
                case OPEN_DELEGATE_NONE:
                    break;
                case OPEN_DELEGATE_READ:
                    offset += readDelegation.writeStruct(buf, offset);
//                    buf.setBoolean(offset + 16, recall);
//                    offset += 20;
                    //NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE 0
//                    buf.setInt(offset, 0);
                    // ace flag 0 ace mask 0 who(len content) 0
//                    offset += 16;
                    break;
                case OPEN_DELEGATE_WRITE:
                    offset += writeDelegation.writeStruct(buf, offset);
//                    offset += 16;
                    //0
//                    buf.setBoolean(offset, recall);
                    //space_limit
                    //NFS4_LIMIT_SIZE 1
//                    buf.setInt(offset + 4, 1);
                    //0 0
//                    offset += 16;
                    //ace permissions
                    //NFS4_ACE_ACCESS_ALLOWED_ACE_TYPE 0
//                    buf.setInt(offset, 0);
                    // 0 0 0
//                    offset += 16;
                    break;
                case OPEN_DELEGATE_NONE_EXT:
                    offset += noneDelegation.writeStruct(buf, offset);
                    break;
            }
            return offset - start;
        }
    }

    @Data
    public static class ReadDelegation {
        public StateId stateId = new StateId();
        public boolean recall;
        public NfsAce4 permissions = new NfsAce4();

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            offset += stateId.writeStruct(buf, offset);
            buf.setBoolean(offset, recall);
            offset += 4;
            offset += permissions.writeStruct(buf, offset);
            return offset - start;
        }
    }

    @Data
    public static class WriteDelegation {
        public StateId stateId = new StateId();
        public boolean recall;
        public SpaceLimit spaceLimit = new SpaceLimit();
        public NfsAce4 permissions = new NfsAce4();

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            offset += stateId.writeStruct(buf, offset);
            buf.setBoolean(offset, recall);
            offset += 4;
            offset += spaceLimit.writeStruct(buf, offset);
            offset += permissions.writeStruct(buf, offset);
            return offset - start;
        }
    }

    @Data
    public static class NoneDelegation {
        public int ondWhy;
        public boolean ondServerWillPushDeleg;
        public boolean ondServerWillSignalAvail;

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setInt(offset, ondWhy);
            switch (ondWhy) {
                case WND4_CONTENTION:
                    buf.setBoolean(offset + 4, ondServerWillPushDeleg);
                    break;
                case WND4_RESOURCE:
                    buf.setBoolean(offset + 4, ondServerWillSignalAvail);
                    break;
                default:
                    break;
            }
            return 8;
        }
    }

    @Data
    public static class NfsAce4 {
        public int type;
        public int flag;
        public int aceMask;
        public byte[] who = new byte[0];

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, type);
            buf.setInt(offset + 4, flag);
            buf.setInt(offset + 8, aceMask);
            buf.setInt(offset + 12, who.length);
            buf.setBytes(offset + 16, who);
            offset += 16 + (who.length + 3) / 4 * 4;
            return offset - start;
        }
    }

    @Data
    public static class SpaceLimit {
        public int limitBy = NFS_LIMIT_SIZE;
        public long fileSize;
        public int numBlocks;
        public int bytePerBlocks;

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setInt(offset, limitBy);
            switch (limitBy) {
                case NFS_LIMIT_SIZE:
                    buf.setLong(offset + 4, fileSize);
                    break;
                case NFS_LIMIT_BLOCKS:
                    buf.setInt(offset + 4, numBlocks);
                    buf.setInt(offset + 8, bytePerBlocks);
                    break;
                default:
                    break;
            }
            return 12;
        }
    }

    public static final int OPEN_DELEGATE_NONE = 0;
    //代理读权限
    public static final int OPEN_DELEGATE_READ = 1;
    public static final int OPEN_DELEGATE_WRITE = 2;
    public static final int OPEN_DELEGATE_NONE_EXT = 3;
    //why no delegation type  4.1
    public static final int WND4_NOT_WANTED = 0;
    public static final int WND4_CONTENTION = 1;
    public static final int WND4_RESOURCE = 2;
    public static final int WND4_NOT_SUPP_FTYPE = 3;
    public static final int WND4_WRITE_DELEG_NOT_SUPP_FTYPE = 4;
    public static final int WND4_NOT_SUPP_UPGRADE = 5;
    public static final int WND4_NOT_SUPP_DOWNGRADE = 6;
    public static final int WND4_CANCELLED = 7;
    public static final int WND4_IS_DIR = 8;

    public static final int NFS_LIMIT_SIZE = 1;
    public static final int NFS_LIMIT_BLOCKS = 2;
}
