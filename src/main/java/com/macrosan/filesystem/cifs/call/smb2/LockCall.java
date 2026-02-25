package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

//https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/6178b960-48b6-4999-b589-669f88e9017d
public class LockCall extends SMB2Body {
    public short structSize;
    public short lockCount;
    public int lockSeqNum;
    public int lockSeqIndex;
    public SMB2FileId fileId = new SMB2FileId();
    public List<LockElement> locks = new LinkedList<>();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        structSize = buf.getShortLE(offset);
        lockCount = buf.getShortLE(offset + 2);
        int lockSeq = buf.getIntLE(offset + 4);
        lockSeqNum = lockSeq & 0x0F;
        lockSeqIndex = (lockSeq >>> 4) & 0xFFFFFFF;
        fileId.readStruct(buf, offset + 8);
        for (int i = 0; i < lockCount; i++) {
            LockElement lockElement = new LockElement();
            offset += lockElement.readStruct(buf, offset + 24);
            locks.add(lockElement);
        }
        return 24 * (lockCount + 1);
    }

    public static class LockElement extends SMB2Body {
        public long offset;
        public long len;
        public int flags;
        //reserved 4

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            this.offset = buf.getLongLE(offset);
            len = buf.getLongLE(offset + 8);
            flags = buf.getIntLE(offset + 16);
            return 24;
        }

        //flags
        public static final int SMB2_LOCKFLAG_SHARED_LOCK = 0x00000001;
        public static final int SMB2_LOCKFLAG_EXCLUSIVE_LOCK = 0x00000002;
        public static final int SMB2_LOCKFLAG_UNLOCK = 0x00000004;
        public static final int SMB2_LOCKFLAG_FAIL_IMMEDIATELY = 0x00000010;
    }
}
