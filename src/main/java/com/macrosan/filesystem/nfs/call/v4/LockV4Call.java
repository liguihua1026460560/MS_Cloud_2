package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.StateOwner;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.*;

@ToString
public class LockV4Call extends CompoundCall {
    public int lockType;
    public boolean reclaim;
    public long offset;
    public long length;
    public boolean isNewOwner;
    public int seqId;
    public StateOwner stateOwner;
    public long clientId;
    public byte[] owner;
    public StateId stateId = new StateId();
    public int lockSeqId;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        lockType = buf.getInt(offset);
        reclaim = buf.getBoolean(offset + 4);
        this.offset = buf.getLong(offset + 8);
        length = buf.getLong(offset + 16);
        isNewOwner = buf.getInt(offset + 24) == 1;
        if (isNewOwner) {
            seqId = buf.getInt(offset + 28);
            stateId.readStruct(buf, offset + 32);
            lockSeqId = buf.getInt(offset + 48);
            clientId = buf.getLong(offset + 52);
            offset += 60;
            int ownerLen = buf.getInt(offset);
            owner = new byte[ownerLen];
            buf.getBytes(offset + 4, owner);
            offset += (ownerLen + 3) / 4 * 4 + 4;
        } else {
            stateId.readStruct(buf, offset + 28);
            lockSeqId = buf.getInt(offset + 44);
            offset += 48;
        }
        return offset - start;
    }




    //lock type
    public static final int UNLOCK_LT = 0;//解除锁
    public static final int READ_LT = 1;//
    public static final int WRITE_LT = 2;//
    public static final int READW_LT = 3;//
    public static final int WRITEW_LT = 4;//
    public static final int UINT32_MAX = 0xffffffff;
    public static final int UINT64_MAX = 0xffffffff;
}

