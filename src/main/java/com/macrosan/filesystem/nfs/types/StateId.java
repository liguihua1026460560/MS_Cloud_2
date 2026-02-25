package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.NFSException;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_BAD_STATEID;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_OLD_STATEID;

@Accessors(chain = true)
@Slf4j
@Data
public class StateId {
    public int seqId;
    public byte[] other = new byte[12];

    public StateId(int seqId, byte[] other) {
        this.seqId = seqId;
        this.other = other;
    }

    public StateId() {
    }

    //stateId type
    public static final int NFS4_OPEN_STID = 1;
    public static final int NFS4_LOCK_STID = 2;
    public static final int NFS4_DELEG_STID = 4;
    public static final int NFS4_CLOSED_STID = 8;
    public static final int NFS4_REVOKED_DELEG_STID = 16;
    //原生 delereturn后stateId type NFS4_DELEG_STID -->  NFS4_CLOSED_DELEG_STID
    public static final int NFS4_CLOSED_DELEG_STID = 32;
    public static final int NFS4_LAYOUT_STID = 64;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateId stateId = (StateId) o;
        return Arrays.equals(other, stateId.other);
    }

    @Override
    public StateId clone() {
        return new StateId()
                .setSeqId(seqId)
                .setOther(other);
    }

    @Override
    public String toString() {
        return "StateId{" +
                "seqId=" + seqId +
                ", other=" + Arrays.toString(other) +
                '}';
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(other);
    }

    public final static StateId CURRENT_STATEID =
            new StateId(1, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    public final static StateId INVAL_STATEID =
            new StateId(0xffffffff, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    public final static StateId ZERO_STATEID =
            new StateId(0, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    public final static StateId ONE_STATEID =
            new StateId(0xffffffff,
                    new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
    public final static StateId BAD_STATEID =
            new StateId(-1, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    public static StateId invalidStateId() {
        return INVAL_STATEID;
    }

    public static StateId zeroStateId() {
        return ZERO_STATEID;
    }

    public static StateId oneStateId() {
        return ONE_STATEID;
    }

    public static boolean isStateLess(StateId stateId) {
        return stateId.equalsWithSeq(ZERO_STATEID) || stateId.equalsWithSeq(ONE_STATEID);
    }

    public boolean equalsWithSeq(StateId otherState) {
        if (otherState == this) {
            return true;
        }
        return otherState.seqId == this.seqId && Arrays.equals(this.other, otherState.other);
    }

    public static void checkStateId(StateId expected, StateId stateId) {
        if (stateId.seqId == 0) {
            return;
        }

        if (expected.seqId > stateId.seqId) {
            throw new NFSException(NFS4ERR_OLD_STATEID, "old stateId");
        }

        if (expected.seqId < stateId.seqId) {
            throw new NFSException(NFS4ERR_BAD_STATEID, "bad stateId");
        }
    }

    public static StateId getCurrStateIdIfNeeded(CompoundContext context, StateId stateId) {
        if (stateId.equalsWithSeq(CURRENT_STATEID)) {
            return context.getCurrStateId();
        }
        return stateId;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, seqId);
        buf.setBytes(offset + 4, other);
        return 16;
    }

    public int readStruct(ByteBuf buf, int offset) {
        seqId = buf.getInt(offset);
        other = new byte[12];
        buf.getBytes(offset + 4, other);
        return 16;
    }
}
