package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.nfs.types.StateOwner;
import com.macrosan.httpserver.ServerConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

import static com.macrosan.filesystem.nfs.api.NFS4Proc.clientControl;
import static com.macrosan.filesystem.nfs.call.v4.LockV4Call.*;

@NoArgsConstructor
@Data
@AllArgsConstructor
public class NFS4Lock extends Lock {
    public int lockType;
    public StateOwner stateOwner = new StateOwner();
    public long offset;
    public long length;
    public String node;
    public long clientId;
//    public String versionNum;

    public static final NFS4Lock DEFAULT_LOCK = new NFS4Lock();
    public static final NFS4Lock ERROR_LOCK = new NFS4Lock().setOffset(-1);
    public static final NFS4Lock NOMATCH_LOCK = new NFS4Lock().setOffset(-2);
    public static final NFS4Lock GET_LOCK = new NFS4Lock().setOffset(-3);


    public static final int  GET_LOCK_TYPE = -3;

    public static NFS4Lock newLock(int lockType, StateOwner stateOwner, long offset, long size, String node, long clientId) {
        return new NFS4Lock(lockType, stateOwner, offset, size, node, clientId);
    }

    public NFS4Lock setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFS4Lock nfs4Lock = (NFS4Lock) o;
        return offset == nfs4Lock.offset && length == nfs4Lock.length && stateOwner.equals(nfs4Lock.stateOwner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateOwner, offset, length);
    }

    @Override
    public boolean needKeep() {
        return node.equals(ServerConfig.getInstance().getHostUuid()) && clientControl.hasClient(clientId);
    }


    public boolean sameOwner(NFS4Lock other) {
        return this.stateOwner.equals(other.stateOwner);
    }

    public boolean typeConflict(NFS4Lock other) {
        return lockType == WRITE_LT || lockType == WRITEW_LT
                || other.lockType == WRITE_LT || other.lockType == WRITEW_LT;
    }

    public boolean typeConflict() {
        return lockType == WRITE_LT || lockType == WRITEW_LT;
    }


    public boolean conflict(NFS4Lock other) {
        return existOverRange(other) && !sameOwner(other) && typeConflict(other);
    }


    public boolean existOverRange(NFS4Lock other) {
        if (other.length == UINT64_MAX && length == UINT64_MAX) {
            return true;
        }
        if (other.length == UINT64_MAX) {
            return offset > other.offset || other.offset - length < offset;
        }
        if (length == UINT64_MAX) {
            return other.offset > offset || offset - other.length < other.offset;
        }

        if (offset > other.offset) {
            return offset - other.offset < other.length;
        }

        if (other.offset > offset) {
            return other.offset - offset < length;
        }
        return true;
    }
}