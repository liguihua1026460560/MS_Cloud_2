
package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_BAD_SEQID;

@Log4j2
@Data
@Accessors(chain = true)
@NoArgsConstructor
public class StateOwner implements Serializable {
    public Owner owner = new Owner();
    private int seq;

    public static StateOwner ANONYMOUS_STATE_OWNER = new StateOwner(-1, new byte[0], 0);
    public static StateOwner ANONYMOUS_STATE_OWNER1 = new StateOwner(-1, new byte[]{-1}, 0);

    public StateOwner(long clientId, byte[] ownerByte, int seq) {
        this.owner.owner = ownerByte;
        this.owner.clientId = clientId;
        this.seq = seq;
    }

    public synchronized void incrSequence(int openSeqId) {
        int next = seq + 1;
        if (next != openSeqId) {
            throw new NFSException(NFS4ERR_BAD_SEQID, "bad seqId");
        }
        seq = next;
    }

    public static Owner newOwner(long clientId, byte[] owner) {
        return new Owner(clientId, owner);
    }

    @Override
    public StateOwner clone() {
        return new StateOwner().setOwner(owner).setSeq(seq);
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class Owner {
        public long clientId;
        public byte[] owner = new byte[0];

        @Override
        public String toString() {
            return "Owner{" +
                    "clientId=" + clientId +
                    ", owner=" + new String(owner) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Owner owner1 = (Owner) o;
            return clientId == owner1.clientId && Arrays.equals(owner, owner1.owner);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(clientId);
            result = 31 * result + Arrays.hashCode(owner);
            return result;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateOwner that = (StateOwner) o;
        return Objects.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner);
    }

    @Override
    public String toString() {
        return "StateOwner{" +
                "owner=" + owner +
                ", seq=" + seq +
                '}';
    }

}
