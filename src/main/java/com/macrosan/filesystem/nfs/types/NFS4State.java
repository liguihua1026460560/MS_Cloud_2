
package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import java.util.*;


@Accessors(chain = true)
@AllArgsConstructor
@Data
@Log4j2
public class NFS4State {
    private StateId stateId;
    private StateOwner owner;
    private boolean confirmed = false;
    private boolean disposed = false;
    private final int type;


    private final NFS4State openState;
    private int shareAccess;
    private int shareDeny;
    private int newShareAccess;
    private int newShareDeny;
    private int lastUpdateSeqId;
    private NFS4Client client;
    private final List<StateDisposeListener> disposeListeners;
    private final Map<NFS4Lock, StateDisposeListener> lockDisposeMap;

//    public NFS4State(StateOwner owner, StateId stateId, int type, NFS4Client client) {
//        this(null, owner, stateId, type, client);
//    }

    public NFS4State(NFS4State openState, StateOwner owner, StateId stateId, int type, int shareAccess, int shareDeny, int lastUpdateSeqId, NFS4Client client) {
        this.openState = openState;
        this.owner = owner;
        this.stateId = stateId;
        this.shareAccess = shareAccess;
        this.shareDeny = shareDeny;
        this.newShareAccess = shareAccess;
        this.newShareDeny = shareDeny;
        this.lastUpdateSeqId = lastUpdateSeqId;
        this.type = type;
        this.client = client;
        disposeListeners = new ArrayList<>();
        lockDisposeMap = new HashMap<>();
    }

    public void incrSeqId() {
        stateId.seqId++;
    }

    public StateId stateId() {
        return stateId;
    }

    public void confirm() {
        confirmed = true;
    }

    public void setStateId(StateId stateId) {
        this.stateId = stateId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NFS4State nfs4State = (NFS4State) o;
        return stateId.equals(nfs4State.stateId) && owner.equals(nfs4State.owner) && client.equals(nfs4State.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateId, owner, client);
    }


    @Override
    public NFS4State clone() {
        return new NFS4State(stateId.clone(), owner.clone(), confirmed, disposed, type,
                openState == null ? null : openState.clone(), shareAccess, shareDeny, newShareAccess, newShareDeny, lastUpdateSeqId, client,
                new ArrayList<>(disposeListeners), new HashMap<>(lockDisposeMap));
    }

    public boolean getConfirmed() {
        return confirmed;
    }

    public int type() {
        return type;
    }

    public void tryDispose() {
        synchronized (this) {
            if (!disposed) {
                Iterator<StateDisposeListener> i = disposeListeners.iterator();
                while (i.hasNext()) {
                    StateDisposeListener listener = i.next();
                    listener.notifyDisposed(this);
                    i.remove();
                }
                disposed = true;
            }
        }

    }


    public void disposeIgnoreFailures() {
        synchronized (this) {
            if (!disposed) {
                Iterator<StateDisposeListener> i = disposeListeners.iterator();
                while (i.hasNext()) {
                    StateDisposeListener listener = i.next();
                    try {
                        listener.notifyDisposed(this);
                    } catch (Exception e) {
//                        log.info("failed to notify: {}", e.getMessage());
                    }
                    i.remove();
                }
                lockDisposeMap.clear();
                disposed = true;
            }
        }
    }

    public NFS4State getOpenState() {
        return openState == null ? this : openState;
    }


    public StateOwner getStateOwner() {
        return owner;
    }

    public void addDisposeListener(StateDisposeListener disposeListener) {
        synchronized (this) {
            disposeListeners.add(disposeListener);
        }
    }

    public void addLockDispose(NFS4Lock lock, StateDisposeListener disposeListener) {
        synchronized (this) {
            disposeListeners.add(disposeListener);
            lockDisposeMap.put(lock, disposeListener);
        }
    }

    public void removeLockDispose(NFS4Lock lock) {
        synchronized (this) {
            StateDisposeListener lockDispose = lockDisposeMap.get(lock);
            if (lockDispose != null) {
                disposeListeners.remove(lockDispose);
                lockDisposeMap.remove(lock);
            }
        }
    }


    public interface StateDisposeListener {
        void notifyDisposed(NFS4State state);
    }

    @Override
    public String toString() {
        return "NFS4State{" +
                "stateId=" + stateId +
                ", owner=" + owner +
                ", confirmed=" + confirmed +
                ", disposed=" + disposed +
                ", type=" + type +
                ", openState=" + null +
                ", shareAccess=" + shareAccess +
                ", shareDeny=" + shareDeny +
                ", client=" + client +
                ", disposeListeners=" + disposeListeners +
                ", lockDisposeMap=" + lockDisposeMap +
                '}';
    }
}
