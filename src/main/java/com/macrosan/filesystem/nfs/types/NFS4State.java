
package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;


@Accessors(chain = true)
@AllArgsConstructor
@Data
@Log4j2
public class NFS4State {
    private StateId stateId;
    private StateOwner owner;
    private AtomicBoolean confirmed = new AtomicBoolean(false);
    private AtomicBoolean disposed = new AtomicBoolean(false);
    private final int type;
    private long nodeId;
    private String bucket;
    private String objName;
    private final NFS4State openState;
    private int shareAccess;
    private int shareDeny;
    private int lastUpdateSeqId;
    private NFS4Client client;
    private final ConcurrentLinkedQueue<StateDisposeListener> disposeListeners;
    private final Map<NFS4Lock, StateDisposeListener> lockDisposeMap;

//    public NFS4State(StateOwner owner, StateId stateId, int type, NFS4Client client) {
//        this(null, owner, stateId, type, client);
//    }

    public NFS4State(NFS4State openState, StateOwner owner, StateId stateId, int type, long nodeId, String bucket, String objName, int shareAccess, int shareDeny, int lastUpdateSeqId, NFS4Client client) {
        this.openState = openState;
        this.owner = owner;
        this.stateId = stateId;
        this.shareAccess = shareAccess;
        this.shareDeny = shareDeny;
        this.lastUpdateSeqId = lastUpdateSeqId;
        this.type = type;
        this.client = client;
        this.nodeId = nodeId;
        this.bucket = bucket;
        this.objName = objName;
        disposeListeners = new ConcurrentLinkedQueue<>();
        lockDisposeMap = new ConcurrentHashMap<>();
    }

    public void incrSeqId() {
        stateId.seqId++;
    }

    public StateId stateId() {
        return stateId;
    }

    public void confirm() {
        confirmed.set(true);
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
        return new NFS4State(stateId.clone(), owner.clone(), confirmed, disposed, type, nodeId, bucket, objName,
                openState == null ? null : openState.clone(), shareAccess, shareDeny, lastUpdateSeqId, client,
                new ConcurrentLinkedQueue<>(disposeListeners), new HashMap<>(lockDisposeMap));
    }

    public boolean getConfirmed() {
        return confirmed.get();
    }

    public int type() {
        return type;
    }

    public Mono<Boolean> tryDispose() {
        List<StateDisposeListener> dispose0 = new ArrayList<>(disposeListeners);
        if (!disposed.get()) {
            return Flux.fromIterable(dispose0)
                    .flatMap(i -> i.notifyDisposed(this).doOnNext(f -> disposeListeners.remove(i)))
                    .collectList().doOnNext(v -> {
                        disposed.set(true);
                    }).map(v -> true);
        }
        return Mono.just(true);
    }


    public Mono<Boolean> disposeIgnoreFailures() {
        List<StateDisposeListener> dispose0 = new ArrayList<>(disposeListeners);
        if (!disposed.get()) {
            return Flux.fromIterable(dispose0)
                    .flatMap(i -> i.notifyDisposed(this).doOnNext(f -> disposeListeners.remove(i)))
                    .collectList().doOnNext(v -> {
                        disposed.set(true);
                        lockDisposeMap.clear();
                    }).map(v -> true);
        }
        return Mono.just(true);
    }

    public NFS4State getOpenState() {
        return openState == null ? this : openState;
    }


    public StateOwner getStateOwner() {
        return owner;
    }

    public void addDisposeListener(StateDisposeListener disposeListener) {
        disposeListeners.add(disposeListener);
    }

    public void addLockDispose(NFS4Lock lock, StateDisposeListener disposeListener) {
        lockDisposeMap.compute(lock, (k, v) -> {
            disposeListeners.add(disposeListener);
            return disposeListener;
        });
    }

    public void removeLockDispose(NFS4Lock lock) {
        if (lock.removeOwner()) {
            List<StateDisposeListener> stateDisposeListeners = new ArrayList<>(lockDisposeMap.values());
            for (StateDisposeListener disposeListener : stateDisposeListeners) {
                disposeListeners.remove(disposeListener);
            }
            lockDisposeMap.clear();
        } else {
            lockDisposeMap.compute(lock, (k, v) -> {
                if (v != null) {
                    disposeListeners.remove(v);
                }
                return null;
            });
        }
    }


    public interface StateDisposeListener {
        Mono<Boolean> notifyDisposed(NFS4State state);
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
                ", lastUpdateSeqId=" + lastUpdateSeqId +

                '}';
    }
}
