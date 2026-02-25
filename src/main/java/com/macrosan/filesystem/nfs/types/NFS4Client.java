
package com.macrosan.filesystem.nfs.types;


import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.auth.Auth;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.v4.CompoundReply;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.FsConstants.RpcAuthType.*;
import static com.macrosan.filesystem.nfs.types.StateId.NFS4_LOCK_STID;

@Log4j2
public class NFS4Client {
    private final byte[] ownerId;
    private final byte[] verifier;
    private final Auth auth;
    private final long clientId;
    private final AtomicBoolean confirmed = new AtomicBoolean(false);
    private int sessionSequence = 1;
    private final Map<StateId, NFS4State> clientStates = new ConcurrentHashMap<>();
    private final Map<NFS4Session.Session, NFS4Session> sessions = new ConcurrentHashMap<>();
    private final Map<StateOwner.Owner, StateOwner> owners = new ConcurrentHashMap<>();
    private final Map<StateOwner.Owner, NFS4State> lockOwners = new ConcurrentHashMap<>();
    private final AtomicLong lastLeaseTime = new AtomicLong(System.currentTimeMillis());
    private final SocketAddress clientAddress;
    //    private final SocketAddress localAddress;
    private final long leaseTime;
    private boolean reclaimCompleted;
    private final int minorVersion;
    private final AtomicInteger stateIdCounter = new AtomicInteger(0);
    private final NFS4ClientControl clientControl;
    //4.0 delegate回调使用
    public NFSHandler nfsHandler;


    public NFS4Client(NFS4ClientControl clientControl, long clientId, int minorVersion, SocketAddress clientAddress,
                      byte[] ownerId, byte[] verifier, Auth auth, long leaseTime, NFSHandler nfsHandler) {

        this.clientControl = clientControl;
        this.ownerId = Arrays.copyOf(ownerId, ownerId.length);
        this.verifier = verifier;
        this.auth = auth;
        this.clientId = clientId;

        this.clientAddress = clientAddress;
//        this.localAddress = localAddress;
        this.leaseTime = leaseTime;
        this.minorVersion = minorVersion;
        reclaimCompleted = this.minorVersion == 0;
        this.nfsHandler = nfsHandler;
    }

    public Auth getAuth() {
        return auth;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public byte[] getOwnerId() {
        return ownerId;
    }

    public byte[] getVerifier() {
        return verifier;
    }

    public long getClientId() {
        return clientId;
    }

    public boolean verifierEquals(byte[] verifier) {
        return Arrays.equals(this.verifier, verifier);
    }

    public boolean getConfirmed() {
        return confirmed.get();
    }

    public void confirmed() {
        confirmed.set(true);
    }

    //    public void setBucketName(String bucketName) {
//        this.bucketName = bucketName;
//    }

//    public String getBucketName() {
//        return this.bucketName;
//    }

    public boolean checkAuth(Auth otherAuth) {
        if (auth.flavor == otherAuth.flavor) {
            switch (auth.flavor) {
                case RPC_AUTH_NULL:
                    return true;
                case RPC_AUTH_UNIX:
                    //暂时比较uid,gid,gids
                    AuthUnix authUnix = (AuthUnix) auth;
                    AuthUnix otherAuthUnix = (AuthUnix) otherAuth;
                    return authUnix.getUid() == otherAuthUnix.getUid() && authUnix.getGid() == otherAuthUnix.getGid()
                            && Arrays.equals(authUnix.getGids(), otherAuthUnix.getGids());
                case RPC_AUTH_GSS:
                    //todo
                    return true;
            }
        }
        return false;
    }

    public boolean leaseValid() {
        return (System.currentTimeMillis() - lastLeaseTime.get()) < leaseTime;
    }

    public void updateLeaseTime() {
        long currentTime = System.currentTimeMillis();
        long delta = currentTime - lastLeaseTime.get();
        if (delta > leaseTime) {
            throw new NFSException(NFS4ERR_EXPIRED, "leasTime expired");
        }
        lastLeaseTime.set(currentTime);
    }

    public void refreshLeaseTime() {
        lastLeaseTime.set(System.currentTimeMillis());
    }


    public synchronized void reset() {
        refreshLeaseTime();
        confirmed.set(false);
    }

    public SocketAddress getRemoteAddress() {
        return clientAddress;
    }


    public int currentSeqId() {
        return sessionSequence;
    }

    public NFS4State createAndPutState(StateOwner stateOwner, NFS4State openState, int type, NFS4State createState, int shareAccess, int shareDeny) {
        if (createState == null) {
            StateId stateId = clientControl.createStateId(clientId, stateIdCounter.incrementAndGet());
            createState = new NFS4State(openState, stateOwner, stateId, type, shareAccess, shareDeny, 0, this);
        }

        //关联stateId,通过openState释放时同时释放lockStateId
        if (openState != null) {
            NFS4State finalCreateState = createState;
            openState.addDisposeListener(s -> {
                NFS4State nfsState = clientStates.remove(finalCreateState.stateId());
                if (nfsState != null) {
                    nfsState.tryDispose();
                    if (type == NFS4_LOCK_STID) {
                        lockOwners.remove(stateOwner.getOwner());
                    }
                }
            });
        }
        clientStates.put(createState.stateId(), createState);
        //owner关联lockState,用于releaseOwner
        if (minorVersion == 0 && type == NFS4_LOCK_STID) {
            lockOwners.put(StateOwner.newOwner(clientId, stateOwner.owner.owner), createState);
        }
        return createState;
    }

    public NFS4State createAndPutState(StateOwner stateOwner, int type, NFS4State createState, int shareAccess, int shareDeny) {
        return createAndPutState(stateOwner, null, type, createState, shareAccess, shareDeny);
    }

    public void releaseState(StateId stateId, int type) {

        NFS4State state = clientStates.get(stateId);
        if (state == null) {
            throw new NFSException(NFS4ERR_BAD_STATEID, " releaseState: not exist stateId, bad stateId");
        }
        if (state.type() != type && type == NFS4_LOCK_STID) {
            throw new NFSException(NFS4ERR_LOCKS_HELD, "releaseState : this stateId not lock stateId");
        }
        //检查stateId是否存在锁
        if (state.type() == type && type == NFS4_LOCK_STID && !state.getLockDisposeMap().isEmpty()) {
            throw new NFSException(NFS4ERR_LOCKS_HELD, "releaseState : this stateId exist lock");
        }
        state.disposeIgnoreFailures();
        clientStates.remove(stateId);
    }

    public void releaseState0(StateId stateId, int type) {
        NFS4State state = clientStates.get(stateId);
        if (state != null && state.type() == type) {
            state.disposeIgnoreFailures();
            clientStates.remove(stateId);
        }
    }

    public void tryReleaseState(StateId stateId, int type) {
        clientStates.compute(stateId, (k, v) -> {
            if (v == null) {
                throw new NFSException(NFS4ERR_BAD_STATEID, " tryReleaseState : not exist stateId, bad stateId");
            }
            if (v.type() != type) {
                throw new NFSException(NFS4ERR_BAD_STATEID, " tryReleaseState : type not equals,call stateId type is " + type + ", server stateId type is " + v.type() + " ,bad stateId");
            }
            v.tryDispose();
            return null;
        });
    }


    public NFS4State state(StateId stateId) {
        NFS4State state = clientStates.get(stateId);
        if (state == null) {
            throw new NFSException(NFS4ERR_BAD_STATEID, "state : not exist stateId,bad stateId");
        }
        return state;
    }

    public NFS4State state(StateId stateId, CompoundReply reply) {
        NFS4State state = clientStates.get(stateId);
        if (state == null) {
            reply.status = NFS4ERR_BAD_STATEID;
        }
        return state;
    }

    public NFS4State state0(StateId stateId) {
        return clientStates.get(stateId);
    }

    public Mono<StateId> openDownGrade(CompoundContext context, NFS4Client client, StateId stateId, int shareAccess, int shareDeny, int seqId, CompoundReply reply) {
        AtomicReference<Tuple2<NFS4State, Boolean>> res = new AtomicReference<>();
        clientStates.compute(stateId, (state, openState) -> {
            if (openState == null) {
                throw new NFSException(NFS4ERR_BAD_STATEID, "state : not exist stateId,bad stateId");
            }
            StateId.checkStateId(openState.stateId(), stateId);
            if (context.getMinorVersion() == 0) {
                openState.getStateOwner().incrSequence(seqId);
            }
            openState.stateId().seqId++;
            int oldShareAccess = openState.getShareAccess();
            int oldShareDeny = openState.getShareDeny();
            res.set(new Tuple2<>(openState.clone().setShareAccess(oldShareAccess).setShareDeny(oldShareDeny), true));
            return openState;
        });
        NFS4State nfs4State = res.get().var1;
        return clientControl.getStateIdOps().downgradeOpen(client, nfs4State, context.getCurrentInode(), shareAccess, shareDeny, context, reply);

    }



    public NFS4State openConfirm(StateId stateId, int seqId) {
        return clientStates.compute(stateId, (state, openState) -> {
            if (openState == null) {
                throw new NFSException(NFS4ERR_BAD_STATEID, "state : not exist stateId,bad stateId");
            }
            openState.getStateOwner().incrSequence(seqId);
            openState.incrSeqId();
            openState.confirm();
            return openState;
        });
    }


    public NFS4State lockState(CompoundContext context, NFS4Client client, StateId stateId, byte[] owner, int lockSeqId, int seqId, boolean newOwner) {
        AtomicReference<NFS4State> res = new AtomicReference<>();
        clientStates.compute(stateId, (state, openState) -> {
            if (openState == null) {
                throw new NFSException(NFS4ERR_BAD_STATEID, "state : not exist stateId,bad stateId");
            }
            NFS4State lockState;
            StateOwner lockOwner;
            StateId.checkStateId(openState.stateId(), stateId);
            if (newOwner) {
                lockOwner = client.getOrCreateOwner(owner, lockSeqId, true);
                lockState = client.createAndPutState(lockOwner, openState, NFS4_LOCK_STID, null, 0, 0);
                lockState.confirm();
                if (context.getMinorVersion() == 0) {
                    openState.getStateOwner().incrSequence(seqId);
                    client.updateLeaseTime();
                }
            } else {
                lockState = client.state(stateId);
                if (lockState.type() != NFS4_LOCK_STID) {
                    throw new NFSException(NFS4ERR_BAD_STATEID, "lockState: not lock stateId");
                }
                lockOwner = lockState.getStateOwner();
                if (context.getMinorVersion() == 0) {
                    lockOwner.incrSequence(lockSeqId);
                    client.updateLeaseTime();
                }
            }
            res.set(lockState.clone());
            return openState;
        });
        return res.get();
    }

    public Collection<NFS4Session> sessions() {
        return sessions.values();
    }

    public synchronized NFS4Session createSession(int sequence, int cacheSize, int maxOps, int maxCbOps, NFSHandler nfsHandler, int cbProgram) {
        if (sequence > sessionSequence && confirmed.get()) {
            throw new NFSException(NFS4ERR_SEQ_MISORDERED, "bad sequence id : req sequence " + sequence + ", sessionSequence " + sessionSequence);
        }

        if (sequence == sessionSequence - 1 && !confirmed.get()) {
            throw new NFSException(NFS4ERR_SEQ_MISORDERED, "bad sequence id : req sequence " + sequence + ", sessionSequence " + sessionSequence);
        }
        //session重传
        if (sequence == sessionSequence - 1) {
            byte[] sessionId = clientControl.createSessionId(clientId, sequence);
            return sessions.get(new NFS4Session.Session(sessionId));
        }

        if (sequence != sessionSequence) {
            throw new NFSException(NFS4ERR_SEQ_MISORDERED, "bad sequence id : req sequence " + sequence + ", sessionSequence " + sessionSequence);
        }

        byte[] sessionId = clientControl.createSessionId(clientId, sessionSequence);
        NFS4Session session = new NFS4Session(this, sessionId, cacheSize, maxOps, maxCbOps, nfsHandler, cbProgram);
        sessions.put(new NFS4Session.Session(sessionId), session);
        sessionSequence++;

        if (!confirmed.get()) {
            confirmed.set(true);
        }

        return session;
    }

    public void removeSession(byte[] sessionId) {
        NFS4Session session = sessions.remove(new NFS4Session.Session(sessionId));
        if (session == null) {
            throw new NFSException(NFS4ERR_BADSESSION, "removeSession : not exist session , bad session ");
        }
    }

    public NFS4Session getSession(byte[] sessionId) {
        NFS4Session session = sessions.get(new NFS4Session.Session(sessionId));
        if (session == null) {
            throw new NFSException(NFS4ERR_DEADSESSION, "getSession :not exist session , bad session ");
        }
        return session;
    }

    public NFS4Session getSession0(byte[] sessionId) {
        return sessions.get(new NFS4Session.Session(sessionId));
    }


    public boolean existSessions() {
        return !sessions.isEmpty();
    }


    public boolean existStates() {
        return !clientStates.isEmpty();
    }

    private synchronized void clearStates() {
        Iterator<NFS4State> i = clientStates.values().iterator();
        while (i.hasNext()) {
            NFS4State state = i.next();
            state.disposeIgnoreFailures();
            i.remove();
        }
    }

    //释放客户端所有资源
    public final void tryDispose() {
        clearStates();
        lockOwners.clear();
        owners.clear();
    }


    public synchronized void reclaimComplete() {
        if (reclaimCompleted) {
            throw new NFSException(NFS4ERR_COMPLETE_ALREADY, "reclaimComplete : complete already");
        }
        clientControl.reclaimComplete(getOwnerId());
        reclaimCompleted = true;
    }


    public synchronized void wantReclaim() {
        if (reclaimCompleted) {
            throw new NFSException(NFS4ERR_NO_GRACE, "wantReclaim : complete already");
        }
        clientControl.wantReclaim(getOwnerId());
    }

    public synchronized boolean needReclaim() {
        return !reclaimCompleted;
    }

    public synchronized StateOwner getOrCreateOwner(byte[] owner, int seq, boolean isLock) {

        StateOwner stateOwner;
        if (minorVersion == 0) {
            StateOwner.Owner key = StateOwner.newOwner(clientId, owner);
            if (isLock) {
                NFS4State openState = lockOwners.get(key);
                if (openState == null) {
                    stateOwner = new StateOwner(clientId, owner, seq);
                } else {
                    stateOwner = openState.getStateOwner();
                    stateOwner.incrSequence(seq);
                }
            } else {
                stateOwner = owners.get(key);
                if (stateOwner == null) {
                    stateOwner = new StateOwner(clientId, owner, seq);
                    owners.put(StateOwner.newOwner(clientId, stateOwner.owner.owner), stateOwner);
                } else {
                    stateOwner.incrSequence(seq);
                }
            }
        } else {
            stateOwner = new StateOwner(clientId, owner, 0);
        }
        return stateOwner;
    }


    public synchronized void releaseOwner(byte[] owner) {
        NFS4State lockState = lockOwners.remove(StateOwner.newOwner(clientId, owner));
        if (lockState == null) {
            throw new NFSException(NFS4ERR_STALE_CLIENTID, "releaseOwner : not exist stateOwner");
        }
        releaseState(lockState.stateId(), NFS4_LOCK_STID);
    }


    public NFS4State newState(StateOwner owner, int type, int shareAccess, int shareDeny) {
        return new NFS4State(null, owner, clientControl.createStateId(clientId, stateIdCounter.incrementAndGet()), type, shareAccess, shareDeny, 0, this);
    }

    public void removeState(StateId stateId) {
        NFS4State remove = clientStates.remove(stateId);
    }

    public boolean existState(StateId stateId) {
        return clientStates.get(stateId) != null;
    }



    public void rollbackSeq(NFS4State state) {
        clientStates.compute(state.getStateId(), (k, openState) -> {
            if (openState == null) {

            } else {
                openState.stateId().seqId--;
                return openState;
            }
            return null;
        });
    }

    public NFS4State shareUpdate(NFS4State state, int shareAccess, int shareDeny) {
        return clientStates.compute(state.getStateId(), (k, openState) -> {
            if (openState == null) {
                //close了
            } else {
                if (openState.getLastUpdateSeqId() < state.stateId().seqId) {
                    openState.setShareAccess(shareAccess);
                    openState.setShareDeny(shareDeny);
                    openState.setLastUpdateSeqId(state.stateId().seqId);
                }
                return openState;
            }
            return null;
        });
    }

}



