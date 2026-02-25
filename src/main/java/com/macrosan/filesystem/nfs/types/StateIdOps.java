
package com.macrosan.filesystem.nfs.types;

import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.call.v4.OpenV4Call;
import com.macrosan.filesystem.nfs.delegate.DelegateClient;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import com.macrosan.filesystem.nfs.lock.NFS4LockClient;
import com.macrosan.filesystem.nfs.reply.v4.CompoundReply;
import com.macrosan.filesystem.nfs.reply.v4.OpenV4Reply;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessClient;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.filesystem.FsConstants.EIO;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.nfs.api.NFS4Proc.delegateSwitch;
import static com.macrosan.filesystem.nfs.call.v4.LockV4Call.UINT64_MAX;
import static com.macrosan.filesystem.nfs.call.v4.OpenV4Call.*;
import static com.macrosan.filesystem.nfs.delegate.DelegateLock.ADD_DELEGATE_TYPE;
import static com.macrosan.filesystem.nfs.delegate.DelegateLock.CHECK_DELEGATE_TYPE;
import static com.macrosan.filesystem.nfs.lock.NFS4Lock.ERROR_LOCK;
import static com.macrosan.filesystem.nfs.lock.NFS4Lock.GET_LOCK;
import static com.macrosan.filesystem.nfs.reply.v4.OpenV4Reply.*;
import static com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock.*;
import static com.macrosan.filesystem.nfs.types.StateId.*;
import static com.macrosan.filesystem.nfs.types.StateOwner.ANONYMOUS_STATE_OWNER;

@Log4j2
public class StateIdOps {
    //所有客户端open的stateId(stateId本地缓存使用)
    private final Map<Long, List<OpenState>> openFileMap = new ConcurrentHashMap<>();
    //委托
    private final Map<Long, List<OpenState>> delegateFileMap = new ConcurrentHashMap<>();
    private final Map<Long, Set<NFS4State>> openStateIds = new ConcurrentHashMap<>();


    public static final int DELEGATE_CONFLICT = -1;
    public static final int LOCK_CONFLICT = -2;

    @Accessors(chain = true)
    @Data
    public static class OpenState {
        private NFS4Client client;
        private NFS4Session session;
        private StateId stateId;
        private StateOwner owner;
        private int shareAccess;
        private int shareDeny;


        public OpenState() {

        }

        public OpenState(NFS4Client client, NFS4Session session, StateOwner owner, StateId stateid, int shareAccess, int shareDeny) {
            this.client = client;
            this.stateId = stateid;
            this.shareAccess = shareAccess;
            this.shareDeny = shareDeny;
            this.owner = owner;
            this.session = session;
        }

        @Override
        public OpenState clone() {
            return new OpenState()
                    .setClient(client)
                    .setSession(session)
                    .setStateId(stateId.clone())
                    .setShareAccess(shareAccess)
                    .setShareDeny(shareDeny)
                    .setOwner(owner.clone());
        }
    }


    public Tuple2<NFS4State, Boolean> prepareState(NFS4Client client, StateOwner stateOwner, Inode inode, int stateIdType, ShareAccessLock lock) {
        AtomicReference<Tuple2<NFS4State, Boolean>> res = new AtomicReference<>();
        openStateIds.compute(inode.getNodeId(), (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            for (NFS4State state : v) {
                if (state.getClient().getClientId() == client.getClientId() && state.getStateOwner().equals(stateOwner)) {

                    state.stateId().seqId++;
                    //lock成功后进行设置share
                    lock.setStateId(state.stateId());
                    res.set(new Tuple2<>(state.clone(), false));
                    return v;
                }
            }
            NFS4State state = client.createAndPutState(stateOwner, null, stateIdType, null, -1, -1);
            state.stateId().seqId++;
            lock.setStateId(state.stateId());
            v.add(state);
            state.addDisposeListener(s -> Mono.just(removeOpen(inode, state.stateId(), openStateIds))
                    .flatMap(b -> ShareAccessClient.unLock(inode.getBucket(), inode.getObjName(), lock)).subscribe());
            res.set(new Tuple2<>(state.clone(), true));
            return v;
        });
        return res.get();
    }

    public void rollbackState(Inode inode, NFS4State openState, NFS4Client client, boolean remove) {
        openStateIds.compute(inode.getNodeId(), (k, v) -> {
            if (v == null) {
                return null;
            }
            if (remove) {
                v.remove(openState);
                client.removeState(openState.stateId());
            } else {
                NFS4State state = client.state0(openState.stateId());
                if (state != null) {
                    state.stateId().seqId--;
                }
            }
            return v;
        });
    }

    public boolean existOpen(long nodeId) {
        return !(openStateIds.get(nodeId) == null || openStateIds.get(nodeId).isEmpty());
    }



    public Mono<StateId> addOpen(NFS4Client client, StateOwner owner, CompoundContext context, int shareAccess, int shareDeny, int stateIdType, CompoundReply reply) {
        Inode inode = context.getCurrentInode();
        ShareAccessLock shareAccessLock = new ShareAccessLock(inode.getBucket(), inode.getObjName(), inode.getNodeId(), null,
                shareAccess, shareDeny, ServerConfig.getInstance().getHostUuid(), context.clientId, context.sessionId, owner.owner.owner,
                ADD_SHARE_TYPE, stateIdType, VersionUtil.getVersionNum());
        Tuple2<NFS4State, Boolean> tuple2 = prepareState(client, owner, inode, stateIdType, shareAccessLock);
        NFS4State openState = tuple2.var1;
        //需要更新shareAccess或shareDeny
        return ShareAccessClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock)
                .flatMap(lock -> {
                    if (ShareAccessLock.ERROR_SHARE.equals(lock)) {
                        rollbackState(inode, openState, client, tuple2.var2);
                        reply.status = EIO;
                    } else if (ShareAccessLock.CONFLICT_SHARE.equals(lock)) {
                        rollbackState(inode, openState, client, tuple2.var2);
                        reply.status = NFS4ERR_SHARE_DENIED;
                    } else if (lock.type == EXIST_SHARE_TYPE) {
                        if (client.shareUpdate(openState, lock.shareAccess, lock.shareDeny) == null) {
                            reply.status = NFS4ERR_BAD_STATEID;
                        }
                    } else {
                        if (client.shareUpdate(openState, lock.shareAccess, lock.shareDeny) == null) {
                            reply.status = NFS4ERR_BAD_STATEID;
                        }
                    }
                    return Mono.just(openState.stateId().clone());
                });
    }


    public Mono<StateId> downgradeOpen(NFS4Client client, NFS4State openState, Inode inode, int shareAccess, int shareDeny, CompoundContext context, CompoundReply reply) {
        ShareAccessLock shareAccessLock = new ShareAccessLock(inode.getBucket(), inode.getObjName(), inode.getNodeId(), openState.stateId(),
                shareAccess, shareDeny, ServerConfig.getInstance().getHostUuid(), context.clientId, context.sessionId, openState.getOwner().owner.owner, EXIST_SHARE_TYPE, openState.getType(), VersionUtil.getVersionNum());

        return ShareAccessClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock)
                .flatMap(lock -> {
                    if (ERROR_SHARE.equals(lock)) {
                        reply.status = EIO;
                    } else if (NOT_FOUND_SHARE.equals(lock)) {
                        reply.status = NFS4ERR_BAD_STATEID;
                    } else if (CONFLICT_SHARE.equals(lock)) {
                        client.rollbackSeq(openState);
                        reply.status = NFS3ERR_INVAL;
                    } else {
                        if (client.shareUpdate(openState, lock.shareAccess, lock.shareDeny) == null) {
                            reply.status = NFS4ERR_BAD_STATEID;
                        }
                    }
                    return Mono.just(openState.getStateId());
                });
    }


    public Mono<Boolean> checkDeny(Inode inode, int shareDeny) {
        ShareAccessLock shareAccessLock = new ShareAccessLock()
                .setBucket(inode.getBucket())
                .setNodeId(inode.getNodeId())
                .setObjName(inode.getObjName())
                .setShareDeny(shareDeny)
                .setType(OPEN_DENY_CONFLICT_TYPE);

        return ShareAccessClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock)
                .flatMap(lock -> {
                    if (CONFLICT_SHARE.equals(lock)) {
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                });
    }


    //删除前检查shareDeny和delegation
    //todo 若open返回OPEN4_RESULT_PRESERVE_UNLINKED标志位需实现存在open状态不应该删除，直到所有open关闭再删除
    public Mono<Inode> removeCheck(Inode inode, CompoundContext context) {
        return checkDeny(inode, OPEN_SHARE_DENY_WRITE)
                .flatMap(b -> {
                    if (b) {
                        return Mono.just(inode);
                    }
                    return Mono.error(new NFSException(NFS4ERR_FILE_OPEN, "can not remove , exist deny open"));
                }).flatMap(i -> hasDelegateConflict(inode, OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true))
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(inode);
                    }
                    return Mono.error(new NFSException(NFS4ERR_DELAY, "can not remove , exist write delegation"));
                });
    }



    public boolean removeOpen(Inode inode, StateId stateId, Map<Long, Set<NFS4State>> map) {
        map.compute(inode.getNodeId(), (nodeId, openStates) -> {
            if (openStates != null) {
                openStates.removeIf(os -> os.stateId().equals(stateId));
                if (openStates.isEmpty()) {
                    return null;
                }
            }
            return openStates;
        });
        return true;
    }



    //文件open前检查是否存在委托冲突
    public Mono<Boolean> hasDelegateConflict(Inode inode, int shareAccess, int shareDeny, CompoundContext context, StateOwner stateOwner, boolean need) {

        if (delegateSwitch.get()) {
            DelegateLock delegateLock = new DelegateLock(inode.getBucket(), inode.getObjName(), inode.getNodeId(), FH2.mapToFH2(inode, context.currFh.fsid), ZERO_STATEID,
                    shareAccess, shareDeny, ServerConfig.getInstance().getHostUuid(), context.clientId, context.sessionId,
                    need ? ANONYMOUS_STATE_OWNER.owner.owner : stateOwner.owner.owner
                    , CHECK_DELEGATE_TYPE, VersionUtil.getVersionNum(), context.minorVersion);
            return DelegateClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), delegateLock);
        }
        return Mono.just(false);
    }



    public Mono<Boolean> hasLockConflict(Inode inode, StateOwner stateOwner, CompoundContext context) {
        NFS4Lock nfs4Lock = NFS4Lock.newLock(NFS4Lock.GET_LOCK_TYPE, stateOwner, 0, UINT64_MAX, ServerConfig.getInstance().getHostUuid(), context.getClientId());
        return NFS4LockClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), nfs4Lock)
                .flatMap(lock -> {
                    if (ERROR_LOCK.equals(lock)) {
                        return Mono.just(false);
                    } else if (!GET_LOCK.equals(lock)) {
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                });
    }


    public Mono<Boolean> hasOpenConflict(Inode inode, StateOwner stateOwner, CompoundContext context, int shareAccess, int shareDeny, StateId openStateId, int stateIdType) {
        if (delegateSwitch.get()) {
            ShareAccessLock shareAccessLock = new ShareAccessLock(inode.getBucket(), inode.getObjName(), inode.getNodeId(), openStateId,
                    shareAccess, shareDeny, ServerConfig.getInstance().getHostUuid(), context.clientId, context.sessionId, stateOwner.owner.owner, OPEN_SHARE_CONFLICT_TYPE, stateIdType, VersionUtil.getVersionNum());
            return ShareAccessClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock)
                    .flatMap(lock -> {
                        if (ERROR_SHARE.equals(lock)) {
                            return Mono.just(false);
                        } else if (CONFLICT_SHARE.equals(lock)) {
                            return Mono.just(false);
                        }
                        return Mono.just(true);
                    });
        }
        return Mono.just(true);
    }




    public static boolean openConflict(int delegateShare, int shareAccess, int delegateDeny, int shareDeny) {
        delegateShare = 1 << delegateShare;
        shareAccess = 1 << shareAccess;
        return (delegateShare & (1 << OPEN_DELEGATE_WRITE)) != 0 ||
                (shareAccess & (1 << OPEN_SHARE_ACCESS_BOTH)) != 0 ||
                (shareAccess & (1 << OPEN_SHARE_ACCESS_WRITE)) != 0;
    }


    public static boolean delegateConflict(int delegateShare, int shareAccess, int delegateDeny, int shareDeny) {
        return (delegateShare & (1 << OPEN_DELEGATE_WRITE)) != 0 ||
                (shareAccess & (1 << OPEN_SHARE_ACCESS_BOTH)) != 0 ||
                (shareAccess & (1 << OPEN_SHARE_ACCESS_WRITE)) != 0;
    }

    public Mono<Integer> checkOpenAndLock(Inode inode, StateOwner owner, int shareAccess, int shareDeny, StateId
            openStateId, CompoundContext context, boolean canDelegate) {
        Mono<Integer> res = Mono.just(-1);
        if (canDelegate && delegateSwitch.get()) {
            res = res.flatMap(b -> hasOpenConflict(inode, owner, context, shareAccess, shareDeny, openStateId, NFS4_OPEN_STID))
                    .flatMap(b -> b ? hasLockConflict(inode, owner, context).flatMap(f -> f ? Mono.just(0) : Mono.just(LOCK_CONFLICT)) : Mono.just(DELEGATE_CONFLICT));
        }
        return res;
    }


    public Mono<Integer> hasDelegateAndLockConflict(Inode inode, StateOwner owner, int shareAccess, int shareDeny, CompoundContext context) {
        Mono<Integer> res = Mono.just(0);
        if (delegateSwitch.get()) {
            res = res.flatMap(b -> hasDelegateConflict(inode, shareAccess, shareDeny, context, null, true).map(f -> !f ? 0 : DELEGATE_CONFLICT));
        }
        return res.flatMap(b -> b != 0 ? Mono.just(b) : hasLockConflict(inode, owner, context).map(f -> f ? 0 : LOCK_CONFLICT));
    }


    public Mono<Boolean> addDelegate(NFS4Client client, CompoundContext context, StateOwner owner, int shareAccess,
                                     int shareDeny, OpenV4Call call, OpenV4Reply reply, StateId openStateId, boolean canDelegate) {
        if (delegateSwitch.get()) {
            //委托数量限制
//            if (delegateFileMap.keySet().size() >= delegateMaxCount) {
//                reply.openDelegation.delegateType = OPEN_DELEGATE_NONE_EXT;
//                reply.openDelegation.noneDelegation.ondWhy = WND4_RESOURCE;
//                return Mono.just(true);
//            }
            //todo 当前只要没有冲突即授予delegate，后续需优化delegate授予策略,比如根据mtime来决定
            Inode inode = context.getCurrentInode();
            NFS4State createState = client.newState(owner, NFS4_DELEG_STID, shareAccess, shareDeny);
            createState.stateId().seqId++;
            FH2 fh2 = FH2.mapToFH2(inode, context.currFh.fsid);
            DelegateLock delegateLock = new DelegateLock(inode.getBucket(), inode.getObjName(), inode.getNodeId(), fh2, createState.stateId(),
                    shareAccess, shareDeny, ServerConfig.getInstance().getHostUuid(), context.clientId, context.sessionId, owner.owner.owner, ADD_DELEGATE_TYPE, VersionUtil.getVersionNum(), context.minorVersion);
//            if (call.want == OPEN_SHARE_ACCESS_WANT_CANCEL) {
//                return DelegateClient.unLock(inode.getBucket(), inode.getObjName(), delegateLock)
//                        .doOnNext(b -> {
//                            fhToDelegateMap.compute(fh2, (k, v) -> {
//                                if (v == null) {
//                                    return null;
//                                }
//
//                                v.remove(delegateLock);
//                                client.releaseState0(delegateLock);
//                                return v.isEmpty() ? null : v;
//                            });
//                        });
//            }
            return DelegateClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), delegateLock)
                    .flatMap(b -> {
                        if (b) {
                            client.createAndPutState(owner, NFS4_DELEG_STID, createState, shareAccess, shareDeny);
                            //delegate或session销毁解锁
                            createState.addDisposeListener(s -> DelegateClient.unLock(inode.getBucket(), String.valueOf(inode.getNodeId()), delegateLock).subscribe());
                            if (shareAccess < OPEN_SHARE_ACCESS_WRITE) {
                                reply.openDelegation.delegateType = OPEN_DELEGATE_READ;
                                reply.openDelegation.readDelegation.stateId = createState.stateId();
                            } else {

                                reply.openDelegation.delegateType = OPEN_DELEGATE_WRITE;
                                reply.openDelegation.writeDelegation.stateId = createState.stateId();
                            }
                        } else {
                            reply.openDelegation.delegateType = OPEN_DELEGATE_NONE;
                        }
                        return Mono.just(b);
                    });

        } else {
            if (call.want != 0 && canDelegate) {
                reply.openDelegation.delegateType = OPEN_DELEGATE_NONE_EXT;
                NoneDelegation noneDelegation = reply.openDelegation.noneDelegation;
                noneDelegation.ondWhy = WND4_RESOURCE;
                switch (call.want) {
                    case OPEN_SHARE_ACCESS_WANT_READ_DELEG:
                    case OPEN_SHARE_ACCESS_WANT_WRITE_DELEG:
                    case OPEN_SHARE_ACCESS_WANT_ANY_DELEG:
                        break;
                    case OPEN_SHARE_ACCESS_WANT_CANCEL:
                        noneDelegation.ondWhy = WND4_CANCELLED;
                        break;
                    case OPEN_SHARE_ACCESS_WANT_NO_DELEG:
                        reply.openDelegation.delegateType = OPEN_DELEGATE_NONE;
                        break;

                }
            }
            return Mono.just(true);
        }
    }

}

