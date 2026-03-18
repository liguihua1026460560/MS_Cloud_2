package com.macrosan.filesystem.nfs.lock;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.nfs.NFSV4;
import com.macrosan.filesystem.nfs.call.v4.CBNotifyLockCall;
import com.macrosan.filesystem.nfs.call.v4.CBSequenceCall;
import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.delegate.DelegateClient;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.types.*;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.api.NFS4Proc.clientControl;
import static com.macrosan.filesystem.nfs.call.v4.LockV4Call.*;
import static com.macrosan.filesystem.utils.Nfs4Utils.buildCBCall;
import static com.macrosan.filesystem.utils.Nfs4Utils.sendCB;
@Slf4j
@Accessors(chain = true)
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
    public byte[] sessionId = new byte[0];
    public FH2 fh2 = new FH2();
    public int optType;
    public boolean clientLock;
    public boolean clientUnLock;
    public int minorVersion;
//    public String versionNum;

    public static final NFS4Lock DEFAULT_LOCK = new NFS4Lock();
    public static final NFS4Lock ERROR_LOCK = new NFS4Lock().setOffset(-1);
    public static final NFS4Lock NOMATCH_LOCK = new NFS4Lock().setOffset(-2);
    public static final NFS4Lock GET_LOCK = new NFS4Lock().setOffset(-3);
    public static Map<Integer, Tuple2<CompoundCall, NFS4Lock>> sendLockMap = new ConcurrentHashMap<>();
    public static Map<NFS4Lock, Long> unLockMap = new ConcurrentHashMap<>();
    public static final int UNLOCK_TYPE = 0;
    public static final int REMOVE_WAIT_TYPE = 1;
    public static final int RECALL_TYPE = 2;


    public static final int GET_LOCK_TYPE = -3;

    public static NFS4Lock newLock(int lockType, StateOwner stateOwner, long offset, long size, String node, long clientId, int minorVersion, byte[] sessionId, FH2 fh2) {
        return new NFS4Lock(lockType, stateOwner, offset, size, node, clientId, sessionId, fh2, UNLOCK_TYPE, false, false, minorVersion);
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
        return offset == nfs4Lock.offset && length == nfs4Lock.length && stateOwner.equals(nfs4Lock.stateOwner) && fh2.ino == nfs4Lock.fh2.ino;
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

    public boolean writeType() {
        return lockType == WRITE_LT || lockType == WRITEW_LT;
    }

    public boolean readType() {
        return lockType == READ_LT || lockType == READW_LT;
    }

    public boolean blockType() {
        return lockType == WRITEW_LT || lockType == READW_LT;
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

    public  boolean removeOwner(){
        return offset == 0 && length == UINT64_MAX;
    }

    public boolean notifyLock() {
        if (node.equals(ServerConfig.getInstance().getHostUuid())) {
            NFS4Client nfs4Client = clientControl.getClient0(clientId);
            if (nfs4Client != null) {
                //todo nfs v4.0委托未实现，需在client基础上实现，当前判断4.0不进行委托
                if (minorVersion >= 1) {
                    NFS4Session session = nfs4Client.getSession0(sessionId);
                    if (session != null) {
                        CBNotifyLockCall cbNotifyLockCall = new CBNotifyLockCall();
                        cbNotifyLockCall.opt = NFSV4.CBOpcode.NFS4PROC_CB_NOTIFY_LOCK.opcode;
                        cbNotifyLockCall.owner = stateOwner.owner;
                        cbNotifyLockCall.fh2 = fh2;
                        List<CompoundCall> cbCallList = new ArrayList<>();
                        cbCallList.add(cbNotifyLockCall);
                        Mono.just(true).doOnNext(f -> {
                            CompoundCall compoundCall = sendCB(session, cbCallList, NFSV4.CBOpcode.NFS4PROC_CB_NOTIFY_LOCK.opcode);

                            sendLockMap.put(compoundCall.callHeader.header.id, new Tuple2<>(compoundCall, this));
                        }).subscribe();

//                        synchronized (session.cbSeqId) {
//
//                            compoundCall.callList.add(cbNotifyLockCall);
//                            compoundCall.setCount(compoundCall.callList.size());
//                            NFSHandler.CBInfoMap.put(compoundCall.callHeader.header.id, new CBInfo(2, session.cbProgram, 1, NFSV4.CBOpcode.NFS4PROC_CB_NOTIFY_LOCK.opcode));
//                            sendLockMap.put(compoundCall.callHeader.header.id, new Tuple2<>(compoundCall, this));
//                            session.recallQueue.add(new Tuple2<>(compoundCall, this));
//                            if (session.canSend.compareAndSet(true, false)) {
//                                Tuple2<CompoundCall, DelegateLock> tuple2 = session.recallQueue.poll();
//                                if (tuple2 != null) {
//                                    CompoundCall sendCall = tuple2.var1;
//                                    DelegateLock sendLock = tuple2.var2;
//                                    session.nfsHandler.write(sendCall);
//                                    //5秒检查是否收到请求回调,未收到直接unlock
//                                    sendCheck(tuple2, session);
//                                } else {
//                                    session.canSend.set(true);
//                                }
//                            }
//                        }
                        return true;
                    }
                } else {
                    //v4.0逻辑
                    return true;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "NFS4Lock{" +
                "lockType=" + lockType +
                ", stateOwner=" + stateOwner +
                ", offset=" + offset +
                ", length=" + length +
                ", node='" + node + '\'' +
                ", clientId=" + clientId +
                ", sessionId=" + new String(sessionId) +
                ", fh2=" + fh2 +
                ", optType=" + optType +
                ", clientLock=" + clientLock +
                ", clientUnLock=" + clientUnLock +
                ", minorVersion=" + minorVersion +
                '}';
    }
}