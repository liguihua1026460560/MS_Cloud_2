package com.macrosan.filesystem.nfs.delegate;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.nfs.NFSV4;
import com.macrosan.filesystem.nfs.call.v4.CBRecallCall;
import com.macrosan.filesystem.nfs.call.v4.CBSequenceCall;
import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.shareAccess.ShareAccessLock;
import com.macrosan.filesystem.nfs.types.*;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.api.NFS4Proc.clientControl;
import static com.macrosan.filesystem.nfs.types.StateId.NFS4_DELEG_STID;
import static com.macrosan.filesystem.utils.Nfs4Utils.buildCBCall;
import static com.macrosan.filesystem.utils.Nfs4Utils.sendCB;

@Log4j2
@Accessors(chain = true)
@NoArgsConstructor
//@AllArgsConstructor
@Data
public class DelegateLock extends Lock {
    public String bucket = "";
    public String objName;
    public long nodeId;
    public FH2 fh2;
    public StateId stateId = new StateId();
    public int shareAccess;
    public int shareDeny;
    public String node = "";
    public long clientId;
    public byte[] sessionId = new byte[0];
    public byte[] owner = new byte[0];
    public int type;
    public String versionNum;
    public int minorVersion;


    public static final int CHECK_DELEGATE_TYPE = -1;
    public static final int ADD_DELEGATE_TYPE = -2;
    public static final int GET_DELEGATE_TYPE = -3;

    public static Map<Integer, Tuple2<CompoundCall, DelegateLock>> sendDelegateMap = new ConcurrentHashMap<>();
    public static Map<DelegateLock, Long> unDelegateMap = new ConcurrentHashMap<>();
//    public static Set<DelegateLock> unDelegateLockSet = ConcurrentHashMap.newKeySet();

    public DelegateLock(String bucket, String objName, long nodeId, FH2 fh2, StateId stateId, int shareAccess, int shareDeny, String node, long clientId, byte[] sessionId, byte[] owner, int type, String versionNum, int minorVersion) {
        this.bucket = bucket;
        this.objName = objName;
        this.nodeId = nodeId;
        this.fh2 = fh2;
        this.stateId = stateId;
        this.shareAccess = shareAccess;
        this.shareDeny = shareDeny;
        this.node = node;
        this.clientId = clientId;
        this.sessionId = sessionId;
        this.owner = owner;
        this.type = type;
        this.versionNum = versionNum;
        this.minorVersion = minorVersion;
    }

    public static final DelegateLock ERROR_DELEGATE = new DelegateLock().setShareAccess(-1).setVersionNum("")
            .setBucket("").setObjName("").setNode("").setClientId(-4).setOwner(new byte[0]);
    public static final DelegateLock CONFLICT_DELEGATE = new DelegateLock().setShareAccess(-2).setVersionNum("a")
            .setBucket("").setObjName("").setNode("").setClientId(-5).setOwner(new byte[0]);
    public static final DelegateLock NOT_FOUND_DELEGATE = new DelegateLock().setShareAccess(-3).setVersionNum("")
            .setBucket("").setObjName("").setNode("").setClientId(-6).setOwner(new byte[0]);


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegateLock that = (DelegateLock) o;
        return nodeId == that.nodeId && clientId == that.clientId && bucket.equals(that.bucket) && node.equals(that.node) && Arrays.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(bucket, nodeId, node, clientId);
        result = 31 * result + Arrays.hashCode(owner);
        return result;
    }

    @Override
    public boolean needKeep() {
        if (node.equals(ServerConfig.getInstance().getHostUuid())) {
            NFS4Client client0 = clientControl.getClient0(clientId);
            if (client0 != null) {
                return minorVersion < 1 || client0.getSession0(sessionId) != null;
            }
        }
        return false;
    }
    //todo v4.0 delegate处理

    public boolean recall() {
        if (node.equals(ServerConfig.getInstance().getHostUuid())) {
            NFS4Client nfs4Client = clientControl.getClient0(clientId);
            if (nfs4Client != null) {
                //todo nfs v4.0委托未实现，需在client基础上实现，当前判断4.0不进行委托
                if (minorVersion >= 1) {
                    NFS4Session session = nfs4Client.getSession0(sessionId);
                    if (session != null) {

                        CBRecallCall cbRecallCall = new CBRecallCall();
                        cbRecallCall.opt = NFSV4.CBOpcode.NFS4PROC_CB_RECALL.opcode;
                        cbRecallCall.stateId = stateId;
                        cbRecallCall.truncated = false;
                        cbRecallCall.fh2 = fh2;
                        List<CompoundCall> cbCallList = new ArrayList<>();
                        cbCallList.add(cbRecallCall);
                        CompoundCall compoundCall = sendCB(session, cbCallList, NFSV4.CBOpcode.NFS4PROC_CB_RECALL.opcode);
                        Mono.just(true).doOnNext(f ->{
                            NFSHandler.CBInfoMap.put(compoundCall.callHeader.header.id, new CBInfo(2, session.cbProgram, 1, NFSV4.CBOpcode.NFS4PROC_CB_RECALL.opcode));
                            sendDelegateMap.put(compoundCall.callHeader.header.id, new Tuple2<>(compoundCall, this));
                        }).subscribe();
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

                        return true;
                    }
                } else {
                    //v4.0逻辑
                    return true;
                }
            }
            DelegateClient.unLock(bucket, String.valueOf(nodeId), this).subscribe();
        }
        return true;
    }



    public void sendCheck(Tuple2<CompoundCall, DelegateLock> tuple, NFS4Session session) {
        CompoundCall sendCall = tuple.var1;
        DelegateLock sendLock = tuple.var2;
        Mono.delay(Duration.ofMillis(5 * 1000))
                .map(f -> sendDelegateMap.containsKey(sendCall.callHeader.header.id))
                .flatMap(b -> b ? DelegateClient.unLock(sendLock.bucket, String.valueOf(sendLock.nodeId), sendLock)
                        .doOnNext(c -> session.canSend.compareAndSet(false, true))
                        .doOnNext(d -> {
                            //移除本地缓存
                            long clientId = sendLock.getClientId();
                            NFS4Client client0 = clientControl.getClient0(clientId);
                            if (client0 != null) {
                                client0.removeState(sendLock.stateId);
                            }
                        }) : Mono.just(true))
                .subscribe(f -> sendDelegateMap.remove(sendCall.callHeader.header.id));
    }
}
