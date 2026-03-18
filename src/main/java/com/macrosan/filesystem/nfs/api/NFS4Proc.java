package com.macrosan.filesystem.nfs.api;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.call.MkNodCall;
import com.macrosan.filesystem.nfs.call.NullCall;
import com.macrosan.filesystem.nfs.call.v4.*;
import com.macrosan.filesystem.nfs.delegate.DelegateClient;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.lock.NFS4Lock;
import com.macrosan.filesystem.nfs.lock.NFS4LockClient;
import com.macrosan.filesystem.nfs.reply.EntryOutReply;
import com.macrosan.filesystem.nfs.reply.NullReply;
import com.macrosan.filesystem.nfs.reply.v4.*;
import com.macrosan.filesystem.nfs.types.*;
import com.macrosan.filesystem.utils.*;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.perf.AddressFSPerfLimiter;
import com.macrosan.utils.perf.BucketFSPerfLimiter;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.FSPerformanceService.Instance_Type.fs_read;
import static com.macrosan.action.managestream.FSPerformanceService.getAddressPerfRedisKey;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NFS4Type.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFS_ACE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.FsConstants.RpcAuthType.*;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.FSID_BUCKET;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.getBucketName;
import static com.macrosan.filesystem.nfs.call.v4.CreateSessionCall.CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
import static com.macrosan.filesystem.nfs.call.v4.ExchangeIdCall.*;
import static com.macrosan.filesystem.nfs.call.v4.LockV4Call.WRITEW_LT;
import static com.macrosan.filesystem.nfs.call.v4.LockV4Call.WRITE_LT;
import static com.macrosan.filesystem.nfs.call.v4.OpenV4Call.*;
import static com.macrosan.filesystem.nfs.call.v4.SecInfoNoNameCall.SECINFO_STYLE4_CURRENT_FH;
import static com.macrosan.filesystem.nfs.call.v4.SecInfoNoNameCall.SECINFO_STYLE4_PARENT;
import static com.macrosan.filesystem.nfs.delegate.DelegateLock.unDelegateMap;
import static com.macrosan.filesystem.nfs.types.FAttr4.*;
import static com.macrosan.filesystem.nfs.types.NFS4Session.SessionConnection.DEFAULT_ADDRESS;
import static com.macrosan.filesystem.nfs.types.StateId.*;
import static com.macrosan.filesystem.utils.InodeUtils.*;
import static com.macrosan.filesystem.utils.Nfs4Utils.*;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class NFS4Proc {
    private static final NFS4Proc instance = new NFS4Proc();
    private static final Node nodeInstance = Node.getInstance();
    public static String node = ServerConfig.getInstance().getHostUuid();
    public static NFS4ClientControl clientControl = new NFS4ClientControl(NFS4_LEASE_TIME, node);
    public static Inode ROOT_INODE = new Inode().setNodeId(0).setMode(0777 | S_IFDIR).setObjName("/");
    public static int ROOT_FSID = -1;
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    public static boolean debug = false;
    public static boolean errorDebug = false;

    private NFS4Proc() {
    }

    public static NFS4Proc getInstance() {
        return instance;
    }


    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_NULL)
    public Mono<RpcReply> v4Null(RpcCallHeader callHeader, ReqInfo reqHeader, NullCall nullCall) {
        return Mono.just(new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_COMPOUND)
    public Mono<RpcReply> compound(RpcCallHeader callHeader, ReqInfo reqHeader, CompoundCall call) {
        return Mono.just(new CompoundReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_EXCHANGE_ID)
    public Mono<RpcReply> exchangeId(RpcCallHeader callHeader, ReqInfo reqHeader, ExchangeIdCall call) {
        ExchangeIdReply reply = new ExchangeIdReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_EXCHANGE_ID.opcode;
        if (call.stateProtect != STATE_PROTECT_SP4_NONE && call.stateProtect != STATE_PROTECT_SP4_MACH_CRED && call.stateProtect != STATE_PROTECT_SP4_SSV) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        if (call.flags != 0 && (call.flags | EXCHGID4_FLAG_MASK) != EXCHGID4_FLAG_MASK) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        if (call.stateProtect != STATE_PROTECT_SP4_NONE) {
            reply.status = NFS3ERR_ACCES;
            return Mono.just(reply);
        }
        CompoundContext context = call.context;
        NFS4Client client = clientControl.clientByOwner(call.clientOwner);
        boolean update = (call.flags & EXCHGID4_FLAG_UPD_CONFIRMED_REC_A) != 0;
        //更新confirm的客户端
        if (update) {
            if (client == null || !client.getConfirmed()) {
                reply.status = NFS3ERR_NOENT;
                return Mono.just(reply);
            }
            if (!client.verifierEquals(call.verifier)) {
                reply.status = NFS4ERR_NOT_SAME;
                return Mono.just(reply);
            }
            if (!client.checkAuth(callHeader.auth)) {
                reply.status = NFS3ERR_PERM;
            }
            client.refreshLeaseTime();
        } else {
            SocketAddress address = reqHeader.nfsHandler.address;
            if (client == null) {
                client = clientControl.createClient(address, context.getMinorVersion(), call.clientOwner, call.verifier,
                        callHeader.auth, reqHeader.nfsHandler);
            } else {
                if (client.getConfirmed()) {
                    //客户端重发
                    if (client.verifierEquals(call.verifier) && callHeader.auth.equals(client.getAuth())) {
                        client.refreshLeaseTime();
                    } else if (client.checkAuth(callHeader.auth)) {
                        //客户端重启
                        clientControl.removeClient(client);
                        client = clientControl.createClient(address, context.getMinorVersion(), call.clientOwner, call.verifier, client.getAuth(), reqHeader.nfsHandler);
                    } else {
                        //oldClient过期或不存在stateId
                        if ((!client.existStates()) || !client.leaseValid()) {
                            clientControl.removeClient(client);
                            client = clientControl.createClient(address, context.getMinorVersion(), call.clientOwner, call.verifier, client.getAuth(), reqHeader.nfsHandler);
                        } else {
                            //客户端冲突
                            reply.status = NFS4ERR_CLID_INUSE;
                            return Mono.just(reply);
                        }
                    }
                } else {
                    //更新not confirm的客户端
                    clientControl.removeClient(client);
                    client = clientControl.createClient(address, context.getMinorVersion(), call.clientOwner, call.verifier, client.getAuth(), reqHeader.nfsHandler);
                }
            }
        }
        client.updateLeaseTime();
        reply.clientId = client.getClientId();
        reply.seqId = client.currentSeqId();
        reply.flags = reply.flags | EXCHGID4_FLAG_USE_NON_PNFS;
        reply.stateProtect = STATE_PROTECT_SP4_NONE;
        reply.minorId = 0;
        //返回服务器名称
        reply.majorId = ServerConfig.getInstance().getHostUuid().getBytes();
        reply.serverScope = "".getBytes();
        if (client.getConfirmed()) {
            reply.flags = reply.flags | EXCHGID4_FLAG_CONFIRMED_R;
        }
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SETCLIENTID)
    public Mono<RpcReply> setClientId(RpcCallHeader callHeader, ReqInfo reqHeader, SetClientIdCall call) {
        SetClientIdReply reply = new SetClientIdReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_SETCLIENTID.opcode;
        CompoundContext context = call.context;
        if (context.minorVersion >= 1) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        NFS4Client client = clientControl.clientByOwner(call.clientOwner);
        if (client != null) {
            if (client.getConfirmed()) {
                if (!client.checkAuth(callHeader.auth)) {
                    reply.netId = call.netId;
                    reply.addr = call.addr;
                    reply.status = NFS4ERR_CLID_INUSE;
                    return Mono.just(reply);
                } else if (client.verifierEquals(call.verifier)) {
                    //客户端重发
                    client.reset();
                } else {
                    //重启 释放所有锁?
                    clientControl.removeClient(client);
                    client = clientControl.createClient(reqHeader.nfsHandler.address, context.getMinorVersion(), call.clientOwner, call.verifier, callHeader.auth, reqHeader.nfsHandler);
                }
            } else {
                clientControl.removeClient(client);
                client = clientControl.createClient(reqHeader.nfsHandler.address, context.getMinorVersion(), call.clientOwner, call.verifier, callHeader.auth, reqHeader.nfsHandler);
            }
        } else {
            client = clientControl.createClient(reqHeader.nfsHandler.address, context.getMinorVersion(), call.clientOwner, call.verifier, callHeader.auth, reqHeader.nfsHandler);
        }
        reply.verifier = client.getVerifier();
        reply.clientId = client.getClientId();
        return Mono.just(reply);

    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SETCLIENTID_CONFIRM)
    public Mono<RpcReply> setClientIdConfirm(RpcCallHeader callHeader, ReqInfo reqHeader, SetClientIdConfirmCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_SETCLIENTID_CONFIRM.opcode;
        CompoundContext context = call.context;
        if (context.minorVersion >= 1) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        NFS4Client client = clientControl.getClient(call.clientId);
        if (client.verifierEquals(call.verifier)) {
            client.confirmed();
        } else {
            reply.status = NFS3ERR_INVAL;
        }
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_RENEW)
    public Mono<RpcReply> renew(RpcCallHeader callHeader, ReqInfo reqHeader, RenewCall call) {
        //更新服务端lease
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_RENEW.opcode;
        CompoundContext context = call.context;
        if (context.minorVersion >= 1) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        NFS4Client client = clientControl.getClient(call.clientId);
        client.updateLeaseTime();
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_CREATE_SESSION)
    public Mono<RpcReply> createSession(RpcCallHeader callHeader, ReqInfo reqHeader, CreateSessionCall call) {
        SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
        CreateSessionReply reply = new CreateSessionReply(header);
        reply.opt = NFSV4.Opcode.NFS4PROC_CREATE_SESSION.opcode;
        CompoundContext context = call.context;
        NFS4Client client = clientControl.getValidClient(call.clientId);
        if (!client.checkAuth(callHeader.auth) && !client.getConfirmed()) {
            reply.status = NFS4ERR_CLID_INUSE;
            return Mono.just(reply);
        }
        //slot缓存(用于缓存给客户端的响应，便于断连后快速恢复)暂不支持(文档可支持可不支持)
        NFS4Session session = client.createSession(call.seqId,
                Math.min(NFS4_MAX_SESSION_SLOTS, call.csaForeChanAttrs.maxReqs),
                Math.min(NFS4_MAX_OPS, call.csaForeChanAttrs.maxOps),
                Math.min(NFS4_MAX_OPS, call.csaBackChanAttrs.maxOps), reqHeader.nfsHandler, call.cbProgram);
        client.refreshLeaseTime();
        reply.seqId = call.seqId;
        reply.csrBackChanAttrs = call.csaBackChanAttrs;
        reply.csrForceChanAttrs = call.csaForeChanAttrs;
        reply.sessionId = session.getSessionId();
        //session开启callback
        reply.csrFlags = reply.csrFlags | CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_BIND_CONN_TO_SESSION)
    public Mono<RpcReply> bindConnToSession(RpcCallHeader callHeader, ReqInfo reqHeader, BindConnToSessionCall call) {
        //one opt
        BindConnToSessionReply reply = new BindConnToSessionReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_BIND_CONN_TO_SESSION.opcode;
//        reply.bctsaUseConnInRdmaMode = call.bctsaUseConnInRdmaMode;
//        reply.sessionId = call.sessionId;
//        reply.bctsaDir = call.bctsaDir;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }


    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SEQUENCE)
    public Mono<RpcReply> sequence(RpcCallHeader callHeader, ReqInfo reqHeader, SequenceCall call) {
        SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
        CompoundContext context = call.context;
        SequenceReply reply = new SequenceReply(header);
        reply.opt = NFSV4.Opcode.NFS4PROC_SEQUENCE.opcode;
        NFS4Client client = clientControl.getClient(call.sessionId);
        NFS4Session session = client.getSession(call.sessionId);
        session.bindIfNeeded(new NFS4Session.SessionConnection(DEFAULT_ADDRESS, reqHeader.nfsHandler.address));
        context.sessionId = call.sessionId;
        context.clientId = client.getClientId();
        client.updateLeaseTime();
        context.setSession(session);
        //todo 未实现slot缓存
//        context.setCacheThis(call.isCache);
        context.setCacheThis(false);
//        context.setSessionSlot(slot);
        reply.highSlotId = session.getHighestSlot();
        reply.slotId = call.slotId;
        reply.targetHighSlotId = session.getHighestSlot();
        reply.sessionId = call.sessionId;
        reply.seqId = call.seqId;
        reply.statusFlags = 0;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_RECLAIM_COMPLETE)
    public Mono<RpcReply> reclaimComplete(RpcCallHeader callHeader, ReqInfo reqHeader, ReclaimCompleteCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_RECLAIM_COMPLETE.opcode;
        CompoundContext context = call.context;
        if (context.getMinorVersion() == 0) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        if (call.reclaimOneFs) {
            Inode currentInode = context.getCurrentInode();
            return Mono.just(reply);
        } else {
            //表示客户端回收全部锁
            context.getSession().getClient().reclaimComplete();
        }
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_PUTROOTFH)
    public Mono<RpcReply> putRootFH(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        CompoundContext context = call.context;
        context.currFh = FH2.mapToFH2(ROOT_INODE, ROOT_FSID);
        context.currStateId = StateId.zeroStateId();
        context.setCurrentInode(ROOT_INODE);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_PUTROOTFH.opcode;
        return nodeInstance.getInode("", ROOT_INODE.getNodeId())
                .flatMap(i -> {
                    if (ERROR_INODE.equals(i)) {
                        reply.status = EIO;
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_PUTPUBFH)
    public Mono<RpcReply> putPubFH(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_PUTPUBFH.opcode;
        CompoundContext context = call.context;
        if (context.minorVersion >= 1) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        context.currFh = FH2.mapToFH2(ROOT_INODE, ROOT_FSID);
        context.currStateId = StateId.zeroStateId();
        context.setCurrentInode(ROOT_INODE);
        return Mono.just(reply);
    }

    //匿名
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SECINFO_NO_NAME)
    public Mono<RpcReply> secInfoNoName(RpcCallHeader callHeader, ReqInfo reqHeader, SecInfoNoNameCall call) {
        SecInfoNoNameReply reply = new SecInfoNoNameReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_SECINFO_NO_NAME.opcode;
        CompoundContext context = call.context;
        Inode currentInode = context.getCurrentInode();
        MonoProcessor<RpcReply> res = MonoProcessor.create();
        Nfs4Utils.checkDir(currentInode);
        switch (call.secInfoStyle) {
            case SECINFO_STYLE4_PARENT:
                String childObjName = currentInode.getObjName();
                if (ROOT_INODE.equals(currentInode)) {
                    reply.status = NFS3ERR_NOENT;
                    return Mono.just(reply);
                }
                reqHeader.bucket = getBucketName(call.context.currFh.fsid);
                reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
                int lastDirIndex = childObjName.lastIndexOf("/");
                //currInode = bucket
                if ("".equals(childObjName) && currentInode.getBucket().equals(reqHeader.bucket) && currentInode.getNodeId() == 1) {
                    return Mono.just(reply);
                }
                Mono.just(true).flatMap(b -> {
                    if (lastDirIndex != -1) {
                        String parentName = childObjName.substring(0, lastDirIndex + 1);
                        return RedLockClient.lock(reqHeader, parentName, LockType.READ, true, false)
                                .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, parentName, reqHeader, false, currentInode.getNodeId(), currentInode.getACEs()));
                    } else {
                        return InodeUtils.getInode(reqHeader.bucket, 1, false);
                    }
                }).doOnNext(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        reply.status = ENOENT;
                    } else if (ERROR_INODE.equals(inode)) {
                        reply.status = EIO;
                    } else {
                        reply.flavor = RPC_AUTH_UNIX;
                        reply.num = 1;
                        reply.status = OK;
                    }
                    res.onNext(reply);
                }).subscribe();
                break;
            case SECINFO_STYLE4_CURRENT_FH:
                reply.flavor = RPC_AUTH_UNIX;
                reply.num = 1;
                res.onNext(reply);
                break;
            default:
                reply.status = NFS4ERR_BADXDR;
                res.onNext(reply);

        }
        return res.doOnNext(r -> context.clearCurrentInode());
    }

    //没有相关配置
    //指定安全验证，如gss
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SECINFO)
    public Mono<RpcReply> secInfo(RpcCallHeader callHeader, ReqInfo reqHeader, SecInfoCall call) {
        SecInfoReply reply = new SecInfoReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_SECINFO.opcode;
        MonoProcessor<RpcReply> res = MonoProcessor.create();
        CompoundContext context = call.context;
        Inode currentInode = context.getCurrentInode().clone();
        checkDirAndName(currentInode, call.name);
        context.clearCurrentInode();
        return Mono.just(currentInode).flatMap(dirInode -> {
            String objName = new String(call.name);
            //不是nfs root目录
            if (dirInode.getNodeId() != 1) {
                String dirName = dirInode.getObjName();
                dirName = dirName.substring(0, dirName.lastIndexOf("/") + 1);
                objName = dirName + objName;
            }
            String finalObjName = objName;
            return RedLockClient.lock(reqHeader, objName, LockType.READ, true, false)
                    .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, finalObjName, reqHeader, false, dirInode.getNodeId(), currentInode.getACEs()));
        }).flatMap(inode -> {
            if (NOT_FOUND_INODE.equals(inode)) {
                reply.status = ENOENT;
            } else if (ERROR_INODE.equals(inode)) {
                reply.status = EIO;
            } else {
                //todo 验证是否有访问目录name权限, NFS3ERR_ACCESS
                reply.status = OK;
                context.currFh = FH2.mapToFH2(inode, context.currFh.fsid);
                context.setCurrentInode(inode);
                context.currStateId = StateId.zeroStateId();
                int flavor = RPC_AUTH_UNIX;
                SecInfoReply.SecInfo[] secInfos = new SecInfoReply.SecInfo[1];
                SecInfoReply.SecInfo secInfo = new SecInfoReply.SecInfo();
                secInfos[0] = secInfo;
                reply.secInfos = secInfos;
                if (flavor == RPC_AUTH_UNIX || flavor == RPC_AUTH_NULL) {
                    secInfo.flavor = flavor;
                } else if (flavor == RPC_AUTH_GSS) {
                    //krb5
                    byte[] oid = new byte[0];
                    secInfo.flavor = RPC_AUTH_GSS;
                    SecInfoReply.RpcSecGssInfo gssInfo = new SecInfoReply.RpcSecGssInfo();
                    secInfo.flavorInfo = gssInfo;
                    gssInfo.qop = 0;
                    gssInfo.oid = oid;
                    //KRB5--RPC_GSS_SVC_NONE,KRB5I--RPC_GSS_SVC_INTEGRITY,KRB5P--RPC_GSS_SVC_PRIVACY
                    gssInfo.service = 1;
                }
            }
            return Mono.just(reply);
        });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_GETFH)
    public Mono<RpcReply> getFH(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        GetFHReply reply = new GetFHReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_GETFH.opcode;
        CompoundContext context = call.context;
        if (context.currFh == null) {
            reply.status = NFS4ERR_NOFILEHANDLE;
        }
        reply.fh = context.currFh;
        return Mono.just(reply);
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_GETATTR)
    public Mono<RpcReply> getAttr(RpcCallHeader callHeader, ReqInfo reqHeader, GetAttrV4Call call) {
        GetAttrV4Reply reply = new GetAttrV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_GETATTR.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        Inode currentInode = context.getCurrentInode();
        if (ROOT_INODE.equals(currentInode)) {
            reply.fAttr4 = new FAttr4(null, fsid, call.mask, context.minorVersion);
            return Mono.just(reply);
        }
        //查看是否存在写委托
        if (call.mask.length > 0 && (call.mask[0] & size) != 0 || (call.mask[0] & change) != 0) {
            return clientControl.getStateIdOps().hasDelegateConflict(context.getCurrentInode(), OPEN_SHARE_ACCESS_READ, OPEN_SHARE_DENY_NONE, context, null, true)
                    .flatMap(b -> {
                        if (!b) {
                            reqHeader.bucket = getBucketName(fsid);
                            reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
                            reply.fAttr4 = new FAttr4(currentInode, fsid, call.mask, context.minorVersion);
                        } else {
                            reply.status = NFS4ERR_DELAY;
                        }
                        return Mono.just(reply);
                    });
        }
        reqHeader.bucket = getBucketName(fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        reply.fAttr4 = new FAttr4(currentInode, fsid, call.mask, context.minorVersion);
        return Mono.just(reply);
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_PUTFH)
    public Mono<RpcReply> putFH(RpcCallHeader callHeader, ReqInfo reqHeader, PutFHCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        //只要需要fh的操作都需先经过该请求
        reply.opt = NFSV4.Opcode.NFS4PROC_PUTFH.opcode;
        CompoundContext context = call.context;
        if (call.fh != null) {
            if (call.fh.fsid == ROOT_FSID) {
                context.currFh = call.fh;
                context.currStateId = StateId.zeroStateId();
                context.setCurrentInode(ROOT_INODE);
                return Mono.just(reply);
            }
            reqHeader.bucket = getBucketName(call.fh.fsid);
            reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
            return CheckUtils.nfsOpenCheck(call.fh.fsid, reqHeader)
                    .flatMap(res -> {
                        if (!res) {
                            reply.status = NfsErrorNo.NFS3ERR_STALE;
                        } else {
                            return nodeInstance.getInode(reqHeader.bucket, call.fh.ino)
                                    .flatMap(i -> {
                                        context.currFh = call.fh;
                                        context.currStateId = StateId.zeroStateId();
                                        context.setCurrentInode(i);
                                        return Mono.just(reply);
                                    });
                        }
                        return Mono.just(reply);
                    });
        } else {
            reply.status = NFS4ERR_NOFILEHANDLE;
            return Mono.just(reply);
        }
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_ACCESS)
    public Mono<RpcReply> access(RpcCallHeader callHeader, ReqInfo reqHeader, AccessV4Call call) {
        AccessV4Reply reply = new AccessV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_ACCESS.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        //未mount
        if (fsid == -1) {
            reply.accessRights = call.access;
            reply.supportedTypes = call.access;
            return Mono.just(reply);
        }
        reqHeader.bucket = getBucketName(fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        return Mono.just(context.getCurrentInode())
                .map(inode -> {
                    reply.status = OK;
                    reply.accessRights = call.access;
                    if (inode.getNodeId() > 1 && callHeader.auth.flavor == RPC_AUTH_UNIX) {
                        int uid = ((AuthUnix) (callHeader.auth)).getUid();
                        if (inode.getUid() != 0 && uid != 0 && uid != inode.getUid()) {
                            reply.accessRights = NFSAccessAcl.READ;
                        }
                    }
                    reply.supportedTypes = call.access;
                    return reply;
                });
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_DESTROY_SESSION)
    public Mono<RpcReply> destroySession(RpcCallHeader callHeader, ReqInfo reqHeader, DestroySessionCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_DESTROY_SESSION.opcode;
        NFS4Client client = clientControl.getClient(call.sessionId);
        NFS4Session session = client.getSession(call.sessionId);
        NFS4Session.SessionConnection sessionConnection = new NFS4Session.SessionConnection(DEFAULT_ADDRESS, reqHeader.nfsHandler.address);
        if (!session.isReleasableBy(sessionConnection)) {
            reply.status = NFS4ERR_CONN_NOT_BOUND_TO_SESSION;
            return Mono.just(reply);
        }
        client.removeSession(call.sessionId);
        return Mono.just(reply);
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_DESTROY_CLIENTID)
    public Mono<RpcReply> destroyClientId(RpcCallHeader callHeader, ReqInfo reqHeader, DestroyClientIdCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_DESTROY_CLIENTID.opcode;
        NFS4Client client = clientControl.getClient(call.clientId);
        if (client.existSessions()) {
            reply.status = NFS4ERR_CLIENTID_BUSY;
            return Mono.just(reply);
        }
        if (client.existStates()) {
            reply.status = NFS4ERR_CLIENTID_BUSY;
        }
        clientControl.removeClient(client);
        return Mono.just(reply);
    }


    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_READDIR, buf = 1 << 20)
    public Mono<RpcReply> readDir(RpcCallHeader callHeader, ReqInfo reqHeader, ReadDirV4Call call) {
        ReadDirV4Reply reply = new ReadDirV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_READDIR.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        AtomicInteger dirBytesLength = new AtomicInteger();
        int minorVersion = context.minorVersion;
        reply.follows = 0;
        if (ROOT_INODE.equals(context.getCurrentInode())) {
            if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(FSID_BUCKET) > 0) {
                Mono<List<reactor.util.function.Tuple2<Inode, Map<String, String>>>> res =
                        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(FSID_BUCKET)
                                .flatMapMany(fsidToBucket -> Flux.fromIterable(fsidToBucket.values()))
                                .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                                .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("nfs")) && bucketInfo.get("nfs").equals("1")
                                        && StringUtils.isNotBlank(bucketInfo.get("fsid")) && Long.parseLong(bucketInfo.get("fsid")) > call.cookie)
                                .flatMap(bucketInfo -> nodeInstance.getInode(bucketInfo.get("bucket_name"), 1L).zipWith(Mono.just(bucketInfo)))
                                .sort(Comparator.comparing(a -> Long.parseLong(a.getT2().get("fsid"))))
                                .collectList();
                return res.flatMap(tuple2s -> {
                    int replySize = reply.size();
                    for (reactor.util.function.Tuple2<Inode, Map<String, String>> tuple2 : tuple2s) {
                        Inode inode = tuple2.getT1();
                        Map<String, String> bucketInfo = tuple2.getT2();
                        int follows = 1;
                        Inode clone = inode.clone();
                        clone.setObjName(bucketInfo.get("bucket_name"));
                        long bucketFsid = Long.parseLong(bucketInfo.get("fsid"));
                        clone.setCookie(bucketFsid);
//                        clone.setNodeId(bucketFsid);
                        DirEntV4 entry = DirEntV4.mapDirEnt(clone, ROOT_FSID, call.mask, follows, minorVersion, dirBytesLength.get());
                        if (replySize + entry.size() >= call.maxCount - SunRpcHeader.SIZE) {
                            break;
                        }
                        reply.entryList.add(entry);
                        replySize += entry.size();
                    }
                    if (reply.entryList.isEmpty()) {
                        reply.eof = 1;
                    }
                    return Mono.just(reply);
                });

            } else {
                reply.eof = 1;
                return Mono.just(reply);
            }
        }
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        Inode[] dirInodes = new Inode[1];
        AtomicInteger dirLength = new AtomicInteger();
        boolean[] isUpAtime = new boolean[1];
        return Mono.just(context.getCurrentInode())
                .flatMap(dirInode -> {
                    dirInodes[0] = dirInode;
                    dirLength.set(dirInode.getObjName().length());
                    dirBytesLength.set(dirInode.getObjName().getBytes(StandardCharsets.UTF_8).length);
                    String prefix = dirInode.getObjName();
                    return ReadDirCache.listAndCache(reqHeader.bucket, prefix, call.cookie, call.maxCount, reqHeader.nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs());
                })
                .map(inodeList -> {
                    int replySize = reply.size();
                    for (Inode inode : inodeList) {
                        int limitFileNameLength = 255;
                        if (inode.getObjName().endsWith("/")) {
                            limitFileNameLength = 256;
                        }
                        int follows = 1;
                        if (inode.getObjName().getBytes(StandardCharsets.UTF_8).length - dirBytesLength.get() <= limitFileNameLength) {
                            DirEntV4 entry = DirEntV4.mapDirEnt(inode, fsid, call.mask, follows, minorVersion, dirBytesLength.get());
                            if (replySize + entry.size() >= call.maxCount - SunRpcHeader.SIZE) {
                                break;
                            }
                            reply.entryList.add(entry);
                            replySize += entry.size();
                        }
                    }
                    if (reply.entryList.isEmpty()) {
                        reply.eof = 1;
                    }
                    if (!InodeUtils.isError(dirInodes[0]) && Inode.isRelaUpdate(dirInodes[0])) {
                        dirInodes[0].setAtime(System.currentTimeMillis() / 1000);
                        dirInodes[0].setAtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));
                        isUpAtime[0] = true;
                    }
                    return (RpcReply) reply;
                })
                .doOnNext(reply0 -> {
                    if (isUpAtime[0]) {
                        nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), bucket, dirInodes[0].getAtime(), dirInodes[0].getAtimensec(), true, false, false);
                    }
                })
                .doOnError(e -> log.error("", e));
    }


    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_LOOKUP)
    public Mono<RpcReply> lookup(RpcCallHeader callHeader, ReqInfo reqHeader, LookupV4Call call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LOOKUP.opcode;
        CompoundContext context = call.context;
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkDirAndNameAndSym(currentInode, call.name);
        String name = new String(call.name);
        if (ROOT_INODE.equals(currentInode)) {
            if (RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).exists(name) != 0
                    && (RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hexists(name, "nfs"))
                    && "1".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(name, "nfs"))) {
                int fsid = Integer.parseInt(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(name, "fsid"));
                Map<String, String> bucketInfo = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hgetall(name);
                if (!CheckUtils.siteCanAccess(bucketInfo)) {
                    reply.status = NFS3ERR_ACCES;
                    return Mono.just(reply);
                }
                NFSBucketInfo.FsInfo fsInfo = new NFSBucketInfo.FsInfo();
                fsInfo.setBucket(name);
                NFSBucketInfo.bucketInfo.put(name, bucketInfo);
                NFSBucketInfo.fsToBucket.put(fsid, fsInfo);
                Inode rootInode = InodeUtils.getAndPutRootInode(name);
                context.currFh = FH2.mapToFH2(rootInode, fsid);
                context.currStateId = StateId.zeroStateId();
                context.setCurrentInode(rootInode);
                return Mono.just(reply);
            }
            reply.status = ENOENT;
            return Mono.just(reply);
        }
        int fsid = context.currFh.fsid;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        if (!CheckUtils.siteCanAccess(reqHeader.bucketInfo)) {
            reply.status = NFS3ERR_ACCES;
            return Mono.just(reply);
        }
        return Mono.just(currentInode).flatMap(dirInode -> {
            String objName = new String(call.name);
            //不是nfs root目录
            if (dirInode.getNodeId() != 1) {
                String dirName = dirInode.getObjName();
                dirName = dirName.substring(0, dirName.lastIndexOf("/") + 1);
                objName = dirName + objName;
            }
            String finalObjName = objName;
            return RedLockClient.lock(reqHeader, objName, LockType.READ, true, false)
                    .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, finalObjName, reqHeader, false, dirInode.getNodeId(), dirInode.getACEs()));
        }).flatMap(inode -> {
            if (NOT_FOUND_INODE.equals(inode)) {
                reply.status = ENOENT;
            } else if (ERROR_INODE.equals(inode)) {
                reply.status = EIO;
            } else {
                reply.status = OK;
                context.currFh = FH2.mapToFH2(inode, fsid);
                context.setCurrentInode(inode);
                context.currStateId = StateId.zeroStateId();
            }
            return Mono.just(reply);
        });

    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_LOOKUPP)
    public Mono<RpcReply> lookupp(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LOOKUPP.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkDirAndSym(currentInode);
        if (ROOT_INODE.equals(currentInode)) {
            reply.status = NFS3ERR_NOENT;
            return Mono.just(reply);
        } else if (StringUtils.isBlank(currentInode.getObjName()) && currentInode.getNodeId() == 1) {
            context.currFh = FH2.mapToFH2(ROOT_INODE, ROOT_FSID);
            context.setCurrentInode(ROOT_INODE);
            return Mono.just(reply);
        }
        return Mono.just(currentInode).flatMap(childNode -> {
            String childObjName = childNode.getObjName();
            int lastDirIndex = childObjName.lastIndexOf("/");
            if (lastDirIndex != -1) {
                String parentObjectName = childObjName.substring(0, lastDirIndex + 1);
                return RedLockClient.lock(reqHeader, parentObjectName, LockType.READ, true, false)
                        .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, parentObjectName, reqHeader, false, childNode.getNodeId(), childNode.getACEs()));
            } else {
                return InodeUtils.getInode(reqHeader.bucket, 1, false);
            }
        }).flatMap(inode -> {
            if (NOT_FOUND_INODE.equals(inode)) {
                reply.status = ENOENT;
            } else if (ERROR_INODE.equals(inode)) {
                reply.status = EIO;
            } else {
                reply.status = OK;
                context.currFh = FH2.mapToFH2(inode, fsid);
                context.setCurrentInode(inode);
            }
            return Mono.just(reply);
        });

    }


    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_CREATE)
    public Mono<RpcReply> create(RpcCallHeader callHeader, ReqInfo reqHeader, CreateV4Call call) {
        CreateV4Reply reply = new CreateV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_CREATE.opcode;
        CompoundContext context = call.context;
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        reply.mask = call.mask;
        ObjAttr objAttr = call.fAttr4.objAttr;
        int defaultMode = 0644;
        switch (call.fType) {
            case NF4DIR:
                objAttr.mode = objAttr.mode == 0 ? 0755 | S_IFDIR : objAttr.mode | S_IFDIR;
                break;
            case NF4LNK:
                objAttr.mode = objAttr.mode == 0 ? 0777 | S_IFLNK : objAttr.mode | S_IFLNK;
                break;
            case NF4BLK:
                objAttr.mode = objAttr.mode == 0 ? defaultMode | S_IFBLK : objAttr.mode | S_IFBLK;
                break;
            case NF4CHR:
                objAttr.mode = objAttr.mode == 0 ? defaultMode | S_IFCHR : objAttr.mode | S_IFCHR;
                break;
            case NF4FIFO:
                objAttr.mode = objAttr.mode == 0 ? defaultMode | S_IFFIFO : objAttr.mode | S_IFFIFO;
                break;
            case NF4SOCK:
                objAttr.mode = objAttr.mode == 0 ? defaultMode | S_IFSOCK : objAttr.mode | S_IFSOCK;
                break;
            default:
                reply.status = NFS3ERR_BADTYPE;
                return Mono.just(reply);
        }
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkDirAndName(currentInode, call.name);
        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        MkNodCall mkNodCall = null;
        if (call.mkNod) {
            mkNodCall = new MkNodCall();
            mkNodCall.specData1 = call.specData1;
            mkNodCall.specData2 = call.specData2;
        }
        MkNodCall finalMkNodCall = mkNodCall;
        return CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                .flatMap(res -> {
                    return NFSBucketInfo.getBucketInfoReactive(reqHeader.bucket)
                            .map(map -> {
                                reqHeader.bucketInfo = map;
                                return res;
                            });
                })
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    return Mono.just(currentInode);
                })
                .flatMap(dirInode -> {
                    int cifsMode = FILE_ATTRIBUTE_ARCHIVE;
                    String callName = new String(call.name);
                    if (callName.endsWith("/")) {
                        cifsMode = FILE_ATTRIBUTE_DIRECTORY;
                    }
                    Map<String, String> parameter = new HashMap<>();
                    if (null != dirInode.getACEs() && !dirInode.getACEs().isEmpty()) {
                        parameter.put(NFS_ACE, Json.encode(dirInode.getACEs()));
                    }
                    return call.fType == NF4LNK ?
                            InodeUtils.create(reqHeader, objAttr.mode | S_IFLNK, cifsMode, dirInode.getObjName() + new String(call.name), new String(call.linkName), -1, "", null, parameter, callHeader) :
                            InodeUtils.nfsCreate(reqHeader, dirInode, objAttr.mode, call.name, entryOutReply, call.context.currFh.fsid, callHeader, finalMkNodCall);
                })
                .flatMap(inode -> {
                    if (entryOutReply.status != 0){
                        reply.status = entryOutReply.status;
                        return Mono.just(inode);
                    }
                    if (InodeUtils.isError(inode) || checkSetAttr(objAttr)) {
                        return Mono.just(inode);
                    }
                    return nodeInstance.setAttr(inode.getNodeId(), reqHeader.bucket, objAttr, null)
                            .doOnNext(i -> {
                                if (InodeUtils.isError(i)) {
                                    log.error("update inode fail.....bucket:{},nodeId:{}", reqHeader.bucket, context.currFh.ino);
                                }
                            });
                })
                .flatMap(i -> Node.getInstance().updateInodeTime(currentInode.getNodeId(), reqHeader.bucket, i.getMtime(), i.getMtimensec(), false, true, true)
                        .map(f -> i))
                .map(inode -> {
                    if (reply.status != 0){
                        return reply;
                    }
                    if (InodeUtils.isError(inode)) {
                        reply.status = EIO;
                    } else {
                        context.currFh = FH2.mapToFH2(inode, context.currFh.fsid);
                        context.setCurrentInode(inode);
                        context.setCurrStateId(StateId.zeroStateId());
                        reply.changeInfo.atomic = 1;
                        reply.changeInfo.beforeChangeId = inode.getMtime();
                        reply.changeInfo.afterChangeId = System.currentTimeMillis();
                    }
                    return reply;
                });
    }


    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_OPEN)
    public Mono<RpcReply> open(RpcCallHeader callHeader, ReqInfo reqHeader, OpenV4Call call) {
        OpenV4Reply reply = new OpenV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_OPEN.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        checkIsRoot(fsid);
        reqHeader.bucket = getBucketName(fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        reply.maskLen = call.maskLen;
        reply.mask = call.mask;
        int minorVersion = context.minorVersion;
        MonoProcessor<RpcReply> res = MonoProcessor.create();
        int shareAccess = call.shareAccess;
        int shareDeny = call.shareDeny;
        NFS4Client client;
        if (context.getMinorVersion() > 0) {
            client = context.getSession().getClient();
        } else {
            client = clientControl.getConfirmedClient(call.clientId);
            client.updateLeaseTime();
        }
        if (call.openType == OPEN_CREATE && call.claimType != NFS4_OPEN_CLAIM_NULL) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        if (client.needReclaim() && call.claimType != NFS4_OPEN_CLAIM_PREVIOUS) {
            reply.status = NFS4ERR_GRACE;
            return Mono.just(reply);
        }
        Inode currentInode = context.getCurrentInode();
        //todo 判断是否在宽限期
//        if (clientControl.isGracePeriod() && call.claimType != NFS4_OPEN_CLAIM_NULL){
//            reply.status = NFS4ERR_GRACE;
//            return Mono.just(reply);
//        }
//        if (!clientControl.isGracePeriod() && call.claimType == NFS4_OPEN_CLAIM_NULL){
//            reply.status = NFS4ERR_NO_GRACE;
//            return Mono.just(reply);
//        }

        StateOwner owner = client.getOrCreateOwner(call.owner, call.seqId, false);
        Mono<Boolean> preMono = CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                .flatMap(result ->
                        NFSBucketInfo.getBucketInfoReactive(reqHeader.bucket)
                                .map(map -> {
                                    reqHeader.bucketInfo = map;
                                    return result;
                                }));
        switch (call.claimType) {
            case NFS4_OPEN_CLAIM_NULL:
                Nfs4Utils.checkDirAndName(currentInode, call.name);
                preMono.flatMap(result -> {
                    if (!result) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        res.onNext(reply);
                        return Mono.empty();
                    }

                    String objName = new String(call.name);
                    if (currentInode.getNodeId() != 1) {
                        String dirName = currentInode.getObjName();
                        dirName = dirName.substring(0, dirName.lastIndexOf("/") + 1);
                        objName = dirName + objName;
                    }
                    String finalObjName = objName;
                    return RedLockClient.lock(reqHeader, objName, LockType.READ, true, false)
                            .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, finalObjName, reqHeader, false, currentInode.getNodeId(), currentInode.getACEs()))
                            .zipWith(Mono.just(currentInode));
                }).doOnNext(tuple2 -> {
                    Inode createNode = tuple2.getT1();
                    Inode dirNode = tuple2.getT2();
                    if (ERROR_INODE.equals(createNode)) {
                        reply.status = EIO;
                        res.onNext(reply);
                        return;
                    }
                    if (call.openType == OPEN_CREATE) {
                        if (call.createMode != CREATE_UNCHECKED && !NOT_FOUND_INODE.equals(createNode)) {
                            reply.status = EEXIST;
                            res.onNext(reply);
                            return;
                        }
                        if (call.createMode == CREATE_UNCHECKED && !NOT_FOUND_INODE.equals(createNode)) {
                            //todo 检查acl
                            ObjAttr objAttr = new ObjAttr();
                            if (call.fAttr4.objAttr.hasSize != 0 && call.fAttr4.objAttr.size == 0) {
                                objAttr.hasSize = 1;
                                objAttr.size = 0;
                            }
                            call.fAttr4.objAttr = objAttr;
                        }
                        int mode = 0;
                        if (call.createMode != CREATE_EXCLUSIVE) {
                            if (call.fAttr4.objAttr.hasMode != 0) {
                                mode = call.fAttr4.objAttr.mode;
                            }
                        }
                        mode = mode == 0 ? 0755 | S_IFREG : mode | S_IFREG;
                        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                        InodeUtils.nfsCreate(reqHeader, dirNode, mode | S_IFREG, call.name, entryOutReply, fsid, callHeader, null)
                                .flatMap(inode -> {
                                    if (entryOutReply.status != 0){
                                        reply.status = entryOutReply.status;
                                        return Mono.just(inode);
                                    }
                                    if (!InodeUtils.isError(inode) && call.createMode != CREATE_EXCLUSIVE && !checkSetAttr(call.fAttr4.objAttr)) {
                                        return nodeInstance.setAttr(inode.getNodeId(), reqHeader.bucket, call.fAttr4.objAttr, null)
                                                .doOnNext(i -> {
                                                    if (InodeUtils.isError(i)) {
                                                        log.error("update inode fail.....bucket:{},nodeId:{}", reqHeader.bucket, context.currFh.ino);
                                                    }
                                                });
                                    }
                                    return Mono.just(inode);
                                })
                                .subscribe(inode -> {
                                    if (reply.status != 0){
                                        res.onNext(reply);
                                        return;
                                    }
                                    if (InodeUtils.isError(inode)) {
                                        reply.status = EIO;
                                        res.onNext(reply);
                                        return;
                                    }
                                    context.currFh = FH2.mapToFH2(inode, fsid);
                                    context.setCurrentInode(inode);
                                    res.onNext(reply);
                                });
                    } else {
                        Nfs4Utils.checkCanAccess(reply, context, createNode, call.shareAccess);
                        if (NOT_FOUND_INODE.equals(createNode)) {
                            reply.status = ENOENT;
                            res.onNext(reply);
                            return;
                        }
                        context.currFh = FH2.mapToFH2(createNode, fsid);
                        context.currStateId = reply.stateId;
                        context.setCurrentInode(createNode);
                        res.onNext(reply);
                    }
                }).subscribe();
                break;
            case NFS4_OPEN_CLAIM_PREVIOUS:
                //todo 客户端重启
                client.wantReclaim();
                res.onNext(reply);
                break;
            case NFS4_OPEN_CLAIM_FH:

                if (context.minorVersion == 0) {
                    reply.status = NFS4ERR_BADXDR;
                    res.onNext(reply);
                    break;
                }
                preMono.subscribe(result -> {
                    if (!result) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        res.onNext(reply);
                        return;
                    }
                    context.currFh = FH2.mapToFH2(context.getCurrentInode(), fsid);
                    res.onNext(reply);
                });
                break;
            //todo 委托操作暂时不支持,c语言中部分不支持
            case NFS4_OPEN_CLAIM_DELEG_PREV_FH:
            case NFS4_OPEN_CLAIM_DELEG_CUR_FH:
                //原生不支持4.0
                if (context.minorVersion == 0) {
                    reply.status = NFS4ERR_BADXDR;
                    res.onNext(reply);
                    break;
                } else {
                    reply.status = NFS3ERR_NOTSUPP;
                    res.onNext(reply);
                    break;
                }
            case NFS4_OPEN_CLAIM_DELEGATE_PREV:
            case NFS4_OPEN_CLAIM_DELEGATE_CUR:
                reply.status = NFS3ERR_NOTSUPP;
                res.onNext(reply);
                break;
            default:
                reply.status = NFS3ERR_INVAL;
                res.onNext(reply);
        }
        return res.flatMap(r -> {
            StateIdOps stateIdOps = clientControl.getStateIdOps();
            OpenV4Reply openReply = (OpenV4Reply) r;
            if (openReply.status == 0) {
                return stateIdOps.hasDelegateConflict(context.getCurrentInode(), shareAccess, shareDeny, context, owner, false)
                        .flatMap(b -> {
                            if (b) {
                                reply.status = NFS4ERR_DELAY;
                                return Mono.just(reply);
                            }
//                            if (call.claimType == NFS4_OPEN_CLAIM_DELEG_CUR_FH){
//                                stateIdOps.
//                            }
                            return stateIdOps.addOpen(client, owner, context, shareAccess, shareDeny, NFS4_OPEN_STID, openReply)
                                    .flatMap(state -> {
                                        if (openReply.status == 0) {
                                            //todo 未实现宽限期不可委托
                                            boolean canDelegate = (call.claimType == NFS4_OPEN_CLAIM_NULL || call.claimType == NFS4_OPEN_CLAIM_FH)
                                                    && (call.openType != OPEN_CREATE || (call.openType == OPEN_CREATE && call.shareAccess == OPEN_SHARE_ACCESS_WRITE))
                                                    && minorVersion >= 1 && (call.want & OPEN_SHARE_ACCESS_WANT_NO_DELEG) == 0 && (context.getCurrentInode().getMode() & S_IFMT) != S_IFDIR;
                                            return stateIdOps.checkOpen(context.getCurrentInode(), owner, state.getShareAccess(), state.getShareDeny(), state.stateId(), context, canDelegate)
                                                    .flatMap(delegate -> {
                                                        return stateIdOps.addDelegate(client, context, owner, state.getShareAccess(), state.getShareDeny(), call, openReply, state.stateId(), canDelegate, delegate).map(f -> openReply);
                                                    }).doOnNext(f -> {
                                                        reply.resultFlags = OPEN4_RESULT_LOCKTYPE_POSIX | OPEN4_RESULT_MAY_NOTIFY_LOCK;
                                                        if (minorVersion == 0) {
                                                            reply.resultFlags = reply.resultFlags | OPEN4_RESULT_CONFIRM;
                                                        }
                                                        context.currStateId = state.stateId();
                                                        reply.stateId = state.stateId();
                                                    });

                                        }
                                        return Mono.just(openReply);
                                    });
                        });

            }
            return Mono.just(openReply);
        });
    }


    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_OPEN_DOWNGRADE)
    public Mono<RpcReply> openDownGrade(RpcCallHeader callHeader, ReqInfo reqHeader, OpenDownGradeCall call) {
        StateIdReply reply = new StateIdReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_OPEN_DOWNGRADE.opcode;
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        long nodeId = context.currFh.ino;
        int shareAccess = call.shareAccess & ~NFS4_SHARE_ACCESS_WANT_DELEG_MASK;
        int shareDeny = call.shareDeny & ~NFS4_SHARE_ACCESS_WANT_DELEG_MASK;
        if ((shareAccess & OPEN_SHARE_ACCESS_BOTH) == 0
                || ((shareAccess & ~OPEN_SHARE_ACCESS_BOTH) != 0)
                || (shareDeny & ~OPEN_SHARE_DENY_BOTH) != 0) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
//        Inode currentInode = context.getCurrentInode();
        StateId stateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(stateId);
        return client.openDownGrade(context, client, stateId, shareAccess, shareDeny, call.seqId, reply)
                .doOnNext(s -> {
                    if (reply.status == 0) {
                        reply.stateId = s;
                    }
                }).map(s -> reply);
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_OPEN_CONFIRM)
    public Mono<RpcReply> openConfirm(RpcCallHeader callHeader, ReqInfo reqHeader, OpenConfirmCall call) {
        OpenConfirmReply reply = new OpenConfirmReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_OPEN_CONFIRM.opcode;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        reqHeader.bucket = getBucketName(fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        if (context.minorVersion >= 1) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkNotDirAndSym(currentInode);
        StateId stateId = call.stateId;
        NFS4Client client = clientControl.getClient(stateId);
        NFS4State openState = client.openConfirm(context, stateId, call.seqId);
        reply.stateId = openState.stateId();
        context.currStateId = openState.stateId();
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SETATTR)
    public Mono<RpcReply> setAttr(RpcCallHeader callHeader, ReqInfo reqHeader, SetAttrV4Call call) {
        SetAttrV4Reply reply = new SetAttrV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        CompoundContext context = call.context;
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reply.opt = NFSV4.Opcode.NFS4PROC_SETATTR.opcode;
        int[] mask = call.fAttr4.mask;
        reply.mask = mask;
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        return CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    return Mono.just(context.getCurrentInode());
                }).flatMap(i -> {
                    //stateId不是特殊id,open时检查了lock和delegation,此处不检查
                    if (!StateId.isStateLess(call.stateId)) {
                        StateId stateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
                        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(stateId);

                        NFS4State state = client.state(stateId, reply);
                        if (reply.status != 0) {
                            return Mono.just(i);
                        }
                        if (!Nfs4Utils.checkOpenMode(true, state.getShareAccess(), state.type())) {
                            reply.status = NFS4ERR_OPENMODE;
                        }
                        return Mono.just(i);
                    } else {
                        return clientControl.getStateIdOps().hasDelegateConflict(i, OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true)
                                .flatMap(b -> {
                                    if (b) {
                                        reply.status = NFS4ERR_DELAY;
                                    }
                                    return Mono.just(i);
                                });
                    }
                })
                .flatMap(inode -> {
                    if (reply.status != 0) {
                        return Mono.just(reply);
                    }
                    return nodeInstance.setAttr(inode.getNodeId(), reqHeader.bucket, call.fAttr4.objAttr, reqHeader.bucketInfo.get(BUCKET_USER_ID))
                            .map(i -> {
                                if (!InodeUtils.isError(i)) {
                                    reply.status = 0;
                                } else {
                                    reply.status = EIO;
                                    log.error("update inode fail.....bucket:{},nodeId:{}", reqHeader.bucket, context.currFh.ino);
                                }
                                return reply;
                            });

                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_CLOSE)
    public Mono<RpcReply> close(RpcCallHeader callHeader, ReqInfo reqHeader, CloseCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        long nodeId = context.currFh.ino;
        StateIdReply reply = new StateIdReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_CLOSE.opcode;
//        Inode currentInode = context.getCurrentInode();
        StateId stateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(stateId);
        //释放所有stateId以及关联lockStateId和lock
        reply.stateId = StateId.invalidStateId();
        return client.tryReleaseState(context, stateId, NFS4_OPEN_STID, call.seqId).map(f -> reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_REMOVE)
    public Mono<RpcReply> remove(RpcCallHeader callHeader, ReqInfo reqHeader, RemoveV4Call call) {
        CompoundContext context = call.context;
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        long start = System.currentTimeMillis();
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        RemoveV4Reply reply = new RemoveV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_REMOVE.opcode;
        String bucketName = reqHeader.bucket;
        String[] objName = new String[1];
        Inode[] dirInodes = new Inode[1];
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkDirAndName(currentInode, call.name);
        String name = new String(call.name);
        boolean esSwitch = ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH));
        boolean[] isRmDirErr = new boolean[1];
        boolean[] isRepeat = new boolean[1];
        long optTime = System.currentTimeMillis();
        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, name + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, name + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)))
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    isRepeat[0] = isRequestRepeat(objName[0], optTime);
                    return Mono.just(currentInode);
                })
                .flatMap(dirInode -> {
                    if (isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, context.currFh.ino, dirInode.getLinkN());
                        return Mono.just(dirInode);
                    }
                    if (dirInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        return Mono.just(dirInode);
                    }
                    dirInodes[0] = dirInode;
                    objName[0] = dirInode.getObjName() + new String(call.name, 0, call.name.length);
                    return RedLockClient.lock(reqHeader, objName[0], LockType.WRITE, true, false)
                            .flatMap(lock -> FsUtils.lookup(bucketName, objName[0], reqHeader, false, -1, null));
                })
                .flatMap(newInode -> {
                    if (!(isError(newInode) || newInode.getLinkN() == NO_PERMISSION_INODE.getLinkN())) {
                        return clientControl.getStateIdOps().removeCheck(newInode, context);
                    }
                    return Mono.just(newInode);
                })
                .flatMap(newInode -> {
                    if (isError(newInode) || newInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        return Mono.just(newInode);
                    }

                    if (newInode.getObjName().endsWith("/")) {
                        objName[0] = objName[0] + "/";
                        String prefix = newInode.getObjName();
                        return ReadDirCache.listAndCache(reqHeader.bucket, prefix, 0, 1024, reqHeader.nfsHandler, dirInodes[0].getNodeId(), null, dirInodes[0].getACEs())
                                .flatMap(list1 -> {
                                    if (!list1.isEmpty()) {
                                        isRmDirErr[0] = true;
                                        return Mono.just(ERROR_INODE);
                                    }
                                    return Mono.just(newInode);
                                });
                    }
                    return Mono.just(newInode);
                })
                .flatMap(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        reply.status = ENOENT;
                        if (isRepeat[0]) {
                            reply.status = NfsErrorNo.NFS3_OK;
                        }
                        return Mono.just(reply);
                    } else if (ERROR_INODE.equals(inode)) {
                        reply.status = EIO;
                        if (isRmDirErr[0]) {
                            reply.status = NfsErrorNo.NFS3ERR_NOTEMPTY;
                        }
                        return Mono.just(reply);
                    } else if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        return Mono.just(reply);
                    } else {
//                        if (clientControl.getStateIdOps().existOpen(context.getCurrentInode().getNodeId())) {
//                            reply.status = NFS4ERR_FILE_OPEN;
//                            return Mono.just(reply);
//                        }
                        return clientControl.getStateIdOps().hasDelegateConflict(inode, OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true)
                                .flatMap(conflict -> {
                                    if (conflict) {
                                        reply.status = NFS4ERR_DELAY;
                                        return Mono.just(reply);
                                    }
                                    return nodeInstance.deleteInode(inode.getNodeId(), bucketName, objName[0])
                                            .flatMap(inode1 -> {
                                                return nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), bucketName, inode1.getCtime(), inode1.getCtimensec(), false, true, true)
                                                        .map(i0 -> {
                                                            if (isError(inode1)) {
                                                                reply.status = EIO;
                                                            } else {
                                                                long end = System.currentTimeMillis();
                                                                if (end - start > 55_000L) {
                                                                    NFSHandler.delTimeout.put(currentInode.getNodeId(), end);
                                                                }
                                                                reply.status = OK;
                                                            }
                                                            return reply;
                                                        }).flatMap(i -> {
                                                            if (esSwitch && !isError(inode1)) {
                                                                return EsMetaTask.delEsMeta(inode1.clone().setObjName(objName[0]), inode.clone().setObjName(objName[0]), bucketName, objName[0], inode.getNodeId(), true)
                                                                        .map(b0 -> reply);
                                                            }
                                                            return Mono.just(reply);
                                                        });
                                            });
                                });
                    }
                }).map(res -> (RpcReply) (res)).doFinally(s -> {
                    deleteRequestInodeTimeMap(objName[0], optTime);
                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_WRITE)
    public Mono<RpcReply> write(RpcCallHeader callHeader, ReqInfo reqHeader, WriteV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        WriteV4Reply reply = new WriteV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_WRITE.opcode;
        String bucketName = reqHeader.bucket;
        long nodeId = context.currFh.ino;
        Inode[] inodes = new Inode[1];
        Inode currentInode = context.getCurrentInode();
        Nfs4Utils.checkNotDirAndSym(context.getCurrentInode());

        return CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    return Mono.just(currentInode);
                }).flatMap(i -> {
                    if (!StateId.isStateLess(call.stateId)) {
                        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(call.stateId);
                        NFS4State state = client.state(call.stateId);
                        if (!Nfs4Utils.checkOpenMode(true, state.getShareAccess(), state.type())) {
                            return Mono.error(new NFSException(NFS4ERR_OPENMODE, "shareAccess not support this opt "));
                        }
                        if (context.getMinorVersion() == 0) {
                            clientControl.updateClientLeaseTime(call.stateId);
                        }
                        return Mono.just(i);
                    } else {
                        StateIdOps stateIdOps = clientControl.getStateIdOps();
                        return stateIdOps.checkDeny(currentInode, OPEN_SHARE_DENY_WRITE).flatMap(b -> {
                            if (!b) {
                                return Mono.error(new NFSException(NFS4ERR_LOCKED, "can not read , share deny write"));
                            }
                            return stateIdOps.hasDelegateConflict(currentInode, OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true);
                        }).flatMap(b -> {
                            if (b) {
                                return Mono.error(new NFSException(NFS4ERR_DELAY, "can not write , exist delegation"));
                            }
                            return Mono.just(i);
                        });
                    }
                }).flatMap(inode -> {
                    if (reply.status != 0) {
                        return Mono.just(reply);
                    }
                    if (NO_PERMISSION_INODE.getLinkN() == inode.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        return Mono.just(reply);
                    }
                    if (InodeUtils.isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, nodeId, inode.getLinkN());
                        return Mono.just(reply);
                    }
                    inodes[0] = inode;
                    return FSQuotaUtils.checkFsQuota(inode)
                            .flatMap(i->{
                                return WriteCache.getCache(bucketName, context.currFh.ino, call.stable, inode.getStorage())
                                        .flatMap(fileCache -> fileCache.nfsWrite(call.offset, call.contents, inode, call.stable))
                                        .map(b -> {
                                            reply.count = call.writeLen;
                                            reply.committed = call.stable;

                                            if (!b) {
                                                reply.status = NfsErrorNo.NFS3ERR_I0;
                                            } else {
                                                reply.status = 0;
                                                inode.setSize(call.offset + call.writeLen);
                                            }
                                            return (RpcReply) reply;
                                        }).onErrorResume(e -> {
                                            log.error("", e);
                                            reply.status = NfsErrorNo.NFS3ERR_I0;
                                            if (e instanceof NFSException) {
                                                int stat = ((NFSException) e).getErrCode();
                                                if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                                                    reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                                                }
                                            }
                                            return Mono.just(reply);
                                        });
                            });


                });
    }

    @NFSV4.Opt(value = NFSV4.Opcode.NFS4PROC_READ, buf = 1 << 21)
    public Mono<RpcReply> read(RpcCallHeader callHeader, ReqInfo reqHeader, ReadV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        ReadV4Reply reply = new ReadV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_READ.opcode;
        Nfs4Utils.checkNotDirAndSym(context.getCurrentInode());
        Inode currentInode = context.getCurrentInode();

        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> RedLockClient.lockDir(reqHeader, context.currFh.ino, LockType.READ, true)))
                .flatMap(lock -> {
                    if (!StateId.isStateLess(call.stateId)) {
                        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(call.stateId);
                        NFS4State state = client.state(call.stateId);
                        if (!Nfs4Utils.checkOpenMode(false, state.getShareAccess(), state.type())) {
                            return Mono.error(new NFSException(NFS4ERR_OPENMODE, "shareAccess not support this opt "));
                        }
                        return Mono.just(lock);
                    } else {
                        StateIdOps stateIdOps = clientControl.getStateIdOps();
                        return stateIdOps.checkDeny(currentInode, OPEN_SHARE_DENY_READ).flatMap(b -> {
                                    if (!b) {
                                        return Mono.error(new NFSException(NFS4ERR_LOCKED, "can not read , share deny read"));
                                    } else {
                                        return stateIdOps.hasDelegateConflict(currentInode, OPEN_SHARE_ACCESS_READ, OPEN_SHARE_DENY_NONE, context, null, true);
                                    }
                                })
                                .flatMap(b -> {
                                    if (b) {
                                        return Mono.error(new NFSException(NFS4ERR_DELAY, "can not read , exist delegation"));
                                    }
                                    return Mono.just(lock);
                                });
                    }
                })
                .flatMap(lock -> nodeInstance.getInode(reqHeader.bucket, context.currFh.ino)
                        .flatMap(inode -> {
                            if (isError(inode)) {
                                log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, context.currFh.ino, inode.getLinkN());
                                reply.status = NfsErrorNo.NFS3ERR_I0;
                                reply.eof = 1;
                                reply.contents = new byte[0];
                                return Mono.just(reply);
                            }
                            MonoProcessor<ReadV4Reply> res = MonoProcessor.create();
                            List<Inode.InodeData> inodeData = inode.getInodeData();
                            if (inodeData.isEmpty()) {
                                if (inode.getSize() <= 0) {//处理0kb大小的文件
                                    reply.status = 0;
                                } else {
                                    reply.status = EIO;
                                }
                                reply.readLen = 0;
                                reply.eof = 1;
                                reply.contents = new byte[0];
                                return Mono.just(reply);
                            }
                            long cur = 0;
                            long readOffset = call.offset;
                            long readEnd = call.offset + call.count;
                            long inodeEnd = inode.getSize();
                            if (readEnd >= inodeEnd) {
                                readEnd = inodeEnd;
                                reply.eof = 1;
                                call.count = (int) (readEnd - call.offset);
                            }

                            if (call.count <= 0) {
                                reply.status = 0;
                                reply.eof = 1;
                                reply.contents = new byte[0];
                                return Mono.just(reply);
                            }

                            int readN = 0;
                            byte[] bytes = new byte[call.count];

                            Flux<Tuple2<Integer, byte[]>> flux = Flux.empty();

                            for (InodeData data : inodeData) {
                                long curEnd = cur + data.size;
                                if (readEnd < cur) {
                                    break;
                                } else if (readOffset > curEnd) {
                                    cur = curEnd;
                                } else {
                                    long readFileOffset = data.offset + (readOffset - cur);
                                    int readFileSize = (int) (Math.min(readEnd, curEnd) - readOffset);

                                    flux = flux.mergeWith(FsUtils.readObj(readN, data.storage, reqHeader.bucket,
                                            data.fileName, readFileOffset, readFileSize, data.size));

                                    readOffset += readFileSize;
                                    readN += readFileSize;

                                    if (readOffset >= readEnd) {
                                        break;
                                    }

                                    cur = curEnd;
                                }
                            }
                            flux.flatMap(t -> BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_read.name() + "-" + BAND_WIDTH_QUOTA, t.var2.length)
                                            .flatMap(waitMillis -> {
                                                String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                                                return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + BAND_WIDTH_QUOTA, t.var2.length).map(waitMillis2 -> waitMillis + waitMillis2);
                                            })
                                            .flatMap(waitMillis -> Mono.just(t).delayElement(Duration.ofMillis(waitMillis)))
                                    ).doOnNext(t -> {
                                        System.arraycopy(t.var2, 0, bytes, t.var1, t.var2.length);
                                    })
                                    .doOnError(e -> {
                                        log.error("", e);
                                        reply.status = EIO;
                                        reply.readLen = 0;
                                        reply.contents = new byte[0];
                                        res.onNext(reply);
                                    })
                                    .doOnComplete(() -> {
                                        reply.status = 0;
                                        reply.readLen = bytes.length;
                                        reply.contents = bytes;
                                        InodeUtils.updateInodeAtime(inode);
                                        res.onNext(reply);

                                    }).subscribe();

                            return res;
                        }));
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_LINK)
    public Mono<RpcReply> link(RpcCallHeader callHeader, ReqInfo reqHeader, LinkV4Call call) {
        CompoundContext context = call.context;
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        LinkV4Reply reply = new LinkV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LINK.opcode;
        String bucket = reqHeader.bucket;
        Inode[] inodes = new Inode[]{ERROR_INODE, ERROR_INODE};
        boolean[] isDirInode = new boolean[]{false};
        Nfs4Utils.checkDirAndNotDir(context.getCurrentInode(), context.savedInode);
        return RedLockClient.lockDir(reqHeader, context.currFh.ino, LockType.WRITE, true)
                .flatMap(l -> CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt))
                .flatMap(res -> {
                    return NFSBucketInfo.getBucketInfoReactive(reqHeader.bucket)
                            .map(map -> {
                                reqHeader.bucketInfo = map;
                                return res;
                            });
                })
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    return Mono.just(context.getCurrentInode());
                })
                .flatMap(dirInode -> {
                    if (InodeUtils.isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, context.currFh.ino, dirInode.getLinkN());
                        return Mono.just(dirInode);
                    }
                    if (dirInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        return Mono.just(dirInode);
                    }
                    return nodeInstance.getInode(bucket, context.saveFh.ino)
                            .flatMap(oldInode -> {
                                if (isError(oldInode)) {
                                    return Mono.just(oldInode);
                                }
                                inodes[0] = oldInode;
                                byte[] buf = call.name;
                                String name = dirInode.getObjName() + new String(buf, 0, buf.length);

                                if (name.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                                    return Mono.just(NAME_TOO_LONG_INODE);
                                }
                                if (inodes[0].getObjName().endsWith("/") || (inodes[0].getMode() & S_IFMT) == S_IFDIR) {
                                    isDirInode[0] = true;
                                    return Mono.just(oldInode);
                                }
                                return nodeInstance.createHardLink(bucket, oldInode.getNodeId(), name);
                            });
                })
                .flatMap(inode -> {
                    inodes[1] = inode;
                    if (isError(inode)) {
                        log.error("create hard link inode fail... {}", inode.getLinkN());
                        reply.status = EIO;
                    } else if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                    } else if (NAME_TOO_LONG_INODE.equals(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_NAMETOOLONG;
                    } else if (FILES_QUOTA_EXCCED_INODE.getLinkN() == inode.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                    } else if (isDirInode[0]) {
                        reply.status = NfsErrorNo.NFS3ERR_ISDIR;
                    } else {
                        reply.status = OK;
                    }
                    if (reply.status == NfsErrorNo.NFS3_OK) {
                        return nodeInstance.updateInodeTime(call.context.currFh.ino, bucket, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true);
                    }
                    return Mono.just(inode);
                }).flatMap(i -> {
                    if (ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH)) && !isError(inodes[1]) && !NAME_TOO_LONG_INODE.equals(inodes[1])) {
                        return EsMetaTask.putLinkEsMeta(inodes[1], inodes[0]).map(b0 -> reply);
                    }
                    return Mono.just(reply);
                })
                .map(i -> reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SAVEFH)
    public Mono<RpcReply> saveFH(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        CompoundContext context = call.context;
//        reqHeader.bucket = getBucketName(context.currFh.fsid);
//        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (context.currFh == null) {
            reply.status = NFS4ERR_NOFILEHANDLE;
            return Mono.just(reply);
        }
        context.savedInode = context.getCurrentInode();
        context.saveFh = context.currFh;
        context.saveStateId = context.currStateId;
        reply.opt = NFSV4.Opcode.NFS4PROC_SAVEFH.opcode;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_RESTOREFH)
    public Mono<RpcReply> restoreFH(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        CompoundContext context = call.context;
//        reqHeader.bucket = getBucketName(context.currFh.fsid);
//        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_RESTOREFH.opcode;
        if (context.saveFh == null) {
            reply.status = NFS4ERR_RESTOREFH;
            return Mono.just(reply);
        }
        context.setCurrentInode(context.savedInode);
        context.currFh = context.saveFh;
        context.currStateId = context.saveStateId;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_COMMIT)
    public Mono<RpcReply> commit(RpcCallHeader callHeader, ReqInfo reqHeader, CommitV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        CommitV4Reply reply = new CommitV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_COMMIT.opcode;
        long nodeId = context.currFh.ino;
        Inode[] inodes = new Inode[1];
        return CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }
                    return Mono.just(context.getCurrentInode());
                }).flatMap(inode -> {
                    if (NO_PERMISSION_INODE.getLinkN() == inode.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        return Mono.just(reply);
                    }
                    if (InodeUtils.isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, nodeId, inode.getLinkN());
                        return Mono.just(reply);
                    }
                    inodes[0] = inode;
                    return WriteCache.getCache(reqHeader.bucket, nodeId, 0, inode.getStorage())
                            .flatMap(nfsCache -> nfsCache.nfsCommit(inode, call.offset, call.count))
                            .map(b -> {
                                if (b) {
                                    reply.status = 0;
                                } else {
                                    reply.status = NfsErrorNo.NFS3ERR_I0;
                                }
                                return (RpcReply) reply;
                            });
                }).onErrorResume(e -> {
                    log.error("", e);
                    reply.status = NfsErrorNo.NFS3ERR_I0;
                    if (e instanceof NFSException) {
                        int stat = ((NFSException) e).getErrCode();
                        if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                            reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                        }
                    }
                    return Mono.just(reply);
                });
    }


    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_READLINK)
    public Mono<RpcReply> readLink(RpcCallHeader callHeader, ReqInfo reqHeader, CommonCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        ReadLinkV4Reply reply = new ReadLinkV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_READLINK.opcode;
        String bucket = reqHeader.bucket;
        Nfs4Utils.checkNotSymlink(context.getCurrentInode());
        return Mono.just(context.getCurrentInode())
                .map(inode -> {
                    if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                        String link = inode.getReference();
                        reply.status = NfsErrorNo.NFS3_OK;
                        reply.link = link.getBytes();
                    } else {
                        log.error("create symlink inode fail...bucket:{},dirInodeId:{}", bucket, context.currFh.ino);
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        reply.link = new byte[0];
                    }
                    return reply;
                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_RENAME)
    public Mono<RpcReply> rename(RpcCallHeader callHeader, ReqInfo reqHeader, ReNameV4Call call) {
        CompoundContext context = call.context;
        checkIsRoot(context.currFh.fsid);
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        ReNameV4Reply reply = new ReNameV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_RENAME.opcode;
        String bucket = reqHeader.bucket;
        AtomicReference<String> oldObjectName = new AtomicReference<>();
        AtomicReference<String> newObjName = new AtomicReference<>();

        AtomicBoolean dir = new AtomicBoolean(false);
        AtomicBoolean overWrite = new AtomicBoolean(false);
        Inode[] dirInodes = new Inode[2];
        boolean[] isReqRepeat = new boolean[]{false};
        long optTime = System.currentTimeMillis();
        Nfs4Utils.checkDirAndDir(context.getCurrentInode(), context.getSavedInode());
//        if (clientControl.getStateIdOps().existOpen(context.getCurrentInode().getNodeId())) {
//            reply.status = NFS4ERR_FILE_OPEN;
//            return Mono.just(reply);
//        }

        return Flux.just(new Tuple2<>(true, context.saveFh.ino), new Tuple2<>(false, context.currFh.ino))
                .flatMap(t -> CheckUtils.writePermissionCheckReactive(reqHeader.bucket, callHeader.opt)
                        .flatMap(res -> {
                            if (!res) {
                                return Mono.just(NO_PERMISSION_INODE);
                            }
                            return nodeInstance.getInode(bucket, t.var2);
                        })
                        .flatMap(dirInode -> {
                            if (isError(dirInode)) {
                                log.info("get inode fail.bucket:{}, from dir nodeId:{},to dir nodeId{}:{}", reqHeader.bucket, context.saveFh.ino, context.currFh.ino, dirInode.getLinkN());
                                return Mono.just(new Tuple2<>(t.var1, dirInode));
                            }
                            if (dirInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                                return Mono.just(new Tuple2<>(t.var1, dirInode));
                            }
                            byte[] name = t.var1 ? call.sourceName : call.targetName;
                            String objName = dirInode.getObjName() + new String(name, 0, name.length);
                            if (t.var1) {
                                dirInodes[0] = dirInode;
                                oldObjectName.set(objName);
                                isReqRepeat[0] = isRequestRepeat(objName, optTime);
                            } else {
                                dirInodes[1] = dirInode;
                                newObjName.set(objName);
                            }
                            return RedLockClient.lock(reqHeader, objName, LockType.WRITE, true, t.var1)
                                    .flatMap(lock -> FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs()))
                                    .map(inode -> {
                                        //mv dir
                                        if (t.var1 && StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/")) {
                                            dir.set(true);
                                        }
                                        return new Tuple2<>(t.var1, inode);
                                    });
                        })).collectList()
                .flatMap(list -> {
                    Inode[] oldInode = new Inode[]{ERROR_INODE};
                    Inode[] newInode = new Inode[]{ERROR_INODE};

                    for (Tuple2<Boolean, Inode> tuple2 : list) {
                        if (tuple2.var1) {
                            oldInode[0] = tuple2.var2;
                        } else {
                            newInode[0] = tuple2.var2;
                        }
                    }
                    if (debug) {
                        log.info("【rename】 old: {} {}, new: {} {}", oldInode[0].getObjName(), oldInode[0].getLinkN(), newInode[0].getObjName(), newInode[0].getLinkN());
                    }

                    if (ERROR_INODE.equals(oldInode[0]) || NOT_FOUND_INODE.equals(oldInode[0])) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        if (NOT_FOUND_INODE.equals(oldInode[0])) {
                            reply.status = NfsErrorNo.NFS3ERR_NOENT;
                            if (isReqRepeat[0]) {
                                reply.status = NfsErrorNo.NFS3_OK;
                            }
                        }
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    if (oldInode[0].getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        reply.status = NfsErrorNo.NFS3ERR_ROFS;
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    // 如果新名称的Inode已存在，则应当覆盖已存在的文件；如果inode为retryInode，表示返回的是当前目录下子目录的inode，为客户端长时间未接收到响应重发的rename请求，应当直接返回
                    if (!isError(newInode[0])) {
                        if (RETRY_INODE.getLinkN() == newInode[0].getLinkN() && newInode[0].getNodeId() == -3) {
                            //正在重命名，返回结果为空
                            return Mono.just(reply)
                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                        }
                        overWrite.set(true);
                    }
                    String[] oldObjName0 = new String[1];
                    String[] newObjName0 = new String[1];

                    oldObjName0[0] = dir.get() ? oldObjectName.get() + '/' : oldObjectName.get();
                    newObjName0[0] = dir.get() ? newObjName.get() + '/' : newObjName.get();
                    if (newObjName0[0].getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                        reply.status = NfsErrorNo.NFS3ERR_NAMETOOLONG;
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }

                    // 如果更改的为目录
                    if (dir.get()) {
                        // 如果更新后的目录名已经存在，则检查这个新的目录名之下是否还有与旧目录名称相同的dirInode
                        return clientControl.getStateIdOps().hasDelegateConflict(oldInode[0], OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true)
                                .flatMap(b -> {
                                    if (b) {
                                        reply.status = NFS4ERR_DELAY;
                                        return Mono.just(reply);
                                    }
                                    return Mono.just(overWrite.get())
                                            .flatMap(isOverWrite -> {
                                                return FSQuotaUtils.canRename(bucket, oldInode[0].getNodeId(), newInode[0])
                                                        .flatMap(can -> {
                                                            if (!can) {
                                                                reply.status = NfsErrorNo.NFS3ERR_I0;
                                                                return Mono.just(reply)
                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                            }
                                                            if (isOverWrite) {
                                                                String prefix = newInode[0].getObjName();
                                                                return ReadDirCache.listAndCache(bucket, prefix, 0, 4096, reqHeader.nfsHandler, newInode[0].getNodeId(), null, newInode[0].getACEs())
                                                                        .flatMap(list1 -> {
                                                                            if (!list1.isEmpty()) {
                                                                                log.error("directory already exists: {}", newObjName0[0]);
                                                                                reply.status = NfsErrorNo.NFS3ERR_EXIST;
                                                                                return Mono.just(reply)
                                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                            }
                                                                            if (newObjName0[0].startsWith(oldInode[0].getObjName())) {
                                                                                reply.status = NfsErrorNo.NFS3ERR_INVAL;
                                                                                return Mono.just(reply)
                                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                            }
                                                                            MonoProcessor<ReNameV4Reply> reNameReplyRes = MonoProcessor.create();
                                                                            Mono.defer(() -> scanAndReName0(oldInode[0], bucket, newObjName0[0], 0, reqHeader, reply, dirInodes, context.saveFh.fsid))
                                                                                    .doOnSubscribe(subscription -> {
                                                                                        reqHeader.optCompleted = false;
                                                                                    })
                                                                                    .doOnNext(reply0 -> {
                                                                                        reqHeader.optCompleted = true;
                                                                                        if (!reqHeader.timeout) {
                                                                                            deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                                                                                        }
                                                                                        reNameReplyRes.onNext(reply0);
                                                                                    })
                                                                                    .subscribe();
                                                                            return reNameReplyRes;
                                                                        });
                                                            } else {
                                                                // 将oldInode视为dirInode，遍历该目录下的所有子目录与文件
                                                                // 重命名时不需要再get新的inode，直接rename即可
                                                                if (newObjName0[0].startsWith(oldInode[0].getObjName())) {
                                                                    reply.status = NfsErrorNo.NFS3ERR_INVAL;
                                                                    return Mono.just(reply)
                                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                }
                                                                MonoProcessor<ReNameV4Reply> reNameReplyRes = MonoProcessor.create();
                                                                Mono.defer(() -> scanAndReName0(oldInode[0], bucket, newObjName0[0], 0, reqHeader, reply, dirInodes, context.saveFh.fsid))
                                                                        .doOnSubscribe(subscription -> {
                                                                            reqHeader.optCompleted = false;
                                                                        })
                                                                        .doOnNext(reply0 -> {
                                                                            reqHeader.optCompleted = true;
                                                                            if (!reqHeader.timeout) {
                                                                                deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                                                                            }
                                                                            reNameReplyRes.onNext(reply0);
                                                                        })
                                                                        .subscribe();
                                                                return reNameReplyRes;
                                                            }
                                                        });
                                            });
                                });
                    } else {
                        // 如果更改的为文件
                        return clientControl.getStateIdOps().hasDelegateConflict(oldInode[0], OPEN_SHARE_ACCESS_WRITE, OPEN_SHARE_DENY_NONE, context, null, true)
                                .flatMap(b -> {
                                    if (b) {
                                        reply.status = NFS4ERR_DELAY;
                                        return Mono.just(reply);
                                    }
                                    return renameFile(oldInode[0].getNodeId(), newInode[0], oldObjName0[0], newObjName0[0], bucket, overWrite.get(), new AtomicInteger(0))
                                            .flatMap(inode -> {
                                                if (isError(inode) || inode.isDeleteMark()) {
                                                    if (NOT_FOUND_INODE.getLinkN() == inode.getLinkN() || inode.isDeleteMark()) {
                                                        reply.status = NfsErrorNo.NFS3ERR_NOENT;
                                                    }
                                                    if (ERROR_INODE.getLinkN() == inode.getLinkN()) {
                                                        reply.status = NfsErrorNo.NFS3ERR_I0;
                                                    }
                                                    return Mono.just(reply)
                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                }
                                                return Mono.just(inode)
                                                        .flatMap(i -> updateTime(dirInodes, reply, bucket))
                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                            });
                                });

                    }
                })
                .map(res -> (RpcReply) res)
                .doFinally(s -> {
                    deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                    if (reqHeader.optCompleted && !reqHeader.repeat) {
                        deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                    } else {
                        log.info("rename running {}", oldObjectName.get());
                    }
                    reqHeader.timeout = true;
                });
    }

    /**
     * rename完成后更新fromDirFh与toDirFh代表的父级目录的mtime和ctime
     **/
    public Mono<ReNameV4Reply> updateTime(Inode[] dirInodes, ReNameV4Reply reNameReply, String bucket) {
        List<Inode> inodeList = Arrays.stream(dirInodes).filter(Objects::nonNull).collect(Collectors.toList());

        // 如果两个inode的nodeId相等，只发送一次更改时间的请求
        if (inodeList.size() == 2 && !isError(dirInodes[0]) &&
                !isError(dirInodes[1]) && dirInodes[0].getNodeId() == dirInodes[1].getNodeId()) {
            return nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), dirInodes[0].getBucket(), System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .map(inode -> {
                        dirInodes[0] = inode;
                        dirInodes[1] = inode;
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 1", e))
                    .onErrorReturn(reNameReply);
        } else {
            long stamp = System.currentTimeMillis() / 1000;
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            return Flux.fromIterable(inodeList)
                    .flatMap(dirInode -> nodeInstance.updateInodeTime(dirInode.getNodeId(), dirInode.getBucket(), stamp, stampNano, false, true, true))
                    .collectList()
                    .map(list -> {
                        if (list.get(0).getNodeId() == dirInodes[0].getNodeId()) {
                            dirInodes[0] = list.get(0);
                            dirInodes[1] = list.get(1);
                        } else {
                            dirInodes[1] = list.get(0);
                            dirInodes[0] = list.get(1);
                        }
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 2", e))
                    .onErrorReturn(reNameReply);
        }
    }

    public Mono<Inode> renameFile(long oldNodeId, Inode newInode, String oldObjName, String newObjName, String bucket, boolean isOverWrite, AtomicInteger renameNum) {
        if (renameNum != null) {
            int num = renameNum.incrementAndGet();
            if ((num % 100000) == 0) {
                log.info("rename {} --> {}, num:{}", oldObjName, newObjName, num);
            }
        }
        boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(bucket).get(ES_SWITCH));
        return Mono.just(isOverWrite)
                .flatMap(b -> {
                    if (b) {
                        // mv a.txt b.txt，如果b.txt文件已存在，则需在改名前删除b.txt
                        return nodeInstance.deleteInode(newInode.getNodeId(), newInode.getBucket(), newInode.getObjName())
                                .flatMap(f -> esSwitch && !isError(f) ? EsMetaTask.delEsMeta(f, newInode, bucket, newObjName, newInode.getNodeId(), false).map(v -> f) : Mono.just(f))
                                .flatMap(inode1 -> {
                                    return InodeUtils.updateParentDirTime(newInode.getObjName(), bucket)
                                            .map(i0 -> {
                                                if (isError(inode1)) {
                                                    log.error("rename: delete {} failed: {}", newObjName, inode1.getLinkN());
                                                    return false;
                                                } else {
                                                    return true;
                                                }
                                            });
                                });
                    } else {
                        return Mono.just(true);
                    }
                })
                .flatMap(b0 -> {
                    if (!b0) {
                        return Mono.just(ERROR_INODE);
                    }

                    return nodeInstance.renameFile(oldNodeId, oldObjName, newObjName, bucket)
                            .flatMap(inode -> {
                                if (isError(inode)) {
                                    log.info("rename {}->{} fail: {}", oldObjName, newObjName, inode.getLinkN());
                                    return Mono.just(inode);
                                }
                                return Mono.just(inode).flatMap(i -> {
                                    if (esSwitch) {
                                        Inode cInode = inode.clone();
                                        return EsMetaTask.mvEsMeta(cInode, bucket, oldObjName, newObjName, oldNodeId);
                                    }
                                    return Mono.just(true);
                                }).map(a -> inode);
                            });
                });
    }

    /**
     * @param dirInode   当前的目录的完整路径 /test0/fio/old/
     * @param newObjName 当前目录所要更改成的新目录名 /test0/fio/new/
     * @param bucket     桶名
     * @param count      offset
     **/
    public Mono<Inode> scanAndReName(Inode dirInode, String bucket, String newObjName, long count, NFSHandler nfsHandler, AtomicInteger renameNum) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        AtomicLong offset = new AtomicLong(count);
        UnicastProcessor<Boolean> listController = UnicastProcessor.create(Queues.<Boolean>unboundedMultiproducer().get());
        listController.concatMap(isEmpty -> {
                    // 如果是非空的话则继续往下遍历；如果是空的话结束遍历，直接更改当前目录本身
                    return scanAndReNamePart(dirInode, bucket, newObjName, offset, nfsHandler, renameNum)
                            .flatMap(list -> {
                                if (!list.isEmpty()) {
                                    listController.onNext(false);
                                    return Mono.just(false);
                                } else {
                                    listController.onComplete();
                                    return Mono.just(true);
                                }
                            })
                            .onErrorResume(throwable -> {
                                listController.onComplete();
                                res.onError(throwable);
                                return Mono.just(false);
                            });
                })
                .doOnComplete(() -> {
                    res.onNext(true);
                })
                .subscribe();
        listController.onNext(false);
        return res
                .flatMap(ignore -> renameFile(dirInode.getNodeId(), null, dirInode.getObjName(), newObjName, bucket, false, renameNum))
                .onErrorResume(throwable -> Mono.just(ERROR_INODE));
    }

    public Mono<ReNameV4Reply> scanAndReName0(Inode oldInode, String bucket, String newObjName, long offset, ReqInfo reqHeader, ReNameV4Reply reNameReply, Inode[] dirInodes, long fsid) {
        AtomicInteger renameNum = new AtomicInteger();
        return scanAndReName(oldInode, bucket, newObjName, offset, reqHeader.nfsHandler, renameNum)
                .flatMap(inode -> {
                    if (isError(inode)) {
                        log.info("rename {}->{} fail: {}", oldInode.getObjName(), newObjName, inode.getLinkN());
                    }
                    if (renameNum.get() >= 100000) {
                        log.info("renameNum {}", renameNum.get());
                    }
                    return updateTime(dirInodes, reNameReply, bucket)
                            .onErrorReturn(reNameReply)
                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                });
    }

    public Mono<List<Inode>> scanAndReNamePart(Inode dirInode, String bucket, String newObjName, AtomicLong offset, NFSHandler nfsHandler, AtomicInteger renameNum) {
        String prefix = dirInode.getObjName();
        return ReadDirCache.listAndCache(bucket, prefix, offset.get(), 1 << 20, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                .flatMap(list -> {
                    if (list.size() > 0) {
                        offset.set(list.get(list.size() - 1).getCookie());
                    }
                    return Mono.just(list);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(inode -> {
                    String curOldName = inode.getObjName();
                    String[] nameArray = inode.getObjName().split("/");
                    String curNewName = newObjName + nameArray[nameArray.length - 1];
//                    offset.accumulateAndGet(inode.getCookie(), (oldVal, newVal) -> oldVal > newVal ? oldVal : newVal);
                    if ((inode.getMode() & S_IFMT) == S_IFDIR) {
                        // 如果是目录则进行递归
                        if (!curNewName.endsWith("/")) {
                            curNewName = curNewName + "/";
                        }
                        return scanAndReName(inode, bucket, curNewName, 0, nfsHandler, renameNum);
                    } else {
                        // 如果是文件则进行重命名
                        return renameFile(inode.getNodeId(), null, curOldName, curNewName, bucket, false, renameNum);
                    }
                })
                .collectList();
    }


    public Mono<ReNameV4Reply> releaseLock(ReqInfo reqHeader, ReNameV4Reply reply) {
        if (!reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_LOCK)
    public Mono<RpcReply> lock(RpcCallHeader callHeader, ReqInfo reqHeader, LockV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        LockV4Reply reply = new LockV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LOCK.opcode;
        Inode inode = context.getCurrentInode();
        if (call.length == 0) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        NFS4Client client;
        StateId oldStateId;
        oldStateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
        client = call.isNewOwner ? context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getConfirmedClient(call.clientId)
                : clientControl.getClient(oldStateId);
        NFS4State lockState = client.lockState(context, client, oldStateId, call.owner, call.lockSeqId, call.seqId, call.isNewOwner);
        byte[] sessionId = context.minorVersion >= 1 ? context.getSessionId() : new byte[0];
        NFS4Lock nfs4Lock = NFS4Lock.newLock(call.lockType, lockState.getStateOwner(), call.offset, call.length, node,
                client.getClientId(), context.minorVersion, sessionId, FH2.mapToFH2(inode, context.currFh.fsid));
        nfs4Lock.clientLock = true;
        nfs4Lock.clientUnLock = true;
        return Mono.just(true).flatMap(b -> {
                    if (call.lockType == WRITEW_LT || call.lockType == WRITE_LT) {
                        NFS4State state = lockState.getOpenState();
                        if (!Nfs4Utils.checkOpenMode(true, state.getShareAccess(), state.type())) {
                            return Mono.error(new NFSException(NFS4ERR_OPENMODE, "shareAccess not support this opt "));
                        }
                    }
                    return Mono.just(true);
                }).flatMap(b -> NFS4LockClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), nfs4Lock))
                .flatMap(lock -> {
                    if (NFS4Lock.ERROR_LOCK.equals(lock)) {
                        client.removeState(lockState.stateId());
                        reply.status = EIO;
                    } else if (NFS4Lock.DEFAULT_LOCK.equals(lock)) {
                        context.setCurrStateId(lockState.stateId());
                        reply.stateId = lockState.stateId();
                        lockState.addLockDispose(nfs4Lock, s -> NFS4LockClient.unLockOrRemoveWait(inode.getBucket(), String.valueOf(inode.getNodeId()), nfs4Lock).map(f -> true));
                    } else {
                        client.removeState(lockState.stateId());
                        reply.status = NFS4ERR_DENIED;
                        reply.lockType = lock.lockType;
                        reply.offset = lock.offset;
                        reply.length = lock.length;
                        reply.owner = lock.stateOwner.owner.owner;
                        reply.clientId = lock.stateOwner.owner.clientId;
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_LOCKU)
    public Mono<RpcReply> lockU(RpcCallHeader callHeader, ReqInfo reqHeader, LockUV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        LockV4Reply reply = new LockV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LOCKU.opcode;
        Inode inode = context.getCurrentInode();
        NFS4Client client = context.getMinorVersion() > 0 ? context.getSession().getClient() : clientControl.getClient(call.stateId);
        NFS4State lockState = client.state(call.stateId);
        if (lockState.type() != NFS4_LOCK_STID) {
            reply.status = NFS4ERR_BAD_STATEID;
            return Mono.just(reply);
        }
        StateId oldStateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
        StateId.checkStateId(lockState.stateId(), oldStateId);
        StateOwner lockOwner = lockState.getStateOwner();
        if (context.getMinorVersion() == 0) {
            lockOwner.incrSequence(call.seqId);
        }
        String node = ServerConfig.getInstance().getHostUuid();
        byte[] sessionId = context.minorVersion >= 1 ? context.getSessionId() : new byte[0];
        NFS4Lock nfs4Lock = NFS4Lock.newLock(call.lockType, lockOwner, call.offset, call.length, node,
                client.getClientId(), context.minorVersion, sessionId, FH2.mapToFH2(inode, context.currFh.fsid));
        nfs4Lock.clientUnLock = true;
        return NFS4LockClient.unLockOrRemoveWait(inode.getBucket(), String.valueOf(inode.getNodeId()), nfs4Lock)
                .flatMap(lock -> {
                    if (NFS4Lock.ERROR_LOCK.equals(lock)) {
                        reply.status = EIO;
                    } else if (NFS4Lock.DEFAULT_LOCK.equals(lock)) {
                        lockState.incrSeqId();
                        context.setCurrStateId(lockState.stateId());
                        lockState.removeLockDispose(nfs4Lock);
                        reply.stateId = lockState.stateId();
                    } else {
                        reply.status = NFS4ERR_BAD_RANGE;
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_LOCKT)
    public Mono<RpcReply> lockT(RpcCallHeader callHeader, ReqInfo reqHeader, LockTV4Call call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_LOCKT.opcode;
        String bucket = reqHeader.bucket;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_FREE_STATEID)
    public Mono<RpcReply> freeStateId(RpcCallHeader callHeader, ReqInfo reqHeader, StateIdCall call) {
        //no putFh
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_FREE_STATEID.opcode;
        CompoundContext context = call.context;
        String bucket = reqHeader.bucket;
        NFS4Client client = context.getSession().getClient();
        StateId stateId = StateId.getCurrStateIdIfNeeded(context, call.stateId);
        return  client.releaseState(stateId, NFS4_LOCK_STID).map(v -> reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_TEST_STATEID)
    public Mono<RpcReply> testStateId(RpcCallHeader callHeader, ReqInfo reqHeader, TestStateIdCall call) {
        //no putFh
        TestStateIdReply reply = new TestStateIdReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_TEST_STATEID.opcode;
        CompoundContext context = call.context;
//        String bucket = reqHeader.bucket;
        NFS4Client client = context.getSession().getClient();
        for (int i = 0; i < call.stateIds.size(); i++) {
            StateId stateId = call.stateIds.get(i);
            try {
                NFS4State state = client.state(stateId);
                int status = state.stateId().seqId < stateId.seqId ? NFS4ERR_OLD_STATEID : OK;
                reply.stateIdStatus.add(status);
            } catch (Exception e) {
                if (e instanceof NFSException) {
                    NFSException ex = (NFSException) e;
                    reply.stateIdStatus.add(ex.getErrCode());
                } else {
                    reply.status = EIO;
                    return Mono.just(reply);
                }
            }
        }
        reply.num = call.num;
        reply.status = OK;
        return Mono.just(reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_DELEGRETURN)
    public Mono<RpcReply> delegreturn(RpcCallHeader callHeader, ReqInfo reqHeader, StateIdCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        long nodeId = context.currFh.ino;
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_DELEGRETURN.opcode;
//        if (context.getMinorVersion() == 0) {
//            reply.status = NFS3ERR_NOTSUPP;
//            return Mono.just(reply);
//        }
        Inode inode = context.getCurrentInode();
        Nfs4Utils.checkNotDir(inode);
        NFS4Client client = clientControl.getClient(call.stateId);
        return client.tryReleaseState(context, call.stateId, NFS4_DELEG_STID, 0).map(v -> reply);
    }


    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_RELEASE_LOCKOWNER)
    public Mono<RpcReply> releaseLockOwner(RpcCallHeader callHeader, ReqInfo reqHeader, ReleaseLockOwnerCall call) {
        //one opt
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_RELEASE_LOCKOWNER.opcode;
        CompoundContext context = call.context;
        String bucket = reqHeader.bucket;
        if (call.getMinorVersion() > 0) {
            reply.status = NFS3ERR_NOTSUPP;
            return Mono.just(reply);
        }
        NFS4Client client = clientControl.getConfirmedClient(call.clientId);
        client.updateLeaseTime();
        return client.releaseOwner(call.owner).map(v -> reply);
    }

    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_VERIFY)
    public Mono<RpcReply> verify(RpcCallHeader callHeader, ReqInfo reqHeader, VerifyCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_VERIFY.opcode;
        String bucket = reqHeader.bucket;
        CompoundContext context = call.context;
        int fsid = context.currFh.fsid;
        FAttr4 fAttr4 = call.fAttr4;
        if (fAttr4.mask.length > 0 && (fAttr4.mask[0] & Mask1.rDAttrError.mask) != 0) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        return Mono.just(context.getCurrentInode())
                .flatMap(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        reply.status = ENOENT;
                    } else if (ERROR_INODE.equals(inode)) {
                        reply.status = EIO;
                    } else if (checkAttrSame(fAttr4.objAttr, inode)) {
                        reply.status = NFS4ERR_NOT_SAME;
                    }
                    return Mono.just(reply);
                });
    }


    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_NVERIFY)
    public Mono<RpcReply> nVerify(RpcCallHeader callHeader, ReqInfo reqHeader, VerifyCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_NVERIFY.opcode;
        CompoundContext context = call.context;
        String bucket = reqHeader.bucket;
        int fsid = context.currFh.fsid;
        FAttr4 fAttr4 = call.fAttr4;
        if (fAttr4.mask.length > 0 && (fAttr4.mask[0] & Mask1.rDAttrError.mask) != 0) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        return Mono.just(context.getCurrentInode())
                .flatMap(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        reply.status = ENOENT;
                    } else if (ERROR_INODE.equals(inode)) {
                        reply.status = EIO;
                    } else if (!checkAttrSame(fAttr4.objAttr, inode)) {
                        reply.status = NFS4ERR_SAME;
                    }
                    return Mono.just(reply);
                });
    }

    //未验证
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_BACKCHANNEL_CTL)
    public Mono<RpcReply> backChannelCtl(RpcCallHeader callHeader, ReqInfo reqHeader, BackChannelCtlCall call) {
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_BACKCHANNEL_CTL.opcode;
        CompoundContext context = call.context;
        String bucket = reqHeader.bucket;
        int fsid = context.currFh.fsid;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    //以下为NFS V4.2请求

    //未验证
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_ALLOCATE)
    public Mono<RpcReply> allocate(RpcCallHeader callHeader, ReqInfo reqHeader, AllocateCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        context.currStateId = call.stateId;
        reply.opt = NFSV4.Opcode.NFS4PROC_ALLOCATE.opcode;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    //未验证
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_DEALLOCATE)
    public Mono<RpcReply> deallocate(RpcCallHeader callHeader, ReqInfo reqHeader, AllocateCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_DEALLOCATE.opcode;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    //真copy
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_COPY)
    public Mono<RpcReply> copy(RpcCallHeader callHeader, ReqInfo reqHeader, CopyCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        //saveFh currFh 更改currStateId
        CopyReply reply = new CopyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_COPY.opcode;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    //未验证
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_SEEK)
    public Mono<RpcReply> seek(RpcCallHeader callHeader, ReqInfo reqHeader, SeekCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        SeekReply reply = new SeekReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_SEEK.opcode;
        reply.status = NFS3ERR_NOTSUPP;
        return Mono.just(reply);
    }

    //参考s3 假copy
    @NFSV4.Opt(NFSV4.Opcode.NFS4PROC_CLONE)
    public Mono<RpcReply> clone(RpcCallHeader callHeader, ReqInfo reqHeader, CloneCall call) {
        CompoundContext context = call.context;
        reqHeader.bucket = getBucketName(context.currFh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        EmptyReply reply = new EmptyReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reply.opt = NFSV4.Opcode.NFS4PROC_CLONE.opcode;
        //暂时先不支持
        reply.status = NFS3ERR_NOTSUPP;
//        if (context.minorVersion <= 1) {
//            reply.status = NFS3ERR_NOTSUPP;
//            return Mono.just(reply);
//        }
        Nfs4Utils.checkNotDirAndNotDir(context.getCurrentInode(), context.savedInode);
        NFS4Client client = context.getSession().getClient();
        NFS4State srcState = client.state(call.srcStateId);
        NFS4State dstState = client.state(call.dstStateId);
        //srcState需要read，dstState需要write
        if (srcState.getShareAccess() == OPEN_SHARE_ACCESS_WRITE || dstState.getShareAccess() == OPEN_SHARE_ACCESS_READ) {
            reply.status = NFS4ERR_OPENMODE;
            return Mono.just(reply);
        }
        Inode dstInode = context.getCurrentInode();
        Inode srcInode = context.savedInode;
        call.count = call.count == 0 ? srcInode.getSize() : call.count;
        if (call.srcOffset > srcInode.getSize() || call.srcOffset + call.count > srcInode.getSize()
                || (dstInode.getNodeId() == srcInode.getNodeId() && !(call.srcOffset + call.count <= call.dstOffset ||
                call.dstOffset + call.count <= call.srcOffset))) {
            reply.status = NFS3ERR_INVAL;
            return Mono.just(reply);
        }
        return Mono.just(reply);
        //涉及两个文件
//        return Mono.just(true).flatMap(b -> {
//            long remaining = call.count;
//            long currentSrcPos = call.srcOffset;
//            long currentDstPos = call.dstOffset;
//            List<Tuple2<InodeData, Long>> inodeDataList = new ArrayList<>();
//            for (InodeData srcData : srcInode.getInodeData()) {
//                if (currentSrcPos >= srcData.size) {
//                    currentSrcPos -= srcData.size;
//                    continue;
//                }
//                long dataAvailable = srcData.size - currentSrcPos;
//                long bytesToClone = Math.min(remaining, dataAvailable);
//                inodeDataList.add(new Tuple2<>(srcData, currentDstPos));
//                remaining -= bytesToClone;
//                currentSrcPos = 0;
//                currentDstPos += bytesToClone;
//                if (remaining <= 0) {
//                    break;
//                }
//            }
//            return Flux.fromIterable(inodeDataList).index().flatMap(tuple -> {
//                Long index = tuple.getT1();
//                Tuple2<InodeData, Long> tuple2 = tuple.getT2();
//                Long offset0 = tuple2.var2;
//                InodeData inodeData = tuple2.var1;
//                if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
//                    return Node.getInstance().getChunk(f -> );
//                }
//                return Node.getInstance().updateInodeData(dstInode.getBucket(), dstInode.getNodeId(), offset0,inodeData, "");
//            });
//        }).flatMap(inodeDataList -> {
//        })
//
//            return Mono.just(reply);
//        });
    }


    @NFSV4.Opt0(NFSV4.CBOpcode.NFS4PROC_CB_NULL)
    public Mono<Object> cbNull(RpcReplyHeader replyHeader, ReqInfo reqHeader, NullReply reply) {
        return Mono.just(true);
    }

    @NFSV4.Opt0(NFSV4.CBOpcode.NFS4PROC_CB_COMPOUND)
    public Mono<Object> compound(RpcReplyHeader replyHeader, ReqInfo reqHeader, CompoundReply reply) {
        return Mono.just(true);
    }

    @NFSV4.Opt0(NFSV4.CBOpcode.NFS4PROC_CB_RECALL)
    public Mono<Boolean> recall(RpcReplyHeader replyHeader, ReqInfo reqHeader, EmptyReply reply) {
        int oldId = replyHeader.getHeader().id;
        Tuple2<CompoundCall, DelegateLock> tuple2 = DelegateLock.sendDelegateMap.get(oldId);
        if (tuple2 != null) {
            CompoundCall compoundCall = tuple2.var1;
            DelegateLock delegateLock = tuple2.var2;
            NFSHandler.CBInfoMap.remove(oldId);
            DelegateLock.sendDelegateMap.remove(oldId);
            long clientId = delegateLock.getClientId();
            NFS4Client client0 = clientControl.getClient0(clientId);
            NFS4Session session0 = null;
            if (client0 != null) {
                session0 = client0.getSession0(delegateLock.sessionId);
//                if (session0 != null) {
//                    if (session0.recallQueue.isEmpty()) {
//                        session0.canSend.set(true);
//                    } else {
//                        Tuple2<CompoundCall, Lock> poll = session0.recallQueue.poll();
//                        if (poll != null) {
//                            session0.nfsHandler.write(poll.var1);
//                            poll.var2.sendCheck(poll, session0);
//                        } else {
//                            session0.canSend.set(true);
//                        }
//                    }
//                }
            }
            if (reply.status != 0) {
                if (reply.status == NFS3ERR_BADHANDLE) {
                    //检查inode是否存在
                    return nodeInstance.getInode(delegateLock.bucket, delegateLock.nodeId)
                            .map(InodeUtils::isError).flatMap(b -> {
                                if (b) {
//                                    NFSHandler.CBInfoMap.remove(replyHeader.getHeader().id);
//                                    DelegateLock.sendDelegateMap.remove(replyHeader.getHeader().id);
                                    return DelegateClient.unLock(delegateLock.bucket, String.valueOf(delegateLock.nodeId), delegateLock);
                                } else {
                                    unDelegateMap.compute(delegateLock, (k, v) -> {
                                        v = System.currentTimeMillis();
                                        k.recall();
                                        return v;
                                    });
                                    return Mono.just(true);
                                }
                            });
                } else if (reply.status == NFS4ERR_BAD_STATEID) {
                    if ((delegateLock.minorVersion == 0 && client0 != null) || (delegateLock.minorVersion >= 1 && session0 != null)) {
                        //判断本地缓存
                        if (client0.existState(delegateLock.stateId)) {
                            unDelegateMap.compute(delegateLock, (k, v) -> {
                                v = System.currentTimeMillis();
                                k.recall();
                                return v;
                            });
                            return Mono.just(true);
                        }
                    }
                    return DelegateClient.unLock(delegateLock.bucket, String.valueOf(delegateLock.nodeId), delegateLock);
                } else {
                    //重新放入队列
                    unDelegateMap.compute(delegateLock, (k, v) -> {
                        v = System.currentTimeMillis();
                        k.recall();
                        return v;
                    });
                }
            }
        }
        return Mono.just(true);
    }

    @NFSV4.Opt0(NFSV4.CBOpcode.NFS4PROC_CB_SEQUENCE)
    public Mono<Object> sequence(RpcReplyHeader replyHeader, ReqInfo reqHeader, SequenceReply reply) {
        return Mono.just(true);
    }

    @NFSV4.Opt0(NFSV4.CBOpcode.NFS4PROC_CB_NOTIFY_LOCK)
    public Mono<Object> notifyLock(RpcReplyHeader replyHeader, ReqInfo reqHeader, EmptyReply reply) {
        int oldId = replyHeader.getHeader().id;
        Tuple2<CompoundCall, NFS4Lock> tuple2 = NFS4Lock.sendLockMap.get(oldId);
        if (tuple2 != null) {
            CompoundCall compoundCall = tuple2.var1;
            NFS4Lock nfs4Lock = tuple2.var2;
            NFSHandler.CBInfoMap.remove(oldId);
            NFS4Lock.sendLockMap.remove(oldId);
        }
        return Mono.just(true);
    }


}
