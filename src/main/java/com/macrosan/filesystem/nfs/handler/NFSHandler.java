package com.macrosan.filesystem.nfs.handler;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.api.NFS4Proc;
import com.macrosan.filesystem.nfs.api.NFSACLProc;
import com.macrosan.filesystem.nfs.auth.AuthGSS;
import com.macrosan.filesystem.nfs.auth.AuthReply;
import com.macrosan.filesystem.nfs.auth.GssVerifier;
import com.macrosan.filesystem.nfs.auth.KrbJni;
import com.macrosan.filesystem.nfs.call.EntryInCall;
import com.macrosan.filesystem.nfs.call.GetAclCall;
import com.macrosan.filesystem.nfs.call.SetAclCall;
import com.macrosan.filesystem.nfs.call.rquota.GetQuotaCall;
import com.macrosan.filesystem.nfs.call.rquota.SetQuotaCall;
import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.reply.*;
import com.macrosan.filesystem.nfs.reply.v4.*;
import com.macrosan.filesystem.nfs.types.CBInfo;
import com.macrosan.filesystem.quota.nfs.NFSRQuotaProc;
import com.macrosan.filesystem.utils.RunNumUtils;
import com.macrosan.filesystem.utils.timeout.FastMonoTimeOut;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.perf.FSProcPerfLimiter;
import com.macrosan.utils.quota.StatisticsRecorder;
import io.netty.buffer.ByteBuf;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Fuseable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.THROUGHPUT_QUOTA;
import static com.macrosan.filesystem.FsConstants.NFS4_CALLBACK_PROGRAM;
import static com.macrosan.filesystem.FsConstants.NFSACLOpCode.*;
import static com.macrosan.filesystem.FsConstants.NFSQuotaOpCode.NFSQUOTAPROC_GETQUOTA;
import static com.macrosan.filesystem.FsConstants.NFSQuotaOpCode.NFSQUOTAPROC_SETQUOTA;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.cifs.handler.SMBHandler.localIpDebug;
import static com.macrosan.filesystem.nfs.NFSV3.Opcode.*;
import static com.macrosan.filesystem.nfs.NFSV4.CBOpcode.NFS4PROC_CB_COMPOUND;
import static com.macrosan.filesystem.nfs.NFSV4.CBOpcode.NFS4PROC_CB_NULL;
import static com.macrosan.filesystem.nfs.NFSV4.Opcode.*;
import static com.macrosan.filesystem.nfs.NFSV4.Opcode.NFS4PROC_REMOVE;
import static com.macrosan.filesystem.nfs.api.NSMProc.isLocalIp;
import static com.macrosan.utils.quota.StatisticsRecorder.addFileStatisticRecord;
import static com.macrosan.utils.quota.StatisticsRecorder.getRequestType;

@Log4j2
public class NFSHandler extends RpcHandler {
    public static final int NFS_TIME_OUT = 55;
    // 当删除文件因压力大等原因执行时间过长、但已成功删除文件时将nodeId存放入delTimeout，后续重发的删除请求可直接返回成功
    public static HashMap<Long, Long> delTimeout = new HashMap<>();
    public static long SCAN_DURATION = 60_000L;
    public static long CLEAR_DURATION = 600_000L;
    public static Map<Integer, CBInfo> CBInfoMap = new ConcurrentHashMap<>();
    public static Map<String, Integer> localIpMap = new ConcurrentHashMap<>();
    public static Map<String, Long> newIpMap = new ConcurrentHashMap<>();
    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("NFSHandlerClear-"));

    static {
        executor.submit(NFSHandler::tryClearCache);
        executor.submit(NFSHandler::tryClearLocalIp);
    }

    public DatagramSocket udpSocket;
    public SocketAddress address;
    public NFSHandler nfsHandler;
    public NetSocket socket;

    public NFSHandler(DatagramSocket udpSocket, SocketAddress address) {
        this.udpSocket = udpSocket;
        this.address = address;
    }

    public NFSHandler(NetSocket socket) {
        this.socket = socket;
    }

    public NFSHandler() {
    }

    public static void tryClearCache() {
        try {
            for (Long nodeId : delTimeout.keySet()) {
                delTimeout.compute(nodeId, (k, time) -> {
                    if (System.currentTimeMillis() - time > CLEAR_DURATION) {
                        return null;
                    } else {
                        return time;
                    }
                });
            }
        } catch (Exception e) {
            log.error("tryClearCache error: ", e);
        } finally {
            executor.schedule(NFSHandler::tryClearCache, SCAN_DURATION, TimeUnit.MILLISECONDS);
        }
    }

    private static void tryClearLocalIp() {
        try {
            for (String localIp : localIpMap.keySet()) {
                if (!isLocalIp(localIp)) {
                    localIpMap.computeIfPresent(localIp, (ip, flag) -> {
                        if (flag < 5) {
                            return flag + 1;
                        } else {
                            if (localIpDebug) {
                                log.info("clear nfs LocalIp : {}", ip);
                            }
                            return null;
                        }
                    });
                } else {
                    localIpMap.put(localIp, 0);
                }
            }
            for (String newIp : newIpMap.keySet()) {
                Long time = newIpMap.getOrDefault(newIp, 0L);
                if (System.nanoTime() - time > 20_000_000_000L || !localIpMap.containsKey(newIp)) {
                    if (localIpDebug) {
                        log.info("clear nfs newIp : {}", newIp);
                    }
                    newIpMap.remove(newIp);
                }
            }
        } catch (Exception e) {
            log.error("clear nfs LocalIp error, ", e);
        } finally {
            executor.schedule(NFSHandler::tryClearLocalIp, 2_000, TimeUnit.MILLISECONDS);
        }
    }

    public static boolean isOptNeedReleaseLock(NFSV3.Opcode opcode) {
        if (opcode.equals(NFSV3.Opcode.NFS3PROC_RENAME)) {
            return true;
        }

        if (NFSV3.NFS_RED_LOCK && ((opcode.equals(NFSV3.Opcode.NFS3PROC_CREATE) || (opcode.equals(NFSV3.Opcode.NFS3PROC_MKDIR)) || (opcode.equals(NFSV3.Opcode.NFS3PROC_MKNOD))))) {
            return true;
        }

        return false;
    }

    public static void releaseLock(ReqInfo reqHeader) {
        try {
            if (!reqHeader.lock.isEmpty()) {
                for (String key : reqHeader.lock.keySet()) {
                    String value = reqHeader.lock.get(key);
                    RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
                }
            }
        } catch (Exception exception) {
            log.info("timeout release lock error", exception);
        }
    }

    public String getClientAddress() {
        if (socket != null) {
            return socket.remoteAddress().host();
        } else {
            return address.host();
        }
    }

    public int getClientPort() {
        if (socket != null) {
            return socket.remoteAddress().port();
        } else {
            return address.port();
        }
    }


    private RpcReply authReply(RpcCallHeader callHeader, RpcReply reply) {
        if (!(callHeader.auth instanceof AuthGSS)) {
            return reply;
        }

        AuthGSS auth = (AuthGSS) callHeader.auth;
        GssVerifier verifier = auth.resVerifier;

        if (verifier == GssVerifier.NULL_VERIFIER || verifier == null) {
            return reply;
        }

        reply.verifier = verifier;
        KrbJni.GssContext context;

        switch (auth.getProcedure()) {
            case RPC_GSS_PROC_INIT:
                context = KrbJni.getContext(auth.authContextId);
                return new AuthReply.InitAuthReply(context, reply);
            case RPC_GSS_PROC_DATA:
                context = KrbJni.getContext(auth.contextId);
                return new AuthReply.DataAuthReply(context, reply, auth.getSeq());
            case RPC_GSS_PROC_DESTROY:
            case RPC_GSS_PROC_CONTINUE_INIT:
            default:
                return reply;
        }
    }

    private void nfs3(NFSV3.Opcode opcode, NFSV3.OptInfo proc, ReadStruct t, RpcCallHeader callHeader, ReqInfo reqHeader) {
        try {

            final AtomicLong startTime = new AtomicLong();
            // 记录操作开始时间
            startTime.set(DateChecker.getCurrentTime());

            if (NFS.nfsDebug) {
                log.info("nfs call {} {}", callHeader, t);
            }

            if (this.socket != null) {
                RunNumUtils.checkRunning(socket, NFS.nfsDebug);
            } else {
                RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
            }


            // 超过55秒后已删除的文件再次被删除时直接返回成功
            if (opcode.equals(NFS3PROC_REMOVE) && !delTimeout.isEmpty()) {
                EntryInCall removeCall = (EntryInCall) t;
                if (delTimeout.containsKey(removeCall.fh.ino)) {
                    RemoveReply reply = new RemoveReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                    if (this.socket != null) {
                        ByteBuf buf = replyToBuf(reply, proc.bufSize);
                        this.socket.write(Buffer.buffer(buf), v -> {
                            RunNumUtils.releaseRunning();
                        });
                    } else {
                        sendUdpMsg(reply, udpSocket, address, proc.bufSize);
                        RunNumUtils.releaseRunning();
                    }

                    delTimeout.compute(removeCall.fh.ino, (k, v) -> null);

                    if (NFS.nfsDebug) {
                        log.info("nfs remove nodeId: {}", removeCall.fh.ino);
                    }

                    return;
                }
            }

            Mono<Long> waitMono = FSProcPerfLimiter.getInstance().limits(opcode.name(), THROUGHPUT_QUOTA, 1L);
            Mono<RpcReply> res;

            if (waitMono instanceof Fuseable.ScalarCallable) {
                long wait = waitMono.block();
                if (wait == 0L) {
                    res = (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t);
                } else {
                    res = Mono.delay(Duration.ofMillis(wait)).flatMap(l -> (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t));
                }
            } else {
                res = waitMono.flatMap(waitMillis -> {
                    if (waitMillis == 0) {
                        return Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t));
                    } else {
                        return (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t);
                    }
                });
            }

            boolean isDelOpt = opcode.equals(NFS3PROC_REMOVE);
            FastMonoTimeOut.fastTimeout(res, isDelOpt ? Duration.ofSeconds(NFS_TIME_OUT * 2) : Duration.ofSeconds(NFS_TIME_OUT))
                    .onErrorResume(e -> {
                        if (e instanceof TimeoutException) {
                            log.error("NFS PROCESS timeout opt:{}", opcode);
                            if (isOptNeedReleaseLock(opcode) && reqHeader.optCompleted) {
                                releaseLock(reqHeader);
                            }
                            if (opcode.equals(NFS3PROC_RENAME) && (!reqHeader.optCompleted || reqHeader.repeat)) {
                                log.info("NFS rename running {} , optCompleted {}, repeat {}", callHeader.getHeader().id, reqHeader.optCompleted, reqHeader.repeat);
                                ReNameReply reNameReply = new ReNameReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                                reNameReply.fromBeforeFollows = 0;
                                reNameReply.fromAfterFollows = 0;
                                reNameReply.toBeforeFollows = 0;
                                reNameReply.toAfterFollows = 0;
                                reNameReply.status = FsConstants.NfsErrorNo.NFS3ERR_JUKEBOX;
                                return Mono.just(reNameReply);
                            }
                            if (opcode.equals(NFSV3.Opcode.NFS3PROC_REMOVE)) {
                                //压力大时服务端删除数据块超时默认返回成功
                                RemoveReply reply = new RemoveReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                                if (NFS.nfsDebug) {
                                    log.info("NFS PROCESS error.opt:{},bucket:{},msg:{}", opcode, reqHeader.bucket, e.getMessage());
                                }
                                reply.status = FsConstants.NfsErrorNo.NFS3_OK;
                                return Mono.just(reply);
                            }
                        } else if (e instanceof NFSException && ((NFSException) e).nfsError) {
                            if (((NFSException) e).getMessage().startsWith("The fsid")) {
                                NFSBucketInfo.logFsidInfo(opcode, ((NFSException) e).getMessage());
                            } else {
                                log.info("NFS PROCESS error.opt:{},{}", opcode, ((NFSException) e).getMessage());
                            }
                            return mapOpcodeToReply(callHeader, opcode, ((NFSException) e).getErrCode(), proc);
                        } else {
                            if (isOptNeedReleaseLock(opcode)) {
                                releaseLock(reqHeader);
                            }
                            log.error("NFS PROCESS error.opt:{},{}", opcode, e);
                        }

                        return Mono.just(new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
                    })
                    .map(reply -> authReply(callHeader, reply))
                    .subscribe(reply -> {
                        if (this.socket != null) {
                            ByteBuf buf = replyToBuf(reply, proc.bufSize);
                            this.socket.write(Buffer.buffer(buf), v -> {
                                RunNumUtils.releaseRunning();
                            });
                        } else {
                            sendUdpMsg(reply, udpSocket, address, proc.bufSize);
                            RunNumUtils.releaseRunning();
                        }

                        if (NFS.nfsDebug) {
                            log.info("nfs reply {}", reply);
                        }

                        long executionTime = DateChecker.getCurrentTime() - startTime.get();
                        String bucket = "";
                        String accountId = "";
                        HttpMethod httpMethod = HttpMethod.OTHER;
                        long size = 0;
                        boolean success = false;

                        if (opcode == NFS3PROC_WRITE) {
                            if (reply instanceof WriteReply) {
                                WriteReply writeReply = (WriteReply) reply;
                                success = writeReply.status == 0;
                                size = writeReply.count;
                                bucket = reqHeader.bucketInfo.get("bucket_name");
                                accountId = reqHeader.bucketInfo.get("user_id");
                                httpMethod = HttpMethod.PUT;
                            }
                        } else if (opcode == NFS3PROC_READ) {
                            if (reply instanceof ReadReply) {
                                ReadReply readReply = (ReadReply) reply;
                                success = readReply.status == 0;
                                size = readReply.count;
                                bucket = reqHeader.bucketInfo.get("bucket_name");
                                accountId = reqHeader.bucketInfo.get("user_id");
                                httpMethod = HttpMethod.GET;
                            }
                        }

                        if (httpMethod != HttpMethod.OTHER) {
                            StatisticsRecorder.RequestType requestType = getRequestType(httpMethod);

                            addFileStatisticRecord(accountId, bucket, httpMethod, -1, requestType, success, executionTime, size);
                        }

                    });
        } catch (Exception e) {
            if (e instanceof NFSException && ((NFSException) e).nfsError) {
                if (((NFSException) e).getErrCode() == FsConstants.NfsErrorNo.NFS3ERR_STALE) {
                    if (((NFSException) e).getMessage().startsWith("The fsid")) {
                        NFSBucketInfo.logFsidError(opcode, ((NFSException) e).getMessage());
                    } else {
                        log.error("NFS PROCESS error.opt:{},{}", opcode, ((NFSException) e).getMessage());
                    }
                } else {
                    log.error("NFS PROCESS error.opt:{}, ", opcode, e);
                }
                mapOpcodeToReply(callHeader, opcode, ((NFSException) e).getErrCode(), proc)
                        .subscribe(reply -> {
                            if (this.socket != null) {
                                ByteBuf buf = replyToBuf(reply, proc.bufSize);
                                this.socket.write(Buffer.buffer(buf), v -> {
                                    RunNumUtils.releaseRunning();
                                });
                            } else {
                                sendUdpMsg(reply, udpSocket, address, proc.bufSize);
                                RunNumUtils.releaseRunning();
                            }
                        });
            } else {
                if (isOptNeedReleaseLock(opcode)) {
                    releaseLock(reqHeader);
                }
                log.error("NFS PROCESS error.opt:{}, ", opcode, e);
            }
        }
    }

    @Override
    protected void handleMsg(int offset, RpcCallHeader callHeader, ByteBuf msg) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.nfsHandler = nfsHandler;
        //NFS V3
        if (callHeader.rpcVersion == 2 && callHeader.program == 100003 && callHeader.programVersion == 3) {
            NFSV3.Opcode opcode = NFSV3.values[callHeader.opt];
            NFSV3.OptInfo proc = NFSV3.v3Opt[callHeader.opt];

            if (proc == null) {
                return;
            }

            try {
                ReadStruct t = (ReadStruct) proc.constructor.newInstance();

                if (t.readStruct(msg, offset) < 0) {
                    log.error("read nfs msg fail {}", callHeader);
                    throw new NFSException(NFS3ERR_I0, "read nfs msg fail");
                }

                ErasureServer.DISK_SCHEDULER.schedule(() -> nfs3(opcode, proc, t, callHeader, reqHeader));
            } catch (Exception e) {
                if (e instanceof NFSException && ((NFSException) e).nfsError) {
                    if (((NFSException) e).getErrCode() == FsConstants.NfsErrorNo.NFS3ERR_STALE) {
                        log.error("NFS PROCESS error.opt:{},{}", opcode, ((NFSException) e).getMessage());
                    } else {
                        log.error("NFS PROCESS error.opt:{}, ", opcode, e);
                    }
                    mapOpcodeToReply(callHeader, opcode, ((NFSException) e).getErrCode(), proc)
                            .subscribe(reply -> {
                                if (this.socket != null) {
                                    ByteBuf buf = replyToBuf(reply, proc.bufSize);
                                    this.socket.write(Buffer.buffer(buf), v -> {
                                        RunNumUtils.releaseRunning();
                                    });
                                } else {
                                    sendUdpMsg(reply, udpSocket, address, proc.bufSize);
                                    RunNumUtils.releaseRunning();
                                }
                            });
                } else {
                    if (isOptNeedReleaseLock(opcode)) {
                        releaseLock(reqHeader);
                    }
                    log.error("NFS PROCESS error.opt:{}, ", opcode, e);
                }
            }
        } else if (callHeader.rpcVersion == 2 && callHeader.program == 100227 && callHeader.programVersion == 3) {
            NFSACLProc proc = new NFSACLProc();
            Mono<RpcReply> procRes = null;
            if (NFS.nfsDebug) {
                log.info("nfs call {} {}", callHeader);
            }

            if (socket != null) {
                RunNumUtils.checkRunning(socket, NFS.nfsDebug);
            } else {
                RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
            }

            try {
                switch (callHeader.opt) {
                    case NFSACLPROC_NULL:
                        RpcReply rpcReply = new RpcReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                        procRes = Mono.just(rpcReply);
                        break;
                    case NFSACLPROC_GETACL:
                        GetAclCall call = new GetAclCall();
                        if (call.readStruct(msg, offset) < 0) {
                            return;
                        }
                        procRes = (Mono<RpcReply>) proc.getAcl(callHeader, reqHeader, call);
                        break;
                    case NFSACLPROC_SETACL:
                        SetAclCall setAclCall = new SetAclCall();
                        if (setAclCall.readStruct(msg, offset) < 0) {
                            return;
                        }
                        procRes = (Mono<RpcReply>) proc.setAcl(callHeader, reqHeader, setAclCall);
                        break;
                }
                if (procRes != null) {
                    procRes.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                            .onErrorResume(e -> {
                                if (e instanceof TimeoutException) {
                                    log.error("NFS PROCESS timeout opt:{}", callHeader.opt);
                                } else if (e instanceof NFSException && ((NFSException) e).nfsError) {
                                    ErrorReply reply = new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                                    log.info("NFS PROCESS error.opt:{},{}", callHeader.opt, ((NFSException) e).getMessage());
                                    reply.status = ((NFSException) e).getErrCode();
                                    return Mono.just(reply);
                                } else {
                                    log.error("NFS PROCESS error.opt:{},{}", callHeader.opt, e);
                                }


                                return Mono.just(new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
                            })
                            .map(reply -> authReply(callHeader, reply))
                            .subscribe(reply -> {

                                if (this.socket != null) {
                                    ByteBuf buf = replyToBuf(reply, 4096);
                                    this.socket.write(Buffer.buffer(buf), v -> {
                                        RunNumUtils.releaseRunning();
                                    });
                                } else {
                                    sendUdpMsg(reply, udpSocket, address, 4096);
                                    RunNumUtils.releaseRunning();
                                }

                                if (NFS.nfsDebug) {
                                    log.info("nfs reply {}", reply);
                                }
                            });
                }
            } catch (Exception e) {
                log.error("", e);
            }
        } else if (callHeader.rpcVersion == 2 && callHeader.program == 100011 && callHeader.programVersion == 2) {
            NFSRQuotaProc proc = new NFSRQuotaProc();
            Mono<RpcReply> procRes = null;
            switch (callHeader.opt) {
                case NFSQUOTAPROC_GETQUOTA:
                    GetQuotaCall call = new GetQuotaCall();
                    if (call.readStruct(msg, offset) < 0) {
                        return;
                    }
                    procRes = (Mono<RpcReply>) proc.getQuota(callHeader, reqHeader, call);
                    break;
                case NFSQUOTAPROC_SETQUOTA:
                    SetQuotaCall setQuotaCall = new SetQuotaCall();
                    if (setQuotaCall.readStruct(msg, offset) < 0) {
                        return;
                    }
                    procRes = (Mono<RpcReply>) proc.setQuota(callHeader, reqHeader, setQuotaCall);
                    break;
            }
            if (procRes != null) {
                procRes.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException) {
                                log.error("NFS PROCESS timeout opt:{}", callHeader.opt);
                            } else if (e instanceof NFSException && ((NFSException) e).nfsError) {
                                ErrorReply reply = new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                                log.info("NFS PROCESS error.opt:{},{}", callHeader.opt, ((NFSException) e).getMessage());
                                return Mono.just(reply);
                            }
                            return Mono.just(new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
                        })
                        .subscribe(reply -> {
                            if (this.socket != null) {
                                ByteBuf buf = replyToBuf(reply, 4096);
                                this.socket.write(Buffer.buffer(buf), v -> {
                                    RunNumUtils.releaseRunning();
                                });
                            } else {
                                sendUdpMsg(reply, udpSocket, address, 4096);
                                RunNumUtils.releaseRunning();
                            }

                            if (NFS.nfsDebug) {
                                log.info("nfs reply {}", reply);
                            }
                        });
            }
        } else if (callHeader.rpcVersion == 2 && callHeader.program == 100003 && callHeader.programVersion == 4) {
            NFSV4.Opcode opcode = NFSV4.values[callHeader.opt];
            NFSV4.OptInfo proc = NFSV4.v4Opt[callHeader.opt];
            if (proc == null) {
                return;
            }
            try {
                ReadStruct t = (ReadStruct) proc.constructor.newInstance();
                if (t.readStruct(msg, offset) < 0) {
                    log.error("read nfs msg fail {}", callHeader);
                    return;
                }
                offset += t.readStruct(msg, offset);
                if (NFS.nfsDebug) {
                    log.info("nfs call {} {}", callHeader, t);
                }

                if (socket != null) {
                    RunNumUtils.checkRunning(socket, NFS.nfsDebug);
                } else {
                    RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
                }

                MonoProcessor<Mono<Tuple2<RpcReply, Integer>>> res = MonoProcessor.create();
                if (callHeader.opt == NFS4PROC_NULL.opcode) {
                    Mono<RpcReply> replyMono = (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t);
                    res.onNext(replyMono.zipWith(Mono.just(proc.bufSize)));
                } else if (callHeader.opt == NFS4PROC_COMPOUND.opcode) {
                    CompoundCall compoundCall = (CompoundCall) t;
                    CompoundReply compoundReply = new CompoundReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                    int finalOffset = offset;
                    Mono.just(true).flatMap(b -> execOpt(compoundCall, compoundReply, 0, callHeader, reqHeader, msg, finalOffset, proc.bufSize))
                            .subscribe(r -> res.onNext(Mono.just(r)));
                }
                res.flatMap(reply -> reply).doOnError(e -> {
                            log.error("", e);
                            RunNumUtils.releaseRunning();
                        })
                        .subscribe(tuple2 -> {
                            RpcReply reply = tuple2.getT1();
                            int bufSize = tuple2.getT2();
                            if (this.socket != null) {
                                ByteBuf buf = replyToBuf(reply, bufSize);
                                this.socket.write(Buffer.buffer(buf), v -> {
                                    RunNumUtils.releaseRunning();
                                });
                            } else {
                                sendUdpMsg(reply, udpSocket, address, bufSize);
                                RunNumUtils.releaseRunning();
                            }
                            if (NFS.nfsDebug) {
                                log.info("nfs reply {}", reply);
                            }
                        });
            } catch (Exception e) {
                log.error("", e);
            }
        } else {
            log.info("no handler msg {}", callHeader);
        }
    }

    @Override
    protected void handleRes(int offset, RpcReplyHeader replyHeader, ByteBuf msg) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.nfsHandler = nfsHandler;
        CBInfo cbInfo = NFSHandler.CBInfoMap.get(replyHeader.getHeader().id);
        if (cbInfo == null) {
            return;
        }

        if (cbInfo.program == NFS4_CALLBACK_PROGRAM && cbInfo.rpcVersion == 2 && cbInfo.programVersion == 1) {
//        if (cbInfo.program == NFS4_CALLBACK_PROGRAM && cbInfo.rpcVersion == 2 && cbInfo.programVersion == 1) {
            NFSHandler.CBInfoMap.remove(replyHeader.getHeader().id);
            int opt = cbInfo.opt == NFS4PROC_CB_NULL.opcode ? NFS4PROC_CB_NULL.opcode : NFS4PROC_CB_COMPOUND.opcode;
            NFSV4.CBOpcode cbOpcode = NFSV4.CBValues[opt];
            NFSV4.OptInfo proc = NFSV4.v4CBOpt[opt];
            if (proc == null) {
                return;
            }

            try {
                CompoundReply t = (CompoundReply) proc.constructor0.newInstance(replyHeader.getHeader());
                if (t.readStruct(msg, offset) < 0) {
                    log.error("NFS V4 callBack read res fail {}", replyHeader);
                    return;
                }
                offset += t.readStruct(msg, offset);
                if (NFS.nfsDebug) {
                    log.info("NFS V4 callBack reply {} {}", replyHeader, t);
                }

                if (this.socket != null) {
                    RunNumUtils.checkRunning(socket, NFS.nfsDebug);
                } else {
                    RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
                }
                MonoProcessor<Mono<Boolean>> res = MonoProcessor.create();
                if (opt == NFS4PROC_CB_NULL.opcode) {
                    res.onNext((Mono<Boolean>) proc.function0.apply(replyHeader, reqHeader, t));
                } else if (opt == NFS4PROC_CB_COMPOUND.opcode) {
                    CompoundReply compoundReply = t;
                    int finalOffset = offset;
                    Mono.just(true).flatMap(b -> execOpt0(compoundReply, 0, replyHeader, reqHeader, msg, finalOffset))
                            .subscribe(r -> res.onNext(Mono.just(r)));
                }
                res.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                        .flatMap(r -> r)
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException) {
                                log.error("NFS V4 callBack PROCESS timeout opt:{}", cbOpcode);
                            } else {
                                log.error("NFS V4 callBack PROCESS error.opt:{},{}", cbOpcode, e);
                            }
                            return Mono.just(false);
                        })
                        .subscribe(isSuccess -> {
                            RunNumUtils.releaseRunning();
                            if (!isSuccess) {
                                log.error("NFS V4 callBack PROCESS reply fail.opt:{}", cbOpcode);
                            }
                        });
            } catch (Exception e) {
                log.error("NFS Handle reply", e);
            }
        } else {
            log.info("NFS no handler res {}", replyHeader);
        }
    }

    public Mono<Tuple2<RpcReply, Integer>> execOpt(CompoundCall compoundCall, CompoundReply compoundReply, int index, RpcCallHeader callHeader,
                                                   ReqInfo reqHeader, ByteBuf buf, int offset, int maxBufSize) {
        if (index >= compoundCall.getCount()) {
            return Mono.just((RpcReply) compoundReply).zipWith(Mono.just(maxBufSize));
        }
        AtomicLong startTime = new AtomicLong(DateChecker.getCurrentTime());
        int opt = buf.getInt(offset);
        NFSV4.Opcode opcode = NFSV4.values[opt];
        NFSV4.OptInfo proc = NFSV4.v4Opt[opt];
        maxBufSize = Math.max(maxBufSize, proc.bufSize);
        offset += 4;
        CompoundCall t = null;
        try {
            t = (CompoundCall) proc.constructor.newInstance();
            t.context = compoundCall.context;
            compoundCall.callList.add(t);
            offset += t.readStruct(buf, offset);
            if (NFS.nfsDebug) {
                log.info("nfs call {} {}", callHeader, t);
            }
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }
        CompoundCall finalT = t;
        Mono<RpcReply> res = FSProcPerfLimiter.getInstance().limits(opcode.name(), THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, finalT)));
        int finalOffset = offset;
        int finalMaxBufSize = maxBufSize;
        return res.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                .onErrorResume(e -> {
                    if (e instanceof TimeoutException) {
                        if (opt == NFS4PROC_RENAME.opcode && reqHeader.optCompleted) {
                            releaseLock(reqHeader);
                        }
                        if (opcode.equals(NFS4PROC_RENAME) && (!reqHeader.optCompleted || reqHeader.repeat)) {
                            log.info("NFS rename running {} , optCompleted {}, repeat {}", callHeader.getHeader().id, reqHeader.optCompleted, reqHeader.repeat);
                            ReNameV4Reply reNameReply = new ReNameV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            reNameReply.opt = NFS4PROC_RENAME.opcode;
                            reNameReply.status = FsConstants.NfsErrorNo.NFS3ERR_JUKEBOX;
                            return Mono.just(reNameReply);
                        }
                        if (opcode.equals(NFS4PROC_REMOVE)) {
                            //压力大时服务端删除数据块超时默认返回成功
                            RemoveV4Reply reply = new RemoveV4Reply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            reply.opt = NFS4PROC_REMOVE.opcode;
                            if (NFS.nfsDebug) {
                                log.info("NFS PROCESS error.opt:{},bucket:{},msg:{}", opcode, reqHeader.bucket, e.getMessage());
                            }
                            reply.status = FsConstants.NfsErrorNo.NFS3_OK;
                            return Mono.just(reply);
                        }
                        log.error("NFS PROCESS timeout opt:{}", opcode);
                    } else if (e instanceof NFSException && ((NFSException) e).nfsError) {
                        CompoundReply reply = new CompoundReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                        if (NFS4Proc.errorDebug ||  ((NFSException) e).getErrCode() == NFS3ERR_DQUOT) {
                           log.error("NFS PROCESS error.opt:{},{}", opcode, ((NFSException) e).getMessage());
                        }
                        reply.opt = opt;
                        reply.status = ((NFSException) e).getErrCode();
                        return Mono.just(reply);
                    } else {
                        log.error("NFS PROCESS error.opt:{},{}", opcode, e);
                    }
                    CompoundReply errorReply = new CompoundReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                    errorReply.opt = opt;
                    errorReply.status = NFS3ERR_I0;
                    return Mono.just(errorReply);
                })
                .doOnNext(rpcReply -> {
                    recordTrafficStatistics(rpcReply, startTime, reqHeader, opcode);
                })
                .flatMap(reply -> {
                    CompoundReply oneReply = (CompoundReply) reply;
                    int index0 = index + 1;
                    if (oneReply.status != 0) {
                        index0 = compoundCall.getCount();
                        if (index + 1 < compoundCall.getCount() && buf.getInt(finalOffset) == NFSV4.Opcode.NFS4PROC_CLOSE.opcode) {
                            index0 = index + 1;
                        }
                    }
                    compoundReply.getReplies().add(reply);
                    return execOpt(compoundCall, compoundReply, index0,
                            callHeader, reqHeader, buf, finalOffset, finalMaxBufSize);
                });
    }


    public Mono<Boolean> execOpt0(CompoundReply compoundReply, int index, RpcReplyHeader replyHeader, ReqInfo reqHeader, ByteBuf buf, int offset) {
        if (index >= compoundReply.count) {
            return Mono.just(true);
        }
        int opt = buf.getInt(offset);
        NFSV4.CBOpcode opcode = NFSV4.CBValues[opt];
        NFSV4.OptInfo proc = NFSV4.v4CBOpt[opt];
        offset += 4;
        CompoundReply t = null;
        try {
            t = (CompoundReply) proc.constructor0.newInstance(replyHeader.getHeader());
            offset += t.readStruct(buf, offset);
            if (NFS.nfsDebug) {
                log.info("nfs cb replay {} {}", replyHeader, t);
            }
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }
        CompoundReply finalT = t;
        Mono<Boolean> res = FSProcPerfLimiter.getInstance().limits(opcode.name(), THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> (Mono<Boolean>) proc.function0.apply(replyHeader, reqHeader, finalT)));
        int finalOffset = offset;
        return res.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                .onErrorResume(e -> {
                    CompoundReply reply = new CompoundReply(SunRpcHeader.newReplyHeader(replyHeader.getHeader().id));
                    if (e instanceof TimeoutException) {
                        log.error("NFS PROCESS timeout opt:{}", opcode);
                    } else if (e instanceof NFSException && ((NFSException) e).nfsError) {

                        log.info("NFS PROCESS error.opt:{},{}", opcode, ((NFSException) e).getMessage());

                    } else {
                        log.error("NFS PROCESS error.opt:{},{}", opcode, e);
                    }
                    return Mono.just(false);
                }).flatMap(reply -> execOpt0(compoundReply, index + 1, replyHeader, reqHeader, buf, finalOffset));
    }


    /**
     * 处理try-catch的错误响应
     * 同时在触发 NFS3ERR_STALE 时，用相应 opcode对应的 reply 添加句柄过期的状态码返回，使客户端显示句柄过期；若用 errorReply添加句柄过期
     * 的状态码返回，则会显示I/O出错
     **/
    public Mono<RpcReply> mapOpcodeToReply(RpcCallHeader callHeader, NFSV3.Opcode opcode, int errCode, NFSV3.OptInfo proc) {
        return Mono.just(opcode)
                .map(opc -> {
                    switch (opc) {
                        case NFS3PROC_FSINFO:
                            FsInfoReply fsInfoReply = new FsInfoReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            fsInfoReply.status = errCode;
                            return fsInfoReply;
                        case NFS3PROC_FSSTAT:
                            FsStatReply fsStatReply = new FsStatReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            fsStatReply.status = errCode;
                            return fsStatReply;
                        case NFS3PROC_RMDIR:
                        case NFS3PROC_REMOVE:
                            RemoveReply removeReply = new RemoveReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            removeReply.status = errCode;
                            return removeReply;
                        case NFS3PROC_LINK:
                            LinkReply linkReply = new LinkReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            linkReply.attrFollows = 0;
                            linkReply.status = errCode;
                            return linkReply;
                        case NFS3PROC_NULL:
                            NullReply nullReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            return nullReply;
                        case NFS3PROC_READ:
                            ReadReply readReply = new ReadReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            readReply.attrFollows = 0;
                            readReply.status = errCode;
                            return readReply;
                        case NFS3PROC_MKDIR:
                        case NFS3PROC_MKNOD:
                        case NFS3PROC_CREATE:
                        case NFS3PROC_SYMLINK:
                            EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            entryOutReply.objFhFollows = 0;
                            entryOutReply.attrFollow = 0;
                            entryOutReply.status = errCode;
                            return entryOutReply;
                        case NFS3PROC_WRITE:
                            WriteReply writeReply = new WriteReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            writeReply.status = errCode;
                            return writeReply;
                        case NFS3PROC_ACCESS:
                            AccessReply accessReply = new AccessReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            accessReply.attrBefore = 0;
                            accessReply.status = errCode;
                            return accessReply;
                        case NFS3PROC_COMMIT:
                            CommitReply commitReply = new CommitReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            commitReply.status = errCode;
                            return commitReply;
                        case NFS3PROC_LOOKUP:
                            LookupReply lookupReply = new LookupReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            lookupReply.attrBefore = 0;
                            lookupReply.attrBefore2 = 0;
                            lookupReply.status = errCode;
                            return lookupReply;
                        case NFS3PROC_RENAME:
                            ReNameReply reNameReply = new ReNameReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            reNameReply.fromBeforeFollows = 0;
                            reNameReply.fromAfterFollows = 0;
                            reNameReply.toBeforeFollows = 0;
                            reNameReply.toAfterFollows = 0;
                            reNameReply.status = errCode;
                            return reNameReply;
                        case NFS3PROC_GETATTR:
                            GetAttrReply getAttrReply = new GetAttrReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            getAttrReply.status = errCode;
                            break;
                        case NFS3PROC_READDIR:
                            ReadDirReply readDirReply = new ReadDirReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            readDirReply.attrBefore = 0;
                            readDirReply.status = errCode;
                            return readDirReply;
                        case NFS3PROC_READDIRPLUS:
                            ReadDirPlusReply readDirPlusReply = new ReadDirPlusReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            readDirPlusReply.status = errCode;
                            return readDirPlusReply;
                        case NFS3PROC_SETATTR:
                            SetAttrReply setAttrReply = new SetAttrReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            setAttrReply.followsNew = 0;
                            setAttrReply.status = errCode;
                            return setAttrReply;
                        case NFS3PROC_PATHCONF:
                            PathConfReply pathConfReply = new PathConfReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            pathConfReply.status = errCode;
                            return pathConfReply;
                        case NFS3PROC_READLINK:
                            ReadLinkReply readLinkReply = new ReadLinkReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            readLinkReply.attrFollows = 0;
                            readLinkReply.status = errCode;
                            return readLinkReply;
                        default:
                            ErrorReply errorReply = new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                            errorReply.status = errCode;
                            return errorReply;
                    }

                    ErrorReply reply = new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
                    reply.status = errCode;
                    return reply;
                });
    }


    public void write(CompoundCall call) {
        ByteBuf buf = callToBuf(call, 4096);
        this.socket.write(Buffer.buffer(buf));
    }

    private void recordTrafficStatistics(RpcReply nfs4Reply, AtomicLong startTime, ReqInfo reqHeader, NFSV4.Opcode opcode) {
        if (!(nfs4Reply instanceof WriteV4Reply)
                && !(nfs4Reply instanceof ReadV4Reply)) {
            return;
        }

        long executionTime = DateChecker.getCurrentTime() - startTime.get();
        HttpMethod httpMethod = HttpMethod.OTHER;
        long size = 0;
        boolean success = false;

        String bucket = reqHeader.bucket;
        if (nfs4Reply != null && StringUtils.isNotEmpty(bucket)) {
            Map<String, String> bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
            if (bucketInfo != null) {
                String accountId = bucketInfo.get("user_id");
                if (!StringUtils.isEmpty(bucket) && !StringUtils.isEmpty(accountId)) {
                    success = ((CompoundReply) nfs4Reply).status == NFS3_OK;
                    if (opcode.opcode == NFSV4.Opcode.NFS4PROC_WRITE.opcode) {
                        WriteV4Reply writeReply = (WriteV4Reply) nfs4Reply;
                        size = writeReply.count;
                        httpMethod = HttpMethod.PUT;
                    } else {
                        ReadV4Reply readReply = (ReadV4Reply) nfs4Reply;
                        size = readReply.readLen;
                        httpMethod = HttpMethod.GET;
                    }

                    StatisticsRecorder.RequestType requestType = getRequestType(httpMethod);

                    addFileStatisticRecord(accountId, bucket, httpMethod, -1, requestType, success, executionTime, size);
                }
            }
        }
    }
}
