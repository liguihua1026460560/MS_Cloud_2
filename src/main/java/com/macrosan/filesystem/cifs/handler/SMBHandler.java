package com.macrosan.filesystem.cifs.handler;

import com.macrosan.filesystem.cifs.*;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.call.smb2.NegprotCall;
import com.macrosan.filesystem.cifs.reply.smb1.EmptyReply;
import com.macrosan.filesystem.cifs.reply.smb2.*;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessServer;
import com.macrosan.filesystem.cifs.types.NegprotInfo;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.CompoundRequest;
import com.macrosan.filesystem.cifs.types.smb2.CreateContext;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.RunNumUtils;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.quota.StatisticsRecorder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_DISK_QUOTA_EXCEEDED;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.cifs.CIFS.cifsDebug;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.*;
import static com.macrosan.filesystem.cifs.SMB2Header.*;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.SMB2_OPLOCK_LEVEL_NONE;
import static com.macrosan.filesystem.nfs.api.NSMProc.isLocalIp;
import static com.macrosan.filesystem.nfs.handler.NFSHandler.NFS_TIME_OUT;
import static com.macrosan.utils.quota.StatisticsRecorder.addFileStatisticRecord;
import static com.macrosan.utils.quota.StatisticsRecorder.getRequestType;

@Log4j2
public class SMBHandler {
    int msgLen = -1;

    LinkedList<ByteBuf> bufList = new LinkedList<>();
    int writeIndex = 0;

    public boolean isUdp = false;
    public static boolean runningDebug = false;
    public static boolean notPrintStackDebug = true;
    public static boolean localIpDebug = false;
    NetSocket socket;

    public SMBHandler(NetSocket socket) {
        this.socket = socket;
    }

    public NetSocket getSocket() {
        return socket;
    }

    private void clear() {
        int clearLen = msgLen + 4;
        writeIndex -= clearLen;

        ListIterator<ByteBuf> iterator = bufList.listIterator();
        while (iterator.hasNext() && clearLen > 0) {
            ByteBuf buf = iterator.next();
            if (buf.readableBytes() <= clearLen) {
                clearLen -= buf.readableBytes();
                buf.release();
                iterator.remove();
            } else {
                buf.readerIndex(buf.readerIndex() + clearLen);
            }
        }

        msgLen = -1;
    }

    protected ByteBuf replyToBuf(SMBHeader header, SMBBody reply, int bufSize) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);

        int size = header.size() + reply.writeStruct(buf, header.size() + 4);
        header.writeStruct(buf, 4);
        buf.setInt(0, size);
        buf.writerIndex(size + 4);

        if (cifsDebug) {
            log.info("cifs reply {}:{}", header, reply);
        }

        return buf;
    }

    //处理多个reply的情况
    protected ByteBuf replyToBuf2(int bufSize, SMB2Reply... replys) {


        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);
        int size = 4;

        for (int i = 0; i < replys.length; i++) {
            SMB2Reply reply = replys[i];
            int start = size;

            int bodySize = reply.getBody().writeStruct(buf, start + SMB2Header.SIZE);
            int cmdSize = SMB2Header.SIZE + bodySize;
            if (replys.length > 1) {
                cmdSize = (cmdSize + 7) & ~7;
                if (i != replys.length - 1) {
                    reply.getHeader().setNextCmd(cmdSize);
                }
                size += cmdSize;
            } else {
                size += cmdSize;
            }

            if (i > 0) {
                int flags = reply.getHeader().getFlags();
                reply.getHeader().setFlags(flags | SMB2_HDR_FLAG_CHAINED);
            }

            reply.getHeader().writeStruct(buf, start);
            Session session = session2Map.get(reply.getHeader().sessionId);

            if ((reply.getHeader().getFlags() & SMB2_HDR_FLAG_SIGNED) != 0
                    || (session != null && session.signed && reply.getHeader().getStatus() == NTStatus.STATUS_SUCCESS)) {
                if (session != null) {
                    session.signed = false;
                    byte[] msg = new byte[cmdSize];
                    buf.getBytes(start, msg);
                    byte[] sign;

                    if (negprotInfo.getDialect() < NegprotCall.SMB_3_0_0) {
                        sign = CifsUtils.hmacSHA256(session.sessionKey, msg);
                    } else {
                        sign = CifsUtils.cmacAES(session.channelSessionKey, msg);
                    }

                    reply.getHeader().setSign(sign);
                    buf.setBytes(start + 48, sign);
                }
            }
        }

        buf.setInt(0, size - 4);
        buf.writerIndex(size);

        return buf;
    }

    public Map<Short, Session> session1Map = new ConcurrentHashMap<>();
    public static Map<Long, Session> session2Map = new ConcurrentHashMap<>();
    public static Map<String, Integer> localIpMap = new ConcurrentHashMap<>();
    public static Map<String, Long> newIpMap = new ConcurrentHashMap<>();
    public Session noSession = new Session((short) 0).setSession1Map(session1Map);
    public static Session noSession2 = new Session(0L, null).setSession2Map(session2Map);
    public static long CLEAR_DURATION = 24 * 60 * 60 * 1000L; // 24 * 60分钟
    public static long SCAN_DURATION = 60_000; // 60秒钟
    public NegprotInfo negprotInfo = new NegprotInfo();
    public Map<Long, Session> thisSession2Map = new ConcurrentHashMap<>();

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("SMBSessionClear-"));

    static {
        executor.submit(SMBHandler::tryClearCache);
        executor.submit(SMBHandler::tryClearLocalIp);

    }

    private static void tryClearCache() {
        try {
            for (Long sessionId : session2Map.keySet()) {
                session2Map.computeIfPresent(sessionId, (k, session) -> {
                    long duration = System.currentTimeMillis() - session.useTime;
                    if (session.lock.isEmpty() && duration > CLEAR_DURATION) {
                        if (session.handler != null) {
                            session.handler.thisSession2Map.remove(sessionId);
                            if (session.handler.thisSession2Map.isEmpty()) {
                                session.handler.getSocket().close();
                            }
                        }
                        release(sessionId);
                        return null;
                    } else {
                        return session;
                    }
                });
            }
        } catch (Exception e) {
            log.error("clear smb session error, ", e);
        } finally {
            executor.schedule(SMBHandler::tryClearCache, SCAN_DURATION, TimeUnit.MILLISECONDS);
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
                                log.info("clear smb LocalIp : {}", ip);
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
                        log.info("clear smb newIp : {}", newIp);
                    }
                    newIpMap.remove(newIp);
                }
            }
        } catch (Exception e) {
            log.error("clear smb LocalIp error, ", e);
        } finally {
            executor.schedule(SMBHandler::tryClearLocalIp, 2_000, TimeUnit.MILLISECONDS);
        }
    }

    public static void release(long sessionId) {
        ShareAccessServer.sessionTreeMap.computeIfPresent(sessionId, (k, v) -> null);
    }

    public void changNotify(SMB2Header header, NotifyReply notifyReply) {
        SMB2Reply reply = new SMB2Reply();
        reply.setHeader(header);
        reply.setBody(notifyReply);
        ByteBuf buf = replyToBuf2(4096, reply);
        this.socket.write(Buffer.buffer(buf));
    }

    public void leaseBreakNotification(SMB2Reply reply) {
        ByteBuf buf = replyToBuf2(4096, reply);
        this.socket.write(Buffer.buffer(buf));
    }

    public void sendPending(SMB2Reply reply) {
        ByteBuf buf = replyToBuf2(4096, reply);
        this.socket.write(Buffer.buffer(buf));
    }

    /**
     * 未处理完的请求回复pending后，后续请求需要延后处理。该条请求处理完毕后，再继续处理后续请求，同时再进行回复
     * @param reply0
     * @param compoundRequest
     */
    public void sendPendingList(SMB2Reply reply0, CompoundRequest compoundRequest) {
        List<Mono<SMB2.SMB2Reply>> replyList = new LinkedList<>();
        replyList.add(Mono.just(reply0));
        if (-compoundRequest.execNum < compoundRequest.replyList.size()) {
            replyList.addAll(compoundRequest.replyList.subList(-compoundRequest.execNum, compoundRequest.replyList.size()));
        }
        Flux.fromStream(replyList.stream())
                //多个msg要保证顺序
                .flatMap(r -> r, 1, 1)
                .collectList()
                .subscribe(replyList0 -> {
                    if (compoundRequest.execNum < 0 || cifsDebug) {
                        for (SMB2Reply reply : replyList0) {
                            log.info("smb2 reply {}:{}", reply.getHeader(), reply.getBody());
                        }
                    }

                    if (this.socket != null) {
                        int maxBufSize = compoundRequest.maxMsgBuf.get();
                        for (SMB2Reply reply : replyList0) {
                            reply.getHeader().flags = reply.getHeader().flags | SMB2_HDR_FLAG_ASYNC;
                            if (reply.getBody() instanceof ReadReply) {
                                maxBufSize += ((ReadReply) reply.getBody()).dataLen;
                            }
                        }

                        ByteBuf buf = replyToBuf2(maxBufSize, replyList0.toArray(new SMB2Reply[0]));
                        this.socket.write(Buffer.buffer(buf), v -> {});
                    }
                });
    }

    public void oplockBreakNotify(SMB2Header reconnectHeader, CreateContext.CreateDurableReConnectV2Handle reconnect) {
        SMB2Reply reply = new SMB2Reply(reconnectHeader);
        reply.getHeader().setMessageId(-1);
        reply.getHeader().setOpcode(SMB2_BREAK.opcode);
        reply.getHeader().setCreditRequested((short) 0);
        reply.getHeader().setCreditCharge((short) 0);
        reply.getHeader().setPid(0);
        reply.getHeader().setTid(0);

        OplockBreakReply body = new OplockBreakReply();
        body.setOplockLevel(SMB2_OPLOCK_LEVEL_NONE);
        body.setFileId(reconnect.oldFileID);
        reply.setBody(body);

        ByteBuf buf = replyToBuf2(4096, reply);
        this.socket.write(Buffer.buffer(buf));
    }

    public String getClientAddress() {
        return socket.remoteAddress().host();
    }

    public String getLocalAddress() {
        return socket.localAddress().host();
    }

    public void readMsg(ByteBuf msg) {
        byte msgType = msg.getByte(0);
        byte[] msgLenBytes = new byte[3];
        msg.getBytes(1, msgLenBytes);
        int msgLen = (msgLenBytes[0] & 0xFF) << 16 |
                (msgLenBytes[1] & 0xFF) << 8 |
                (msgLenBytes[2] & 0xFF);
        if (msgLen == 0) {
            return;
        }

        // 记录操作开始时间
        final AtomicLong startTime = new AtomicLong(DateChecker.getCurrentTime());

        try {
            switch (msgType) {
                case NBSSMessage:
                    RunNumUtils.checkRunning(socket, cifsDebug);

                    int magic = msg.getIntLE(4);
                    //smb2
                    if (magic == SMB2Header.MAGIC) {
                        int msgOffset = 4;
                        SMB2Header header;
                        AtomicInteger maxMsgBuf = new AtomicInteger(4096);
                        List<Mono<SMB2Reply>> replyList = new LinkedList<>();
                        CompoundRequest compoundRequest = new CompoundRequest();
                        compoundRequest.maxMsgBuf = maxMsgBuf;
                        compoundRequest.replyList = replyList;
                        do {
                            header = new SMB2Header(this, compoundRequest);
                            header.readStruct(msg, msgOffset);

                            SMB2.SMB2_OPCODE op = SMB2.SMB2_OPCODES[header.opcode & 0xff];
                            SMB2.OptInfo optInfo = SMB2.smb2Opt[header.opcode & 0xff];
                            if (optInfo != null) {
                                SMB2Header finalHeader = header;
                                Session session = getSMB2Session(header.sessionId, optInfo);
                                if (op == SMB2_CREATE) {
                                    session.socket = socket;
                                }
                                int callOff = SMB2Header.SIZE + msgOffset;

//                                log.info("smb2 opcode: {}", op.name());
                                //增加一层调用保证执行顺序
                                Mono<SMB2Reply> res = Mono.just(true)
                                        .flatMap(b -> optInfo
                                                .run(finalHeader, session, msg, callOff)
                                                .doOnSubscribe(subscription -> {
                                                    if (compoundRequest.execNum >= 0) {
                                                        compoundRequest.execNum++;
                                                    } else {
                                                        compoundRequest.execNum--;
                                                    }
                                                }))
                                        .name("cifs res");

                                if (optInfo.bufSize > maxMsgBuf.get()) {
                                    maxMsgBuf.set(optInfo.bufSize);
                                }

                                replyList.add(res.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                                        .onErrorResume(e -> {
                                            SMB2Reply reply = new SMB2Reply(finalHeader);

                                            if (e instanceof TimeoutException) {
                                                log.error("SMB2 PROCESS timeout opt:{}", op);
                                                if (op.equals(SMB2_SETINFO) && !session.lock.isEmpty()) {
                                                    boolean renameRun = false;
                                                    for (String key : session.lock.keySet()) {
                                                        if (!session.renameMap.containsKey(key)) {
                                                            String value = session.lock.get(key);
                                                            RedLockClient.unlock(NFSBucketInfo.getBucketName(finalHeader.tid), key, value, true).subscribe();
                                                        } else {
                                                            log.info("CIFS rename fileId:{}, {}", compoundRequest.getFileId(), session.renameMap.get(key));
                                                            if (compoundRequest.getFileId().equals(session.renameMap.get(key))) {
                                                                renameRun = true;
                                                            }
                                                        }
                                                    }
                                                    if (renameRun) {
                                                        log.info("CIFS rename running {}", finalHeader.getCompoundRequest().getFileId());
                                                        SetInfoReply body = new SetInfoReply();
                                                        reply.setBody(body);
                                                        reply.getHeader().flags = reply.getHeader().flags | SMB2_HDR_FLAG_ASYNC;
                                                        reply.getHeader().status = NTStatus.STATUS_PENDING;
                                                        return Mono.just(reply);
                                                    }
                                                }
                                            } else {
                                                if (notPrintStackDebug) {
                                                    log.error("CIFS PROCESS error.opt:{},{}", op, e);
                                                } else {
                                                    log.error("CIFS PROCESS error.opt:{}", op, e);
                                                }
                                            }

                                            reply.getHeader().status = NTStatus.STATUS_TIMEOUT;
                                            if (e instanceof NFSException && ((NFSException) e).getErrCode() == NFS3ERR_DQUOT) {
                                                reply.getHeader().setStatus(STATUS_DISK_QUOTA_EXCEEDED);
                                            }
                                            return Mono.just(reply);
                                        }));
                            } else {
                                if (op != null) {
                                    log.info("no handler smb2 msg {}:>>>>>>>>>>>>>>> 【{}】 <<<<<<<<<<<<<<<<", header, op.name());
                                } else {
                                    log.info("no handler smb2 msg {}:>>>>>>>>>>>>>>> 【opcode:{}】 <<<<<<<<<<<<<<<<", header, header.opcode & 0xff);
                                }
                                SMB2Reply reply = new SMB2Reply(header);
                                reply.getHeader().setStatus(NTStatus.STATUS_NOT_IMPLEMENTED);
                                replyList.add(Mono.just(reply));
                            }

                            //单个msg包含了多个op
                            if (header.getNextCmd() != 0) {
                                msgOffset += header.getNextCmd();
                            }
                        } while (header.getNextCmd() != 0);

                        Flux.fromStream(replyList.stream())
                                //多个msg要保证顺序
                                .flatMap(r -> r, 1, 1)
                                .doOnNext(smb2Reply -> {
                                    recordTrafficStatistics(smb2Reply, startTime);
                                })
                                .takeUntil(r -> compoundRequest.execNum < 0) // 有请求未处理完毕，后续请求延后
                                .collectList()
                                .subscribe(replyList0 -> {
                                    if (compoundRequest.execNum < 0 || cifsDebug) {
                                        for (SMB2Reply reply : replyList0) {
                                            log.info("smb2 reply {}:{}", reply.getHeader(), reply.getBody());
                                        }
                                    }

                                    if (this.socket != null) {
                                        int maxBufSize = maxMsgBuf.get();
                                        for (SMB2Reply reply : replyList0) {
                                            if (reply.getBody() instanceof ReadReply) {
                                                maxBufSize += ((ReadReply) reply.getBody()).dataLen;
                                            }
                                        }

                                        ByteBuf buf = replyToBuf2(maxBufSize, replyList0.toArray(new SMB2Reply[0]));
                                        this.socket.write(Buffer.buffer(buf), v -> {
                                            RunNumUtils.releaseRunning();
                                        });
                                    }
                                });
                        return;
                    }
                    //smb1
                    else if (magic == SMB1Header.MAGIC) {
                        SMB1Header header = new SMB1Header();
                        header.readStruct(msg, 4);
                        SMB1.SMB1_OPCODE op = SMB1.SMB1_OPCODES[header.opcode & 0xff];

                        SMB1.OptInfo optInfo = SMB1.smb1Opt[op.opcode & 0xff];
                        if (optInfo == null) {
                            if (cifsDebug) {
                                log.info("no handler smb1 msg {}:{}", header, op.name());
                            }
                            SMB1Header replyHeader = SMB1Header.newReplyHeader(header);
                            replyHeader.status = NTStatus.STATUS_NOT_IMPLEMENTED;
                            ByteBuf buf = replyToBuf(replyHeader, EmptyReply.DEFAULT, 4096);
                            socket.write(Buffer.buffer(buf), v -> {
                                RunNumUtils.releaseRunning();
                            });
                        } else {
                            SMBBody call = (SMBBody) optInfo.constructor.newInstance();
                            if (call.readStruct(msg, 36) < 0) {
                                log.error("read smb1 msg fail {}", header);
                                return;
                            }

                            if (cifsDebug) {
                                log.info("smb1 call {} {}", header, call);
                            }

                            Session session = session1Map.getOrDefault(header.uid, noSession);

                            Mono<SMBHeader.SMBReply> res = (Mono<SMBHeader.SMBReply>) optInfo.function.apply(header, session, call);
                            res.timeout(Duration.ofSeconds(NFS_TIME_OUT))
                                    .onErrorResume(e -> {
                                        SMB1Reply reply = new SMB1Reply(header);

                                        if (e instanceof TimeoutException) {
                                            log.error("SMB1 PROCESS timeout opt:{}", op);

                                        } else {
                                            log.error("NFS PROCESS error.opt:{},{}", op, e);
                                        }

                                        reply.getHeader().status = NTStatus.STATUS_TIMEOUT;


                                        return Mono.just(reply);
                                    })
                                    .subscribe(reply -> {
                                        if (this.socket != null) {
                                            ByteBuf buf = replyToBuf(reply.getHeader(), reply.getBody(), optInfo.bufSize);
                                            this.socket.write(Buffer.buffer(buf), v -> {
                                                RunNumUtils.releaseRunning();
                                            });
                                        }

                                        if (cifsDebug) {
                                            log.info("smb1 reply {}:{}", reply.getHeader(), reply.getBody());
                                        }
                                    });
                        }
                        return;
                    } else {
                        log.info("get msg type:{} len:{},magic:{}", msgType, msgLen, magic);
                        RunNumUtils.releaseRunning();
                    }
                    break;
                case NSSSSessionKeepalive:
                    if (cifsDebug) {
                        log.info("get msg type:{} len:{}", msgType, msgLen);
                    }
                    //TODO return keepalive
                    break;
                //不需要处理
                case NBSSPositive:
                case NBSSNegative:
                case NBSSRetarget:
                case NBSSKeepalive:
                default:
                    if (cifsDebug) {
                        log.info("get msg type:{} len:{}", msgType, msgLen);
                    }
                    break;
            }

            log.info("default handler close socket");
            socket.close();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void recordTrafficStatistics(SMB2Reply smb2Reply, AtomicLong startTime) {
        long executionTime = DateChecker.getCurrentTime() - startTime.get();
        HttpMethod httpMethod = HttpMethod.OTHER;
        long size = 0;
        boolean success = false;
        SMB2.OptInfo optInfoReply = SMB2.smb2Opt[smb2Reply.getHeader().opcode & 0xff];

        if (optInfoReply != null) {
            Session sessionReply = getSMB2Session(smb2Reply.getHeader().sessionId, optInfoReply);
            String bucket = sessionReply.bucket;
            if (sessionReply.bucketInfo != null) {
                String accountId = sessionReply.bucketInfo.get("user_id");
                if (!StringUtils.isEmpty(bucket) && !StringUtils.isEmpty(accountId)) {
                    success = smb2Reply.getHeader().status == NTStatus.STATUS_SUCCESS;
                    if (smb2Reply.getBody() instanceof WriteReply) {
                        WriteReply writeReply = (WriteReply) smb2Reply.getBody();
                        size = writeReply.writeCount;
                        httpMethod = HttpMethod.PUT;
                    } else if (smb2Reply.getBody() instanceof ReadReply) {
                        ReadReply readReply = (ReadReply) smb2Reply.getBody();
                        size = readReply.dataLen;
                        httpMethod = HttpMethod.GET;
                    }

                    if (httpMethod != HttpMethod.OTHER) {
                        StatisticsRecorder.RequestType requestType = getRequestType(httpMethod);

                        addFileStatisticRecord(accountId, bucket, httpMethod, -1, requestType, success, executionTime, size);
                    }
                }
            }
        }
    }

    private void loop() {
        if (msgLen < 0 && writeIndex >= 4) {
            if (bufList.isEmpty()) {
                log.info("do not have buf..");
                return;
            }
            ByteBuf buf = bufList.get(0);

            if (buf.readableBytes() < 4) {
                byte[] bytes = new byte[4];
                int index = 0;
                int n = 0;
                while (n < bytes.length) {
                    int read = Math.min(bufList.get(index).readableBytes(), bytes.length - n);
                    int start = bufList.get(index).readerIndex();
                    for (int k = 0; k < read; k++) {
                        bytes[n + k] = bufList.get(index).getByte(start + k);
                    }
                    index++;
                    n += read;
                }

                msgLen = (bytes[0] & 0xFF) << 24 |
                        (bytes[1] & 0xFF) << 16 |
                        (bytes[2] & 0xFF) << 8 |
                        (bytes[3] & 0xFF);
            } else {
                msgLen = buf.getInt(buf.readerIndex());
            }
        }

        if (msgLen >= 0) {
            if (writeIndex < msgLen + 4) {
                return;
            }

            ByteBuf msg = Unpooled.wrappedBuffer(bufList.toArray(new ByteBuf[bufList.size()]));
            readMsg(msg);
            clear();
            loop();
        }
    }

    public void handle(ByteBuf buf) {
        buf.retain();
        bufList.add(buf.duplicate());
        writeIndex += buf.readableBytes();
        loop();
    }

    public Session getSMB2Session(long sessionId, SMB2.OptInfo optInfo) {
        Session session;
        // 如 session setup, keepalive 等请求含有allowNoSession，返回默认noSession2；其余sessionId如果找不到说明已经清除，返回null
        if (optInfo.allowNoSession) {
            session = session2Map.getOrDefault(sessionId, noSession2);
        } else {
            session = session2Map.get(sessionId);
        }

        if (null != session && session.sessionId > 0) {
            session.useTime = System.currentTimeMillis();
        }
        return session;
    }

    public static String info() {
        String str = "";
        try {
            str = "\nsession2Map->" + session2Map.keySet();
        } catch (Exception e) {
            log.error("CIFS ShareAccess info: ", e);
        }
        return str;
    }
}
