package com.macrosan.filesystem.nfs.handler;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.reply.ErrorReply;
import com.macrosan.filesystem.utils.RunNumUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.DatagramSocketAddress;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramPacket;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class NLMHandler extends RpcHandler {

    public static Map<Integer, Integer[]> NLMResMap = new ConcurrentHashMap<>(); // xid, [rpcVersion, program, programVersion, opt]

    private static AtomicLong running = new AtomicLong();
    private static final int MAX_RUN_NUM = 1600;

    public static final int NLM_TIME_OUT = 55;

    NetSocket socket;

    public DatagramSocket udpSocket;
    public SocketAddress address;
    public SocketAddress localAddress;
    public NLMHandler nlmHandler;

    public NLMHandler(DatagramSocket udpSocket, DatagramPacket socket) {
        this.udpSocket = udpSocket;
        this.address = socket.sender();
        try {
            io.vertx.core.datagram.DatagramPacket delegate = socket.getDelegate();
            Class<?> socketClass = delegate.getClass();
            Field field = socketClass.getDeclaredField("sender");
            field.setAccessible(true);
            DatagramSocketAddress datagramSocketAddress = ((DatagramSocketAddress) field.get(delegate)).localAddress();
            this.localAddress = SocketAddress.inetSocketAddress(datagramSocketAddress.getPort(), datagramSocketAddress.getHostString());
        } catch (Exception e) {
            this.localAddress = udpSocket.localAddress();
            log.error("NLM Handle", e);
        }
    }

    public NLMHandler(DatagramSocket udpSocket, SocketAddress address) {
        this.udpSocket = udpSocket;
        this.address = address;
        try {
            this.localAddress = udpSocket.localAddress();
        } catch (Exception ignored) {}
    }

    public NLMHandler(NetSocket socket) {
        this.socket = socket;
        this.address = socket.remoteAddress();
        try {
            this.localAddress = socket.localAddress();
        } catch (Exception ignored) {}
    }

    public NLMHandler() {

    }

    @Override
    protected void handleMsg(int offset, RpcCallHeader callHeader, ByteBuf msg) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.nlmHandler = nlmHandler;
        // NLM4
        if (callHeader.rpcVersion == 2 && callHeader.program == 100021 && callHeader.programVersion == 4) {
            NLM4.Opcode opcode = NLM4.values[callHeader.opt];
            NLM4.OptInfo proc = NLM4.NLM4Opt[callHeader.opt];

            if (proc == null) {
                return;
            }

            try {
                ReadStruct t = (ReadStruct) proc.constructor.newInstance();

                if (t.readStruct(msg, offset) < 0) {
                    log.error("NLM read msg fail {}", callHeader);
                    return;
                }

                if (NFS.nfsDebug) {
                    log.info("NLM call {} {}", callHeader, t);
                }

                if (this.socket != null) {
                    RunNumUtils.checkRunning(socket, NFS.nfsDebug);
                } else {
                    RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
                }

                Mono<RpcReply> res = (Mono<RpcReply>) proc.function.apply(callHeader, reqHeader, t);
                res.timeout(Duration.ofSeconds(NLM_TIME_OUT))
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException) {
                                log.error("NLM PROCESS timeout opt:{}", opcode);
                            } else {
                                log.error("NLM PROCESS error.opt:{},{}", opcode, e);
                            }
                            return Mono.just(new ErrorReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id)));
                        })
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
                                log.info("NLM reply {}", reply);
                            }
                        });

            } catch (Exception e) {
                log.error("NLM Handle call", e);
            }
        } else {
            log.info("NLM no handler msg {}", callHeader);
        }
    }

    @Override
    protected void handleRes(int offset, RpcReplyHeader replyHeader, ByteBuf msg) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.nlmHandler = nlmHandler;
        Integer[] NLMOwner = NLMHandler.NLMResMap.get(replyHeader.getHeader().id);
        if (NLMOwner == null) {
            return;
        }
        if (NLMOwner[0] == 2 && NLMOwner[1] == 100021 && NLMOwner[2] == 4) {
            NLM4.Opcode opcode = NLM4.values[NLMOwner[3]];
            NLM4.OptInfo proc = NLM4.NLM4Opt[NLMOwner[3]];

            if (proc == null) {
                return;
            }

            try {
                ReadStruct t = (ReadStruct) proc.constructor2.newInstance();

                if (t.readStruct(msg, offset) < 0) {
                    log.error("NLM read res fail {}", replyHeader);
                    return;
                }

                if (NFS.nfsDebug) {
                    log.info("NLM reply {} {}", replyHeader, t);
                }

                if (this.socket != null) {
                    RunNumUtils.checkRunning(socket, NFS.nfsDebug);
                } else {
                    RunNumUtils.checkRunning(udpSocket, NFS.nfsDebug);
                }

                Mono<Boolean> res = (Mono<Boolean>) proc.function2.apply(replyHeader, reqHeader, t);
                res.timeout(Duration.ofSeconds(NLM_TIME_OUT))
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException) {
                                log.error("NLM PROCESS timeout opt:{}", opcode);
                            } else {
                                log.error("NLM PROCESS error.opt:{},{}", opcode, e);
                            }
                            return Mono.just(false);
                        })
                        .subscribe(isSuccess -> {
                            RunNumUtils.releaseRunning();
                            if (!isSuccess) {
                                log.error("NLM PROCESS reply fail.opt:{}", opcode);
                            }
                        });
            } catch (Exception e) {
                log.error("NLM Handle reply", e);
            }
        } else {
            log.info("NLM no handler res {}", replyHeader);
        }
    }

}
