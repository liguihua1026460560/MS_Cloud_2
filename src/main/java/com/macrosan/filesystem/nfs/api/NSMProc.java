package com.macrosan.filesystem.nfs.api;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthNull;
import com.macrosan.filesystem.nfs.call.GetPortCall;
import com.macrosan.filesystem.nfs.call.nsm.*;
import com.macrosan.filesystem.nfs.handler.NSMHandler;
import com.macrosan.filesystem.nfs.handler.PortMapHandler;
import com.macrosan.filesystem.nfs.lock.NLMLockClient;
import com.macrosan.filesystem.nfs.lock.NSMServer;
import com.macrosan.filesystem.nfs.reply.nsm.MonReply;
import com.macrosan.filesystem.nfs.reply.nsm.NullReply;
import com.macrosan.filesystem.nfs.reply.nsm.UnmonReply;
import com.macrosan.filesystem.nfs.types.Owner;
import com.macrosan.filesystem.nfs.types.Sm;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;


@Log4j2
public class NSMProc {

    private static long nsmXid = 0;

    private static final NSMProc instance = new NSMProc();

    public static boolean debug = false;

    private NSMProc() {

    }

    public static NSMProc getInstance() {
        return instance;
    }

    @NSM.Opt(value = NSM.Opcode.SM_NULL)
    public Mono<RpcReply> proNull(RpcCallHeader callHeader, ReqInfo reqHeader, NullCall call) {
        NullReply nullReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        return Mono.just(nullReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_STAT)
    public Mono<RpcReply> stat(RpcCallHeader callHeader, ReqInfo reqHeader, StatCall call) {
        MonReply statReply = new MonReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        statReply.stat = FsConstants.NSMStats.STAT_FAIL;
        statReply.state = 0;

        return Mono.just(statReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_MON)
    public Mono<RpcReply> mon(RpcCallHeader callHeader, ReqInfo reqHeader, MonCall call) {
        MonReply monReply = new MonReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        monReply.stat = FsConstants.NSMStats.STAT_SUCC;
        monReply.state = 0;

        return Mono.just(monReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_UNMON)
    public Mono<RpcReply> unmon(RpcCallHeader callHeader, ReqInfo reqHeader, UnmonCall call) {
        UnmonReply unmonReply = new UnmonReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        unmonReply.state = 0;

        return Mono.just(unmonReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_UNMON_ALL)
    public Mono<RpcReply> unmonAll(RpcCallHeader callHeader, ReqInfo reqHeader, UnmonCall call) {
        UnmonReply unmonReply = new UnmonReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        unmonReply.state = 0;

        return Mono.just(unmonReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_SIMU_CRASH)
    public Mono<RpcReply> simuCrash(RpcCallHeader callHeader, ReqInfo reqHeader, NullCall call) {
        NullReply nullReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        return Mono.just(nullReply);
    }

    @NSM.Opt(value = NSM.Opcode.SM_NOTIFY)
    public Mono<RpcReply> notify(RpcCallHeader callHeader, ReqInfo reqHeader, NotifyCall call) {
        NullReply nullReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        MonoProcessor<NullReply> res = MonoProcessor.create();
        Set<Sm> originalSet = NSMServer.smMap.get(reqHeader.nsmHandler.address.host() + " " + reqHeader.nsmHandler.localAddress.host());
        Set<Sm> sms = new HashSet<>(originalSet == null ? new HashSet<>() : originalSet);
        return Mono.just(sms.isEmpty())
                .flatMap(isNull -> {
                    if (isNull) {
                        res.onNext(nullReply);
                        return res;
                    } else {
                        log.info("NSM client notify, unlock:{}", sms);
                        return Flux.fromIterable(sms)
                                .flatMap(nsmLock -> NLMLockClient.unlock(nsmLock.bucket, String.valueOf(nsmLock.ino),
                                                new Owner(nsmLock.ip, nsmLock.clientName, nsmLock.svid, nsmLock.ino, nsmLock.local, nsmLock.offset, nsmLock.len))
                                        .onErrorResume(e -> Mono.just(false)))
                                .collectList()
                                .flatMap(a -> {
                                    res.onNext(nullReply);
                                    return res;
                                });
                    }
                });

    }

    @NSM.Opt(value = NSM.Opcode.SM_NOTIFY)
    public Mono<Boolean> notify(RpcReplyHeader replyHeader, ReqInfo reqHeader, NullReply NSMReply) {
        return Mono.just(true);
    }

    public static Mono<Boolean> sendGetPortExec(MonoProcessor<Boolean> res, String ip, String local, int retry) {
        GetPortCall getPortCall = createGetPortNSMCall();
        long timeMillis = System.currentTimeMillis();
        return getPortExec(ip, local, getPortCall)
                .flatMap(port -> {
                    if (port != 0) {
                        return sendNotifyExec(res, ip, local, port, 1);
                    }
                    return Mono.just(false);
                })
                .flatMap(sendSuccess -> {
                    if (sendSuccess) {
                        res.onNext(true);
                        return res;
                    } else {
                        if (!NSMServer.smBakMap.containsKey(ip + " " + local)) {
                            res.onNext(true);
                            return res;
                        }
                        if (retry >= 30) {
                            res.onNext(false);
                            return res;
                        }
                        return Mono.delay(Duration.ofMillis(30_000 - (System.currentTimeMillis() - timeMillis)),
                                        Schedulers.boundedElastic())
                                .flatMap(ignored -> sendGetPortExec(res, ip, local, retry + 1));
                    }
                });
    }

    public static Mono<Boolean> sendNotifyExec(MonoProcessor<Boolean> res, String ip, String local, int port, int retry) {
        NotifyCall notifyCall = createNotifyCall();
        long timeMillis = System.currentTimeMillis();
        return notifyExec(ip, local, port, notifyCall)
                .flatMap(sendSuccess -> {
                    if (sendSuccess) {
                        res.onNext(true);
                        return res;
                    } else {
                        if (!NSMServer.smBakMap.containsKey(ip + " " + local)) {
                            res.onNext(true);
                            return res;
                        }
                        if (retry >= 4) {
                            NSM.NSMPortMap.remove(ip);
                            res.onNext(false);
                            return res;
                        }
                        return Mono.delay(Duration.ofMillis((1L << retry) * 1000 - (System.currentTimeMillis() - timeMillis)),
                                        Schedulers.boundedElastic())
                                .flatMap(ignored -> sendNotifyExec(res, ip, local, port, retry + 1));
                    }
                });
    }

    public static NotifyCall createNotifyCall() {
        int id = (int) nsmNextXid();
        NotifyCall notifyCall = new NotifyCall(SunRpcHeader.newCallHeader(id));

        notifyCall.monName = System.getenv("HOSTNAME").getBytes(StandardCharsets.UTF_8);
        notifyCall.state = NSMServer.state;

        AuthNull authNull = new AuthNull();
        notifyCall.auth = authNull;

        return notifyCall;
    }


    public static GetPortCall createGetPortNSMCall() {
        int id = (int) nsmNextXid();
        GetPortCall getPortCall = new GetPortCall(SunRpcHeader.newCallHeader(id));

        getPortCall.program = 100024;
        getPortCall.programVersion = 1;
        getPortCall.proto = 17;
        getPortCall.port = 0;

        AuthNull authNull = new AuthNull();
        getPortCall.auth = authNull;

        return getPortCall;
    }

    public static Mono<Integer> getPortExec(String ip, String local, GetPortCall getPortCall) {
        MonoProcessor<Integer> portRes = MonoProcessor.create();
        if (!isLocalIp(local)) {
            portRes.onNext(0);
            return portRes.timeout(Duration.ofSeconds(2), Mono.just(0));
        }
        Integer port = NSM.NSMPortMap.get(ip);
        Vertx vertx = NSM.vertx;
        DatagramSocket udpSocket = vertx.createDatagramSocket();
        if (port != null && port != 0) {
            portRes.onNext(port);
        } else {
            try {
                PortMapHandler.getPortMap.put(getPortCall.getHeader().id, new Integer[]{100024, 1});
                SocketAddress address = SocketAddress.inetSocketAddress(111, ip);
                PortMapHandler handler = new PortMapHandler(udpSocket, address);
                handler.isUdp = true;
                Buffer buf = Buffer.buffer(handler.callToBufUdp(getPortCall, 4096));
                udpSocket.listen(0, local, res -> {
                    if (res.succeeded()) {
                        udpSocket.send(buf, 111, ip, sendRes -> {
                            if (!sendRes.succeeded()) {
                                log.info("NSM send getPort fail");
                                portRes.onNext(0);
                            }
                        });
                    } else {
                        log.info("NSM failed to connect udp: " + res.cause());
                        portRes.onNext(0);
                    }
                });

                udpSocket.handler(packet -> {
                    Buffer b = packet.data();
                    ByteBuf buf0 = Unpooled.wrappedBuffer(b.getBytes());
                    handler.handle(buf0);
                    portRes.onNext(NSM.NSMPortMap.get(ip) == null ? 0 : NSM.NSMPortMap.get(ip));
                });
            } catch (Exception e) {
                e.printStackTrace();
                log.error("NSM getPort fail", e);
                portRes.onNext(0);
            }
        }
        return portRes.timeout(Duration.ofSeconds(2), Mono.just(0)).doFinally(signalType -> {
            PortMapHandler.getPortMap.remove(getPortCall.getHeader().id);
            udpSocket.close();
        });
    }

    public static Mono<Boolean> notifyExec(String ip, String local, int port, NotifyCall notifyCall) {
        MonoProcessor<Boolean> monoNotify = MonoProcessor.create();
        if (!isLocalIp(local)) {
            monoNotify.onNext(false);
            return monoNotify.timeout(Duration.ofSeconds(2), Mono.just(false));
        }
        Vertx vertx = NSM.vertx;
        DatagramSocket udpSocket = vertx.createDatagramSocket();
        try {
            NSMHandler.NSMResMap.put(notifyCall.getHeader().id, new Integer[]{2, 100024, 1, 6});
            SocketAddress address = SocketAddress.inetSocketAddress(port, ip);
            NSMHandler handler = new NSMHandler(udpSocket, address);
            handler.isUdp = true;
            Buffer buf = Buffer.buffer(handler.callToBufUdp(notifyCall, 4096));
            udpSocket.listen(0, local, res -> {
                if (res.succeeded()) {
                    udpSocket.send(buf, port, ip, sendRes -> {
                        if (!sendRes.succeeded()) {
                            log.info("NSM send notify udp fail, ip: {}", ip);
                            monoNotify.onNext(false);
                        }
                    });
                } else {
                    log.info("NSM failed to connect udp: " + res.cause());
                    monoNotify.onNext(false);
                    udpSocket.close();
                }
            });

            udpSocket.handler(packet -> {
                Buffer b = packet.data();
                ByteBuf buf0 = Unpooled.wrappedBuffer(b.getBytes());
                handler.handle(buf0);
                monoNotify.onNext(true);
            });
        } catch (Exception e) {
            log.error("NSM notify fail", e);
            monoNotify.onNext(false);
        }
        return monoNotify.timeout(Duration.ofSeconds(2), Mono.just(false)).doFinally(signalType -> {
            NSMHandler.NSMResMap.remove(notifyCall.getHeader().id);
            udpSocket.close();
        });
    }


    public static synchronized long nsmNextXid() {
        if (nsmXid == 0) {
            long pid = getProcessId();
            long currentTimeMillis = System.currentTimeMillis();
            nsmXid = (pid ^ currentTimeMillis);
        }
        return nsmXid++;
    }

    private static long getProcessId() {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        String pidStr = jvmName.split("@")[0];
        return Long.parseLong(pidStr);
    }

    public static boolean isLocalIp(String ip) {
        try {
            InetAddress address = InetAddress.getByName(ip);
            // 获取本机所有的网络接口
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                // 获取每个网络接口的所有 IP 地址
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress localAddress = addresses.nextElement();
                    // 排除本地回环地址（127.0.0.1）和其他不可用的地址
                    if (!localAddress.isLoopbackAddress() && !localAddress.isLinkLocalAddress()) {
                        if (localAddress.equals(address)) {
                            return networkInterface.isUp(); // 如果找到匹配的 IP 地址且为 up 状态, 返回 true
                        }
                    }
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }
}
