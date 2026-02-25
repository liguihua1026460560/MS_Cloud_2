package com.macrosan.filesystem.nfs.api;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.call.GetPortCall;
import com.macrosan.filesystem.nfs.call.nlm4.*;
import com.macrosan.filesystem.nfs.handler.NLMHandler;
import com.macrosan.filesystem.nfs.handler.PortMapHandler;
import com.macrosan.filesystem.nfs.lock.NLMLockClient;
import com.macrosan.filesystem.nfs.lock.NLMLockWait;
import com.macrosan.filesystem.nfs.lock.NSMServer;
import com.macrosan.filesystem.nfs.reply.nlm4.NLMReply;
import com.macrosan.filesystem.nfs.reply.nlm4.NullReply;
import com.macrosan.filesystem.nfs.reply.nlm4.ShareReply;
import com.macrosan.filesystem.nfs.types.NLM4Lock;
import com.macrosan.filesystem.nfs.types.Owner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.net.NetClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetClient;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.nfs.NFSBucketInfo.getBucketName;

@Log4j2
public class NLM4Proc {
    private static final NLM4Proc instance = new NLM4Proc();

    public static boolean debug = false;

    private NLM4Proc() {

    }

    public static NLM4Proc getInstance() {
        return instance;
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_NULL)
    public Mono<RpcReply> proNull(RpcCallHeader callHeader, ReqInfo reqHeader, NullCall call) {
        NullReply nullReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        return Mono.just(nullReply);
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_TEST)
    public Mono<RpcReply> test(RpcCallHeader callHeader, ReqInfo reqHeader, TestCall call) {
        NLMReply NLMReply = new NLMReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        NLMReply.cookie = call.cookie;
        NLMReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;

        return Mono.just(NLMReply);
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_LOCK)
    public Mono<RpcReply> lock(RpcCallHeader callHeader, ReqInfo reqHeader, LockCall call) {
        NLMReply NLMReply = new NLMReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        reqHeader.bucket = getBucketName(call.NLM4Lock.fh2.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        NLMReply.cookie = call.cookie;
        String key = String.valueOf(call.NLM4Lock.fh2.ino);
        Owner owner = new Owner(reqHeader.nlmHandler.address.host(), new String(call.NLM4Lock.clientName),
                call.NLM4Lock.svid, call.NLM4Lock.fh2.ino, reqHeader.nlmHandler.localAddress.host(), call.NLM4Lock.offset, call.NLM4Lock.len);
        owner.local = reqHeader.nlmHandler.localAddress.host();
        owner.exclusive = call.exclusive;
        MonoProcessor<NLMReply> res = MonoProcessor.create();
        return Mono.just(NSMServer.reclaim)
                .flatMap(reclaim -> {
                    if (reclaim.get() && !call.reclaim) {
                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_DENIED_GRACE_PERIOD;
                        res.onNext(NLMReply);
                        return res;
                    } else {
                        return NLMLockClient.lock(reqHeader.bucket, key, owner, call.block)
                                .onErrorResume(e -> Mono.just(false))
                                .flatMap(lock -> {
                                    if (lock) {
                                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
                                        return Mono.just(true);
                                    } else {
                                        if (call.block) {
                                            NLMLockWait.grantedMap.computeIfAbsent(reqHeader.bucket, bucket0 -> new ConcurrentHashMap<>())
                                                    .computeIfAbsent(key, key0 -> new ConcurrentHashMap<>())
                                                    .put(owner, Tuples.of(reqHeader.nlmHandler.isUdp, call.NLM4Lock));
                                            NLMReply.stat = FsConstants.NLM4Stats.NLM4_BLOCKED;
                                        } else {
                                            NLMReply.stat = FsConstants.NLM4Stats.NLM4_DENIED;
                                        }
                                        return Mono.just(true);
                                    }
                                })
                                .flatMap(flag -> {
                                    res.onNext(NLMReply);
                                    return res;
                                });
                    }
                });


    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_CANCEL)
    public Mono<RpcReply> cancel(RpcCallHeader callHeader, ReqInfo reqHeader, CancelCall call) {
        NLMReply NLMReply = new NLMReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        reqHeader.bucket = getBucketName(call.NLM4Lock.fh2.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        NLMReply.cookie = call.cookie;
        NLMReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
        String key = String.valueOf(call.NLM4Lock.fh2.ino);

        Owner owner = new Owner(reqHeader.nlmHandler.address.host(), new String(call.NLM4Lock.clientName),
                call.NLM4Lock.svid, call.NLM4Lock.fh2.ino, reqHeader.nlmHandler.localAddress.host(), call.NLM4Lock.offset, call.NLM4Lock.len);
        MonoProcessor<NLMReply> res = MonoProcessor.create();

        return NLMLockClient.cancel(reqHeader.bucket, key, owner)
                .flatMap(b -> NLMLockClient.unlock(reqHeader.bucket, key, owner))
                .onErrorResume(e -> Mono.just(false))
                .flatMap(cancel -> {
                    if (cancel) {
                        NLMLockWait.grantedMap.computeIfPresent(reqHeader.bucket, (bucket0, keyMap) -> {
                            keyMap.computeIfPresent(key, (key0, valueMap) -> {
                                valueMap.remove(owner);
                                return valueMap.isEmpty() ? null : valueMap;
                            });
                            return keyMap.isEmpty() ? null : keyMap;
                        });
                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
                    } else {
                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_DENIED;
                    }
                    res.onNext(NLMReply);
                    return res;
                });
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_UNLOCK)
    public Mono<RpcReply> unlock(RpcCallHeader callHeader, ReqInfo reqHeader, UnlockCall call) {
        NLMReply NLMReply = new NLMReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        reqHeader.bucket = getBucketName(call.NLM4Lock.fh2.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        NLMReply.cookie = call.cookie;
        String key = String.valueOf(call.NLM4Lock.fh2.ino);
        Owner owner = new Owner(reqHeader.nlmHandler.address.host(), new String(call.NLM4Lock.clientName),
                call.NLM4Lock.svid, call.NLM4Lock.fh2.ino, reqHeader.nlmHandler.localAddress.host(), call.NLM4Lock.offset, call.NLM4Lock.len);
        MonoProcessor<NLMReply> res = MonoProcessor.create();

        return NLMLockClient.unlock(reqHeader.bucket, key, owner)
                .onErrorResume(e -> Mono.just(false))
                .flatMap(unlock -> {
                    if (unlock) {
                        NLMLockWait.grantedMap.computeIfPresent(reqHeader.bucket, (bucket0, keyMap) -> {
                            keyMap.computeIfPresent(key, (key0, valueMap) -> {
                                valueMap.remove(owner);
                                return valueMap.isEmpty() ? null : valueMap;
                            });
                            return keyMap.isEmpty() ? null : keyMap;
                        });
                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
                    } else {
                        NLMReply.stat = FsConstants.NLM4Stats.NLM4_DENIED_GRACE_PERIOD;
                    }
                    res.onNext(NLMReply);
                    return res;
                });

    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_GRANTED)
    public Mono<Boolean> granted(RpcReplyHeader replyHeader, ReqInfo reqHeader, NLMReply NLMReply) {
        if (NLMReply.stat == FsConstants.NLM4Stats.NLM4_GRANTED) {
            return Mono.just(true);
        } else {
            return Mono.just(true);
        }
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_SHARE)
    public Mono<RpcReply> share(RpcCallHeader callHeader, ReqInfo reqHeader, ShareCall call) {
        ShareReply shareReply = new ShareReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        shareReply.cookie = call.cookie;
        shareReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
        shareReply.sequence = 0;

        return Mono.just(shareReply);
    }

    @NLM4.Opt(value = NLM4.Opcode.NLMPROC4_UNSHARE)
    public Mono<RpcReply> unshare(RpcCallHeader callHeader, ReqInfo reqHeader, UnshareCall call) {
        ShareReply shareReply = new ShareReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));

        shareReply.cookie = call.cookie;
        shareReply.stat = FsConstants.NLM4Stats.NLM4_GRANTED;
        shareReply.sequence = 0;

        return Mono.just(shareReply);
    }

    public static GrantedCall createGrantedCall(boolean exclusive, NLM4Lock nlm4Lock) {
        int id = (int) NSMProc.nsmNextXid();
        GrantedCall grantedCall = new GrantedCall(SunRpcHeader.newCallHeader(id));

        AuthUnix authUnix = new AuthUnix();
        authUnix.setFlavor(1);
        authUnix.setStamp(0);
        String name = System.getenv("HOSTNAME");
        authUnix.setNameSize(name.length());
        authUnix.setName(name.getBytes());
        authUnix.setUid(0);
        authUnix.setGid(0);
        authUnix.setGidN(0);
        authUnix.setGids(new int[0]);
        authUnix.setPadding(0);
        authUnix.setAuthLen(authUnix.length());
        grantedCall.auth = authUnix;

        grantedCall.cookie = new byte[4];
        grantedCall.exclusive = exclusive;
        grantedCall.NLM4Lock = nlm4Lock;

        return grantedCall;
    }

    public static GetPortCall createGetPortNLMCall(boolean isUdp) {
        int id = (int) NSMProc.nsmNextXid();
        GetPortCall getPortCall = new GetPortCall(SunRpcHeader.newCallHeader(id));

        getPortCall.program = 100021;
        getPortCall.programVersion = 4;
        if (isUdp) {
            getPortCall.proto = 17;
        } else {
            getPortCall.proto = 6;
        }
        getPortCall.port = 0;

        AuthUnix authUnix = new AuthUnix();
        authUnix.setFlavor(1);
        authUnix.setStamp(0);
        String name = System.getenv("HOSTNAME");
        authUnix.setNameSize(name.length());
        authUnix.setName(name.getBytes());
        authUnix.setUid(0);
        authUnix.setGid(0);
        authUnix.setGidN(0);
        authUnix.setGids(new int[0]);
        authUnix.setPadding(0);
        authUnix.setAuthLen(authUnix.length());
        getPortCall.auth = authUnix;

        return getPortCall;
    }

    public static void sendGranted(Owner owner, Tuple2<Boolean, NLM4Lock> tuple2, int retry) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        sendGrantedExec(res, owner, tuple2, retry)
                .subscribe(sendSuccess -> {
                });
    }

    public static Mono<Boolean> sendGrantedExec(MonoProcessor<Boolean> res, Owner owner, Tuple2<Boolean, NLM4Lock> tuple2, int retry) {
        GetPortCall getPortCall = createGetPortNLMCall(tuple2.getT1());
        GrantedCall grantedCall = createGrantedCall(owner.exclusive, tuple2.getT2());
        long timeMillis = System.currentTimeMillis();
        return getPortExec(tuple2.getT1(), owner, getPortCall)
                .flatMap(port -> {
                    if (port != 0) {
                        return grantedExec(tuple2.getT1(), owner, port, grantedCall);
                    }
                    return Mono.just(false);
                })
                .flatMap(sendSuccess -> {
                    if (sendSuccess) {
                        res.onNext(true);
                        return res;
                    } else {
                        if (retry >= 5) {
                            NLM4.NLMPortMap.remove(owner.ip);
                            res.onNext(false);
                            return res;
                        }
                        return Mono.delay(Duration.ofMillis(4000 - (System.currentTimeMillis() - timeMillis)),
                                        Schedulers.boundedElastic())
                                .flatMap(ignored -> sendGrantedExec(res, owner, tuple2, retry + 1));
                    }
                });
    }

    public static Mono<Integer> getPortExec(boolean isUdp, Owner owner, GetPortCall getPortCall) {
        MonoProcessor<Integer> portRes = MonoProcessor.create();
        if (!NSMProc.isLocalIp(owner.local)) {
            portRes.onNext(0);
            return portRes.timeout(Duration.ofSeconds(2), Mono.just(0));
        }
        Integer port = NLM4.NLMPortMap.get(owner.ip);
        Vertx vertx = NLM4.vertx;
        NetClient client = vertx.createNetClient(new NetClientOptions()
                .setLocalAddress(owner.local));
        DatagramSocket udpSocket = vertx.createDatagramSocket();
        if (port != null && port != 0) {
            portRes.onNext(port);
        } else {
            try {
                PortMapHandler.getPortMap.put(getPortCall.getHeader().id, new Integer[]{100021, 4});
                if (!isUdp) {
                    client.connect(111, owner.ip, res -> {
                        if (res.succeeded()) {
                            NetSocket socket = res.result();
                            PortMapHandler handler = new PortMapHandler(socket);

                            ByteBuf buf = handler.callToBuf(getPortCall, 4096);
                            socket.write(Buffer.buffer(buf), v -> {
                                if (!v.succeeded()) {
                                    log.info("NLM send getPort tcp fail, ip: {}", owner.ip);
                                }
                            });

                            socket.handler(b -> {
                                try {
                                    handler.handle(b.getByteBuf());
                                    portRes.onNext(NLM4.NLMPortMap.get(owner.ip) == null ? 0 : NLM4.NLMPortMap.get(owner.ip));
                                } catch (Exception e) {
                                    log.error("NLM getPort tcp fail", e);
                                    portRes.onNext(0);
                                }
                            });
                        } else {
                            log.info("NLM failed to connect tcp: " + res.cause());
                            portRes.onNext(0);
                        }
                    });
                } else {
                    SocketAddress address = SocketAddress.inetSocketAddress(111, owner.ip);
                    PortMapHandler handler = new PortMapHandler(udpSocket, address);
                    handler.isUdp = true;
                    Buffer buf = Buffer.buffer(handler.callToBufUdp(getPortCall, 4096));
                    udpSocket.listen(0, owner.local, res -> {
                        if (res.succeeded()) {
                            udpSocket.send(buf, 111, owner.ip, sendRes -> {
                                if (!sendRes.succeeded()) {
                                    log.info("NLM send getPort udp fail, ip: {}", owner.ip);
                                    portRes.onNext(0);
                                }
                            });
                        } else {
                            log.info("NLM failed to connect udp: " + res.cause());
                            portRes.onNext(0);
                        }
                    });

                    udpSocket.handler(packet -> {
                        Buffer b = packet.data();
                        ByteBuf buf0 = Unpooled.wrappedBuffer(b.getBytes());
                        handler.handle(buf0);
                        portRes.onNext(NLM4.NLMPortMap.get(owner.ip) == null ? 0 : NLM4.NLMPortMap.get(owner.ip));
                    });
                }
            } catch (Exception e) {
                log.error("NLM getPort fail", e);
                portRes.onNext(0);
            }
        }
        return portRes.timeout(Duration.ofSeconds(2), Mono.just(0)).doFinally(signalType -> {
            PortMapHandler.getPortMap.remove(getPortCall.getHeader().id);
            client.close();
            udpSocket.close();
        });
    }

    public static Mono<Boolean> grantedExec(boolean isUdp, Owner owner, int port, GrantedCall grantedCall) {
        MonoProcessor<Boolean> monoGranted = MonoProcessor.create();
        if (!NSMProc.isLocalIp(owner.local)) {
            monoGranted.onNext(false);
            return monoGranted.timeout(Duration.ofSeconds(2), Mono.just(false));
        }
        Vertx vertx = NLM4.vertx;
        NetClient client = vertx.createNetClient(new NetClientOptions()
                .setLocalAddress(owner.local));
        DatagramSocket udpSocket = vertx.createDatagramSocket();
        try {
            NLMHandler.NLMResMap.put(grantedCall.getHeader().id, new Integer[]{2, 100021, 4, 5});
            if (!isUdp) {
                client.connect(port, owner.ip, res -> {
                    if (res.succeeded()) {
                        NetSocket socket = res.result();
                        NLMHandler handler = new NLMHandler(socket);

                        ByteBuf buf = handler.callToBuf(grantedCall, 4096);
                        socket.write(Buffer.buffer(buf), v -> {
                            if (!v.succeeded()) {
                                log.info("NLM send granted tcp fail, ip: {}", owner.ip);
                            }
                        });

                        socket.handler(b -> {
                            try {
                                handler.handle(b.getByteBuf());
                                monoGranted.onNext(true);
                            } catch (Exception e) {
                                log.error("NLM granted tcp fail", e);
                                monoGranted.onNext(false);
                            }
                        });
                    } else {
                        log.info("NLM failed to connect tcp: " + res.cause());
                        monoGranted.onNext(false);
                    }
                });
            } else {
                SocketAddress address = SocketAddress.inetSocketAddress(port, owner.ip);
                NLMHandler handler = new NLMHandler(udpSocket, address);
                handler.isUdp = true;
                Buffer buf = Buffer.buffer(handler.callToBufUdp(grantedCall, 4096));
                udpSocket.listen(0, owner.local, res -> {
                    if (res.succeeded()) {
                        udpSocket.send(buf, port, owner.ip, sendRes -> {
                            if (!sendRes.succeeded()) {
                                log.info("NLM send granted udp fail, ip: {}", owner.ip);
                                monoGranted.onNext(false);
                            }
                        });
                    } else {
                        log.info("NLM failed to connect udp: " + res.cause());
                        monoGranted.onNext(false);
                    }
                });

                udpSocket.handler(packet -> {
                    Buffer b = packet.data();
                    ByteBuf buf0 = Unpooled.wrappedBuffer(b.getBytes());
                    handler.handle(buf0);
                    monoGranted.onNext(true);
                });

            }
        } catch (Exception e) {
            log.error("NLM granted fail", e);
            monoGranted.onNext(false);
        }
        return monoGranted.timeout(Duration.ofSeconds(2), Mono.just(false)).doFinally(signalType -> {
            NLMHandler.NLMResMap.remove(grantedCall.getHeader().id);
            client.close();
            udpSocket.close();
        });
    }

}
