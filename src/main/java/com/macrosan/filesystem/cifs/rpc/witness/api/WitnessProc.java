package com.macrosan.filesystem.cifs.rpc.witness.api;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.witness.WITNESS;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.ack.*;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.*;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.utils.functional.Tuple2;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.filesystem.cifs.rpc.witness.WITNESS.witnessDebug;
import static com.macrosan.utils.store.StoreUtils.bytesToHex;

@Log4j2
public class WitnessProc {
    public static String WITNESS_PREFIX = "witness_";
    // witness是否是正常取消注册，或者是节点异常取消注册
    public static boolean status = true;
    // 首次连接时各个节点的状态
    public static Map<String, Boolean> cifsStatus = null;
    private final int MAX_POLLING_COUNT = 10 * 60 * 2;
    int nodeExcetionSignal = 0;
    AtomicInteger pollingCount;
    boolean firstAsyncAck = true;

    String cifsServerIp;

    public WitnessProc(Session session) {
        cifsStatus = getCifsStatusMap();
        cifsServerIp = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("cifs_witness_list", session.clientIP);
        pollingCount = new AtomicInteger(MAX_POLLING_COUNT);
    }

    public Mono<Ack> bind(BindCall call, Session session) {
        BindAck bindAck = new BindAck(call, session);
        return Mono.just(bindAck);
    }

    public Mono<Ack> auth(AuthCall call, Session session) {
        // nullSession 参考
        byte[] sessionBaseKey = new byte[16];
        // 参考 https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/d86303b5-b29e-4fb9-b119-77579c761370
        byte[] KeyExchangeKey = sessionBaseKey;
        // 参考 https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/f9e6fbc4-a953-4f24-b229-ccdcc213b9ec
        session.exportedSessionKey = CifsUtils.rc4Encrypt(KeyExchangeKey, call.authenticate.encryptedRandomSessionKeyPayload);

        try {
            byte[] clientSealKey = CifsUtils.ntlmv2Key(session.exportedSessionKey, "session key to client-to-server sealing key magic constant");
            session.clientCipher = Cipher.getInstance("RC4");
            session.clientCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(clientSealKey, "RC4"));

            byte[] serverSealKey = CifsUtils.ntlmv2Key(session.exportedSessionKey, "session key to server-to-client sealing key magic constant");
            session.serverCipher = Cipher.getInstance("RC4");
            session.serverCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(serverSealKey, "RC4"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (witnessDebug) {
            log.info("exportedSessionKey : {}", bytesToHex(session.exportedSessionKey));
        }

        return Mono.empty();
    }

    public Mono<Ack> alterContext(AlterContextCall alterContext, Session session) {
        AlterContextAck alterContextAck = new AlterContextAck(alterContext);

        return Mono.just(alterContextAck);
    }


    public Mono<Ack> getInterfaceList(GetInterfaceListRequestCall call, Session session) {
        cifsStatus = getCifsStatusMap();
        if (call.header.allocHint == 0) {
            // witness V1
            GetInterfaceListRequestV1Ack ack = new GetInterfaceListRequestV1Ack(call, session);
            generateSign(session, ack, call.seqNum);
            return Mono.just(ack);
        } else {
            // witness v2
            GetInterfaceListRequestV2Ack ack = new GetInterfaceListRequestV2Ack(call, session);
            generateSign(session, ack, call.seqNum);
            return Mono.just(ack);
        }
    }


    public Mono<Ack> registerEx(RegisterExRequestCall call, Session session) {
        RegisterExRequestAck ack = new RegisterExRequestAck(call, session);
        generateSign(session, ack, call.seqNum);

        // 存入redis
        String netName = new String(call.stub.netName.data, StandardCharsets.UTF_16LE);
        String ipAddress = new String(call.stub.ipAddress.data, StandardCharsets.UTF_16LE);
        String clientComputerName = new String(call.stub.clientComputerName.data, StandardCharsets.UTF_16LE);
        String value = new JsonObject()
                .put("netName", netName)
                .put("ipAddress", ipAddress)
                .put("clientComputerName", clientComputerName).encode();

        RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX)
                .hset("cifs_witness_list", WITNESS_PREFIX + session.clientIP, value);


        return Mono.just(ack);
    }

    public Mono<Ack> orphaned(OrphanedCall call, Session session) {
        OrphanedAck orphanedAck = new OrphanedAck(call, session);

        return Mono.just(orphanedAck);
    }

    public Mono<Ack> unRegister(UnRegisterRequestCall call, Session session) {
        UnRegisterRequestAck ack = null;
        if (call.stub.verificationTrailer.length != 0) {
            ack = new UnRegisterRequestAck(call, session, false);
        } else {
            ack = new UnRegisterRequestAck(call, session, status);
        }

        status = true;
        generateSign(session, ack, call.seqNum);

        // 删除redis中数据
        RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX)
                .hdel("cifs_witness_list", WITNESS_PREFIX + session.clientIP);


        return Mono.just(ack);
    }

    // TODO 处理连接异常断开 / 重连时，断开witness之前所有的连接
    public Mono<Ack> asyncNotify(AsyncNotifyRequestCall call, Session session) {
        MonoProcessor<Ack> res = MonoProcessor.create();
        asyncNotify(call, session, res);
        return res;
    }


    private void asyncNotify(AsyncNotifyRequestCall call, Session session, MonoProcessor<Ack> res) {
        // 检查连接是否正常
        if (session.socketIsClose) {
            res.onNext(null);
            return;
        }

        if (nodeExcetionSignal > 1) {
            nodeExcetionSignal = 0;
            pollingCount.set(MAX_POLLING_COUNT);
            status = false;
            AsyncNotifyRequestNotFountAck ack = new AsyncNotifyRequestNotFountAck(call, session);
            generateSign(session, ack, call.seqNum);
            res.onNext(ack);
            return;
        }

        // cifs是否断开
        if (!isCofsOpen(cifsServerIp)) {
            if (firstAsyncAck) {
                nodeExcetionSignal += 2;
                AsyncNotifyRequestOkFirstAck ack = new AsyncNotifyRequestOkFirstAck(call, session, cifsServerIp);
                generateSign(session, ack, call.seqNum);
                res.onNext(ack);
            } else {
                nodeExcetionSignal++;
                AsyncNotifyRequestOkSecondAck ack = new AsyncNotifyRequestOkSecondAck(call, session, cifsServerIp);
                generateSign(session, ack, call.seqNum);
                res.onNext(ack);
            }
        } else {
            if (pollingCount.decrementAndGet() > 0) {
                FsUtils.fsExecutor.schedule(() -> asyncNotify(call, session, res), 100, TimeUnit.MILLISECONDS);
            } else {
                pollingCount.set(MAX_POLLING_COUNT);
                firstAsyncAck = false;
                AsyncNotifyRequestTimeOutAck ack = new AsyncNotifyRequestTimeOutAck(call, session);
                generateSign(session, ack, call.seqNum);
                res.onNext(ack);
            }
        }

    }

    private void generateSign(Session session, WitnessAck ack, byte[] seqNum) {
        byte[] sessionKey = session.exportedSessionKey;
        gerateSign(ack, seqNum, sessionKey, session.serverCipher);
    }

    private void gerateSign(WitnessAck ack, byte[] seqNum, byte[] sessionKey, Cipher cipher) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(4096, 4096);
        int size = ack.writeHeader(buf, 0);
        size += ack.writeStub(buf, size);
        buf.writerIndex(size);

        byte[] respMessage = new byte[ack.header.fragLength - ack.header.authLength];
        buf.getBytes(0, respMessage);
        ack.sign = getServerSign(sessionKey, respMessage, seqNum, cipher);
    }

    // 参考 https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/a92716d5-d164-4960-9e15-300f4eef44a8 与 samba源码
    private byte[] getServerSign(byte[] sessionKey, byte[] message, byte[] seqNum, Cipher cipher) {
        byte[] version = new byte[]{1, 0, 0, 0};

        byte[] serverSignKey = CifsUtils.ntlmv2Key(sessionKey, "session key to server-to-client signing key magic constant");

        try {
            Mac mac = Mac.getInstance("HmacMD5");
            mac.init(new SecretKeySpec(serverSignKey, "HmacMD5"));
            mac.update(seqNum);
            byte[] sign0 = mac.doFinal(message);

            byte[] encryptedData = CifsUtils.rc4Encrypt(cipher, Arrays.copyOf(sign0, 8));

            byte[] sign = new byte[16];
            System.arraycopy(version, 0, sign, 0, 4);
            System.arraycopy(encryptedData, 0, sign, 4, 8);
            System.arraycopy(seqNum, 0, sign, 12, 4);
            return sign;
        } catch (Exception e) {
            log.error(e);
        }
        return null;
    }

    // 参考 https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/a92716d5-d164-4960-9e15-300f4eef44a8 与 samba源码
    private boolean checkClientSign(byte[] checkSign, byte[] message, byte[] exportedSessionKey, byte[] seqNum, Cipher cipher) {
        try {
            log.info("message : {}", bytesToHex(message));
            log.info("exportedSessionKey : {}", bytesToHex(exportedSessionKey));
            log.info("seqNum : {}", bytesToHex(seqNum));
            byte[] version = new byte[]{1, 0, 0, 0};
            byte[] clientSignKey = CifsUtils.ntlmv2Key(exportedSessionKey, "session key to client-to-server signing key magic constant");
            Mac mac = Mac.getInstance("HmacMD5");
            mac.init(new SecretKeySpec(clientSignKey, "HmacMD5"));
            mac.update(seqNum);
            mac.update(message);
            byte[] sign0 = mac.doFinal();

            byte[] encryptedData = CifsUtils.rc4Encrypt(cipher, Arrays.copyOf(sign0, 8));
            byte[] sign = new byte[16];
            System.arraycopy(version, 0, sign, 0, 4);
            System.arraycopy(encryptedData, 0, sign, 4, 8);
            System.arraycopy(seqNum, 0, sign, 12, 4);
            log.info("sign: {} {}", bytesToHex(sign), bytesToHex(checkSign));
            return bytesToHex(sign).equals(bytesToHex(checkSign));
        } catch (Exception e) {
            log.error(e);
        }
        return false;
    }

    public static List<Tuple2<String, Boolean>> getCifsStatusList() {
        List<Tuple2<String, Boolean>> list = new ArrayList<>();
        WITNESS.businessIps.forEach(ip -> list.add(new Tuple2<>(ip, isCofsOpen(ip))));
        return list;
    }

    public static Map<String, Boolean> getCifsStatusMap() {
        Map<String, Boolean> map = new HashMap<>();
        WITNESS.businessIps.forEach(ip -> map.put(ip, isCofsOpen(ip)));
        return map;
    }


    public static boolean isCofsOpen(String host) {
        int port = 445;
        int timeout = 100;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeout);
            return true;
        } catch (IOException e) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), timeout);
                return true; // 第二次成功，返回 true
            } catch (IOException e2) {
                return false; // 第二次也失败，返回 false
            }
        }
    }
}
