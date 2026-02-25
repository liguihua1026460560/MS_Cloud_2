package com.macrosan.filesystem.cifs.types;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.CompoundRequest;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.utils.msutils.MsException;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.filesystem.cifs.handler.SMBHandler.localIpDebug;

@Data
@Accessors(chain = true)
@Log4j2
public class Session {
    public byte[] serverChallenge = new byte[8];

    //smb1.0 use uid
    //smb2.0 use sessionId
    public short uid;
    public long sessionId;

    public boolean signed = false;
    public short flags;
    public String account;
    public String domain;
    public boolean utf16 = false;
    public SMBHandler handler;
    public NetSocket socket;
    public boolean domainLogin;
    public Map<Short, Session> session1Map;

    public Map<Long, Session> session2Map;

    public byte[] sessionKey;
    public byte[] channelSessionKey;

    //一个消息中有多个msg时，会共用SMB2FileId，当前存在session中
    public SMB2FileId lastUsedFileId;

    public static AtomicLong sessionId0 = new AtomicLong();
    public NFSHandler nfsHandler;
    public Map<String, String> lock = new ConcurrentHashMap<>();
    public Map<String, SMB2FileId> renameMap = new ConcurrentHashMap<>();
    public static Map<SMB2FileId, Tuple2<Session, CompoundRequest>> renameReplyMap = new ConcurrentHashMap<>();
    public long useTime;
    public Map<Integer, String> tidToS3Id = new HashMap<>();
    public String localIp;

    public String bucket;
    public Map<String, String> bucketInfo;

    public Session(short uid) {
        this.uid = uid;
        ThreadLocalRandom.current().nextBytes(serverChallenge);
        nfsHandler = new NFSHandler();
    }

    public Session(long sessionId, SMBHandler handler) {
        this.sessionId = sessionId;
        ThreadLocalRandom.current().nextBytes(serverChallenge);
        nfsHandler = new NFSHandler();
        this.useTime = System.currentTimeMillis();
        this.handler = handler;
    }

    public static Session getNextSession1(Map<Short, Session> sessionMap) {
        AtomicReference<Session> res = new AtomicReference<>();

        for (short i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
            if (i == 0) {
                continue;
            }
            sessionMap.compute(i, (k, v) -> {
                if (v == null) {
                    v = new Session(k);
                    res.set(v);
                    return v;
                }

                return v;
            });

            if (res.get() != null) {
                return res.get();
            }
        }

        throw new MsException(ErrorNo.UNKNOWN_ERROR, "session map full");
    }

    public static Session getNextSession2(Map<Long, Session> sessionMap, SMBHandler handler) {
        long nextSessionId = sessionId0.incrementAndGet();
        Session session = new Session(nextSessionId, handler);
        session.localIp = handler.getLocalAddress();
        if (!SMBHandler.localIpMap.containsKey(session.localIp)) {
            if (localIpDebug) {
                log.info("add smb ip : {}", session.localIp);
            }
            SMBHandler.localIpMap.put(session.localIp, 0);
            SMBHandler.newIpMap.put(session.localIp, System.nanoTime());
        }
        sessionMap.put(nextSessionId, session);
        handler.thisSession2Map.put(nextSessionId, session);
        return session;
    }

    public void addS3Account(int tid, String s3Id) {
        if (StringUtils.isBlank(s3Id)) {
            return;
        }

        tidToS3Id.compute(tid, (k ,v) -> s3Id);
    }

    public String getTreeAccount(int tid) {
        return tidToS3Id.compute(tid, (k, v) -> {
            if (null == v) {
                v = DEFAULT_USER_ID;
            }

            return v;
        });
    }
}
