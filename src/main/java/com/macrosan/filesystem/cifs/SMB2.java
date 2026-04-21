package com.macrosan.filesystem.cifs;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cache.WriteCacheClient;
import com.macrosan.filesystem.cifs.reply.smb2.ErrorReply;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessClient;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function3;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.CIFS.cifsDebug;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.READ_STRUCT_INVALID_PARAMETER;

@Log4j2
public class SMB2 {
    public static final SMB2_OPCODE[] SMB2_OPCODES = new SMB2_OPCODE[256];

    public enum SMB2_OPCODE {
        SMB2_NEGPROT(0x00),
        SMB2_SESSSETUP(0x01),
        SMB2_LOGOFF(0x02),
        SMB2_TCON(0x03),
        SMB2_TDIS(0x04),
        SMB2_CREATE(0x05),
        SMB2_CLOSE(0x06),
        SMB2_FLUSH(0x07),
        SMB2_READ(0x08),
        SMB2_WRITE(0x09),
        SMB2_LOCK(0x0a),
        SMB2_IOCTL(0x0b),
        SMB2_CANCEL(0x0c),
        SMB2_KEEPALIVE(0x0d),
        SMB2_QUERY_DIRECTORY(0x0e),
        SMB2_NOTIFY(0x0f),
        SMB2_GETINFO(0x10),
        SMB2_SETINFO(0x11),
        SMB2_BREAK(0x12);

        public final byte opcode;

        SMB2_OPCODE(int opcode) {
            this.opcode = (byte) opcode;
        }
    }

    static {
        for (SMB2_OPCODE opcode : SMB2_OPCODE.values()) {
            SMB2_OPCODES[opcode.opcode & 0xff] = opcode;
        }
    }

    public static boolean caseSensitive;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Smb2Opt {
        SMB2_OPCODE value();

        boolean allowNoSession() default false;

        boolean allowIPCSession() default false;

        int buf() default 4096;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SMB2Reply extends SMBHeader.SMBReply {
        public SMB2Reply(SMB2Header reqHeader) {
            super.header = SMB2Header.newReplyHeader(reqHeader);
            body = ErrorReply.EMPTY_ERROR;
        }

        public SMB2Reply() {
            
        }

        @Override
        public SMB2Header getHeader() {
            return (SMB2Header) header;
        }
    }

    public static final String IPC = "IPC$";

    public static class OptInfo<T extends ReadStruct> {
        public Function3<SMB2Header, Session, T, Mono<SMB2Reply>> function;
        public Constructor<T> constructor;
        public int bufSize;
        public boolean allowNoSession;
        public boolean allowIPC;
        public SMB2_OPCODE opcode;

        OptInfo(Class<T> tClass) throws NoSuchMethodException {
            constructor = tClass.getDeclaredConstructor();
            constructor.setAccessible(true);
        }

        public Mono<SMB2Reply> run(SMB2Header callHeader, Session session0, ByteBuf buf, int offset) {
            try {
                T t = constructor.newInstance();
                int readResult = t.readStruct(buf, offset);
                if (readResult < 0) {

                    SMB2Reply reply = new SMB2Reply(callHeader);
                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_NOT_IMPLEMENTED);
                    if (readResult == READ_STRUCT_INVALID_PARAMETER) {
                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_INVALID_PARAMETER);
                    }
                    return Mono.just(reply);
                }

                if (cifsDebug) {
                    log.info("【{}】smb2 call {} {}", opcode, callHeader, t);
                }

                if (null == session0 || (session0.sessionId == 0 && !allowNoSession)) {
                    if (null == session0) {
                        log.info("The session: {} has been removed and will be re-established, remoteIp: {}, opcode: {}", callHeader.sessionId, callHeader.handler.getSocket().remoteAddress().host(), callHeader.opcode);
                    } else {
                        log.error("no session set up: session0.sessionId: {}, callHeader.sessionId: {}, allowNoSession: {}, opcode: {}", session0.sessionId, callHeader.sessionId, allowNoSession, callHeader.opcode);
                    }
                    SMB2Reply reply = new SMB2Reply(callHeader);
                    if (null == session0) {
                        reply.getHeader().setStatus(STATUS_NETWORK_SESSION_EXPIRED);
                    } else {
                        reply.getHeader().setStatus(STATUS_BAD_LOGON_SESSION_STATE);
                    }
                    return Mono.just(reply);
                }

                if (session0.sessionId != 0 && callHeader.tid == -1 && !allowIPC) {
                    SMB2Reply reply = new SMB2Reply(callHeader);
                    reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                    return Mono.just(reply);
                }

                return function.apply(callHeader, session0, t);
            } catch (Exception e) {
                log.error("run opt {} fail", opcode, e);

                SMB2Reply reply = new SMB2Reply(callHeader);
                if (e instanceof NFSException && ((NFSException) e).nfsError
                        && FsConstants.NfsErrorNo.NFS3ERR_STALE == ((NFSException) e).getErrCode()) {
                    reply.getHeader().setStatus(STATUS_NETWORK_NAME_DELETED);
                    return Mono.just(reply);
                } else {
                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_IO_DEVICE_ERROR);
                }
                return Mono.just(reply);
            }
        }
    }

    public static OptInfo[] smb2Opt = new OptInfo[SMB2_OPCODES.length];

    public static void initProc() {
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.cifs.api.smb2", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(Smb2Opt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if (param[0] == SMB2Header.class && param[1] == Session.class) {
                            if (SMB2Body.class.isAssignableFrom(param[2])) {
                                return true;
                            }
                        }
                    }

                    log.info("fail init method {} from", m);
                    return false;
                }).subscribe(m -> {
                    try {
                        Smb2Opt opt = m.getAnnotation(Smb2Opt.class);
                        OptInfo info = new OptInfo(m.getParameterTypes()[2]);
                        info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        info.bufSize = opt.buf();
                        info.allowNoSession = opt.allowNoSession();
                        info.allowIPC = opt.allowIPCSession();
                        info.opcode = opt.value();
                        smb2Opt[opt.value().opcode & 0xff] = info;
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init smb2 opt num {}", succ.get());

        // 从redis获取 cifs_case_sensitive 配置, 0 = false; 1 = true;
        RedisConnPool pool = RedisConnPool.getInstance();
        try {
            String result = pool.getCommand(REDIS_SYSINFO_INDEX).get("cifs_case_sensitive");
            caseSensitive = !"0".equals(result);
        } catch (Exception e) {
            log.error("get 'cifs_case_sensitive' fail from redis");
        }
        try {
            String result = pool.getCommand(REDIS_SYSINFO_INDEX).hget("cifs_controls", "share_access");
            ShareAccessClient.shareAccessSwitch = !"0".equals(result);
        } catch (Exception e) {
            log.error("get 'cifs share_access' fail from redis", e);
        }
        try {
            String writeNum = pool.getCommand(REDIS_SYSINFO_INDEX).get("cifs_write_cache_num");
            if (StringUtils.isNumeric(writeNum)) {
                WriteCacheClient.writeNum = Integer.parseInt(writeNum);
            }
            log.info("cifs_write_cache_num: " + WriteCacheClient.writeNum);
        } catch (Exception e) {
            log.error("get 'cifs_write_cache_num' fail from redis", e);
            log.info("cifs_write_cache_num: " + WriteCacheClient.writeNum);
        }
    }
}
