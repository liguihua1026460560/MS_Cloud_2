package com.macrosan.filesystem.cifs;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.reply.smb1.EmptyReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function3;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_BAD_LOGON_SESSION_STATE;
import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.SMB_NEGPROT;

@Log4j2
public class SMB1 {
    public static final SMB1_OPCODE[] SMB1_OPCODES = new SMB1_OPCODE[256];

    public enum SMB1_OPCODE {
        SMB_MKDIR(0x00), /* create directory */
        SMB_RMDIR(0x01), /* delete directory */
        SMB_OPEN(0x02), /* open file */
        SMB_CREATE(0x03), /* create file */
        SMB_CLOSE(0x04), /* close file */
        SMB_FLUSH(0x05), /* flush file */
        SMB_UNLINK(0x06), /* delete file */
        SMB_MV(0x07), /* rename file */
        SMB_GETATR(0x08), /* get file attributes */
        SMB_SETATR(0x09), /* set file attributes */
        SMB_READ(0x0A), /* read from file */
        SMB_WRITE(0x0B), /* write to file */
        SMB_LOCK(0x0C), /* lock byte range */
        SMB_UNLOCK(0x0D), /* unlock byte range */
        SMB_CTEMP(0x0E), /* create temporary file */
        SMB_MKNEW(0x0F), /* make new file */
        SMB_CHECKPATH(0x10), /* check directory path */
        SMB_EXIT(0x11), /* process exit */
        SMB_LSEEK(0x12), /* seek */
        SMB_TCON(0x70), /* tree connect */
        SMB_TCONX(0x75), /* tree connect and X*/
        SMB_TDIS(0x71), /* tree disconnect */
        SMB_NEGPROT(0x72), /* negotiate protocol */
        SMB_DSKATTR(0x80), /* get disk attributes */
        SMB_SEARCH(0x81), /* search directory */
        SMB_SPLOPEN(0xC0), /* open print spool file */
        SMB_SPLWR(0xC1), /* write to print spool file */
        SMB_SPLCLOSE(0xC2), /* close print spool file */
        SMB_SPLRETQ(0xC3), /* return print queue */
        SMB_SENDS(0xD0), /* send single block message */
        SMB_SENDB(0xD1), /* send broadcast message */
        SMB_FWDNAME(0xD2), /* forward user name */
        SMB_CANCELF(0xD3), /* cancel forward */
        SMB_GETMAC(0xD4), /* get machine name */
        SMB_SENDSTRT(0xD5), /* send start of multi-block message */
        SMB_SENDEND(0xD6), /* send end of multi-block message */
        SMB_SENDTXT(0xD7), /* send text of multi-block message */
        SMB_LOCKREAD(0x13), /* Lock a range and read */
        SMB_WRITEUNLOCK(0x14), /* Unlock a range then write */
        SMB_READBRAW(0x1a), /* read a block of data with no smb header */
        SMB_WRITEBRAW(0x1d), /* write a block of data with no smb header */
        SMB_WRITEC(0x20), /* secondary write request */
        SMB_WRITECLOSE(0x2c), /* write a file then close it */
        SMB_READBMPX(0x1B), /* read block multiplexed */
        SMB_READBS(0x1C), /* read block (secondary response) */
        SMB_WRITEBMPX(0x1E), /* write block multiplexed */
        SMB_WRITEBS(0x1F), /* write block (secondary request) */
        SMB_SETATTRE(0x22), /* set file attributes expanded */
        SMB_GETATTRE(0x23), /* get file attributes expanded */
        SMB_LOCKINGX(0x24), /* lock/unlock byte ranges and X */
        SMB_TRANS(0x25), /* transaction - name, bytes in/out */
        SMB_TRANSS(0x26), /* transaction (secondary request/response) */
        SMB_IOCTL(0x27), /* IOCTL */
        SMB_IOCTLS(0x28), /* IOCTL (secondary request/response) */
        SMB_COPY(0x29), /* copy */
        SMB_MOVE(0x2A), /* move */
        SMB_ECHO(0x2B), /* echo */
        SMB_OPENX(0x2D), /* open and X */
        SMB_READX(0x2E), /* read and X */
        SMB_WRITEX(0x2F), /* write and X */
        SMB_SESSSETUPX(0x73), /* Session Set Up & X (including User Logon) */
        SMB_FFIRST(0x82), /* find first */
        SMB_FUNIQUE(0x83), /* find unique */
        SMB_FCLOSE(0x84), /* find close */
        SMB_INVALID(0xFE), /* invalid command */
        SMB_TRANS2(0x32), /* TRANS2 protocol set */
        SMB_TRANSS2(0x33), /* TRANS2 protocol set, secondary command */
        SMB_FINDCLOSE(0x34), /* Terminate a TRANSACT2_FINDFIRST */
        SMB_FINDNCLOSE(0x35), /* Terminate a TRANSACT2_FINDNOTIFYFIRST */
        SMB_ULOGOFFX(0x74), /* user logoff */
        SMB_NTTRANS(0xA0), /* NT transact */
        SMB_NTTRANSS(0xA1), /* NT transact secondary */
        SMB_NTCREATEX(0xA2), /* NT create and X */
        SMB_NTCANCEL(0xA4), /* NT cancel */
        SMB_NTRENAME(0xA5), /* NT rename */;

        public final byte opcode;

        SMB1_OPCODE(int opcode) {
            this.opcode = (byte) opcode;
        }
    }

    static {
        for (SMB1.SMB1_OPCODE opcode : SMB1.SMB1_OPCODE.values()) {
            SMB1.SMB1_OPCODES[opcode.opcode & 0xff] = opcode;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Smb1Opt {
        SMB1_OPCODE value();

        boolean allowNoSession() default false;

        int buf() default 4096;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SMB1Reply extends SMBHeader.SMBReply {
        public SMB1Reply(SMB1Header reqHeader) {
            super.header = SMB1Header.newReplyHeader(reqHeader);
            super.body = EmptyReply.DEFAULT;
        }

        @Override
        public SMB1Header getHeader() {
            return (SMB1Header) header;
        }

        @Override
        public SMB1Body getBody() {
            return (SMB1Body) body;
        }


    }

    public static class OptInfo<T extends ReadStruct> {
        public Function3<SMB1Header, Session, T, Mono<SMBHeader.SMBReply>> function;
        public Constructor<T> constructor;
        public int bufSize;
        public boolean allowNoSession;

        OptInfo(Class<T> tClass) throws NoSuchMethodException {
            constructor = tClass.getDeclaredConstructor();
            constructor.setAccessible(true);
        }

        public Mono<SMBHeader.SMBReply> run(SMB1Header callHeader, Session session0, ByteBuf buf, int offset) throws Exception {
            T t = constructor.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            if (session0.uid == 0 && !allowNoSession) {
                log.error("no session set up");
                SMB1Reply reply = new SMB1Reply(callHeader);
                reply.setBody(EmptyReply.DEFAULT);
                reply.getHeader().setStatus(STATUS_BAD_LOGON_SESSION_STATE);
                return Mono.just(reply);
            }

            return function.apply(callHeader, session0, t);
        }
    }

    public static OptInfo[] smb1Opt = new OptInfo[SMB1_OPCODES.length];

    public static void initProc() {
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.cifs.api.smb1", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(Smb1Opt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if (param[0] == SMB1Header.class && param[1] == Session.class) {
                            if (SMB1Body.class.isAssignableFrom(param[2])) {
                                if (m.getAnnotation(Smb1Opt.class).value() != SMB_NEGPROT) {
                                    return false;
                                }
                                return true;

                            }
                        }
                    }

                    log.info("fail init method {} from", m);
                    return false;
                }).subscribe(m -> {
                    try {
                        Smb1Opt opt = m.getAnnotation(Smb1Opt.class);
                        OptInfo info = new OptInfo(m.getParameterTypes()[2]);
                        info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        info.bufSize = opt.buf();
                        info.allowNoSession = opt.allowNoSession();
                        smb1Opt[opt.value().opcode & 0xff] = info;
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init smb1 opt num {}", succ.get());

    }
}
