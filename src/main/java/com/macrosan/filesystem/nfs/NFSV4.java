package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function3;
import io.netty.buffer.ByteBuf;
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

@Log4j2
public class NFSV4 {
    public enum Opcode {
        NFS4PROC_NULL(0),
        NFS4PROC_COMPOUND(1),
        NFS4PROC_NONE(2),//补位
        NFS4PROC_ACCESS(3),
        NFS4PROC_CLOSE(4),
        NFS4PROC_COMMIT(5),
        NFS4PROC_CREATE(6),
        NFS4PROC_DELEGPURGE(7),
        NFS4PROC_DELEGRETURN(8),
        NFS4PROC_GETATTR(9),
        NFS4PROC_GETFH(10),
        NFS4PROC_LINK(11),
        NFS4PROC_LOCK(12),
        NFS4PROC_LOCKT(13),
        NFS4PROC_LOCKU(14),
        NFS4PROC_LOOKUP(15),
        NFS4PROC_LOOKUPP(16),
        NFS4PROC_NVERIFY(17),
        NFS4PROC_OPEN(18),
        NFS4PROC_OPENATTR(19),
        NFS4PROC_OPEN_CONFIRM(20),
        NFS4PROC_OPEN_DOWNGRADE(21),
        NFS4PROC_PUTFH(22),
        NFS4PROC_PUTPUBFH(23),
        NFS4PROC_PUTROOTFH(24),
        NFS4PROC_READ(25),
        NFS4PROC_READDIR(26),
        NFS4PROC_READLINK(27),
        NFS4PROC_REMOVE(28),
        NFS4PROC_RENAME(29),
        NFS4PROC_RENEW(30),
        NFS4PROC_RESTOREFH(31),
        NFS4PROC_SAVEFH(32),
        NFS4PROC_SECINFO(33),
        NFS4PROC_SETATTR(34),
        NFS4PROC_SETCLIENTID(35),
        NFS4PROC_SETCLIENTID_CONFIRM(36),
        NFS4PROC_VERIFY(37),
        NFS4PROC_WRITE(38),
        NFS4PROC_RELEASE_LOCKOWNER(39),
        NFS4PROC_BACKCHANNEL_CTL(40),
        NFS4PROC_BIND_CONN_TO_SESSION(41),
        NFS4PROC_EXCHANGE_ID(42),
        NFS4PROC_CREATE_SESSION(43),
        NFS4PROC_DESTROY_SESSION(44),
        NFS4PROC_FREE_STATEID(45),
        NFS4PROC_GET_DIR_DELEGATION(46),
        NFS4PROC_GETDEVICEINFO(47),
        NFS4PROC_GETDEVICELIST(48),
        NFS4PROC_LAYOUTCOMMIT(49),
        NFS4PROC_LAYOUTGET(50),
        NFS4PROC_LAYOUTRETURN(51),
        NFS4PROC_SECINFO_NO_NAME(52),
        NFS4PROC_SEQUENCE(53),
        NFS4PROC_SET_SSV(54),
        NFS4PROC_TEST_STATEID(55),
        NFS4PROC_WANT_DELEGATION(56),
        NFS4PROC_DESTROY_CLIENTID(57),
        NFS4PROC_RECLAIM_COMPLETE(58),
        NFS4PROC_ALLOCATE(59),
        NFS4PROC_COPY(60),
        NFS4PROC_COPY_NOTIFY(61),
        NFS4PROC_DEALLOCATE(62),
        NFS4PROC_IO_ADVISE(63),
        NFS4PROC_LAYOUTERROR(64),
        NFS4PROC_LAYOUTSTATS(65),
        NFS4PROC_OFFLOAD_CANCEL(66),
        NFS4PROC_OFFLOAD_STATUS(67),
        NFS4PROC_READ_PLUS(68),
        NFS4PROC_SEEK(69),
        NFS4PROC_WRITE_SAME(70),
        NFS4PROC_CLONE(71);

        public final int opcode;

        Opcode(int opcode) {
            this.opcode = opcode;
        }
    }

    public enum CBOpcode{
        NFS4PROC_CB_NULL(0),
        NFS4PROC_CB_COMPOUND(1),
        NFS4PROC_CB_NONE(2),//占位
        NFS4PROC_CB_GETATTR(3),
        NFS4PROC_CB_RECALL(4),
        NFS4PROC_CB_LAYOUTRECALL(5),
        NFS4PROC_CB_NOTIFY(6),
        NFS4PROC_CB_PUSH_DELEG(7),
        NFS4PROC_CB_RECALL_ANY(8),
        NFS4PROC_CB_RECALLABLE_OBJ_AVAIL(9),
        NFS4PROC_CB_RECALL_SLOT(10),
        NFS4PROC_CB_SEQUENCE(11),
        NFS4PROC_CB_WANTS_CANCELLED(12),
        NFS4PROC_CB_NOTIFY_LOCK(13),
        NFS4PROC_CB_NOTIFY_DEVICEID(14),
        NFS4PROC_CB_ILLEGAL(10044);

        public final int opcode;

        CBOpcode(int opcode) {
            this.opcode = opcode;
        }
    }

    public static final Opcode[] values = Opcode.values();
    public static final CBOpcode[] CBValues = CBOpcode.values();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Opt {
        Opcode value();

        int buf() default 4096;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Opt0 {
        CBOpcode value();

        int buf() default 4096;
    }

    public static class OptInfo<T extends ReadStruct> {
        public Function3<RpcCallHeader, ReqInfo, T, Mono<RpcReply>> function;
        public Function3<RpcReplyHeader, ReqInfo, T, Mono<Object>> function0;
        public Constructor<T> constructor;
        public Constructor<T> constructor0;
        public int bufSize;

//        OptInfo(Class<T> tClass) throws NoSuchMethodException {
//            constructor = tClass.getDeclaredConstructor();
//            constructor.setAccessible(true);
//        }

        public void setConstructor(Class<T> tClass) throws NoSuchMethodException{
            this.constructor = tClass.getDeclaredConstructor();
            constructor.setAccessible(true);
        }

        public void setConstructor0(Class<T> tClass) throws NoSuchMethodException{
            this.constructor0 = tClass.getDeclaredConstructor(SunRpcHeader.class);
            constructor0.setAccessible(true);
        }

        public Mono<RpcReply> run(RpcCallHeader callHeader, ReqInfo ReqInfo, ByteBuf buf, int offset) throws Exception {
            T t = constructor.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            return function.apply(callHeader, ReqInfo, t);
        }

        public Mono<Object> run(RpcReplyHeader replyHeader, ReqInfo ReqInfo, ByteBuf buf, int offset) throws Exception {
            T t = constructor.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            return function0.apply(replyHeader, ReqInfo, t);
        }
    }

    public static OptInfo[] v4Opt = new OptInfo[NFSV4.values.length];
    public static OptInfo[] v4CBOpt = new OptInfo[NFSV4.CBValues.length];

    public static void initProc() {
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.nfs.api", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(Opt.class) != null || m.getAnnotation(Opt0.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if ((param[0] == RpcCallHeader.class || param[0] == RpcReplyHeader.class) && param[1] == ReqInfo.class) {
                            if (ReadStruct.class.isAssignableFrom(param[2]) || RpcReply.class.isAssignableFrom(param[2])) {
                                return true;
                            }
                        }
                    }

                    log.info("fail init method {}", m);
                    return false;
                }).subscribe(m -> {
                    try {

                        Class[] param = m.getParameterTypes();
                        NFSV4.OptInfo info = new NFSV4.OptInfo();
                        if (param[0] == RpcCallHeader.class) {
                            Opt opt = m.getAnnotation(Opt.class);
                            info.setConstructor(m.getParameterTypes()[2]);
                            info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                            v4Opt[opt.value().opcode] = info;
                            info.bufSize = opt.buf();
                        }else if (param[0] == RpcReplyHeader.class){
                            Opt0 opt = m.getAnnotation(Opt0.class);
                            info.setConstructor0(m.getParameterTypes()[2]);
                            info.function0 = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                            v4CBOpt[opt.value().opcode] = info;
                            info.bufSize = opt.buf();
                        }
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init nfs v4 opt num {}", succ.get());

    }
}
