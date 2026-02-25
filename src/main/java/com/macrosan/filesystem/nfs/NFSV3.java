package com.macrosan.filesystem.nfs;

import com.macrosan.database.redis.RedisConnPool;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

@Log4j2
public class NFSV3 {
    public enum Opcode {
        NFS3PROC_NULL(0),
        NFS3PROC_GETATTR(1),
        NFS3PROC_SETATTR(2),
        NFS3PROC_LOOKUP(3),
        NFS3PROC_ACCESS(4),
        NFS3PROC_READLINK(5),
        NFS3PROC_READ(6),
        NFS3PROC_WRITE(7),
        NFS3PROC_CREATE(8),
        NFS3PROC_MKDIR(9),
        NFS3PROC_SYMLINK(10),
        NFS3PROC_MKNOD(11),
        NFS3PROC_REMOVE(12),
        NFS3PROC_RMDIR(13),
        NFS3PROC_RENAME(14),
        NFS3PROC_LINK(15),
        NFS3PROC_READDIR(16),
        NFS3PROC_READDIRPLUS(17),
        NFS3PROC_FSSTAT(18),
        NFS3PROC_FSINFO(19),
        NFS3PROC_PATHCONF(20),
        NFS3PROC_COMMIT(21);

        public final int opcode;

        Opcode(int opcode) {
            this.opcode = opcode;
        }
    }

    public static final Opcode[] values = Opcode.values();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Opt {
        Opcode value();

        int buf() default 4096;
    }

    public static class OptInfo<T extends ReadStruct> {
        public Function3<RpcCallHeader, ReqInfo, T, Mono<RpcReply>> function;
        public Constructor<T> constructor;
        public int bufSize;

        OptInfo(Class<T> tClass) throws NoSuchMethodException {
            constructor = tClass.getDeclaredConstructor();
            constructor.setAccessible(true);
        }

        public Mono<RpcReply> run(RpcCallHeader callHeader, ReqInfo reqHeader, ByteBuf buf, int offset) throws Exception {
            T t = constructor.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            return function.apply(callHeader, reqHeader, t);
        }
    }

    public static OptInfo[] v3Opt = new OptInfo[NFSV3.values.length];
    public static boolean NFS_RED_LOCK = false;
    public static Map<Integer, String> NFSV3_ACTION_MAP = new HashMap<>();

    public static void initProc() {
        try{
            // nfs_red_lock:0、未设置->关闭一致性锁；1->开启一致性锁
            String result = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("nfs_red_lock");
            if (null != result) {
                NFS_RED_LOCK = !"0".equals(result);
            }
        } catch (Exception e) {
            log.error("get 'nfs_red_lock' fail from redis", e);
        }

        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.nfs.api", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(Opt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if (param[0] == RpcCallHeader.class && param[1] == ReqInfo.class) {
                            if (ReadStruct.class.isAssignableFrom(param[2])) {
                                return true;
                            }
                        }
                    }

                    log.info("fail init method {}", m);
                    return false;
                }).subscribe(m -> {
                    try {
                        Opt opt = m.getAnnotation(Opt.class);
                        OptInfo info = new OptInfo(m.getParameterTypes()[2]);
                        info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        info.bufSize = opt.buf();
                        v3Opt[opt.value().opcode] = info;
                        String action = "nfs:" + m.getName();
                        NFSV3_ACTION_MAP.put(opt.value().opcode, action);
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("fail init method {}", m, e);
                    }
                });

        log.info("success init nfs v3 opt num {}", succ.get());
    }
}
