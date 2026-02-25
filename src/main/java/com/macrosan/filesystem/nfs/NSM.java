package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.lock.NSMServer;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function3;
import io.netty.buffer.ByteBuf;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class NSM {
    public static Vertx vertx;

    public static Map<String, Integer> NSMPortMap = new ConcurrentHashMap<>(); // ip, NSM端口

    public enum Opcode {
        SM_NULL(0),
        SM_STAT(1),
        SM_MON(2),
        SM_UNMON(3),
        SM_UNMON_ALL(4),
        SM_SIMU_CRASH(5),
        SM_NOTIFY(6);

        public final int opcode;

        Opcode(int opcode) {
            this.opcode = opcode;
        }
    }

    public static final NSM.Opcode[] values = NSM.Opcode.values();

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Opt {
        NSM.Opcode value();
        int buf() default 4096;
    }

    public static class OptInfo<T extends ReadStruct> {
        public Function3<RpcCallHeader, ReqInfo, T, Mono<RpcReply>> function;
        public Function3<RpcReplyHeader, ReqInfo, T, Mono<Boolean>> function2;
        public Constructor<T> constructor;
        public Constructor<T> constructor2;
        public int bufSize;

        public void setConstructor(Class<T> tClass) throws NoSuchMethodException {
            this.constructor = tClass.getDeclaredConstructor();
            constructor.setAccessible(true);
        }

        public void setConstructor2(Class<T> tClass)  throws NoSuchMethodException  {
            this.constructor2 = tClass.getDeclaredConstructor();
            constructor2.setAccessible(true);
        }

        OptInfo() {}

        public Mono<RpcReply> run(RpcCallHeader callHeader, ReqInfo reqHeader, ByteBuf buf, int offset) throws Exception {
            T t = constructor.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            return function.apply(callHeader, reqHeader, t);
        }

        public Mono<Boolean> run(RpcReplyHeader replyHeader, ReqInfo reqHeader, ByteBuf buf, int offset) throws Exception {
            T t = constructor2.newInstance();
            if (t.readStruct(buf, offset) < 0) {
                return null;
            }

            return function2.apply(replyHeader, reqHeader, t);
        }
    }

    public static OptInfo[] NSMOpt = new OptInfo[NSM.values.length];

    public static void initProc(Vertx vertx0) {
        vertx = vertx0;
        AtomicInteger succ = new AtomicInteger();
        ClassUtils.getClassFlux("com.macrosan.filesystem.nfs.api", ".class")
                .flatMap(cl -> Flux.fromArray(cl.getDeclaredMethods()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getAnnotation(Opt.class) != null)
                .filter(m -> {
                    Class[] param = m.getParameterTypes();
                    if (param.length == 3) {
                        if ((param[0] == RpcCallHeader.class || param[0] == RpcReplyHeader.class) && param[1] == ReqInfo.class) {
                            if (ReadStruct.class.isAssignableFrom(param[2])) {
                                return true;
                            }
                        }
                    }

                    log.info("NSM fail init method {}", m);
                    return false;
                }).subscribe(m -> {
                    try {
                        Class[] param = m.getParameterTypes();

                        Opt opt = m.getAnnotation(Opt.class);
                        OptInfo info = new OptInfo();
                        if (NSMOpt[opt.value().opcode] != null) {
                            info = NSMOpt[opt.value().opcode];
                        }

                        if (param[0] == RpcCallHeader.class) {
                            info.setConstructor(m.getParameterTypes()[2]);
                            info.function = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        } else {
                            info.setConstructor2(m.getParameterTypes()[2]);
                            info.function2 = ClassUtils.generateFunction3(m.getDeclaringClass(), m);
                        }
                        info.bufSize = opt.buf();
                        NSMOpt[opt.value().opcode] = info;
                        succ.incrementAndGet();
                    } catch (Exception e) {
                        log.info("NSM fail init method {}", m, e);
                    }
                });
        log.info("NSM success init nsm opt num {}", succ.get());

        NSMServer.init();
    }
}
