package com.macrosan.ec.error;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.error.ErrorConstant.ECErrorType;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.DefaultParameterNameDiscoverer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

/**
 * @author gaozhiyuan
 * 缓存错误处理函数
 */
@Log4j2

public class ErrorFunctionCache {
    /**
     * 在一些异常处理handler中，METHOD_RETURN_TYPE为true或false不表示异常处理本身是否成功。
     * 因为一些client中的方法返回true只表示成功响应>=k (或者去调用别的接口)，并不表示没有错误。很可能磁盘并未可用，消息本身并没有达到预期的修复效果。这种情况下，handler恒返回false。
     * 在一般队列消费时不论true或false均可ack。缓冲队列中若为true，表示磁盘状态已确定正常；false，表示该消息消费时磁盘状态不一定正常，需要延迟消费。
     * 当一条消息消费时确定相关的盘已经可用，才允许返回true。
     */
    private static final String METHOD_RETURN_TYPE = "reactor.core.publisher.Mono<java.lang.Boolean>";
    private static Map<ECErrorType, Function<SocketReqMsg, Mono<Boolean>>> cache = new HashMap<>();
    private static final DefaultParameterNameDiscoverer PARAMETER_NAME_DISCOVERER = new DefaultParameterNameDiscoverer();
    private static long TIMEOUT = 20_000L;

    public static final TypeReference<List<Tuple3<String, String, String>>> NODE_LIST_TYPE_REFERENCE =
            new TypeReference<List<Tuple3<String, String, String>>>() {
            };

    private static final TypeReference<List<Integer>> INTEGER_LIST_TYPE_REFERENCE =
            new TypeReference<List<Integer>>() {
            };

    private static final TypeReference<List<PartInfo>> PARTINFO_LIST_TYPE_REFERENCE =
            new TypeReference<List<PartInfo>>() {
            };

    private static final TypeReference<Map<String, String>> POOL_PREFIX_MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, String>>() {
            };
    public static final TypeReference<List<Tuple2<String, Tuple3<String, String, String>>>> SNAPSHOT_NODE_LIST_TYPE_REFERENCE =
            new TypeReference<List<Tuple2<String, Tuple3<String, String, String>>>>() {
            };

    private static Function<Object[], String> getDefaultKeyFunction(String[] parameterTypes, String[] parameterNames) {
        int bucketIndex = -1;
        int objectIndex = -1;
        int uploadIdIndex = -1;

        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i].equals(String.class.getName())) {
                if (parameterNames[i].contains("bucket")) {
                    bucketIndex = i;
                } else if (parameterNames[i].contains("object")) {
                    objectIndex = i;
                } else if (parameterNames[i].contains("uploadId")) {
                    uploadIdIndex = i;
                }
            }
        }

        if (bucketIndex >= 0 && objectIndex >= 0) {
            if (uploadIdIndex >= 0) {
                int index1 = bucketIndex;
                int index2 = objectIndex;
                int index3 = uploadIdIndex;

                return args -> args[index1] + ((String) args[index2]) + args[index3];
            } else {
                int index1 = bucketIndex;
                int index2 = objectIndex;
                return args -> args[index1] + ((String) args[index2]);
            }
        }

        return null;
    }

    /**
     * getParametersFunction:
     *
     * @param method 异常处理的handler
     * @return [getParametersFunction, keyFunction]
     */
    private static Tuple2<Function<SocketReqMsg, Object[]>, Function<Object[], String>> getFunctions(Method method) {
        String[] parameterNames = PARAMETER_NAME_DISCOVERER.getParameterNames(method);
        String[] parameterTypes = Arrays.stream(method.getGenericParameterTypes()).map(Type::getTypeName).toArray(String[]::new);
        //从消息中的msg获取参数所使用的方法，按参数顺序依次保存在数组中
        Function<SocketReqMsg, Object>[] parameterFunctions = new Function[parameterNames.length];
        //从消息提取出指定信息拼接为key的方法，用作该条消息的标识。用来保证同一个对象在同一时间只有一条相关消息在处理。<errorHandler参数集合，key>
        Function<Object[], String> keyFunction = null;

        for (int i = 0; i < parameterFunctions.length; i++) {
            int index = i;

            if (parameterTypes[i].equals(String.class.getTypeName())) {
                parameterFunctions[i] = msg -> msg.get(parameterNames[index]);
            } else if (parameterTypes[i].equals(String[].class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, String[].class);
                };
            } else if (parameterTypes[i].equals(MetaData.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, MetaData.class);
                };

                keyFunction = args -> {
                    MetaData metaData = (MetaData) args[index];
                    return metaData.bucket + metaData.key;
                };
            } else if (parameterTypes[i].equals(InitPartInfo.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, InitPartInfo.class);
                };

                keyFunction = args -> {
                    InitPartInfo initPartInfo = (InitPartInfo) args[index];
                    return initPartInfo.bucket + initPartInfo.object + initPartInfo.uploadId;
                };
            } else if (parameterTypes[i].equals(PartInfo.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, PartInfo.class);
                };

                keyFunction = args -> {
                    PartInfo partInfo = (PartInfo) args[index];
                    return partInfo.bucket + partInfo.object + partInfo.uploadId;
                };
            } else if (parameterTypes[i].equals(NODE_LIST_TYPE_REFERENCE.getType().getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, NODE_LIST_TYPE_REFERENCE);
                };
            } else if (parameterTypes[i].equals(INTEGER_LIST_TYPE_REFERENCE.getType().getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    if (str == null) {
                        str = "[]";

                    }
                    return Json.decodeValue(str, INTEGER_LIST_TYPE_REFERENCE);
                };
            } else if (parameterTypes[i].equals(PARTINFO_LIST_TYPE_REFERENCE.getType().getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, PARTINFO_LIST_TYPE_REFERENCE);
                };
            } else if (parameterTypes[i].equals(POOL_PREFIX_MAP_TYPE_REFERENCE.getType().getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, POOL_PREFIX_MAP_TYPE_REFERENCE);
                };
            } else if (parameterTypes[i].equals(SocketReqMsg.class.getTypeName())) {
                parameterFunctions[i] = msg -> msg;
            } else if (parameterTypes[i].equals(BucketInfo.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, BucketInfo.class);
                };

                keyFunction = args -> {
                    BucketInfo bucketInfo = (BucketInfo) args[index];
                    return bucketInfo.bucketName;
                };
            } else if (parameterTypes[i].equals(EsMeta.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, EsMeta.class);
                };

                keyFunction = args -> {
                    EsMeta esMeta = (EsMeta) args[index];
                    return esMeta.bucketName + esMeta.objName + esMeta.versionId;
                };
            } else if (parameterTypes[i].equals(UnSynchronizedRecord.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, UnSynchronizedRecord.class);
                };
                keyFunction = args -> {
                    UnSynchronizedRecord record = (UnSynchronizedRecord) args[index];
                    return record.bucket + record.object;
                };
            } else if (parameterTypes[i].equals(DedupMeta.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, DedupMeta.class);
                };

                keyFunction = args -> {
                    DedupMeta dedupMeta = (DedupMeta) args[index];
                    return dedupMeta.etag + dedupMeta.storage;
                };

            } else if (parameterTypes[i].equals(Boolean.class.getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Boolean.parseBoolean(str);
                };
            } else if (parameterTypes[i].equals(SNAPSHOT_NODE_LIST_TYPE_REFERENCE.getType().getTypeName())) {
                parameterFunctions[i] = msg -> {
                    String str = msg.get(parameterNames[index]);
                    return Json.decodeValue(str, SNAPSHOT_NODE_LIST_TYPE_REFERENCE);
                };
            } else {
                log.error("load error function {} fail. parameter type {} limit exceeded", method, parameterTypes);
            }
        }

        if (keyFunction == null) {
            keyFunction = getDefaultKeyFunction(parameterTypes, parameterNames);
        }

        //生成各个errorHandler需要的参数。<消息中的SocketReqMsg, 根据msg生成的用于errorHanlder的参数集合>
        Function<SocketReqMsg, Object[]> getParametersFunction = msg -> {
            Object[] objects = new Object[parameterNames.length];
            for (int i = 0; i < objects.length; i++) {
                objects[i] = parameterFunctions[i].apply(msg);
            }

            return objects;
        };

        return new Tuple2<>(getParametersFunction, keyFunction);
    }

    @AllArgsConstructor
    @Data
    @Accessors(chain = true)
    private static class Request {
        MonoProcessor<Boolean> res;
        /**
         * 每个消息的标识符。相同的消息key一致。一个消费者同时处理的消息中不能有相同的key
         */
        String key;
        MethodHandle mh;
        Object[] args;
        boolean end;
    }

    private static Set<String> set = new HashSet<>();
    private static UnicastProcessor<Request> flux = UnicastProcessor.create(Queues.<Request>unboundedMultiproducer().get());

    public static void init() {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        cache.clear();

        flux.subscribe(r -> {
            try {
                if (r.end) {
                    set.remove(r.key);
                } else {
                    if (set.contains(r.key)) {
                        Mono.delay(Duration.ofMillis(TIMEOUT)).subscribe(l -> {
                            r.res.onError(new MsException(ErrorNo.REPEATED_KEY, "only allow one error handler in same object"));
                        });
                    } else {
                        set.add(r.key);
                        Mono<Boolean> res;
                        //实际处理位置
                        try {
                            res = (Mono<Boolean>) r.mh.invokeWithArguments(r.args);
                        } catch (Throwable e) {
                            log.error("deal error", e);
                            res = Mono.just(false);
                        }

                        res.doOnError(r.res::onError).subscribe(b -> {
                            r.res.onNext(b);
                            flux.onNext(r.setEnd(true));
                        });
                    }
                }
            } catch (Exception e) {
                log.error("", e);
            }
        });

        ClassUtils.getClassFlux(ErrorFunctionCache.class.getPackage().getName(), ".class")
                .flatMap(cls -> Flux.fromArray(cls.getDeclaredMethods())
                        .filter(method -> method.getAnnotation(HandleErrorFunction.class) != null)
                        .filter(method -> METHOD_RETURN_TYPE.equals(method.getGenericReturnType().getTypeName()))
                        .filter(method -> Modifier.isStatic(method.getModifiers()))
                        .doOnNext(method -> method.setAccessible(true))
                        .map(method -> {
                            Tuple2<Function<SocketReqMsg, Object[]>, Function<Object[], String>> tuple2 = getFunctions(method);

                            HandleErrorFunction annotation = method.getAnnotation(HandleErrorFunction.class);
                            MethodType type = MethodType.methodType(method.getReturnType(), method.getParameterTypes());
                            try {
                                MethodHandle mh = lookup.findStatic(cls, method.getName(), type);
                                Function<SocketReqMsg, Mono<Boolean>> function;
                                if (tuple2.var2 == null) {
                                    function = msg -> {
                                        Mono<Boolean> res;
                                        try {
                                            Object[] args = tuple2.var1.apply(msg);
                                            res = (Mono<Boolean>) mh.invokeWithArguments(args);
                                        } catch (Throwable e) {
                                            if (e instanceof MsException && e.getMessage().contains("set link lock fail")) {
                                                log.debug("", e);
                                            } else {
                                                log.error("", e);
                                            }
                                            res = Mono.just(false);
                                        }

                                        return res;
                                    };
                                } else {
                                    //新建一个流处理重复的消息
                                    function = msg -> {
                                        MonoProcessor<Boolean> res = MonoProcessor.create();
                                        Object[] args = tuple2.var1.apply(msg);
                                        String key = tuple2.var2.apply(args);

                                        Request request = new Request(res, key, mh, args, false);
                                        flux.onNext(request);

                                        return res.doOnError(e -> flux.onNext(request.setEnd(true)));
                                    };
                                }


                                //timeout的处理
                                if (annotation.timeout() > 0L) {
                                    cache.put(annotation.value(), msg -> function.apply(msg).timeout(Duration.ofMillis(annotation.timeout())));
                                } else {
                                    cache.put(annotation.value(), function);
                                }
                            } catch (Throwable e) {
                                log.error("get Function {} fail", method, e);
                            }

                            return method;
                        })
                ).subscribe(s -> {
                }, e -> {
                }, () -> log.info("Get {} EC error handler method", cache.size()));
    }

    public static Function<SocketReqMsg, Mono<Boolean>> get(ECErrorType type) {
        return cache.get(type);
    }
}
