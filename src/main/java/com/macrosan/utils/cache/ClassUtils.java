package com.macrosan.utils.cache;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.functional.Function2;
import com.macrosan.utils.functional.Function3;
import com.macrosan.utils.functional.ImmutableTuple;
import io.reactivex.annotations.Nullable;
import io.vertx.reactivex.core.http.HttpServerRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Flux;

import java.io.File;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.jar.JarFile;

import static com.macrosan.utils.functional.exception.ThrowingFunction.throwingFunctionWrapper;
import static com.macrosan.utils.functional.exception.ThrowingSupplier.throwingSupplierWrapper;

/**
 * ClassUtils
 * 提供扫描包下面的类以及类中成员方法的方法
 * 初步采用了流式计算
 * 注：目前只能在main目录下正常使用，在test目录下使用会找不到包地址,且非线程安全
 *
 * @author liyixin
 * @date 2018/11/1
 */
public class ClassUtils {

    private static final Logger logger = LogManager.getLogger(ClassUtils.class.getName());

    /**
     * 固定返回值类型
     */
    private static final FastList<Class<?>> RETURN_TYPE_LIST = FastList.newListWith(ResponseMsg.class, int.class);

    /**
     * 参数类型数组
     */
    private static final Class<?>[] PARAM_TYPE_ARRAY_1 = {UnifiedMap.class};
    private static final Class<?>[] PARAM_TYPE_ARRAY_2 = {Map.class};
    private static final Class<?>[] PARAM_TYPE_ARRAY_3 = {HttpServerRequest.class};
    private static final Class<?>[] PARAM_TYPE_ARRAY_4 = {MsHttpRequest.class};

    private static final String FILE = "file";
    private static final String JAR = "jar";
    private static final String SERVICE = "Service.class";

    private ClassUtils() {
    }

    /**
     * 根据包名拿到包的绝对地址
     *
     * @param packageName 完整包名
     * @return 包所在的绝对地址
     */
    private static String getPackagePath(String packageName) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource(packageName.replace('.', '/'));
        return Optional.ofNullable(url)
                .map(URL::getPath)
                .orElseGet(() -> {
                    logger.error("No package Found");
                    return "null";
                });
    }

    /**
     * 扫描指定Jar包以给定字符串结尾的类，工作在JAR包中
     * <p>
     * 该方法为惰性计算，调用的时候不会立刻扫描
     *
     * @param packageName 完整包名
     * @param endStr      结尾字符串
     * @return 包含类的cold stream
     */
    private static Flux<? extends Class<?>> getClassFluxInJar(String packageName, String endStr) {
        /* 感叹号前面是jar包所在的位置，后面是要扫描的包 */
        String path = getPackagePath(packageName).split("!")[0];
        final String jarPath = path.substring(path.indexOf('/'));

        return Flux.fromStream(throwingSupplierWrapper(() -> new JarFile(jarPath).stream()))
                .filter(jarEntry -> {
                    String name = packageName.replace('.', '/') + '/';
                    String jarName = jarEntry.getName();
                    return jarName.startsWith(name) && !jarName.equals(name) && jarName.endsWith(endStr);
                })
                .map(throwingFunctionWrapper(jarEntry -> {
                    String name = jarEntry.getName();
                    /* 此处不能尾调用优化 */
                    name = name.replace('/', '.').substring(0, name.lastIndexOf('.'));
                    return Class.forName(name);
                }))
                .onErrorContinue((e, s) -> logger.error(s + "is not found"));
    }

    /**
     * 扫描指定目录中以给定字符串结尾的类，工作在IDE环境下
     * <p>
     * 该方法为惰性计算，调用的时候不会立刻扫描
     *
     * @param packageName 完整包名
     * @param endStr      结尾字符串
     * @return 包含类的cold stream
     */
    @Nullable
    private static Flux<? extends Class<?>> getClassFluxInDir(String packageName, String endStr) {
        return Flux
                .fromStream(() -> {
                    File packageDir = new File(getPackagePath(packageName));
                    if (packageDir.isDirectory()) {
                        String[] classStrArray = packageDir.list((dir, name) -> name.endsWith(endStr));
                        return Optional.ofNullable(classStrArray)
                                .map(Arrays::stream)
                                .orElseGet(() -> {
                                    logger.error("Connot found class");
                                    return Arrays.stream(new String[0]);
                                });
                    }
                    logger.error("Connot found dir");
                    return Arrays.stream(new String[0]);
                })
                .map(throwingFunctionWrapper(s -> {
                    StringBuilder builder = new StringBuilder(packageName);
                    /* 附上包名并且删除末尾的.class */
                    builder.append('.').append(s).delete(builder.length() - 6, builder.length());
                    return Class.forName(builder.toString());
                }))
                .onErrorContinue((e, s) -> logger.error(s + "is not found", e));
    }

    /**
     * 扫描指定目录中以给定字符串结尾的类，适配IDE环境和JAR包
     * <p>
     * 该方法为惰性计算，调用的时候不会立刻扫描
     *
     * @param packageName 完整包名
     * @param endStr      结尾字符串
     * @return 包含类的cold stream
     */
    public static Flux<? extends Class<?>> getClassFlux(String packageName, String endStr) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource(packageName.replace('.', '/'));
        Flux<? extends Class<?>> flux = Flux.fromArray(new Class<?>[0]);
        if (url == null) {
            logger.error("No package Found");
            return flux;
        }
        if (FILE.equals(url.getProtocol())) {
            flux = getClassFluxInDir(packageName, endStr);
        } else if (JAR.equals(url.getProtocol())) {
            flux = getClassFluxInJar(packageName, endStr);
        }
        return flux;
    }

    /**
     * 扫描指定目录中以Service结尾的类中所有的方法
     * <p>
     * TODO 还能用并行流加速初始化
     *
     * @param packageName 完整包名
     * @return 类和方法集合的映射
     */
    public static <T, R> IntObjectHashMap<Function<T, R>> getAllMethod(String packageName) {
        IntObjectHashMap<Function<T, R>> resMap = new IntObjectHashMap<>();
        /* 用flatMap生成另一个流 */
        getClassFlux(packageName, SERVICE)
                .flatMap(cls -> Flux
                        .fromArray(cls.getMethods())
                        .filter(m -> RETURN_TYPE_LIST.contains(m.getReturnType()) &&
                                (Arrays.equals(m.getParameterTypes(), PARAM_TYPE_ARRAY_1) ||
                                        Arrays.equals(m.getParameterTypes(), PARAM_TYPE_ARRAY_2) ||
                                        Arrays.equals(m.getParameterTypes(), PARAM_TYPE_ARRAY_3) ||
                                        Arrays.equals(m.getParameterTypes(), PARAM_TYPE_ARRAY_4)))
                        .map(m -> new ImmutableTuple<String, Function<T, R>>
                                (getSignature(cls, m.getName()), generateLambda(cls, m))))
                .subscribe(tuple -> resMap.put(tuple.var1.hashCode(), tuple.var2));
        logger.info("Get " + resMap.size() + " method in " + packageName);

        return resMap;
    }

    public static <T, R> IntObjectHashMap<Function<T, R>> getStsMethod(String packageName) {
        IntObjectHashMap<Function<T, R>> resMap = new IntObjectHashMap<>();
        /* 用flatMap生成另一个流 */
        getClassFlux(packageName, "RoleService.class")
                .flatMap(cls -> Flux
                        .fromArray(cls.getMethods())
                        .filter(m -> "assumeRoleDouble".equals(m.getName()))
                        .map(m -> new ImmutableTuple<String, Function<T, R>>
                                (getSignature(cls, m.getName()), generateLambda(cls, m))))
                .subscribe(tuple -> {
                    logger.info("key: {}, value: {}", tuple.var1.hashCode(), tuple.var2);
                    resMap.put(tuple.var1.hashCode(), tuple.var2);
                });

        return resMap;
    }

    /**
     * 获得指定类的实例
     *
     * @param cls 需要实例化的类
     * @return 类的实例
     */
    @Nullable
    private static Object getInstance(Class<?> cls) {
        try {
            Method getInstanceMethod = cls.getDeclaredMethod("getInstance");
            return getInstanceMethod.invoke(null);
        } catch (Exception e) {
            logger.error("get instance fail, {} cause :", cls.getName(), e);
            return null;
        }
    }

    /**
     * 根据类和方法名动态生成Function接口实现类
     *
     * @param cls    方法所在的类
     * @param method 方法名称
     * @return Function接口实现类
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T, R> Function<T, R> generateLambda(Class<?> cls, Method method) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType type = MethodType.methodType(method.getReturnType(), method.getParameterTypes());

        try {
            MethodHandle mh = lookup.findVirtual(cls, method.getName(), type);
            /* TODO 这里的反射会被多次调用，但是找的是同一个对象，有待优化 */
            Object instance = getInstance(cls);
            if (instance == null) {
                return null;
            }

            MethodHandle factory = LambdaMetafactory.metafactory(
                    lookup, "apply", MethodType.methodType(Function.class, cls),
                    type.generic(), mh, type).getTarget();

            /* invokeExact有严格的参数匹配，不能从Object转换到具体类型，应该使用invoke,或者在调用invokeExact之前bindTo */
            return (Function<T, R>) factory.bindTo(instance).invokeExact();
        } catch (Throwable e) {
            logger.error("get Function fail, cause :", e);
            return null;
        }
    }

    public static <T1, T2, R> Function2<T1, T2, R> generateFunction2(Class<?> cls, Method method) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType type = MethodType.methodType(method.getReturnType(), method.getParameterTypes());

        try {
            MethodHandle mh = lookup.findVirtual(cls, method.getName(), type);
            /* TODO 这里的反射会被多次调用，但是找的是同一个对象，有待优化 */
            Object instance = getInstance(cls);
            if (instance == null) {
                return null;
            }

            MethodHandle factory = LambdaMetafactory.metafactory(
                    lookup, "apply", MethodType.methodType(Function2.class, cls),
                    type.generic(), mh, type).getTarget();

            /* invokeExact有严格的参数匹配，不能从Object转换到具体类型，应该使用invoke,或者在调用invokeExact之前bindTo */
            return (Function2<T1, T2, R>) factory.bindTo(instance).invokeExact();
        } catch (Throwable e) {
            logger.error("get Function fail, cause :", e);
            return null;
        }
    }

    public static <T1, T2, T3, R> Function3<T1, T2, T3, R> generateFunction3(Class<?> cls, Method method) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType type = MethodType.methodType(method.getReturnType(), method.getParameterTypes());

        try {
            MethodHandle mh = lookup.findVirtual(cls, method.getName(), type);
            /* TODO 这里的反射会被多次调用，但是找的是同一个对象，有待优化 */
            Object instance = getInstance(cls);
            if (instance == null) {
                return null;
            }

            MethodHandle factory = LambdaMetafactory.metafactory(
                    lookup, "apply", MethodType.methodType(Function3.class, cls),
                    type.generic(), mh, type).getTarget();

            /* invokeExact有严格的参数匹配，不能从Object转换到具体类型，应该使用invoke,或者在调用invokeExact之前bindTo */
            return (Function3<T1, T2, T3, R>) factory.bindTo(instance).invokeExact();
        } catch (Throwable e) {
            logger.error("get Function fail, cause :", e);
            return null;
        }
    }

    /**
     * 将类和方法名拼接成唯一确定一个lambda的签名
     *
     * @param cls    方法所在的类
     * @param method 方法名
     * @return 签名（classname-methodname）
     */
    private static String getSignature(Class<?> cls, String method) {
        return cls.getSimpleName() + '-' + method;
    }

}
