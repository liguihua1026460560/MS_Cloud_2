package com.macrosan.utils.cache;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.mqmessage.ResponseMsg;
import io.reactivex.annotations.Nullable;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

import static com.macrosan.utils.cache.ClassUtils.getAllMethod;
import static com.macrosan.utils.cache.ClassUtils.getStsMethod;

/**
 * LambdaCache
 * 缓存机制，保存实现函数式接口的lambda
 * 非线程安全的单例模式，只能在启动过程中使用
 * <p>
 * 缓存机制更新：
 * 之前是方法签名作为key，lambda作为value，这样需要先从routeMap中根据sign查方法签名，再从lambdaMap中
 * 根据签名查value。现在则在初始化时就建立好sign和lambda的映射关系，即可减少一次查询同时降低内存占用
 *
 * @author liyixin
 * @date 2018/11/2
 */
public class LambdaCache {

    private static LambdaCache instance = null;

    private static final String MANAGE_PACKAGE_NAME = "com.macrosan.action.managestream";

    private static final String DATA_PACKAGE_NAME = "com.macrosan.action.datastream";

    private static IntObjectHashMap<Function<UnifiedMap<String, String>, ResponseMsg>> manageLambdaMap;

    private static IntObjectHashMap<Function<? super HttpServerRequest, Integer>> dataLambdaMap;

    protected static IntObjectHashMap<Function<UnifiedMap<String, String>, Mono<ResponseMsg>>> stsLambdaMap;

    /**
     * LambdaCache的构造器
     * 生成实例的时候扫描指定包并且填充lambdaMap
     */
    private LambdaCache() {
    }

    public static void initCache() {
        instance = new LambdaCache();
        manageLambdaMap = new IntObjectHashMap<>(32);
        dataLambdaMap = new IntObjectHashMap<>(16);
        stsLambdaMap = new IntObjectHashMap<>(2);
        ServerConfig config = ServerConfig.getInstance();

        IntIntHashMap routeMap = config.getManageRoute();
        IntObjectHashMap<Function<UnifiedMap<String, String>, ResponseMsg>> manageMethodMap = getAllMethod(MANAGE_PACKAGE_NAME);
        routeMap.forEachKeyValue((key, value) -> manageLambdaMap.put(key, manageMethodMap.get(value)));

        routeMap = config.getDataRoute();
        IntObjectHashMap<Function<HttpServerRequest, Integer>> dataMethodMap = getAllMethod(DATA_PACKAGE_NAME);
        routeMap.forEachKeyValue((key, value) -> dataLambdaMap.put(key, dataMethodMap.get(value)));

        IntObjectHashMap<Function<UnifiedMap<String, String>, Mono<ResponseMsg>>> stsMethodMap = getStsMethod(MANAGE_PACKAGE_NAME);
        stsMethodMap.values().forEach(value -> stsLambdaMap.put("POST/?Action=AssumeRole".hashCode(), value));
    }

    public static LambdaCache getInstance() {
        return Optional
                .ofNullable(instance)
                .orElseGet(LambdaCache::new);
    }

    /**
     * 根据请求签名拿到管理流lambda
     *
     * @param sign 请求的签名
     * @return 被缓存的lambda
     */
    @Nullable
    public Function<UnifiedMap<String, String>, ResponseMsg> getManageLambda(int sign) {
        return manageLambdaMap.get(sign);
    }

    /**
     * 根据请求签名拿到数据流lambda
     *
     * @param sign 请求的签名
     * @return 被缓存的lambda
     */
    public Function<? super HttpServerRequest, Integer> getDataLambda(int sign) {
        return dataLambdaMap.get(sign);
    }

    @Nullable
    public Function<UnifiedMap<String, String>, Mono<ResponseMsg>> getStsLambda(int sign) {
        return stsLambdaMap.get(sign);
    }

}
