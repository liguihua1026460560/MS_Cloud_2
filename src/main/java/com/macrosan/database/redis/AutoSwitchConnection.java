package com.macrosan.database.redis;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * 和多个URI建立连接，在URI不可用时自动切换
 *
 * @author gaozhiyuan
 * @date 2019.10.25
 */
public class AutoSwitchConnection {
    private List<StatefulRedisConnection<String, String>> internalConnection;
    private List<RedisURI> URIList;
    private BitSet connectionStatus;
    private RedisClient client;
    private int index = 0;
    /**
     * 表示是否进行网口切换
     */
    private Boolean switchStart = false;

    /**
     * 构造方法，根据提供的URIList建立相同数量的Redis连接
     *
     * @param URIList
     */
    public AutoSwitchConnection(List<RedisURI> URIList) {
        this.URIList = URIList;
        client = RedisClient.create(VertxEventLoopProvider.resources);
        //自动切换需要禁用lettuce的自动重连
        client.setOptions(ClientOptions.builder().autoReconnect(false).timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(5)).build()).build());
        internalConnection = new ArrayList<>(URIList.size());
        connectionStatus = new BitSet(URIList.size());

        for (int i = 0; i < URIList.size(); i++) {
            internalConnection.add(null);
            if (connect(i)) {
                connectionStatus.set(i);
            }
        }
    }

    /**
     * 判断index为i的连接是否可用
     *
     * @param i 连接的index
     * @return boolean
     */
    private boolean connect(int i) {
        try {
            StatefulRedisConnection<String, String> connection = client.connect(URIList.get(i));
            internalConnection.set(i, connection);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获得下一个index，如果到头（超出connection的数量）则重新从0开始
     *
     * @return int 下一个index
     */
    private int nextIndex() {
        int i = index++;
        if (i >= internalConnection.size()) {
            index = 0;
            return 0;
        } else {
            return i;
        }
    }

    /**
     * 获得一个connectionStatus不为false的连接
     *
     * @return Tuple2类型的实例。T:int，连接在internalConnection中的index;
     */
    private Tuple2<Integer, StatefulRedisConnection<String, String>> getConnection() {
        int i = nextIndex();
        while (!connectionStatus.get(i) && !connectionStatus.isEmpty()) {
            i = nextIndex();
        }

        return new Tuple2<>(i, internalConnection.get(i));
    }

    private static final ClassLoader CLASS_LOADER = RedisReactiveCommands.class.getClassLoader();
    private static final Class<?>[] INTERFACES = new Class[]{RedisReactiveCommands.class};

    /**
     * @param commands redis连接
     * @param index    该redis连接在connectionStatus中的index
     * @param method   把代理对象(commands)当前调用的方法传递进来
     * @param args     method中的参数
     * @return Object
     * @throws Throwable throwable
     */
    private Object proxyHandler(RedisReactiveCommands<String, String> commands, int index, Method method, Object[] args)
            throws Throwable {
        if (Mono.class == method.getReturnType()) {
            Mono<Object> res = (Mono<Object>) method.invoke(commands, args);
            /**
             * 连接出现错误，连接状态改为false，一秒后执行invoke
             */
            return res.onErrorResume(e -> {
                if (!connectionStatus.isEmpty()) {
                    connectionStatus.clear(index);
                    synchronized (switchStart) {
                        if (!switchStart) {
                            ServerConfig.getInstance().getVertx().setTimer(1000, l -> switchEth());
                        }
                    }
                    try {
                        return (Mono<Object>) method.invoke(reactive(), args);
                    } catch (Exception ex) {
                        return res;
                    }
                } else {
                    return res;
                }
            });
        } else {
            return method.invoke(commands, args);
        }
    }

    private RedisReactiveCommands<String, String> getProxyCommands(RedisReactiveCommands<String, String> commands, int index) {
        return (RedisReactiveCommands<String, String>) Proxy.newProxyInstance(CLASS_LOADER, INTERFACES,
                (p, m, a) -> proxyHandler(commands, index, m, a));
    }

    public RedisReactiveCommands<String, String> reactive() {
        Tuple2<Integer, StatefulRedisConnection<String, String>> tuple2 = getConnection();
        RedisReactiveCommands<String, String> commands = tuple2.var2.reactive();
        return getProxyCommands(commands, tuple2.var1);
    }

    /**
     * 如果存在不可用的网口（连接），则递归，直至网口全部正常
     */
    public void switchEth() {
        synchronized (switchStart) {
            switchStart = true;
        }

        /*
        是否停止网口切换
         */
        boolean allReady = true;
        for (int i = 0; i < internalConnection.size(); i++) {
            /*
            对status为false的连接是否可用进行判断，可用则status改为true（set），
             */
            if (!connectionStatus.get(i)) {
                if (connect(i)) {
                    connectionStatus.set(i);
                } else {
                    allReady = false;
                }
            }
        }

        if (!allReady) {
            ServerConfig.getInstance().getVertx().setTimer(1000, l -> switchEth());
        } else {
            synchronized (switchStart) {
                switchStart = false;
            }
        }
    }

}
