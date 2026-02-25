package com.macrosan.database.redis;

import com.macrosan.httpserver.ServerConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.lang.reflect.Proxy;
import java.util.stream.IntStream;

import static com.macrosan.constants.RedisConstants.*;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.REDIS_TOKEN_INDEX;

/**
 * RedisConnPool Lettuce连接池。Lettuce构建在Netty上，连接多数时候是线程安全的，但是如下几种情况除外： 1.禁用flush
 * after command然后用命令的批量下发 2.BLPOP之类的阻塞操作 3.事务 4.多数据库的切换
 * 故本连接池维持了两种连接，一种是可复用的Shared连接,跟数据库绑定，一种是负责事务操作的Thread-Local连接，跟线程名字绑定
 *
 * @author liyixin
 * @date 2018/11/20
 */
public class Redis6380ConnPool {
    private static final Logger logger = LogManager.getLogger(Redis6380ConnPool.class.getName());

    private static Redis6380ConnPool instance;

    private final String masterVip1 = ServerConfig.getInstance().getMasterVip1();

    private final String masterVip2 = ServerConfig.getInstance().getMasterVip2();

    private IntObjectHashMap<SampleConnection> threadLocalConnectionMap;

    private IntObjectHashMap<AbstractConnection> sharedConnectionMap;

    public static Redis6380ConnPool getInstance() {
        return instance;
    }

    private static final Class<?>[] CLS = new Class<?>[]{RedisCommands.class};

    public static void init() {
        if (instance == null) {
            instance = new Redis6380ConnPool();
        }
    }

    private Redis6380ConnPool() {
        RedisClient client = RedisClient.create(VertxEventLoopProvider.resources);
        this.client = client;
        sharedConnectionMap = new IntObjectHashMap<>(22);

        threadLocalConnectionMap = new IntObjectHashMap<>((int) (PROC_NUM / 0.75) + 1);

        /* 初始化跟数据库绑定的线程间共享的连接 */
        IntStream.range(0, 16).forEach(n -> sharedConnectionMap.put(n, new SharedConnection(client, getNodes(n))));

        /* 这里node都是一样的，所以放在循环外面new */
        final FastList<RedisURI> threadLocalNodes = getNodes();
        /* 初始化跟线程绑定的线程独占连接 */
        IntStream.range(0, 1).forEach(n -> threadLocalConnectionMap.put(n, new SampleConnection(client, getMainNodes(REDIS_TOKEN_INDEX).get(0))));
    }

    public RedisClient client;

    /**
     * getSocketUri
     * <p>
     * 根据数据库编号和redis host来构造uri，用于Shared SharedConnection
     *
     * @param host     redis地址
     * @param database 要连接的数据库
     * @return 表明redis的uri
     */
    private RedisURI getSocketUri(String host, int database) {
        return RedisURI.Builder.socket(host)
                .withPassword(REDIS_PASSWD)
                .withDatabase(database)
                .build();
    }

    /**
     * getUri
     * <p>
     * 根据数据库编号和redis host来构造uri，用于Shared SharedConnection
     *
     * @param host     redis地址
     * @param database 要连接的数据库
     * @return 表明redis的uri
     */
    private RedisURI getUri(String host, int database) {
        return RedisURI.Builder.redis(host, 6380)
                .withPassword(REDIS_PASSWD)
                .withDatabase(database)
                .build();
    }

    /**
     * getUri
     * <p>
     * 根据redis host来构造uri，用于Thread Local Connection
     *
     * @param host redis地址
     * @return 表明redis的uri
     */
    private RedisURI getUri(String host) {
        return RedisURI.Builder.redis(host, 6380)
                .withPassword(REDIS_PASSWD)
                .build();
    }

    /**
     * getNodes
     * <p>
     * 用LOCALHOST，masterVip1和masterVip2构造一个静态拓扑，用于Shared SharedConnection。
     *
     * @param database 要连接的数据库
     * @return 静态拓扑
     */
    private FastList<RedisURI> getNodes(int database) {
        return new FastList<RedisURI>(4)
                .with(getSocketUri(IAM_UDS_ADDRESS, database), getUri(LOCALHOST, database), getUri(LOCALHOST, database));
    }

    /**
     * getNodes
     * <p>
     * 用LOCALHOST，masterVip1和masterVip2构造一个静态拓扑，用于Thread Local SharedConnection。
     *
     * @return 静态拓扑
     */
    private FastList<RedisURI> getNodes() {
        return new FastList<RedisURI>(4)
                .with(getUri(LOCALHOST), getUri(LOCALHOST), getUri(LOCALHOST));
    }

    public FastList<RedisURI> getMainNodes(int database) {
        return new FastList<RedisURI>(4)
                .with(getUri(masterVip1, database), getUri(masterVip2, database));
    }


    public AbstractConnection getSharedConnection(int database) {
        if (database < 0 || database > 15) {
            logger.error("Database number error :" + database);
            return null;
        }
        return sharedConnectionMap.get(database);
    }

    /**
     * 用于简单的获取同步命令集
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    public RedisCommands<String, String> getCommand(int database) {
        return getSharedConnection(database).sync();
    }

    /**
     * 用于简单的获取响应式命令集
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    public RedisReactiveCommands<String, String> getReactive(int database) {
        return getSharedConnection(database).reactive();
    }

    /**
     * 获得线程独占的响应式命令集
     *
     * @return 命令集
     */
    public RedisReactiveCommands<String, String> getThreadLocalConnReactive() {
        String threadName = Thread.currentThread().getName();
//        int n = Integer.parseInt(threadName.substring(threadName.length() - 1));
        SampleConnection res = threadLocalConnectionMap.get(0);
        if (res == null) {
            logger.error("Thread :" + threadName + " does not have such threadlocal connection");
            return null;
        }
        return res.reactive();
    }

    /**
     * 用于简单的获取操作主的同步命令集,短连接版本，每次新建连接，用完后自动释放
     *
     * @param database 要操作的数据库
     * @return 命令集
     */
    @SuppressWarnings("unchecked")
    public RedisCommands<String, String> getShortMasterCommand(int database) {
        StatefulRedisConnection<String, String> tmpConnection = getSharedConnection(database).newMaster();
        RedisCommands<String, String> target = tmpConnection.sync();
        return (RedisCommands<String, String>) Proxy.newProxyInstance(RedisCommands.class.getClassLoader(), CLS
                , (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(target, args);
                        if (args.length > 0 && args[0] instanceof String) {
                            target.publish("redis-cache-" + database, (String) args[0]);
                        }
                        tmpConnection.close();
                    } finally {
                        if (tmpConnection.isOpen()) {
                            tmpConnection.close();
                        }
                    }
                    return result;
                });
    }
}
